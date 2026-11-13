using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;

namespace Raft.Transport;

/// <summary>One bounded multiplexed session per peer. A write failure invalidates the whole framed stream.</summary>
internal sealed class TcpPeerConnection
{
    private sealed record Pending(RaftRpc Request)
    {
        public readonly TaskCompletionSource<RpcEnvelope> Completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
    private sealed class Session(TcpClient client, Stream stream, CancellationToken stop)
    {
        public readonly TcpClient Client = client;
        public readonly Stream Stream = stream;
        public readonly CancellationTokenSource Stop = CancellationTokenSource.CreateLinkedTokenSource(stop);
        public readonly ConcurrentDictionary<long, Pending> Pending = new();
        public readonly SemaphoreSlim WriteGate = new(1, 1);
        public Task Reader = Task.CompletedTask;
        public int Closed;
        public void Close(Exception error)
        {
            if (Interlocked.Exchange(ref Closed, 1) != 0) return;
            Stop.Cancel();
            Client.Dispose();
            Stream.Dispose();
            foreach (var pair in Pending)
                if (Pending.TryRemove(pair.Key, out var pending)) pending.Completion.TrySetException(error);
        }
    }

    private readonly IPEndPoint _endpoint;
    private readonly string _peerId;
    private readonly RaftOptions _options;
    private readonly RaftTransportSecurity? _security;
    private readonly SemaphoreSlim _connect = new(1, 1), _capacity;
    private readonly CancellationTokenSource _stop;
    private Session? _session;
    private long _exchangeId;

    public TcpPeerConnection(IPEndPoint endpoint, string peerId, RaftOptions options,
        RaftTransportSecurity? security, CancellationToken stop)
    {
        _endpoint = new(endpoint.Address, endpoint.Port);
        _peerId = peerId;
        _options = options;
        _security = security;
        _stop = CancellationTokenSource.CreateLinkedTokenSource(stop);
        _capacity = new(options.WireVersion == 1 ? 1 : options.MaxInFlightMessages + 4);
    }

    public async ValueTask<RpcEnvelope> SendAsync(RaftRpc request, CancellationToken token)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(token, _stop.Token);
        await _capacity.WaitAsync(linked.Token).ConfigureAwait(false);
        Session? session = null;
        long id = 0;
        var pending = new Pending(request);
        try
        {
            session = await ConnectAsync(linked.Token).ConfigureAwait(false);
            id = _options.WireVersion == 2 ? Interlocked.Increment(ref _exchangeId) : 0;
            if (!session.Pending.TryAdd(id, pending)) throw new InvalidOperationException("Duplicate exchange ID.");
            if (Volatile.Read(ref session.Closed) != 0) throw new IOException("Peer session closed.");
            using var writeToken = CancellationTokenSource.CreateLinkedTokenSource(linked.Token, session.Stop.Token);
            await session.WriteGate.WaitAsync(writeToken.Token).ConfigureAwait(false);
            try
            {
                using var frame = RaftWireCodec.Encode(_options.GroupId, _options.NodeId, request, _options.MaxFrameBytes, _options.WireVersion, id);
                try { await session.Stream.WriteAsync(frame.Memory, writeToken.Token).ConfigureAwait(false); }
                catch (Exception error) { Break(session, error); throw; } // partial frame is uncertain
            }
            finally { session.WriteGate.Release(); }
            return await pending.Completion.Task.WaitAsync(linked.Token).ConfigureAwait(false);
        }
        catch (Exception error)
        {
            // A v1 response has no ID, so a canceled response cannot be safely skipped.
            if (session is not null && _options.WireVersion == 1) Break(session, error);
            throw;
        }
        finally
        {
            session?.Pending.TryRemove(id, out _);
            pending.Completion.TrySetCanceled(linked.Token);
            // If connection closure raced cancellation, observe its private completion as well.
            if (pending.Completion.Task.IsFaulted) _ = pending.Completion.Task.Exception;
            _capacity.Release();
        }
    }

    private async ValueTask<Session> ConnectAsync(CancellationToken token)
    {
        if (Volatile.Read(ref _session) is { Closed: 0 } ready) return ready;
        await _connect.WaitAsync(token).ConfigureAwait(false);
        try
        {
            if (_session is { Closed: 0 } existing) return existing;
            var client = new TcpClient(_endpoint.AddressFamily) { NoDelay = true };
            try
            {
                await client.ConnectAsync(_endpoint.Address, _endpoint.Port, token).ConfigureAwait(false);
                Stream stream = client.GetStream();
                if (_security is not null)
                {
                    stream = await _security.AuthenticateClientAsync(stream, _peerId, token).ConfigureAwait(false);
                    await _security.AuthorizePeerAsync(stream, _peerId, token).ConfigureAwait(false);
                }
                var session = new Session(client, stream, _stop.Token);
                Volatile.Write(ref _session, session);
                session.Reader = ReadAsync(session);
                return session;
            }
            catch { client.Dispose(); throw; }
        }
        finally { _connect.Release(); }
    }

    private async Task ReadAsync(Session session)
    {
        try
        {
            var header = new byte[4];
            while (!session.Stop.IsCancellationRequested)
            {
                var reply = await RaftWireCodec.ReadAsync(session.Stream, header, _options, session.Stop.Token).ConfigureAwait(false);
                if (reply.Version != _options.WireVersion || reply.GroupId != _options.GroupId || reply.From != _peerId ||
                    (_options.WireVersion == 2 && (reply.ExchangeId < 1 || reply.ExchangeId > Interlocked.Read(ref _exchangeId))))
                    throw new InvalidDataException("Response group, identity, version, or correlation is invalid.");
                if (!session.Pending.TryRemove(reply.ExchangeId, out var pending)) continue; // canceled or duplicate response
                if (!RaftRpcProtocol.IsExpectedResponse(pending.Request, reply.Message))
                {
                    var error = new InvalidDataException("Unexpected RPC response kind.");
                    pending.Completion.TrySetException(error);
                    throw error;
                }
                pending.Completion.TrySetResult(reply);
            }
        }
        catch (Exception error) { Break(session, error); }
    }

    private void Break(Session session, Exception error)
    {
        Interlocked.CompareExchange(ref _session, null, session);
        session.Close(error);
    }

    public async ValueTask CloseAsync()
    {
        _stop.Cancel();
        await _connect.WaitAsync().ConfigureAwait(false);
        Session? session;
        try
        {
            session = Interlocked.Exchange(ref _session, null);
            session?.Close(new IOException("Transport closed."));
        }
        finally { _connect.Release(); }
        if (session is not null) await session.Reader.ConfigureAwait(false);
    }
}
