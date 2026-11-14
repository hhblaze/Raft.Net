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
        public long DirectoryRevision = -1;
        public readonly SemaphoreSlim DirectoryGate = new(1, 1);
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

    private readonly EndPoint _endpoint;
    private readonly string _peerId;
    private readonly RaftOptions _options;
    private readonly RaftTransportSecurity? _security;
    private readonly SemaphoreSlim _connect = new(1, 1), _capacity;
    private readonly CancellationTokenSource _stop;
    private Session? _session;
    private long _exchangeId;

    public TcpPeerConnection(IPEndPoint endpoint, string peerId, RaftOptions options,
        RaftTransportSecurity? security, CancellationToken stop)
        : this((EndPoint)new IPEndPoint(endpoint.Address, endpoint.Port), peerId, options, security, stop) { }

    internal TcpPeerConnection(PeerDescriptor peer, RaftOptions options, RaftTransportSecurity? security, CancellationToken stop)
        : this(IPAddress.TryParse(peer.Host, out var address) ? new IPEndPoint(address, peer.Port) : new DnsEndPoint(peer.Host, peer.Port),
            peer.NodeId, options, security, stop) { }

    private TcpPeerConnection(EndPoint endpoint, string peerId, RaftOptions options,
        RaftTransportSecurity? security, CancellationToken stop)
    {
        _endpoint = endpoint;
        _peerId = peerId;
        _options = options;
        _security = security;
        _stop = CancellationTokenSource.CreateLinkedTokenSource(stop);
        _capacity = new(options.WireVersion == 1 ? 1 : options.MaxInFlightMessages + 4);
    }

    public async ValueTask<RaftRpc> SendAsync(RaftRpc request, CancellationToken token)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(token, _stop.Token);
        await _capacity.WaitAsync(linked.Token).ConfigureAwait(false);
        Session? session = null;
        long id = 0;
        var pending = new Pending(request);
        try
        {
            session = await ConnectAsync(linked.Token).ConfigureAwait(false);
            id = _options.WireVersion >= 2 ? Interlocked.Increment(ref _exchangeId) : 0;
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
            return (await pending.Completion.Task.WaitAsync(linked.Token).ConfigureAwait(false)).Message;
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

    internal async ValueTask<MembershipState?> ExchangeDirectoryAsync(MembershipState directory, CancellationToken token)
    {
        var session = await ConnectAsync(token).ConfigureAwait(false);
        if (Volatile.Read(ref session.DirectoryRevision) >= directory.Revision) return null;
        await session.DirectoryGate.WaitAsync(token).ConfigureAwait(false);
        try
        {
            if (Volatile.Read(ref session.DirectoryRevision) >= directory.Revision) return null;
            var reply = await SendAsync(new PeerDirectoryRequest(directory), token).ConfigureAwait(false);
            if (ReferenceEquals(session, Volatile.Read(ref _session)))
                Volatile.Write(ref session.DirectoryRevision, directory.Revision);
            return ((PeerDirectoryResponse)reply).Directory;
        }
        finally { session.DirectoryGate.Release(); }
    }

    internal bool NeedsDirectoryExchange(long revision) =>
        Volatile.Read(ref _session) is not { Closed: 0 } session || Volatile.Read(ref session.DirectoryRevision) < revision;

    private async ValueTask<Session> ConnectAsync(CancellationToken token)
    {
        if (Volatile.Read(ref _session) is { Closed: 0 } ready) return ready;
        await _connect.WaitAsync(token).ConfigureAwait(false);
        try
        {
            if (_session is { Closed: 0 } existing) return existing;
            var client = _endpoint is IPEndPoint ipEndpoint ? new TcpClient(ipEndpoint.AddressFamily) : new TcpClient();
            client.NoDelay = true;
            try
            {
                if (_endpoint is IPEndPoint ip)
                    await client.ConnectAsync(ip.Address, ip.Port, token).ConfigureAwait(false);
                else if (_endpoint is DnsEndPoint dns)
                    await client.ConnectAsync(dns.Host, dns.Port, token).ConfigureAwait(false);
                else throw new InvalidOperationException("Unknown peer endpoint kind.");
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
                    (_options.WireVersion >= 2 && (reply.ExchangeId < 1 || reply.ExchangeId > Interlocked.Read(ref _exchangeId))))
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
