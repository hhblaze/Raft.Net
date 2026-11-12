using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;

namespace Raft.Transport;

/// <summary>Persistent framed TCP RPCs for trusted networks. Member IDs are checked but are not authenticated.</summary>
public sealed class TcpRaftTransport : IRaftTransport, IAsyncDisposable
{
    private sealed class Connection(IPEndPoint endpoint)
    {
        private readonly SemaphoreSlim _gate = new(1, 1);
        private readonly byte[] _header = new byte[4];
        private TcpClient? _client;

        public async ValueTask<RpcEnvelope> SendAsync(RaftOptions options, RaftRpc request, CancellationToken token)
        {
            await _gate.WaitAsync(token).ConfigureAwait(false);
            try
            {
                if (_client is null)
                {
                    _client = new TcpClient(endpoint.AddressFamily) { NoDelay = true };
                    await _client.ConnectAsync(endpoint.Address, endpoint.Port, token).ConfigureAwait(false);
                }
                var stream = _client.GetStream();
                using var frame = RaftWireCodec.Encode(options.GroupId, options.NodeId, request, options.MaxAppendBytes + 1024);
                await stream.WriteAsync(frame.Memory, token).ConfigureAwait(false);
                return await RaftWireCodec.ReadAsync(stream, _header, options, token).ConfigureAwait(false);
            }
            catch
            {
                _client?.Dispose();
                _client = null;
                throw;
            }
            finally { _gate.Release(); }
        }

        public async ValueTask CloseAsync()
        {
            await _gate.WaitAsync().ConfigureAwait(false);
            try { _client?.Dispose(); _client = null; }
            finally { _gate.Release(); }
        }
    }

    private readonly object _lifecycle = new();
    private readonly RaftOptions _options;
    private readonly Dictionary<string, Connection> _outbound;
    private readonly TcpListener _listener;
    private readonly CancellationTokenSource _stop = new();
    private readonly SemaphoreSlim _slots;
    private readonly ConcurrentDictionary<long, TcpClient> _clients = new();
    private readonly ConcurrentDictionary<long, Task> _handlers = new();
    private Func<string, RaftRpc, CancellationToken, ValueTask<RaftRpc>>? _handler;
    private Task? _acceptTask, _disposeTask;
    private long _connectionId, _connectionErrors;
    private bool _disposed;

    public TcpRaftTransport(RaftOptions options, IReadOnlyDictionary<string, IPEndPoint> endpoints)
    {
        _options = options.Freeze();
        if (endpoints.Count != _options.Members.Count || _options.Members.Any(id => !endpoints.ContainsKey(id)))
            throw new ArgumentException("Supply an endpoint for every configured member, including self.", nameof(endpoints));
        _listener = new TcpListener(new IPEndPoint(endpoints[_options.NodeId].Address, endpoints[_options.NodeId].Port));
        _outbound = _options.Members.Where(id => id != _options.NodeId).ToDictionary(id => id,
            id => new Connection(new IPEndPoint(endpoints[id].Address, endpoints[id].Port)), StringComparer.Ordinal);
        _slots = new SemaphoreSlim(Math.Max(4, _options.Members.Count * 2));
    }

    public IPEndPoint LocalEndPoint => (IPEndPoint)_listener.LocalEndpoint;
    public long ConnectionErrors => Interlocked.Read(ref _connectionErrors);

    public void Start(Func<string, RaftRpc, CancellationToken, ValueTask<RaftRpc>> handler)
    {
        lock (_lifecycle)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_acceptTask is not null) throw new InvalidOperationException("Transport has already started.");
            _handler = handler ?? throw new ArgumentNullException(nameof(handler));
            _listener.Start();
            _acceptTask = AcceptAsync();
        }
    }

    public async ValueTask<RaftRpc> SendAsync(string peerId, RaftRpc request, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_outbound.TryGetValue(peerId, out var connection)) throw new ArgumentException("Unknown peer.", nameof(peerId));
        using var token = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _stop.Token);
        var envelope = await connection.SendAsync(_options, request, token.Token).ConfigureAwait(false);
        if (envelope.GroupId != _options.GroupId || envelope.From != peerId)
            throw new InvalidDataException("RPC response came from a different group or member.");
        return envelope.Message;
    }

    private async Task AcceptAsync()
    {
        try
        {
            while (!_stop.IsCancellationRequested)
            {
                var client = await _listener.AcceptTcpClientAsync(_stop.Token).ConfigureAwait(false);
                if (!_slots.Wait(0)) { client.Dispose(); continue; }
                client.NoDelay = true;
                var id = Interlocked.Increment(ref _connectionId);
                _clients.TryAdd(id, client);
                var task = ServeAsync(client);
                _handlers.TryAdd(id, task);
                _ = task.ContinueWith(completed =>
                {
                    _handlers.TryRemove(id, out var ignoredTask);
                    _clients.TryRemove(id, out var ignoredClient);
                }, CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
            }
        }
        catch (OperationCanceledException) when (_stop.IsCancellationRequested) { }
        catch (SocketException) when (_stop.IsCancellationRequested) { }
        catch (ObjectDisposedException) when (_stop.IsCancellationRequested) { }
    }

    private async Task ServeAsync(TcpClient client)
    {
        try
        {
            var stream = client.GetStream();
            var header = new byte[4];
            while (!_stop.IsCancellationRequested)
            {
                using var deadline = CancellationTokenSource.CreateLinkedTokenSource(_stop.Token);
                // Bound idle/partial frames as well as frame sizes. Normal heartbeats keep a connection active.
                deadline.CancelAfter(TimeSpan.FromSeconds(30));
                var envelope = await RaftWireCodec.ReadAsync(stream, header, _options, deadline.Token).ConfigureAwait(false);
                if (envelope.GroupId != _options.GroupId || !_outbound.ContainsKey(envelope.From) ||
                    envelope.Message is not VoteRequest and not AppendRequest)
                    throw new InvalidDataException("RPC group, member, or direction is invalid.");
                var response = await _handler!(envelope.From, envelope.Message, deadline.Token).ConfigureAwait(false);
                using var frame = RaftWireCodec.Encode(_options.GroupId, _options.NodeId, response, _options.MaxAppendBytes + 1024);
                await stream.WriteAsync(frame.Memory, deadline.Token).ConfigureAwait(false);
            }
        }
        catch (Exception) when (_stop.IsCancellationRequested) { }
        catch (Exception) { Interlocked.Increment(ref _connectionErrors); }
        finally { client.Dispose(); _slots.Release(); }
    }

    public ValueTask DisposeAsync()
    {
        lock (_lifecycle)
        {
            _disposed = true;
            return new(_disposeTask ??= DisposeCoreAsync());
        }
    }

    private async Task DisposeCoreAsync()
    {
        _stop.Cancel();
        _listener.Stop();
        foreach (var client in _clients.Values) client.Dispose();
        if (_acceptTask is not null) await _acceptTask.ConfigureAwait(false);
        await Task.WhenAll(_handlers.Values).ConfigureAwait(false);
        foreach (var connection in _outbound.Values) await connection.CloseAsync().ConfigureAwait(false);
    }
}
