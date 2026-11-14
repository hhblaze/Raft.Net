using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;

namespace Raft.Transport;

/// <summary>Bounded persistent RPC multiplexing, with optional authenticated stream security.</summary>
public sealed partial class TcpRaftTransport : IRaftTransport, IPeerDirectoryTransport, IAsyncDisposable
{
    private readonly object _lifecycle = new();
    private readonly RaftOptions _options;
    private readonly RaftTransportSecurity? _security;
    private readonly ConcurrentDictionary<string, TcpPeerConnection> _outbound = new(StringComparer.Ordinal);
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
        : this(options, endpoints, null) { }

    public TcpRaftTransport(RaftOptions options, IReadOnlyDictionary<string, IPEndPoint> endpoints, RaftTransportSecurity? security)
    {
        _options = options.Freeze();
        _security = security;
        if (endpoints.Count > 1024 || _options.Members.Concat(_options.Learners).Any(id => !endpoints.ContainsKey(id)) ||
            !endpoints.ContainsKey(_options.NodeId))
            throw new ArgumentException("Supply an endpoint for every configured member, including self.", nameof(endpoints));
        _listener = new TcpListener(new IPEndPoint(endpoints[_options.NodeId].Address, endpoints[_options.NodeId].Port));
        foreach (var id in endpoints.Keys)
        {
            RaftOptions.ValidateId(id, nameof(endpoints));
            if (id != _options.NodeId) _outbound.TryAdd(id, new(endpoints[id], id, _options, security, _stop.Token));
        }
        _slots = new SemaphoreSlim(Math.Max(4, endpoints.Count * 2));
        InitializeDirectory(endpoints);
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

    /// <summary>Changes only routing for an already provisioned identity; this never changes voting membership.</summary>
    public async ValueTask UpdatePeerAsync(string peerId, IPEndPoint endpoint)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_options.WireVersion == 3) throw new NotSupportedException("Wire3 routes are owned by the replicated directory.");
        if (peerId == _options.NodeId || !_options.KnownPeers.Contains(peerId, StringComparer.Ordinal))
            throw new ArgumentException("Provision the peer ID in KnownPeers before adding its route.", nameof(peerId));
        var replacement = new TcpPeerConnection(endpoint, peerId, _options, _security, _stop.Token);
        while (true)
        {
            if (_outbound.TryGetValue(peerId, out var previous))
            {
                if (!_outbound.TryUpdate(peerId, replacement, previous)) continue;
                await previous.CloseAsync().ConfigureAwait(false);
                return;
            }
            if (_outbound.TryAdd(peerId, replacement)) return;
        }
    }

    public ValueTask<RaftRpc> SendAsync(string peerId, RaftRpc request, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_outbound.TryGetValue(peerId, out var connection)) throw new ArgumentException("Unknown peer.", nameof(peerId));
        if (_options.WireVersion == 3)
        {
            var published = Volatile.Read(ref _directory);
            if (connection.NeedsDirectoryExchange(published.Revision))
                return SendWithDirectoryAsync(connection, peerId, request, published, cancellationToken);
        }
        // The connection already links caller cancellation to transport/connection shutdown.
        // Keep its task directly: another CTS and async continuation per ordinary RPC are redundant.
        return connection.SendAsync(request, cancellationToken);
    }

    private async ValueTask<RaftRpc> SendWithDirectoryAsync(TcpPeerConnection connection, string peerId, RaftRpc request,
        MembershipState published, CancellationToken token)
    {
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(token, _stop.Token);
        var directory = await connection.ExchangeDirectoryAsync(published, stop.Token).ConfigureAwait(false);
        if (directory is not null) ImportRoutes(directory, peerId, requireSender: false);
        return await connection.SendAsync(request, stop.Token).ConfigureAwait(false);
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
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(_stop.Token);
        var jobs = new ConcurrentDictionary<long, Task>();
        // Reserve a worst-case frame for each concurrent handler, bounding decoded request memory.
        var parallelism = _options.WireVersion == 1 ? 1 :
            (int)Math.Max(1, Math.Min(_options.MaxInFlightMessages + 4L, _options.MaxInFlightBytes / _options.MaxFrameBytes));
        using var capacity = new SemaphoreSlim(parallelism);
        using var writes = new SemaphoreSlim(1, 1);
        Exception? failure = null;
        long jobId = 0;
        try
        {
            Stream stream = client.GetStream();
            if (_security is not null)
            {
                using var handshake = CancellationTokenSource.CreateLinkedTokenSource(stop.Token);
                handshake.CancelAfter(_options.RpcTimeout);
                stream = await _security.AuthenticateServerAsync(stream, handshake.Token).ConfigureAwait(false);
            }
            await using var ownedStream = stream;
            var header = new byte[4];
            string? identity = null;
            while (!stop.IsCancellationRequested)
            {
                await capacity.WaitAsync(stop.Token).ConfigureAwait(false);
                RpcEnvelope envelope;
                try
                {
                    using var deadline = CancellationTokenSource.CreateLinkedTokenSource(stop.Token);
                    deadline.CancelAfter(TimeSpan.FromSeconds(30));
                    envelope = await RaftWireCodec.ReadAsync(stream, header, _options, deadline.Token).ConfigureAwait(false);
                    if (envelope.GroupId != _options.GroupId || envelope.Version != _options.WireVersion ||
                        (!(_options.WireVersion == 3 && envelope.Message is PeerDirectoryRequest) && !_outbound.ContainsKey(envelope.From)) ||
                        envelope.From == _options.NodeId || !RaftRpcProtocol.IsRequest(envelope.Message) ||
                        (identity is not null && identity != envelope.From))
                        throw new InvalidDataException("RPC group, identity, version, or direction is invalid.");
                    if (identity is null && _security is not null)
                        await _security.AuthorizePeerAsync(stream, envelope.From, deadline.Token).ConfigureAwait(false);
                    identity = envelope.From;
                }
                catch { capacity.Release(); throw; }
                var id = ++jobId;
                var task = ReplyAsync(envelope);
                jobs.TryAdd(id, task);
                _ = task.ContinueWith(_ => jobs.TryRemove(id, out var ignored),
                    CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
            }

            async Task ReplyAsync(RpcEnvelope request)
            {
                try
                {
                    RaftRpc response;
                    if (request.Message is PeerDirectoryRequest directory)
                    {
                        ImportRoutes(directory.Directory, request.From, requireSender: true);
                        response = new PeerDirectoryResponse(Volatile.Read(ref _directory));
                    }
                    else response = await _handler!(request.From, request.Message, stop.Token).ConfigureAwait(false);
                    using var frame = RaftWireCodec.Encode(_options.GroupId, _options.NodeId, response, _options.MaxFrameBytes,
                        request.Version, request.ExchangeId);
                    await writes.WaitAsync(stop.Token).ConfigureAwait(false);
                    try { await stream.WriteAsync(frame.Memory, stop.Token).ConfigureAwait(false); }
                    finally { writes.Release(); }
                }
                catch (Exception error)
                {
                    Interlocked.CompareExchange(ref failure, error, null);
                    stop.Cancel();
                    client.Dispose();
                }
                finally { capacity.Release(); }
            }
        }
        catch (Exception error) { Interlocked.CompareExchange(ref failure, error, null); }
        finally
        {
            stop.Cancel();
            client.Dispose();
            await Task.WhenAll(jobs.Values).ConfigureAwait(false);
            if (failure is not null && !_stop.IsCancellationRequested) Interlocked.Increment(ref _connectionErrors);
            _slots.Release();
        }
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
        await DrainRetiredConnectionsAsync().ConfigureAwait(false);
    }
}
