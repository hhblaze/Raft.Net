using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Net;

namespace Raft.Transport;

public sealed partial class TcpRaftTransport
{
    private readonly object _directoryGate = new();
    private readonly ConcurrentDictionary<string, PeerDescriptor> _routeDescriptors = new(StringComparer.Ordinal);
    private readonly ConcurrentQueue<Task> _retiredConnections = new();
    private ImmutableArray<PeerDescriptor> _bootstrapPeers = ImmutableArray<PeerDescriptor>.Empty;
    private MembershipState _directory = new(0, 0, ImmutableArray<PeerDescriptor>.Empty);
    private long _routingRevision;
    private int _slotTarget;

    public IReadOnlyList<PeerDescriptor> BootstrapPeers => _bootstrapPeers;

    /// <summary>Starts a new/joining replica with its local listen endpoint and canonical bootstrap peers.</summary>
    public TcpRaftTransport(RaftOptions options, PeerDescriptor localPeer, RaftTransportSecurity? security = null)
        : this(options, BootstrapEndpoints(options, localPeer), security) { }

    private static IReadOnlyDictionary<string, IPEndPoint> BootstrapEndpoints(RaftOptions options, PeerDescriptor local)
    {
        local.Validate();
        if (local.NodeId != options.NodeId) throw new ArgumentException("Local descriptor must name this node.");
        var peers = options.InitialPeers.Append(local).GroupBy(peer => peer.NodeId, StringComparer.Ordinal).Select(group => group.Last());
        // Listener construction is synchronous; DNS routing is resolved asynchronously when connecting.
        // The local listener must have a literal IP; bootstrap DNS routes are installed immediately afterward.
        if (!IPAddress.TryParse(local.Host, out var localAddress))
            throw new ArgumentException("The local listen descriptor requires a literal IP address.", nameof(local));
        var result = new Dictionary<string, IPEndPoint>(StringComparer.Ordinal);
        foreach (var peer in peers)
            result[peer.NodeId] = new(IPAddress.TryParse(peer.Host, out var address) ? address : IPAddress.Loopback, peer.Port);
        result[local.NodeId] = new(localAddress, local.Port);
        return result;
    }

    private void InitializeDirectory(IReadOnlyDictionary<string, IPEndPoint> endpoints)
    {
        _bootstrapPeers = (_options.InitialPeers.Count == 0
            ? endpoints.Select(pair => new PeerDescriptor(pair.Key, pair.Value.Address.ToString(), pair.Value.Port))
            : _options.InitialPeers).OrderBy(peer => peer.NodeId, StringComparer.Ordinal).ToImmutableArray();
        foreach (var pair in endpoints)
            _routeDescriptors[pair.Key] = new(pair.Key, pair.Value.Address.ToString(), pair.Value.Port);
        _directory = new(0, 0, _bootstrapPeers);
        _slotTarget = Math.Max(4, endpoints.Count * 2);
        if (_options.WireVersion == 3) ApplyRoutes(_directory);
    }

    /// <summary>Publishes bounded routes synchronously; closes superseded sockets outside the caller/owner.</summary>
    public ValueTask UpdateDirectoryAsync(MembershipState state, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ObjectDisposedException.ThrowIf(_disposed, this);
        state.Validate();
        if (_options.WireVersion != 3) throw new NotSupportedException("Dynamic directory requires wire3.");
        lock (_directoryGate)
        {
            if (state.Index >= _directory.Index)
            {
                Volatile.Write(ref _directory, state);
                ApplyRoutes(state);
            }
        }
        return ValueTask.CompletedTask;
    }

    private void ApplyRoutes(MembershipState state)
    {
        if (state.Revision < _routingRevision) return;
        if (_routeDescriptors.Keys.Concat(state.Peers.Select(peer => peer.NodeId)).Distinct(StringComparer.Ordinal).Take(1025).Count() > 1024)
            throw new InvalidDataException("Routing identity bound exceeded.");
        foreach (var peer in state.Peers)
        {
            if (peer.NodeId == _options.NodeId) continue;
            if (_routeDescriptors.TryGetValue(peer.NodeId, out var previous) &&
                previous.Host == peer.Host && previous.Port == peer.Port && _outbound.ContainsKey(peer.NodeId))
            {
                _routeDescriptors[peer.NodeId] = peer;
                continue;
            }
            var replacement = new TcpPeerConnection(peer, _options, _security, _stop.Token);
            if (_outbound.TryGetValue(peer.NodeId, out var old))
                _retiredConnections.Enqueue(Task.Run(() => CloseRetiredAsync(old)));
            _outbound[peer.NodeId] = replacement;
            _routeDescriptors[peer.NodeId] = peer;
        }
        _routingRevision = state.Revision;
        var target = Math.Max(4, _routeDescriptors.Count * 2);
        if (target > _slotTarget)
        {
            _slots.Release(target - _slotTarget);
            _slotTarget = target;
        }
        while (_retiredConnections.TryPeek(out var completed) && completed.IsCompleted)
            _retiredConnections.TryDequeue(out _);
        // Retired routes remain bounded by the 1024-ID directory and permit final retirement ACKs.
        // Their idle sockets expire; they never become voters merely by remaining routable.
    }

    private void ImportRoutes(MembershipState directory, string sender, bool requireSender)
    {
        directory.Validate();
        lock (_directoryGate)
        {
            if (requireSender && !_outbound.ContainsKey(sender) &&
                (!directory.Peers.Any(peer => peer.NodeId == sender && !peer.IsRetired) ||
                 !directory.Peers.Any(peer => peer.NodeId == _options.NodeId)))
                throw new InvalidDataException("An unknown directory sender must introduce itself and this replica.");
            // These trusted hints repair transport only. The reducer still derives all voting authority from its log.
            if (directory.Revision > _routingRevision) ApplyRoutes(directory);
        }
    }

    private async Task CloseRetiredAsync(TcpPeerConnection connection)
    {
        try { await connection.CloseAsync().ConfigureAwait(false); }
        catch { Interlocked.Increment(ref _connectionErrors); }
    }

    private async ValueTask DrainRetiredConnectionsAsync()
    {
        while (_retiredConnections.TryDequeue(out var task))
            await task.ConfigureAwait(false);
    }
}
