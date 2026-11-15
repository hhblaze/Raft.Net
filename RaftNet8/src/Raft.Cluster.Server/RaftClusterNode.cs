using Raft.Storage;
using Raft.Transport;

namespace Raft.Cluster.Server;

public sealed record RaftClusterNodeOptions
{
    public required RaftOptions Raft { get; init; }
    public required PeerDescriptor Peer { get; init; }
    public required string DataDirectory { get; init; }
    public required ClusterServerOptions ClientServer { get; init; }
    public long CacheBytes { get; init; } = 32 * 1024 * 1024;
    public RaftTransportSecurity? PeerSecurity { get; init; }
}

/// <summary>Owns one disk-backed wire3/v4 node, its message ledger, peer transport and client listener.</summary>
public sealed class RaftClusterNode : IAsyncDisposable
{
    private int _disposed;
    public RaftNode Node { get; }
    public MessageLedger Ledger { get; }
    public RaftClusterServer Server { get; }
    public RaftOptions Options { get; }
    private readonly TcpRaftTransport _peers;
    private RaftClusterNode(RaftOptions options, RaftNode node, MessageLedger ledger, TcpRaftTransport peers, RaftClusterServer server)
    { Options = options; Node = node; Ledger = ledger; _peers = peers; Server = server; }

    /// <summary>Enlarge byte budgets for 16 MB messages; leave consensus timers/batching unchanged.</summary>
    public static RaftOptions ConfigureMessageLimits(RaftOptions source) => source with
    {
        MaxCommandBytes = Math.Max(source.MaxCommandBytes, ClusterLimits.MaxCommandBytes),
        MaxAppendBytes = Math.Max(source.MaxAppendBytes, ClusterLimits.MaxCommandBytes + 64),
        MaxInFlightBytes = Math.Max(source.MaxInFlightBytes, 64L * 1024 * 1024),
        MaxUncommittedBytes = Math.Max(source.MaxUncommittedBytes, 64L * 1024 * 1024),
        // A 64 MiB automatic frame ceiling would reduce the TCP receive flight window to one message.
        // Keep the frame near one logical message so the 64 MiB peer window permits several flights.
        MaxReplicationBytes = Math.Max(source.MaxReplicationBytes ?? 0, Math.Max(source.MaxCommandBytes, ClusterLimits.MaxCommandBytes) + 64),
        MaxProposalBatchBytes = source.MaxProposalBatchBytes is { } proposals ? Math.Max(proposals, ClusterLimits.MaxCommandBytes + 64) : null
    };

    public static async Task<RaftClusterNode> StartAsync(RaftClusterNodeOptions settings, CancellationToken token = default)
    {
        if (settings.Raft.WireVersion != 3 || !Path.IsPathFullyQualified(settings.DataDirectory))
            throw new ArgumentException("Message cluster hosting requires wire3/v4 and an absolute data directory.");
        if (settings.Peer.NodeId != settings.Raft.NodeId || settings.ClientServer.Endpoint.NodeId != settings.Raft.NodeId)
            throw new ArgumentException("Node, peer, and client endpoint identities must agree.");
        var options = ConfigureMessageLimits(settings.Raft) with
        { SnapshotDirectory = settings.Raft.SnapshotDirectory ?? Path.Combine(settings.DataDirectory, "images") };
        var ledger = new MessageLedger(Path.Combine(settings.DataDirectory, "messages"), settings.CacheBytes);
        RaftStorageAdapter? log = null;
        RaftNode? node = null;
        TcpRaftTransport? peers = null;
        RaftClusterServer? server = null;
        try
        {
            log = new(new DBreezeLogStorage(Path.Combine(settings.DataDirectory, "log"), options, DBreezeLogLayout.UnifiedTableV4));
            peers = new(options, settings.Peer, settings.PeerSecurity);
            node = await RaftNode.CreateAsync(options, log, peers, ledger, cancellationToken: token).ConfigureAwait(false);
            await node.StartAsync().ConfigureAwait(false);
            peers.Start(node.HandleAsync);
            server = await RaftClusterServer.StartAsync(node, ledger, settings.ClientServer with { GroupId = options.GroupId }, token).ConfigureAwait(false);
            return new(options, node, ledger, peers, server);
        }
        catch
        {
            if (server is not null) await server.DisposeAsync().ConfigureAwait(false);
            if (peers is not null) await peers.DisposeAsync().ConfigureAwait(false);
            if (node is not null) await node.DisposeAsync().ConfigureAwait(false);
            else if (log is not null) await log.DisposeAsync().ConfigureAwait(false);
            ledger.Dispose(); throw;
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        try { await Server.DisposeAsync().ConfigureAwait(false); }
        finally
        {
            try { await _peers.DisposeAsync().ConfigureAwait(false); }
            finally { try { await Node.DisposeAsync().ConfigureAwait(false); } finally { Ledger.Dispose(); } }
        }
    }
}
