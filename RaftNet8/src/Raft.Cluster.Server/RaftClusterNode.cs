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
    public ClusterLedgerVersion ApplicationVersion { get; init; } = ClusterLedgerVersion.V2;
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
        if (settings.ClientServer.Security.SecureTransport && settings.PeerSecurity is not TlsRaftTransportSecurity)
            throw new ArgumentException("The secure cluster profile requires peer mutual TLS.");
        if (settings.ClientServer.Security.SecureTransport && (settings.ClientServer.Endpoint.Address.Scheme != "https" ||
            settings.ClientServer.InitialEndpoints.Any(endpoint => endpoint.Address.Scheme != "https") ||
            settings.ClientServer.Management is { Endpoint.Scheme: not "https" } ||
            settings.ClientServer.Management?.InitialEndpoints.Any(endpoint => endpoint.Address.Scheme != "https") == true))
            throw new ArgumentException("The secure cluster profile requires HTTPS client and management routes.");
        if ((settings.ClientServer.Management is not null || settings.ClientServer.Security.RequireClientTokens) && settings.ApplicationVersion != ClusterLedgerVersion.V2)
            throw new ArgumentException("Native administration and client security require cluster-v2.");
        var options = ConfigureMessageLimits(settings.Raft) with
        { SnapshotDirectory = settings.Raft.SnapshotDirectory ?? Path.Combine(settings.DataDirectory, "images") };
        var ledger = new MessageLedger(Path.Combine(settings.DataDirectory, "messages"), settings.CacheBytes, settings.ApplicationVersion);
        RaftStorageAdapter? log = null;
        RaftNode? node = null;
        TcpRaftTransport? peers = null;
        RaftClusterServer? server = null;
        try
        {
            log = new(new DBreezeLogStorage(Path.Combine(settings.DataDirectory, "log"), options, DBreezeLogLayout.UnifiedTableV4));
            peers = new(options, settings.Peer, settings.ApplicationVersion == ClusterLedgerVersion.V2
                ? new ClusterApplicationTransport(settings.PeerSecurity) : settings.PeerSecurity);
            node = await RaftNode.CreateAsync(options, log, peers, ledger, cancellationToken: token).ConfigureAwait(false);
            await node.StartAsync().ConfigureAwait(false);
            peers.Start(node.HandleAsync);
            server = await RaftClusterServer.StartAsync(node, ledger, settings.ClientServer with
                { GroupId = options.GroupId, Management = settings.ClientServer.Management is { } management
                    ? management with { DataDirectory = settings.DataDirectory, LocalPeer = settings.Peer,
                        EffectiveConfiguration = DescribeOptions(options, settings) } : null }, token).ConfigureAwait(false);
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
    private static IReadOnlyDictionary<string, string> DescribeOptions(RaftOptions options, RaftClusterNodeOptions settings)
    {
        var values = typeof(RaftOptions).GetProperties().Where(property =>
            (Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType) is var type &&
            (type.IsPrimitive || type == typeof(TimeSpan))).ToDictionary(property => property.Name,
                property => Convert.ToString(property.GetValue(options), System.Globalization.CultureInfo.InvariantCulture) ?? "automatic", StringComparer.Ordinal);
        values["MessagePayloadBytes"] = ClusterLimits.MaxPayloadBytes.ToString(System.Globalization.CultureInfo.InvariantCulture);
        values["MessageCacheBytes"] = settings.CacheBytes.ToString(System.Globalization.CultureInfo.InvariantCulture);
        values["MaxEntities"] = ClusterLimits.MaxEntities.ToString(System.Globalization.CultureInfo.InvariantCulture);
        values["MaxEndpoints"] = ClusterLimits.MaxEndpoints.ToString(System.Globalization.CultureInfo.InvariantCulture);
        values["HttpConcurrency"] = settings.ClientServer.MaxConcurrentRequests.ToString(System.Globalization.CultureInfo.InvariantCulture);
        values["RequireClientTokens"] = settings.ClientServer.Security.RequireClientTokens.ToString();
        values["SecureTransport"] = settings.ClientServer.Security.SecureTransport.ToString();
        if (settings.ClientServer.Management is { } management)
        {
            values["Telemetry.SampleIntervalSeconds"] = management.SampleIntervalSeconds.ToString(System.Globalization.CultureInfo.InvariantCulture);
            values["Telemetry.FileScanIntervalSeconds"] = management.FileScanIntervalSeconds.ToString(System.Globalization.CultureInfo.InvariantCulture);
            values["Management.BatchObservationReads"] = management.BatchObservationReads.ToString();
        }
        return new System.Collections.ObjectModel.ReadOnlyDictionary<string, string>(values);
    }
}
