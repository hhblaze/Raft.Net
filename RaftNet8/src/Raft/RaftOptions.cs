namespace Raft;

/// <summary>Bootstrap membership and validated memory/network limits for one node.</summary>
public sealed record RaftOptions
{
    public required string NodeId { get; init; }
    public string GroupId { get; init; } = "default";
    public required IReadOnlyList<string> Members { get; init; }
    public IReadOnlyList<string> Learners { get; init; } = Array.Empty<string>();
    /// <summary>Provisioned peer IDs allowed to contact this group, including future/retired members. Never counted as voters.</summary>
    public IReadOnlyList<string> KnownPeers { get; init; } = Array.Empty<string>();
    /// <summary>Wire3 canonical initial directory, identical on original nodes and joining nodes.</summary>
    public IReadOnlyList<PeerDescriptor> InitialPeers { get; init; } = Array.Empty<PeerDescriptor>();
    /// <summary>Wire3 only: allow self outside the original configuration while awaiting logged membership. Confers no membership or vote.</summary>
    public bool JoiningNode { get; init; }
    public int MinimumVoters { get; init; } = 1;
    public int MaxLearnerPromotionLag { get; init; } = 0;
    public TimeSpan TickInterval { get; init; } = TimeSpan.FromMilliseconds(25);
    public int HeartbeatTicks { get; init; } = 1;
    public int WireVersion { get; init; } = 2;
    public bool EnablePreVote { get; init; } = true;
    public int SnapshotChunkBytes { get; init; } = 64 * 1024;
    public string? SnapshotDirectory { get; init; }
    public long SnapshotIntervalEntries { get; init; } = 0;
    public int SnapshotRetainEntries { get; init; } = 128;
    public int SnapshotRetainedImages { get; init; } = 2;
    public long MaxSnapshotBytes { get; init; } = 64L * 1024 * 1024 * 1024;
    public long MaxSnapshotUnpackedBytes { get; init; } = 128L * 1024 * 1024 * 1024;
    public long SnapshotBytesPerSecond { get; init; } = 16 * 1024 * 1024;
    public int ElectionMinTicks { get; init; } = 12;
    public int ElectionMaxTicks { get; init; } = 24;
    public TimeSpan RpcTimeout { get; init; } = TimeSpan.FromMilliseconds(200);
    public int MaxAppendEntries { get; init; } = 128;
    public bool EnablePipelining { get; init; } = true;
    public int MaxInFlightMessages { get; init; } = 4;
    public long MaxInFlightBytes { get; init; } = 4 * 1024 * 1024;
    /// <summary>Piggyback ordinary wire2 commit-only notifications on next data/heartbeat. Client quorum and local-apply completion are unchanged.</summary>
    public bool CoalesceCommitNotifications { get; init; } = true;
    public long MaxUncommittedBytes { get; init; } = 64 * 1024 * 1024;
    public TimeSpan ProposalBatchDelay { get; init; } = TimeSpan.FromMilliseconds(2);
    /// <summary>Maximum off-owner collection window for byte-limited groups. Null uses up to four small-group windows, capped at one heartbeat/second; zero flushes these groups immediately.</summary>
    public TimeSpan? BulkProposalBatchDelay { get; init; }
    public int MaxAppendBytes { get; init; } = 256 * 1024;
    /// <summary>Wire2 replication bytes. Null uses four application budgets plus entry metadata headroom, capped by the peer byte window and 64 MiB. Wire1 retains MaxAppendBytes.</summary>
    public int? MaxReplicationBytes { get; init; }
    /// <summary>Local encoded-byte group bound. Null uses four application budgets plus entry metadata headroom, capped by uncommitted bytes and 64 MiB.</summary>
    public int? MaxProposalBatchBytes { get; init; }
    public int MaxCommandBytes { get; init; } = 64 * 1024;
    public int MaxPendingProposals { get; init; } = 1024;
    public int MaxPendingReads { get; init; } = 1024;
    public int LogCacheEntries { get; init; } = 1024;
    public long LogCacheBytes { get; init; } = 8 * 1024 * 1024;

    internal RaftOptions Freeze(bool allowMissingSelf = false)
    {
        ValidateId(NodeId, nameof(NodeId));
        ValidateId(GroupId, nameof(GroupId));
        ArgumentNullException.ThrowIfNull(Members);
        var members = Members.ToArray();
        ArgumentNullException.ThrowIfNull(Learners);
        var learners = Learners.ToArray();
        ArgumentNullException.ThrowIfNull(KnownPeers);
        var known = members.Concat(learners).Concat(KnownPeers).Distinct(StringComparer.Ordinal).ToArray();
        ArgumentNullException.ThrowIfNull(InitialPeers);
        var initialPeers = InitialPeers.ToArray();
        if (initialPeers.Length > 1024 || initialPeers.Select(peer => peer.NodeId).Distinct(StringComparer.Ordinal).Count() != initialPeers.Length)
            throw new ArgumentException("Invalid initial peer directory.", nameof(InitialPeers));
        foreach (var peer in initialPeers) peer.Validate();
        if (WireVersion == 3 && initialPeers.Length == 0)
            throw new ArgumentException("Wire3 requires a descriptor for every bootstrap voter.", nameof(InitialPeers));
        if (known.Length > 1024) throw new ArgumentException("At most 1024 provisioned peer IDs are supported.");
        foreach (var id in known) ValidateId(id, nameof(KnownPeers));
        if (MinimumVoters is < 1 or > 1024 || MaxLearnerPromotionLag < 0 ||
            members.Length is < 1 or > 1024 || members.Distinct(StringComparer.Ordinal).Count() != members.Length ||
            (!allowMissingSelf && !JoiningNode && !members.Contains(NodeId, StringComparer.Ordinal) && !learners.Contains(NodeId, StringComparer.Ordinal)) ||
            members.Length + learners.Length > 1024 ||
            members.Concat(learners).Distinct(StringComparer.Ordinal).Count() != members.Length + learners.Length)
            throw new ArgumentException("Membership requires 1..1024 unique members and must include self unless wire3 JoiningNode is enabled.");
        foreach (var member in members) ValidateId(member, nameof(Members));
        foreach (var learner in learners) ValidateId(learner, nameof(Learners));
        if (TickInterval < TimeSpan.FromMilliseconds(1) || TickInterval > TimeSpan.FromSeconds(10) ||
            HeartbeatTicks < 1 || ElectionMinTicks < HeartbeatTicks * 10L ||
            ElectionMaxTicks <= ElectionMinTicks || ElectionMaxTicks > 1_000_000 ||
            RpcTimeout <= TimeSpan.Zero || RpcTimeout.TotalMilliseconds > uint.MaxValue - 1d ||
            RpcTimeout.TotalMilliseconds >= TickInterval.TotalMilliseconds * ElectionMinTicks ||
            ProposalBatchDelay < TimeSpan.Zero || ProposalBatchDelay > TimeSpan.FromSeconds(1) ||
            ProposalBatchDelay.TotalMilliseconds > TickInterval.TotalMilliseconds * HeartbeatTicks / 2)
            throw new ArgumentOutOfRangeException(nameof(ElectionMinTicks), "Election and RPC deadlines must allow heartbeat delivery.");
        if (BulkProposalBatchDelay is { } bulkDelay && (bulkDelay < TimeSpan.Zero || bulkDelay > TimeSpan.FromSeconds(1) ||
            bulkDelay.Ticks > TickInterval.Ticks * HeartbeatTicks))
            throw new ArgumentOutOfRangeException(nameof(BulkProposalBatchDelay), "Bulk collection must allow heartbeat delivery.");
        if (MaxAppendEntries is < 1 or > 4096 || MaxCommandBytes is < 1 or > 16 * 1024 * 1024 ||
            MaxAppendBytes < MaxCommandBytes + LogEntry.EncodedOverhead || MaxAppendBytes > 64 * 1024 * 1024 ||
            MaxInFlightMessages is < 1 or > 256 || MaxInFlightBytes < MaxAppendBytes + 64L || MaxInFlightBytes > 1024L * 1024 * 1024 ||
            MaxUncommittedBytes < MaxAppendBytes || MaxUncommittedBytes > 1024L * 1024 * 1024 * 1024 ||
            MaxPendingProposals is < 1 or > 1_000_000 || MaxPendingReads is < 1 or > 1_000_000 || LogCacheEntries < 0 || LogCacheBytes < 0)
            throw new ArgumentOutOfRangeException(nameof(MaxAppendBytes), "Invalid batch, admission, or cache limits.");
        if (MaxProposalBatchBytes is { } proposalBytes &&
            (proposalBytes < MaxCommandBytes + LogEntry.EncodedOverhead || proposalBytes > 64 * 1024 * 1024 || proposalBytes > MaxUncommittedBytes))
            throw new ArgumentOutOfRangeException(nameof(MaxProposalBatchBytes), "Invalid local durable proposal group bound.");
        if (MaxReplicationBytes is { } replicationBytes &&
            (WireVersion < 2 || replicationBytes < MaxCommandBytes + LogEntry.EncodedOverhead ||
                replicationBytes > 64 * 1024 * 1024 || replicationBytes + 64L > MaxInFlightBytes))
            throw new ArgumentOutOfRangeException(nameof(MaxReplicationBytes), "Invalid modern replication byte budget.");
        if (WireVersion is not 1 and not 2 and not 3 || (WireVersion == 1 && EnablePreVote) || (JoiningNode && WireVersion != 3) ||
            SnapshotChunkBytes is < 1024 or > 4 * 1024 * 1024 || SnapshotIntervalEntries < 0 ||
            SnapshotRetainEntries is < 0 or > 1_000_000 || MaxSnapshotBytes < SnapshotChunkBytes ||
            SnapshotRetainedImages is < 2 or > 64 ||
            MaxSnapshotUnpackedBytes < MaxSnapshotBytes || SnapshotBytesPerSecond < 1024 ||
            (SnapshotDirectory is not null && !Path.IsPathFullyQualified(SnapshotDirectory)) ||
            (SnapshotIntervalEntries > 0 && SnapshotDirectory is null) || (SnapshotDirectory is not null && WireVersion < 2))
            throw new ArgumentException("Invalid protocol version/features or snapshot chunk bound.");
        if (WireVersion == 3 && (MaxAppendBytes < MembershipCodec.MaxRecordBytes + LogEntry.EncodedOverhead ||
            ReplicationByteLimit < MembershipCodec.MaxRecordBytes + LogEntry.EncodedOverhead))
            throw new ArgumentException("Wire3 byte budgets must fit a bounded membership record.");
        return this with { Members = Array.AsReadOnly(members), Learners = Array.AsReadOnly(learners), KnownPeers = Array.AsReadOnly(known),
            InitialPeers = Array.AsReadOnly(initialPeers) };
    }
    internal int MaxFrameBytes => checked(Math.Max(ReplicationByteLimit, SnapshotChunkBytes + (WireVersion == 3 ? MembershipCodec.MaxDirectoryBytes : 280_000)) + 1024);
    internal int ConfigurationByteLimit => WireVersion == 3
        ? Math.Min(270_000, Math.Min(MaxAppendBytes, ReplicationByteLimit) - LogEntry.EncodedOverhead) : MaxCommandBytes;
    internal int ReplicationByteLimit => WireVersion == 1 ? MaxAppendBytes : MaxReplicationBytes ??
        (int)Math.Min(Math.Min(MaxAppendBytes * 4L + MaxAppendEntries * (long)LogEntry.EncodedOverhead, MaxInFlightBytes - 64), 64 * 1024 * 1024);
    internal TimeSpan BulkProposalBatchDelayLimit => BulkProposalBatchDelay ??
        TimeSpan.FromTicks(Math.Min(ProposalBatchDelay.Ticks * 4, Math.Min(TickInterval.Ticks * HeartbeatTicks, TimeSpan.TicksPerSecond)));
    internal int ProposalBatchByteLimit => MaxProposalBatchBytes ??
        (int)Math.Min(Math.Min(MaxAppendBytes * 4L + MaxAppendEntries * (long)LogEntry.EncodedOverhead,
            64 * 1024 * 1024), MaxUncommittedBytes);

    internal static void ValidateId(string id, string parameter)
    {
        if (string.IsNullOrEmpty(id) || id.Length > 128)
            throw new ArgumentException("IDs must contain 1..128 ASCII letters, digits, '-' or '_'.", parameter);
        foreach (var c in id)
            if (!char.IsAsciiLetterOrDigit(c) && c is not '-' and not '_')
                throw new ArgumentException("IDs must contain 1..128 ASCII letters, digits, '-' or '_'.", parameter);
    }
}
