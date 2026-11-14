using System.Collections.Immutable;

namespace Raft.Storage;

/// <summary>Reference to a complete durable image; image bytes are streamed separately in bounded chunks.</summary>
public sealed record SnapshotMetadata(long LastIncludedIndex, long LastIncludedTerm,
    ImmutableArray<string> Voters, ImmutableArray<string> Learners, string StorageId, ImmutableArray<byte> Digest)
{
    [System.Text.Json.Serialization.JsonIgnore(Condition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull)]
    public MembershipState? Membership { get; init; }
}

public sealed record StorageIdentity(string GroupId, string NodeId, ClusterConfiguration Bootstrap);

public sealed record RecoveredLogState(HardState HardState, long LastIndex, long LastTerm, SnapshotMetadata? Snapshot = null)
{
    public long FirstIndex { get; init; } = 1;
    public long UncommittedBytes { get; init; }
    public ClusterConfiguration? Configuration { get; init; }
    public StorageIdentity? Identity { get; init; }
    public bool SupportsConfiguration { get; init; }
    public ImmutableArray<string> KnownMembers { get; init; } = ImmutableArray<string>.Empty;
    public SnapshotInstallIntent? PendingSnapshotInstall { get; init; }
    internal MembershipView? Membership { get; init; }
}

/// <summary>Optional synchronous backend used by the bounded I/O adapter; never called by the pure core.</summary>
public interface IExtendedRaftStorage : IRaftStorage
{
    RecoveredLogState Recover();
    LogReadResult Query(LogQuery query);
    void Write(LogWrite write);
}

public interface IRaftStorageIdentity
{
    StorageIdentity Identity { get; }
}

/// <summary>
/// Bounded owned reads and durable-on-completion mutations. A failed mutation has an uncertain outcome:
/// halt the node and recover instead of retrying in place. The v2 backend supports snapshot publication/install;
/// the compatibility v1 adapter reports no snapshot and rejects publication.
/// </summary>
public interface ILogStore : IAsyncDisposable
{
    ValueTask<RecoveredLogState> RecoverAsync(CancellationToken cancellationToken);
    ValueTask<LogReadResult> ReadAsync(LogQuery query, CancellationToken cancellationToken);
    ValueTask WriteAsync(LogWrite write, CancellationToken cancellationToken);
}

/// <summary>Optional durable metadata cached by a store; publishing follows successful write completion.</summary>
public interface ILogStoreProgress
{
    long UncommittedBytes { get; }
}

public abstract record LogQuery
{
    public sealed record Term(long Index) : LogQuery;
    public sealed record Terms(long FromIndex, int Count) : LogQuery;
    public sealed record FirstIndexOfTerm(long Index) : LogQuery;
    public sealed record LastIndexOfTerm(long Value) : LogQuery;
    public sealed record Entries(long FromIndex, int MaxCount, int MaxBytes) : LogQuery;
    public sealed record Replication(long PreviousIndex, int MaxCount, int MaxBytes) : LogQuery;
    public sealed record Snapshot : LogQuery;
    public sealed record ConfigurationAt(long Index) : LogQuery;
    public sealed record EncodedBytes(long FromIndex, long ThroughIndex) : LogQuery;
    internal sealed record Membership : LogQuery;
    internal sealed record MembershipAt(long Index) : LogQuery;
    internal sealed record MembershipOperation(Guid OperationId, bool IncludePending = false) : LogQuery;
    internal sealed record MembershipHistory(string? AfterKey, int Count, long ThroughIndex) : LogQuery;
}

public sealed record LogReadResult(long Number = 0, long PreviousTerm = 0,
    ImmutableArray<LogEntry> Entries = default, ImmutableArray<long> Terms = default, SnapshotMetadata? Snapshot = null)
{
    public ClusterConfiguration? Configuration { get; init; }
    internal MembershipView? Membership { get; init; }
    internal MembershipOperationStatus? MembershipOperation { get; init; }
    internal ImmutableArray<MembershipOperationStatus> MembershipHistory { get; init; }
    internal string? ContinuationKey { get; init; }
}

public abstract record LogWrite
{
    public sealed record HardStateUpdate(HardState State) : LogWrite;
    public sealed record Append(ImmutableArray<LogEntry> Entries) : LogWrite;
    public sealed record ReplaceSuffix(long FromIndex, ImmutableArray<LogEntry> Entries) : LogWrite;
    public sealed record PublishSnapshot(SnapshotMetadata Metadata) : LogWrite
    {
        public int RetainEntries { get; init; }
    }
    public sealed record Batch(ImmutableArray<LogWrite> Writes) : LogWrite;
    public sealed record BeginSnapshotInstall(SnapshotInstallIntent Intent) : LogWrite;
    public sealed record CompleteSnapshotInstall(SnapshotInstallIntent Intent) : LogWrite;
    internal sealed record StageMembershipHistory(string Generation, ImmutableArray<MembershipOperationStatus> Entries, bool Complete = false, long ThroughIndex = 0) : LogWrite;
    internal sealed record PruneMembershipHistory(ImmutableArray<string> ProtectedGenerations) : LogWrite;
}
