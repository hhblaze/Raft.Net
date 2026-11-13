namespace Raft;

public enum LogEntryKind : byte { Command = 0, NoOp = 1, Configuration = 2 }

/// <summary>Immutable owned log data. The constructor copies caller memory.</summary>
public sealed class LogEntry
{
    internal const int EncodedOverhead = 21;
    public long Index { get; }
    public long Term { get; }
    public bool IsNoOp => Kind != LogEntryKind.Command;
    public LogEntryKind Kind { get; }
    public ReadOnlyMemory<byte> Command { get; }

    public LogEntry(long index, long term, ReadOnlySpan<byte> command, bool isNoOp = false)
        : this(index, term, command.ToArray(), isNoOp) { }

    internal LogEntry(long index, long term, byte[] ownedCommand, bool isNoOp)
        : this(index, term, ownedCommand, isNoOp ? LogEntryKind.NoOp : LogEntryKind.Command) { }

    public LogEntry(long index, long term, ReadOnlySpan<byte> command, LogEntryKind kind)
        : this(index, term, command.ToArray(), kind) { }

    internal LogEntry(long index, long term, byte[] ownedCommand, LogEntryKind kind)
    {
        if (index < 1 || term < 1 || kind > LogEntryKind.Configuration ||
            (kind == LogEntryKind.NoOp && ownedCommand.Length != 0))
            throw new ArgumentException("Log entries need positive index/term and empty data for no-ops.");
        Index = index;
        Term = term;
        Command = ownedCommand;
        Kind = kind;
    }
}

public abstract record RaftRpc(long Term);
public sealed record VoteRequest(long Term, long LastLogIndex, long LastLogTerm) : RaftRpc(Term);
public sealed record VoteResponse(long Term, bool Granted) : RaftRpc(Term);
public sealed record AppendRequest(long Term, long RequestId, long PreviousIndex, long PreviousTerm,
    long LeaderCommit, IReadOnlyList<LogEntry> Entries) : RaftRpc(Term)
{
    public long ReadContext { get; init; }
}
public sealed record AppendResponse(long Term, long RequestId, bool Success, long MatchIndex,
    long ConflictIndex = 0, long ConflictTerm = 0) : RaftRpc(Term)
{
    public long ReadContext { get; init; }
}
public sealed record PreVoteRequest(long Term, long Round, long LastLogIndex, long LastLogTerm) : RaftRpc(Term);
public sealed record PreVoteResponse(long Term, long Round, long ProspectiveTerm, bool Granted) : RaftRpc(Term);
public sealed record TimeoutNowRequest(long Term, long RequestId) : RaftRpc(Term);
public sealed record TimeoutNowResponse(long Term, long RequestId, bool Accepted) : RaftRpc(Term);
public sealed record SnapshotChunkRequest(long Term, long RequestId, string StorageId, long Offset, long TotalLength,
    uint Checksum, bool Final, Storage.SnapshotMetadata? Metadata, ReadOnlyMemory<byte> Data) : RaftRpc(Term);
public sealed record SnapshotChunkResponse(long Term, long RequestId, string StorageId, long NextOffset, bool Success, bool Complete) : RaftRpc(Term);

public static class RaftRpcProtocol
{
    public static bool IsRequest(RaftRpc message) =>
        message is VoteRequest or AppendRequest or PreVoteRequest or TimeoutNowRequest or SnapshotChunkRequest;
    public static bool IsExpectedResponse(RaftRpc request, RaftRpc response) => (request, response) switch
    {
        (VoteRequest, VoteResponse) or (AppendRequest, AppendResponse) or (PreVoteRequest, PreVoteResponse) or
        (TimeoutNowRequest, TimeoutNowResponse) or (SnapshotChunkRequest, SnapshotChunkResponse) => true,
        _ => false
    };
}

public enum RaftRole { Follower, Candidate, Leader, PreCandidate, Learner, Removed }
public sealed record RaftStatus(string NodeId, RaftRole Role, long Term, string? LeaderId,
    long CommitIndex, long LastLogIndex, long LastApplied, Exception? Fault = null)
{
    public string? LeadershipTransferTarget { get; init; }
    public ClusterConfiguration? Configuration { get; init; }
    public IReadOnlyList<RaftPeerStatus> Peers { get; init; } = Array.Empty<RaftPeerStatus>();
    public Storage.SnapshotMetadata? Snapshot { get; init; }
    public long? InstallingSnapshotIndex { get; init; }
    public long UncommittedBytes { get; init; }
}
public enum ReplicationMode { Probe, Replicate, Snapshot }
public readonly record struct RaftPeerStatus(string NodeId, long MatchIndex, long NextIndex, bool IsVoter)
{
    public ReplicationMode Mode { get; init; }
    public int InFlightMessages { get; init; }
    public long InFlightBytes { get; init; }
    public long SnapshotOffset { get; init; }
}
public sealed class UncommittedLogLimitException(long limit)
    : InvalidOperationException($"Uncommitted log reached its {limit} byte limit. Retry after quorum progress.");
public readonly record struct ProposalResult(long Index, long Term);
public readonly record struct ReadIndexResult(long Index, long Term);
public sealed class LeadershipTransferInProgressException(string target)
    : InvalidOperationException($"Leadership is being transferred to '{target}'.")
{
    public string Target { get; } = target;
}
public sealed class NotLeaderException(string? leaderId)
    : InvalidOperationException(leaderId is null ? "No leader is currently known." : $"Send the request to leader '{leaderId}'.")
{
    public string? LeaderId { get; } = leaderId;
}

/// <summary>Transport must enforce deadlines/cancellation and return only a response from the requested peer.</summary>
public interface IRaftTransport : ITransport
{
    new ValueTask<RaftRpc> SendAsync(string peerId, RaftRpc request, CancellationToken cancellationToken);
    ValueTask<RaftRpc> ITransport.SendAsync(string peerId, RaftRpc request, CancellationToken cancellationToken) =>
        SendAsync(peerId, request, cancellationToken);
}

/// <summary>
/// Apply is serialized and receives no-ops too. Persist effects and LastApplied atomically, or replay safely.
/// A fresh in-memory implementation reports zero and rebuilds from the retained committed log.
/// </summary>
public interface IRaftStateMachine : IStateMachine
{
    new long LastApplied { get; }
    new ValueTask ApplyAsync(LogEntry entry, CancellationToken cancellationToken);
    long IStateMachine.LastApplied => LastApplied;
    ValueTask IStateMachine.ApplyAsync(LogEntry entry, CancellationToken cancellationToken) =>
        ApplyAsync(entry, cancellationToken);
}

/// <summary>Optional batching: apply an ordered committed range and advance LastApplied through its final entry.</summary>
public interface IRaftBatchStateMachine : IRaftStateMachine, IBatchStateMachine
{
    new ValueTask ApplyBatchAsync(IReadOnlyList<LogEntry> entries, CancellationToken cancellationToken);
    ValueTask IBatchStateMachine.ApplyBatchAsync(IReadOnlyList<LogEntry> entries, CancellationToken cancellationToken) =>
        ApplyBatchAsync(entries, cancellationToken);
}
