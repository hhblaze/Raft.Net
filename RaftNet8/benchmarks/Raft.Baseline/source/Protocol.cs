namespace Raft;

/// <summary>Immutable owned log data. The constructor copies caller memory.</summary>
public sealed class LogEntry
{
    internal const int EncodedOverhead = 21;
    public long Index { get; }
    public long Term { get; }
    public bool IsNoOp { get; }
    public ReadOnlyMemory<byte> Command { get; }

    public LogEntry(long index, long term, ReadOnlySpan<byte> command, bool isNoOp = false)
        : this(index, term, command.ToArray(), isNoOp) { }

    internal LogEntry(long index, long term, byte[] ownedCommand, bool isNoOp)
    {
        if (index < 1 || term < 1 || (isNoOp && ownedCommand.Length != 0))
            throw new ArgumentException("Log entries need positive index/term and empty data for no-ops.");
        Index = index;
        Term = term;
        Command = ownedCommand;
        IsNoOp = isNoOp;
    }
}

public abstract record RaftRpc(long Term);
public sealed record VoteRequest(long Term, long LastLogIndex, long LastLogTerm) : RaftRpc(Term);
public sealed record VoteResponse(long Term, bool Granted) : RaftRpc(Term);
public sealed record AppendRequest(long Term, long RequestId, long PreviousIndex, long PreviousTerm,
    long LeaderCommit, IReadOnlyList<LogEntry> Entries) : RaftRpc(Term);
public sealed record AppendResponse(long Term, long RequestId, bool Success, long MatchIndex,
    long ConflictIndex = 0, long ConflictTerm = 0) : RaftRpc(Term);

public enum RaftRole { Follower, Candidate, Leader }
public sealed record RaftStatus(string NodeId, RaftRole Role, long Term, string? LeaderId,
    long CommitIndex, long LastLogIndex, long LastApplied, Exception? Fault = null);
public readonly record struct ProposalResult(long Index, long Term);
public sealed class NotLeaderException(string? leaderId)
    : InvalidOperationException(leaderId is null ? "No leader is currently known." : $"Send the request to leader '{leaderId}'.")
{
    public string? LeaderId { get; } = leaderId;
}

/// <summary>Transport must enforce deadlines/cancellation and return only a response from the requested peer.</summary>
public interface IRaftTransport
{
    ValueTask<RaftRpc> SendAsync(string peerId, RaftRpc request, CancellationToken cancellationToken);
}

/// <summary>
/// Apply is serialized and receives no-ops too. Persist effects and LastApplied atomically, or replay safely.
/// A fresh in-memory implementation reports zero and rebuilds from the retained committed log.
/// </summary>
public interface IRaftStateMachine
{
    long LastApplied { get; }
    ValueTask ApplyAsync(LogEntry entry, CancellationToken cancellationToken);
}

/// <summary>Optional batching: apply an ordered committed range and advance LastApplied through its final entry.</summary>
public interface IRaftBatchStateMachine : IRaftStateMachine
{
    ValueTask ApplyBatchAsync(IReadOnlyList<LogEntry> entries, CancellationToken cancellationToken);
}
