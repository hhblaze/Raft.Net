namespace Raft;

/// <summary>Deadline-aware transport. Implementations may supply TCP, TLS/mTLS, or a simulated network.</summary>
public interface ITransport
{
    ValueTask<RaftRpc> SendAsync(string peerId, RaftRpc request, CancellationToken cancellationToken);
}

/// <summary>Serialized deterministic application. Persist effects and LastApplied atomically or replay safely.</summary>
public interface IStateMachine
{
    long LastApplied { get; }
    ValueTask ApplyAsync(LogEntry entry, CancellationToken cancellationToken);
}

public interface IBatchStateMachine : IStateMachine
{
    ValueTask ApplyBatchAsync(IReadOnlyList<LogEntry> entries, CancellationToken cancellationToken);
}

public readonly record struct AppliedCheckpoint(long Index, long Term);

/// <summary>Durable application effects and this index/term pair share one atomic transaction.</summary>
public interface ICheckpointStateMachine : IStateMachine
{
    AppliedCheckpoint Checkpoint { get; }
}

/// <summary>Samples on the single runtime owner, never inside the consensus reducer.</summary>
public interface IRandom
{
    int Next(int minInclusive, int maxExclusive);
}

/// <summary>Creates a ticker owned and disposed by the node. No clock enters the consensus core.</summary>
public interface IClock
{
    ITicker CreateTicker(TimeSpan interval);
}

public interface ITicker : IAsyncDisposable
{
    ValueTask<bool> WaitForNextTickAsync(CancellationToken cancellationToken);
}
