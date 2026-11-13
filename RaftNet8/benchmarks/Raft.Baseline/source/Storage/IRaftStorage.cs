namespace Raft.Storage;

public sealed record HardState(long Term = 0, string? VotedFor = null, long CommitIndex = 0);
public readonly record struct StorageMetrics(long ReadTransactions, long WriteTransactions,
    long CacheHits, int CachedEntries, long CachedBytes);

/// <summary>All mutations are durable on return. Implementations must serialize concurrent calls.</summary>
public interface IRaftStorage : IDisposable
{
    HardState HardState { get; }
    long LastIndex { get; }
    long LastTerm { get; }
    void SaveHardState(HardState state);
    void Append(IReadOnlyList<LogEntry> entries);
    void ReplaceSuffix(long fromIndex, IReadOnlyList<LogEntry> entries);
    long GetTerm(long index);
    IReadOnlyList<LogEntry> Read(long fromIndex, int maxCount, int maxBytes);
    long FirstIndexOfTerm(long index);
    long LastIndexOfTerm(long term);
}
