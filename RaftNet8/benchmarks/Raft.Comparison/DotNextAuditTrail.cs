using DotNext.IO;
using DotNext.IO.Log;
using DotNext.Net.Cluster;
using DotNext.Net.Cluster.Consensus.Raft;
using NativeEntry = DotNext.Net.Cluster.Consensus.Raft.IRaftLogEntry;

namespace Raft.Benchmarks;

/// <summary>
/// Benchmark-only application adapter around the unmodified native persistent WAL.
/// Native term/vote/append/commit and transport remain DotNext's implementation.
/// The public commit frontier and waiter advance only after the common business transaction.
/// </summary>
internal sealed class DotNextAuditTrail : IPersistentState, IDisposable
{
    private sealed class NativeWal(string path, DotNextAuditTrail owner) : MemoryBasedStateMachine(path, 65536, new()
    {
        WriteMode = WriteMode.AutoFlush, // explicitly FlushToDisk on native partition flush
        IntegrityCheck = true,
        UseCaching = true,
        CacheEvictionPolicy = LogEntryCacheEvictionPolicy.OnCommit,
        CompactionMode = CompactionMode.Background // no ForceCompaction calls
    })
    {
        // Own payloads while the native application callback still has access to the
        // cache. Reading them again after OnCommit eviction would unfairly add disk I/O.
        protected override async ValueTask ApplyAsync(LogEntry entry)
        {
            var data = await entry.ToByteArrayAsync();
            owner._staged.Add(new(entry.Index, entry.Term, data.AsSpan()));
        }
        protected override SnapshotBuilder CreateSnapshotBuilder(in SnapshotBuilderContext context) =>
            throw new InvalidOperationException("Compaction is disabled for this retained-log benchmark.");
    }

    private readonly NativeWal _wal;
    private readonly IPersistentState _native;
    private readonly ComparisonMachine _machine;
    private readonly SemaphoreSlim _commitGate = new(1, 1), _appendGate = new(1, 1);
    private readonly int _applyBatch;
    private readonly List<global::Raft.LogEntry> _staged = [];
    private int _stagePosition;
    private TaskCompletionSource _applied = NewSignal();
    private Exception? _failure;
    private int _disposed;
    internal long AppendBatches, AppendedEntries, ApplyBatches, AppliedCommands;
    internal int MaxObservedApplyBatch;
    internal Exception? Failure => Volatile.Read(ref _failure);
    internal long BusinessIndex => _machine.LastApplied;
    internal ComparisonMachine Machine => _machine;
    internal Func<ValueTask>? BeforeBusinessApply { get; init; }

    public DotNextAuditTrail(string path, ComparisonMachine machine, int batch, int payloadBytes)
    {
        _wal = new(path, this);
        _native = _wal;
        _machine = machine;
        _applyBatch = Math.Min(batch, 256 * 1024 / (payloadBytes + 21));
    }

    private static TaskCompletionSource NewSignal() => new(TaskCreationOptions.RunContinuationsAsynchronously);
    public long LastCommittedEntryIndex => _machine.LastApplied;
    public long LastEntryIndex => _native.LastEntryIndex;
    public long Term => _native.Term;
    public bool IsLogEntryLengthAlwaysPresented => true;
    public bool IsVotedFor(in ClusterMemberId member) => _native.IsVotedFor(in member);
    public ValueTask<long> IncrementTermAsync(ClusterMemberId member, CancellationToken token = default) => _native.IncrementTermAsync(member, token);
    public ValueTask UpdateTermAsync(long term, bool resetLastVote, CancellationToken token = default) => _native.UpdateTermAsync(term, resetLastVote, token);
    public ValueTask UpdateVotedForAsync(ClusterMemberId member, CancellationToken token = default) => _native.UpdateVotedForAsync(member, token);
    public Task InitializeAsync(CancellationToken token = default) => _native.InitializeAsync(token);
    public ValueTask<TResult> ReadAsync<TResult>(ILogEntryConsumer<NativeEntry, TResult> reader, long startIndex, long endIndex, CancellationToken token = default) =>
        _native.ReadAsync(reader, startIndex, endIndex, token);
    public ValueTask AppendAsync<TEntry>(ILogEntryProducer<TEntry> entries, long startIndex, bool skipCommitted = false, CancellationToken token = default)
        where TEntry : NativeEntry => _native.AppendAsync(entries, startIndex, skipCommitted, token);
    public ValueTask AppendAsync<TEntry>(TEntry entry, long startIndex, CancellationToken token = default)
        where TEntry : NativeEntry => _native.AppendAsync(entry, startIndex, token);
    public ValueTask<long> AppendAsync<TEntry>(TEntry entry, CancellationToken token = default)
        where TEntry : NativeEntry => _native.AppendAsync(entry, token);

    public async ValueTask<long> AppendBatchAsync(BinaryLogEntry[] entries, CancellationToken token)
    {
        await _appendGate.WaitAsync(token);
        try
        {
            var start = LastEntryIndex + 1;
            await _native.AppendAsync(new Producer(entries), start, false, token);
            Interlocked.Increment(ref AppendBatches);
            Interlocked.Add(ref AppendedEntries, entries.Length);
            return start + entries.Length - 1;
        }
        finally { _appendGate.Release(); }
    }

    public async ValueTask<long> CommitAsync(long endIndex, CancellationToken token = default)
    {
        await _commitGate.WaitAsync(token);
        try
        {
            var count = await _native.CommitAsync(endIndex, token);
            await ApplyCommittedAsync(token);
            return count;
        }
        catch (Exception error) { Fail(error); throw; }
        finally { _commitGate.Release(); }
    }

    public async ValueTask<long> AppendAndCommitAsync<TEntry>(ILogEntryProducer<TEntry> entries,
        long startIndex, bool skipCommitted, long commitIndex, CancellationToken token = default) where TEntry : NativeEntry
    {
        await _commitGate.WaitAsync(token);
        try
        {
            var count = await _native.AppendAndCommitAsync(entries, startIndex, skipCommitted, commitIndex, token);
            await ApplyCommittedAsync(token);
            return count;
        }
        catch (Exception error) { Fail(error); throw; }
        finally { _commitGate.Release(); }
    }

    private async ValueTask ApplyCommittedAsync(CancellationToken token)
    {
        var through = _native.LastCommittedEntryIndex;
        _stagePosition = 0;
        while (_machine.LastApplied < through)
        {
            var start = _machine.LastApplied + 1;
            var end = Math.Min(through, start + _applyBatch - 1);
            await _native.ReadAsync(new ApplicationReader(this, start), start, end, token);
            Interlocked.Exchange(ref _applied, NewSignal()).TrySetResult();
        }
        if (_stagePosition != _staged.Count) throw new InvalidDataException("Native application staged an unexpected index range.");
        _staged.Clear();
    }

    public async ValueTask WaitForApplyAsync(CancellationToken token = default) =>
        await Volatile.Read(ref _applied).Task.WaitAsync(token);

    public async ValueTask WaitForApplyAsync(long index, CancellationToken token = default)
    {
        while (true)
        {
            var signal = Volatile.Read(ref _applied);
            if (Failure is { } error) throw new InvalidOperationException("DotNext benchmark application failed.", error);
            if (_machine.LastApplied >= index) return;
            await signal.Task.WaitAsync(token);
        }
    }

    private void Fail(Exception error)
    {
        Interlocked.CompareExchange(ref _failure, error, null);
        Interlocked.Exchange(ref _applied, NewSignal()).TrySetResult();
    }

    private sealed class ApplicationReader(DotNextAuditTrail owner, long start) : ILogEntryConsumer<NativeEntry, bool>
    {
        public bool LogEntryMetadataOnly => true;
        public async ValueTask<bool> ReadAsync<TEntry, TList>(TList entries, long? snapshotIndex, CancellationToken token)
            where TEntry : NativeEntry where TList : IReadOnlyList<TEntry>
        {
            if (snapshotIndex is not null || entries.Count == 0) throw new InvalidDataException("Unexpected snapshot/empty application range.");
            var batch = new global::Raft.LogEntry[entries.Count];
            for (var i = 0; i < entries.Count; i++)
            {
                var entry = entries[i];
                var index = start + i;
                if (owner._stagePosition < owner._staged.Count && owner._staged[owner._stagePosition].Index == index)
                    batch[i] = owner._staged[owner._stagePosition++];
                else if (entry.Length == 0)
                    batch[i] = new(index, entry.Term, ReadOnlySpan<byte>.Empty, isNoOp: true);
                else throw new InvalidDataException("Missing native application payload.");
            }
            // All async decoding finishes before the common synchronous DBreeze transaction.
            if (owner.BeforeBusinessApply is { } beforeApply) await beforeApply();
            await owner._machine.ApplyBatchAsync(batch, token);
            owner.ApplyBatches++;
            owner.AppliedCommands += batch.Count(entry => !entry.IsNoOp);
            owner.MaxObservedApplyBatch = Math.Max(owner.MaxObservedApplyBatch, batch.Length);
            return true;
        }
    }

    private sealed class Producer(BinaryLogEntry[] entries) : ILogEntryProducer<BinaryLogEntry>
    {
        private int _index = -1;
        public BinaryLogEntry Current => entries[_index];
        public long RemainingCount => entries.Length - _index - 1;
        public ValueTask<bool> MoveNextAsync() => ValueTask.FromResult(++_index < entries.Length);
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        _wal.Dispose();
        _commitGate.Dispose();
        _appendGate.Dispose();
    }
}
