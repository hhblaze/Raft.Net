using System.Collections.Immutable;
using System.Threading.Channels;

namespace Raft.Storage;

/// <summary>
/// Owns an existing store and executes its synchronous operations on a bounded channel worker.
/// Transactions begin and end on that worker; cancellation never abandons an already executing commit.
/// DBreezeRaftStorage can share its engine with the application as before.
/// </summary>
public sealed class RaftStorageAdapter : ILogStore, ILogStoreProgress
{
    private sealed record Operation(LogQuery? Query, LogWrite? Write, bool Recover, CancellationToken Token)
    {
        public TaskCompletionSource<object> Completion { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    private readonly IRaftStorage _storage;
    private readonly Channel<Operation> _operations = Channel.CreateBounded<Operation>(
        new BoundedChannelOptions(2) { SingleReader = true, AllowSynchronousContinuations = false });
    private readonly Task _worker;
    private int _disposed;
    private long _uncommittedBytes;
    public long UncommittedBytes => Interlocked.Read(ref _uncommittedBytes);

    public RaftStorageAdapter(IRaftStorage storage)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        _uncommittedBytes = Uncommitted(storage);
        // DBreeze operations are synchronous, including durable flushes. Keep them
        // off the shared ThreadPool so blocked disks cannot starve Raft/TCP continuations.
        var stopped = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _worker = stopped.Task;
        var thread = new Thread(() =>
        {
            try { Run(); stopped.TrySetResult(); }
            catch (Exception error) { stopped.TrySetException(error); }
        })
        {
            IsBackground = true,
            Name = "Raft storage " + (storage is IRaftStorageIdentity identity ? identity.Identity.NodeId : "worker")
        };
        thread.Start();
    }

    public async ValueTask<RecoveredLogState> RecoverAsync(CancellationToken cancellationToken) =>
        (RecoveredLogState)await ExecuteAsync(new(null, null, true, cancellationToken)).ConfigureAwait(false);

    public async ValueTask<LogReadResult> ReadAsync(LogQuery query, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        cancellationToken.ThrowIfCancellationRequested();
        if (_storage is DBreezeLogStorage cached && cached.TryQueryCached(query, out var result)) return result!;
        return (LogReadResult)await ExecuteAsync(new(query, null, false, cancellationToken)).ConfigureAwait(false);
    }

    public async ValueTask WriteAsync(LogWrite write, CancellationToken cancellationToken) =>
        _ = await ExecuteAsync(new(null, write, false, cancellationToken)).ConfigureAwait(false);

    private async ValueTask<object> ExecuteAsync(Operation operation)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        await _operations.Writer.WriteAsync(operation, operation.Token).ConfigureAwait(false);
        // Once admitted, observe the actual result even if caller cancellation races a disk commit.
        return await operation.Completion.Task.ConfigureAwait(false);
    }

    private void Run()
    {
        while (WaitForOperation())
        while (_operations.Reader.TryRead(out var operation))
        {
            try
            {
                operation.Token.ThrowIfCancellationRequested();
                object result;
                if (operation.Recover) result = Recover(_storage);
                else if (operation.Query is { } query) result = Read(_storage, query);
                else
                {
                    Write(_storage, operation.Write!);
                    Interlocked.Exchange(ref _uncommittedBytes, Uncommitted(_storage));
                    result = true;
                }
                operation.Completion.TrySetResult(result);
            }
            catch (OperationCanceledException canceled) { operation.Completion.TrySetCanceled(canceled.CancellationToken); }
            catch (Exception error) { operation.Completion.TrySetException(error); }
        }
    }

    private bool WaitForOperation()
    {
        var ready = _operations.Reader.WaitToReadAsync();
        return ready.IsCompletedSuccessfully ? ready.Result : ready.AsTask().GetAwaiter().GetResult();
    }

    internal static RecoveredLogState Recover(IRaftStorage storage) => storage is IExtendedRaftStorage extended
        ? extended.Recover() : new(storage.HardState, storage.LastIndex, storage.LastTerm)
        { Identity = (storage as IRaftStorageIdentity)?.Identity, UncommittedBytes = Uncommitted(storage) };

    // V2 reads cached frontier metadata. The compatibility adapter scans only the uncommitted suffix.
    internal static long Uncommitted(IRaftStorage storage)
    {
        if (storage is ILogStoreProgress progress) return progress.UncommittedBytes;
        if (storage is IExtendedRaftStorage extended) return extended.Recover().UncommittedBytes;
        long bytes = 0;
        var through = storage.LastIndex;
        for (var from = storage.HardState.CommitIndex + 1; from <= through;)
        {
            var entries = storage.Read(from, 128, 64 * 1024 * 1024);
            if (entries.Count == 0) throw new InvalidDataException("Missing uncommitted log suffix.");
            foreach (var entry in entries) bytes = checked(bytes + entry.Command.Length + LogEntry.EncodedOverhead);
            from = entries[^1].Index + 1;
        }
        return bytes;
    }

    internal static LogReadResult Read(IRaftStorage storage, LogQuery query)
    {
        if (storage is IExtendedRaftStorage extended) return extended.Query(query);
        switch (query)
        {
            case LogQuery.Snapshot: return new();
            case LogQuery.Term term: return new(Number: storage.GetTerm(term.Index));
            case LogQuery.FirstIndexOfTerm first: return new(Number: storage.FirstIndexOfTerm(first.Index));
            case LogQuery.LastIndexOfTerm last: return new(Number: storage.LastIndexOfTerm(last.Value));
            case LogQuery.Terms terms:
                var values = ImmutableArray.CreateBuilder<long>(terms.Count);
                for (var i = 0; i < terms.Count; i++) values.Add(storage.GetTerm(checked(terms.FromIndex + i)));
                return new(Terms: values.MoveToImmutable());
            case LogQuery.Entries entries:
                return new(Entries: ImmutableArray.CreateRange(storage.Read(entries.FromIndex, entries.MaxCount, entries.MaxBytes)));
            case LogQuery.Replication replication:
                var batch = storage.Read(checked(replication.PreviousIndex + 1), replication.MaxCount, replication.MaxBytes);
                return new(PreviousTerm: storage.GetTerm(replication.PreviousIndex), Entries: ImmutableArray.CreateRange(batch));
            default: throw new ArgumentException("Unknown log query.", nameof(query));
        }
    }

    internal static void Write(IRaftStorage storage, LogWrite write)
    {
        if (storage is IExtendedRaftStorage extended) { extended.Write(write); return; }
        switch (write)
        {
            case LogWrite.HardStateUpdate hard: storage.SaveHardState(hard.State); break;
            case LogWrite.Append append: storage.Append(append.Entries); break;
            case LogWrite.ReplaceSuffix suffix: storage.ReplaceSuffix(suffix.FromIndex, suffix.Entries); break;
            case LogWrite.PublishSnapshot: throw new NotSupportedException("raft8-v1 does not support snapshot publication.");
            case LogWrite.Batch batch:
                foreach (var item in batch.Writes) Write(storage, item);
                break;
            default: throw new ArgumentException("Unknown log mutation.", nameof(write));
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        _operations.Writer.TryComplete();
        await _worker.ConfigureAwait(false);
        _storage.Dispose();
    }
}

/// <summary>Inline effect executor for synchronous simulation/benchmark adapters; never part of the core.</summary>
internal sealed class InlineLogStore(IRaftStorage storage) : ILogStore, ILogStoreProgress
{
    public long UncommittedBytes { get; private set; } = RaftStorageAdapter.Uncommitted(storage);
    public ValueTask<RecoveredLogState> RecoverAsync(CancellationToken cancellationToken) => new(RaftStorageAdapter.Recover(storage));
    public ValueTask<LogReadResult> ReadAsync(LogQuery query, CancellationToken cancellationToken) => new(RaftStorageAdapter.Read(storage, query));
    public ValueTask WriteAsync(LogWrite write, CancellationToken cancellationToken)
    {
        RaftStorageAdapter.Write(storage, write);
        UncommittedBytes = RaftStorageAdapter.Uncommitted(storage);
        return ValueTask.CompletedTask;
    }
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
