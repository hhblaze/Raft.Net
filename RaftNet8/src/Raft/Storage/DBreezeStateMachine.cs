using System.Buffers.Binary;
using DBreeze;
using DBreeze.Transactions;

namespace Raft.Storage;

public enum ApplicationPersistenceBoundary { BeforeTransaction, AfterCommands, BeforeCommit, AfterCommit, AfterCheckpointPublication }

/// <summary>
/// Reusable atomic application/checkpoint integration. The handler is synchronous and deterministic.
/// The supplied engine remains caller-owned; reserve every application write table at construction.
/// </summary>
public sealed class DBreezeStateMachine : ICheckpointStateMachine, IRaftBatchStateMachine
{
    private sealed record CheckpointValue(AppliedCheckpoint Value);
    private readonly object _gate = new();
    private readonly DBreezeEngine _engine;
    private readonly string _checkpointTable;
    private readonly string[] _tables;
    private readonly Action<Transaction, LogEntry> _applyCommand;
    private readonly Action<IReadOnlyList<LogEntry>>? _afterCommit;
    private readonly Action<ApplicationPersistenceBoundary>? _observe;
    private CheckpointValue _checkpoint;
    private Exception? _failure;

    public DBreezeStateMachine(DBreezeEngine engine, string checkpointTable, IEnumerable<string> applicationWriteTables,
        Action<Transaction, LogEntry> applyCommand, Action<IReadOnlyList<LogEntry>>? afterCommit = null,
        Action<ApplicationPersistenceBoundary>? observe = null)
    {
        _engine = engine ?? throw new ArgumentNullException(nameof(engine));
        _checkpointTable = checkpointTable;
        _applyCommand = applyCommand ?? throw new ArgumentNullException(nameof(applyCommand));
        _afterCommit = afterCommit;
        _observe = observe;
        _tables = applicationWriteTables.Append(checkpointTable).Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal).ToArray();
        foreach (var table in _tables)
            if (string.IsNullOrWhiteSpace(table) || table.StartsWith("raft8/", StringComparison.Ordinal) ||
                table.IndexOfAny(['*', '#', '$', '@', '\\', '^', '~', '´', '\r', '\n', '\t']) >= 0)
                throw new ArgumentException("Invalid or reserved application table name.", nameof(applicationWriteTables));
        using var tx = _engine.GetTransaction();
        var row = tx.Select<string, byte[]>(_checkpointTable, "applied");
        _checkpoint = new(row.Exists ? DecodeCheckpoint(row.Value) : new(0, 0));
    }

    public AppliedCheckpoint Checkpoint => Volatile.Read(ref _checkpoint).Value;
    public long LastApplied => Checkpoint.Index;
    public ValueTask ApplyAsync(LogEntry entry, CancellationToken cancellationToken) =>
        ApplyBatchAsync(new[] { entry }, cancellationToken);

    public ValueTask ApplyBatchAsync(IReadOnlyList<LogEntry> entries, CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            if (_failure is not null) throw new InvalidOperationException("Application failed; recreate it from durable state.", _failure);
            cancellationToken.ThrowIfCancellationRequested();
            if (entries.Count == 0) return ValueTask.CompletedTask;
            var previous = Checkpoint;
            foreach (var entry in entries)
            {
                if (entry.Index != previous.Index + 1 || entry.Term < previous.Term)
                    throw new InvalidDataException("Application entries must be contiguous with nondecreasing terms.");
                previous = new(entry.Index, entry.Term);
            }
            try
            {
                _observe?.Invoke(ApplicationPersistenceBoundary.BeforeTransaction);
                using (var tx = _engine.GetTransaction())
                {
                    tx.SynchronizeTables(_tables);
                    var durable = tx.Select<string, byte[]>(_checkpointTable, "applied");
                    if ((durable.Exists ? DecodeCheckpoint(durable.Value) : new AppliedCheckpoint(0, 0)) != Checkpoint)
                        throw new InvalidDataException("Application checkpoint changed outside its owner.");
                    foreach (var entry in entries)
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        if (!entry.IsNoOp) _applyCommand(tx, entry);
                    }
                    _observe?.Invoke(ApplicationPersistenceBoundary.AfterCommands);
                    tx.Insert<string, byte[]>(_checkpointTable, "applied", EncodeCheckpoint(previous));
                    _observe?.Invoke(ApplicationPersistenceBoundary.BeforeCommit);
                    tx.Commit();
                }
                _observe?.Invoke(ApplicationPersistenceBoundary.AfterCommit);
                Volatile.Write(ref _checkpoint, new(previous));
                _observe?.Invoke(ApplicationPersistenceBoundary.AfterCheckpointPublication);
                _afterCommit?.Invoke(entries);
                return ValueTask.CompletedTask;
            }
            catch (Exception error) { _failure = error; throw; }
        }
    }

    public static byte[] EncodeCheckpoint(AppliedCheckpoint checkpoint)
    {
        Span<byte> bytes = stackalloc byte[16];
        BinaryPrimitives.WriteInt64LittleEndian(bytes, checkpoint.Index);
        BinaryPrimitives.WriteInt64LittleEndian(bytes[8..], checkpoint.Term);
        return StorageCodecV2.Wrap(bytes);
    }
    public static AppliedCheckpoint DecodeCheckpoint(byte[] record)
    {
        var bytes = StorageCodecV2.Open(record);
        if (bytes.Length != 16) throw new InvalidDataException("Invalid checkpoint encoding.");
        var index = BinaryPrimitives.ReadInt64LittleEndian(bytes);
        var term = BinaryPrimitives.ReadInt64LittleEndian(bytes[8..]);
        if (index < 0 || term < 0 || ((index == 0) != (term == 0))) throw new InvalidDataException("Invalid index/term checkpoint.");
        return new(index, term);
    }
}
