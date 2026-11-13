using System.Text;
using System.Text.Json;
using DBreeze;
using DBreeze.Transactions;
using DBreeze.Utils;
using Raft.Storage;

namespace Raft.Tutorial;

public sealed record SetValue(int Version, string RequestId, string Key, string Value);
public sealed record AppliedPut(string RequestId, string Key, string Value, long Index, long Term);

public interface IBookState : IBatchStateMachine, ICheckpointStateMachine, IDisposable
{
    string? Get(string key);
    AppliedPut? FindRequest(string requestId);
}

public static class BookData
{
    public const string Table = "book/data";
    public const string CheckpointTable = "book/checkpoint";
    public const string ApplicationId = "book-kv-v1";

    // book:start encode
    public static byte[] Encode(SetValue command, int maxCommandBytes)
    {
        Validate(command);
        var bytes = JsonSerializer.SerializeToUtf8Bytes(command);
        if (bytes.Length > maxCommandBytes)
            throw new ArgumentException("The encoded command exceeds MaxCommandBytes.");
        return bytes;
    }

    private static void Validate(SetValue command)
    {
        if (command.Version != 1 || string.IsNullOrWhiteSpace(command.RequestId) ||
            command.RequestId.Length > 128 || string.IsNullOrWhiteSpace(command.Key) ||
            command.Key.Length > 256 || command.Value is null)
            throw new ArgumentException("Expected version 1, request ID (1..128), key (1..256), and value.");
    }
    // book:end encode

    // book:start apply
    public static void Apply(Transaction tx, LogEntry entry)
    {
        // DBreeze helpers skip no-op/configuration effects and checkpoint them.
        var command = JsonSerializer.Deserialize<SetValue>(entry.Command.Span)
            ?? throw new InvalidDataException("Missing command.");
        Validate(command); // Failure here is an execution failure: the node halts.

        var requestKey = 2.ToIndex(command.RequestId);
        if (tx.Select<byte[], byte[]>(Table, requestKey).Exists)
            return; // Keep the first result; the caller can detect an ID conflict.

        tx.Insert<byte[], string>(Table, 1.ToIndex(command.Key), command.Value);
        var result = new AppliedPut(command.RequestId, command.Key, command.Value,
            entry.Index, entry.Term);
        tx.Insert<byte[], byte[]>(Table, requestKey, JsonSerializer.SerializeToUtf8Bytes(result));
        // No Commit(), await, live-engine access, clock, random, or network here.
        // The helper commits these effects and (LastApplied, term) atomically.
    }
    // book:end apply

    public static string? Get(Transaction tx, string key)
    {
        var row = tx.Select<byte[], string>(Table, 1.ToIndex(key));
        return row.Exists ? row.Value : null;
    }

    public static AppliedPut? FindRequest(Transaction tx, string requestId)
    {
        var row = tx.Select<byte[], byte[]>(Table, 2.ToIndex(requestId));
        return row.Exists ? JsonSerializer.Deserialize<AppliedPut>(row.Value) : null;
    }
}

// book:start disk-state
public sealed class DiskBookState : IBookState, ISnapshotStateMachine
{
    private readonly DBreezeGenerationStateMachine _database;

    public DiskBookState(string absoluteDirectory) => _database = new(
        absoluteDirectory, BookData.ApplicationId, BookData.CheckpointTable,
        [BookData.Table], BookData.Apply);

    public long LastApplied => _database.LastApplied;
    public AppliedCheckpoint Checkpoint => _database.Checkpoint;
    public string? Get(string key) => _database.Read(tx => BookData.Get(tx, key));
    public AppliedPut? FindRequest(string id) => _database.Read(tx => BookData.FindRequest(tx, id));
    public ValueTask ApplyAsync(LogEntry entry, CancellationToken token) => _database.ApplyAsync(entry, token);
    public ValueTask ApplyBatchAsync(IReadOnlyList<LogEntry> entries, CancellationToken token) =>
        _database.ApplyBatchAsync(entries, token);
    public ValueTask CreateSnapshotAsync(SnapshotBuildContext context, string path, CancellationToken token) =>
        _database.CreateSnapshotAsync(context, path, token);
    public ValueTask PrepareSnapshotAsync(SnapshotImage image, long maxBytes, CancellationToken token) =>
        _database.PrepareSnapshotAsync(image, maxBytes, token);
    public ValueTask InstallSnapshotAsync(SnapshotImage image, CancellationToken token) =>
        _database.InstallSnapshotAsync(image, token);
    public void Dispose() => _database.Dispose();
}
// book:end disk-state

// book:start memory-state
public sealed class MemoryBookState : IBookState
{
    private readonly DBreezeEngine _engine;
    private readonly DBreezeStateMachine _database;

    public MemoryBookState()
    {
        _engine = new(new DBreezeConfiguration { Storage = DBreezeConfiguration.eStorage.MEMORY });
        try
        {
            _database = new(_engine, BookData.CheckpointTable, [BookData.Table], BookData.Apply);
        }
        catch { _engine.Dispose(); throw; }
    }

    public long LastApplied => _database.LastApplied;
    public AppliedCheckpoint Checkpoint => _database.Checkpoint;
    public string? Get(string key)
    {
        using var tx = _engine.GetTransaction();
        return BookData.Get(tx, key); // Materialize the value before disposal.
    }
    public AppliedPut? FindRequest(string id)
    {
        using var tx = _engine.GetTransaction();
        return BookData.FindRequest(tx, id);
    }
    public ValueTask ApplyAsync(LogEntry entry, CancellationToken token) => _database.ApplyAsync(entry, token);
    public ValueTask ApplyBatchAsync(IReadOnlyList<LogEntry> entries, CancellationToken token) =>
        _database.ApplyBatchAsync(entries, token);
    public void Dispose() => _engine.Dispose();
}
// book:end memory-state
