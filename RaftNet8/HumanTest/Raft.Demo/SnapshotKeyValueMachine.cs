using System.Text.Json;
using Raft.Storage;

namespace Raft.Demo;

/// <summary>Durable deduplicated key/value application; the replay handler writes only its supplied transaction.</summary>
internal sealed class SnapshotKeyValueMachine : IRaftBatchStateMachine, ISnapshotStateMachine, IDisposable
{
    private readonly DBreezeGenerationStateMachine _database;
    private readonly string _nodeId;
    private readonly Action<string> _trace;

    public SnapshotKeyValueMachine(string directory, string nodeId, Action<string>? trace = null)
    {
        _nodeId = nodeId;
        _trace = trace ?? (_ => { });
        _database = new(directory, "demo-kv-v2", "demo/checkpoint", ["demo/values", "demo/requests", "demo/meta"], (tx, entry) =>
        {
            var command = JsonSerializer.Deserialize(entry.Command.Span, DemoJsonContext.Default.SetCommand);
            if (command is null || string.IsNullOrEmpty(command.RequestId) || string.IsNullOrEmpty(command.Key))
                throw new InvalidDataException("Invalid set command.");
            if (tx.Select<string, long>("demo/requests", command.RequestId).Exists) return;
            if (!tx.Select<string, string>("demo/values", command.Key).Exists)
            {
                var count = tx.Select<string, long>("demo/meta", "count");
                tx.Insert<string, long>("demo/meta", "count", (count.Exists ? count.Value : 0) + 1);
            }
            tx.Insert<string, string>("demo/values", command.Key, command.Value);
            tx.Insert<string, long>("demo/requests", command.RequestId, entry.Index);
        });
    }
    public long LastApplied => _database.LastApplied;
    public AppliedCheckpoint Checkpoint => _database.Checkpoint;
    public long Count => _database.Read(tx => tx.Select<string, long>("demo/meta", "count").Value);
    public string? Get(string key) => _database.Read(tx =>
    {
        var row = tx.Select<string, string>("demo/values", key);
        return row.Exists ? row.Value : null;
    });
    public ValueTask ApplyAsync(LogEntry entry, CancellationToken token) => ApplyBatchAsync([entry], token);
    public async ValueTask ApplyBatchAsync(IReadOnlyList<LogEntry> entries, CancellationToken token)
    {
        await _database.ApplyBatchAsync(entries, token).ConfigureAwait(false);
        foreach (var entry in entries)
        {
            if (entry.Kind != LogEntryKind.Command) continue;
            var command = JsonSerializer.Deserialize(entry.Command.Span, DemoJsonContext.Default.SetCommand)!;
            _trace($"APPLY {_nodeId} index={entry.Index} term={entry.Term}: {command.Key}={command.Value}");
        }
    }
    public ValueTask CreateSnapshotAsync(SnapshotBuildContext context, string outputPath, CancellationToken token) => _database.CreateSnapshotAsync(context, outputPath, token);
    public ValueTask PrepareSnapshotAsync(SnapshotImage image, long maxBytes, CancellationToken token) => _database.PrepareSnapshotAsync(image, maxBytes, token);
    public ValueTask InstallSnapshotAsync(SnapshotImage image, CancellationToken token) => _database.InstallSnapshotAsync(image, token);
    public static byte[] Encode(string key, string value) => KeyValueMachine.Encode(key, value);
    public void Dispose() => _database.Dispose();
}
