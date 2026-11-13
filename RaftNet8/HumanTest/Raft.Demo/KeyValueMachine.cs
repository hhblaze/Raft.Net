using System.Collections.Concurrent;
using System.Text.Json;
using System.Text.Json.Serialization;
using DBreeze;
using Raft;

namespace Raft.Demo;

internal sealed record SetCommand(string RequestId, string Key, string Value);

[JsonSerializable(typeof(SetCommand))]
internal partial class DemoJsonContext : JsonSerializerContext;

/// <summary>Example of effects, request deduplication, and application checkpoint in one DBreeze transaction.</summary>
internal sealed class KeyValueMachine : IRaftBatchStateMachine, ICheckpointStateMachine
{
    private sealed record AppliedState(long Index, long Term);
    private readonly DBreezeEngine _engine;
    private readonly string _nodeId;
    private readonly Action<string> _trace;
    private readonly ConcurrentDictionary<string, string> _values = new(StringComparer.Ordinal);
    private AppliedState _applied = new(0, 0);
    public long LastApplied => Volatile.Read(ref _applied).Index;
    public AppliedCheckpoint Checkpoint
    {
        get { var value = Volatile.Read(ref _applied); return new(value.Index, value.Term); }
    }
    public int Count => _values.Count;
    public string? Get(string key) => _values.GetValueOrDefault(key);

    public KeyValueMachine(DBreezeEngine engine, string nodeId, Action<string> trace)
    {
        _engine = engine;
        _nodeId = nodeId;
        _trace = trace;
        using var transaction = engine.GetTransaction();
        var checkpoint = transaction.Select<string, long>("demo/checkpoint", "lastApplied");
        var term = transaction.Select<string, long>("demo/checkpoint", "lastAppliedTerm");
        if (checkpoint.Exists && checkpoint.Value > 0 && !term.Exists)
            throw new InvalidDataException("Index-only demo checkpoint requires offline checkpoint-term migration.");
        _applied = new(checkpoint.Exists ? checkpoint.Value : 0, term.Exists ? term.Value : 0);
        foreach (var row in transaction.SelectForward<string, string>("demo/values")) _values[row.Key] = row.Value;
    }

    public ValueTask ApplyAsync(LogEntry entry, CancellationToken cancellationToken) => ApplyBatchAsync(new[] { entry }, cancellationToken);

    public ValueTask ApplyBatchAsync(IReadOnlyList<LogEntry> entries, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var commands = new SetCommand?[entries.Count];
        for (var i = 0; i < entries.Count; i++)
        {
            var entry = entries[i];
            if (entry.Index != LastApplied + i + 1) throw new InvalidDataException("Application received a non-contiguous index.");
            var command = entry.IsNoOp ? null : JsonSerializer.Deserialize(entry.Command.Span, DemoJsonContext.Default.SetCommand);
            if (!entry.IsNoOp && (command is null || string.IsNullOrEmpty(command.RequestId) || string.IsNullOrEmpty(command.Key)))
                throw new InvalidDataException("Invalid set command.");
            commands[i] = command;
        }
        using (var transaction = _engine.GetTransaction())
        {
            transaction.SynchronizeTables("demo/checkpoint", "demo/values", "demo/requests");
            for (var i = 0; i < entries.Count; i++)
            {
                if (commands[i] is not { } command) continue;
                if (!transaction.Select<string, long>("demo/requests", command.RequestId).Exists)
                {
                    transaction.Insert<string, string>("demo/values", command.Key, command.Value);
                    transaction.Insert<string, long>("demo/requests", command.RequestId, entries[i].Index);
                }
                else commands[i] = null;
            }
            transaction.Insert<string, long>("demo/checkpoint", "lastApplied", entries[^1].Index);
            transaction.Insert<string, long>("demo/checkpoint", "lastAppliedTerm", entries[^1].Term);
            transaction.Commit();
        }
        for (var i = 0; i < commands.Length; i++)
        {
            if (commands[i] is not { } command) continue;
            _values[command.Key] = command.Value;
            _trace($"APPLY {_nodeId} index={entries[i].Index} term={entries[i].Term}: {command.Key}={command.Value}");
        }
        Volatile.Write(ref _applied, new(entries[^1].Index, entries[^1].Term));
        return ValueTask.CompletedTask;
    }

    public static byte[] Encode(string key, string value) => JsonSerializer.SerializeToUtf8Bytes(
        new SetCommand(Guid.NewGuid().ToString("N"), key, value), DemoJsonContext.Default.SetCommand);
}
