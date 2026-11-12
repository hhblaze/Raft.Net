using System.Text.Json;
using DBreeze;
using Raft.Demo;

namespace Raft.Tests;

public sealed class DemoStateMachineTests
{
    [Fact]
    public async Task Batching_persists_effects_checkpoint_and_request_deduplication_atomically()
    {
        using var engine = new DBreezeEngine(new DBreezeConfiguration { Storage = DBreezeConfiguration.eStorage.MEMORY });
        var machine = new KeyValueMachine(engine, "n1", _ => { });
        var command = JsonSerializer.SerializeToUtf8Bytes(new SetCommand("same-request", "key", "first"), DemoJsonContext.Default.SetCommand);
        var retry = JsonSerializer.SerializeToUtf8Bytes(new SetCommand("same-request", "key", "duplicate"), DemoJsonContext.Default.SetCommand);
        await machine.ApplyBatchAsync(new[] { new LogEntry(1, 1, command.AsSpan()), new LogEntry(2, 1, retry.AsSpan()),
            new LogEntry(3, 1, Array.Empty<byte>(), true) }, CancellationToken.None);
        Assert.Equal(3, machine.LastApplied);
        Assert.Equal("first", machine.Get("key"));
        var recovered = new KeyValueMachine(engine, "n1", _ => { });
        Assert.Equal(3, recovered.LastApplied);
        Assert.Equal("first", recovered.Get("key"));
        await recovered.ApplyAsync(new LogEntry(4, 2, retry.AsSpan()), CancellationToken.None);
        Assert.Equal("first", recovered.Get("key"));
        Assert.Equal(4, recovered.LastApplied);
    }

    [Fact]
    public async Task An_invalid_command_in_a_batch_cannot_leave_partial_effects_or_checkpoint()
    {
        using var engine = new DBreezeEngine(new DBreezeConfiguration { Storage = DBreezeConfiguration.eStorage.MEMORY });
        var machine = new KeyValueMachine(engine, "n1", _ => { });
        await Assert.ThrowsAsync<JsonException>(() => machine.ApplyBatchAsync(new[]
        {
            new LogEntry(1, 1, KeyValueMachine.Encode("key", "value").AsSpan()), new LogEntry(2, 1, "not-json"u8)
        }, CancellationToken.None).AsTask());
        var recovered = new KeyValueMachine(engine, "n1", _ => { });
        Assert.Equal(0, recovered.LastApplied);
        Assert.Null(recovered.Get("key"));
    }
}
