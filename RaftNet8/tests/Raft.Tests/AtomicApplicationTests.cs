using DBreeze;
using DBreeze.Transactions;
using Raft.Storage;

namespace Raft.Tests;

public sealed class AtomicApplicationTests
{
    private static DBreezeEngine Memory() => new(new DBreezeConfiguration { Storage = DBreezeConfiguration.eStorage.MEMORY });
    private static void Increment(Transaction tx, LogEntry entry)
    {
        var row = tx.Select<string, long>("values", "counter");
        tx.Insert<string, long>("values", "counter", (row.Exists ? row.Value : 0) + 1);
    }
    private static DBreezeStateMachine Create(DBreezeEngine engine, Action<ApplicationPersistenceBoundary>? observe = null) =>
        new(engine, "checkpoint", ["values"], Increment, observe: observe);

    [Fact]
    public async Task Effects_and_checkpoint_index_term_include_noops_and_configuration_entries()
    {
        using var engine = Memory();
        var machine = Create(engine);
        await machine.ApplyBatchAsync([
            new LogEntry(1, 1, "increment"u8),
            new LogEntry(2, 1, ReadOnlySpan<byte>.Empty, true),
            new LogEntry(3, 2, new ClusterConfiguration(["n1"], []).Encode().AsSpan(), LogEntryKind.Configuration)
        ], default);
        Assert.Equal(new AppliedCheckpoint(3, 2), machine.Checkpoint);
        var recovered = Create(engine);
        Assert.Equal(machine.Checkpoint, recovered.Checkpoint);
        using var tx = engine.GetTransaction();
        Assert.Equal(1, tx.Select<string, long>("values", "counter").Value);
        Assert.Equal(new AppliedCheckpoint(3, 2), DBreezeStateMachine.DecodeCheckpoint(tx.Select<string, byte[]>("checkpoint", "applied").Value));
    }

    [Theory]
    [InlineData(ApplicationPersistenceBoundary.AfterCommands, false)]
    [InlineData(ApplicationPersistenceBoundary.BeforeCommit, false)]
    [InlineData(ApplicationPersistenceBoundary.AfterCommit, true)]
    public async Task An_uncertain_failure_requires_recovery_and_does_not_repeat_committed_effects(ApplicationPersistenceBoundary boundary, bool committed)
    {
        using var engine = Memory();
        var expected = new ArgumentException("application storage failure");
        var machine = Create(engine, point => { if (point == boundary) throw expected; });
        Assert.Same(expected, await Assert.ThrowsAsync<ArgumentException>(() => machine.ApplyAsync(new LogEntry(1, 5, "increment"u8), default).AsTask()));
        await Assert.ThrowsAsync<InvalidOperationException>(() => machine.ApplyAsync(new LogEntry(1, 5, "retry"u8), default).AsTask());
        var recovered = Create(engine);
        Assert.Equal(new AppliedCheckpoint(committed ? 1 : 0, committed ? 5 : 0), recovered.Checkpoint);
        if (!committed) await recovered.ApplyAsync(new LogEntry(1, 5, "increment"u8), default);
        using var tx = engine.GetTransaction();
        Assert.Equal(1, tx.Select<string, long>("values", "counter").Value);
    }

    private sealed class NoTransport : IRaftTransport
    {
        public ValueTask<RaftRpc> SendAsync(string peerId, RaftRpc request, CancellationToken cancellationToken) =>
            throw new IOException("unreachable");
    }

    [Fact]
    public async Task Node_rejects_an_application_checkpoint_with_the_wrong_term()
    {
        using var engine = Memory();
        var options = new RaftOptions { NodeId = "n1", Members = ["n1"] };
        using var log = DBreezeRaftStorage.CreateInMemory(options);
        log.SaveHardState(new(2));
        log.Append([new LogEntry(1, 1, "increment"u8)]);
        log.SaveHardState(new(2, null, 1));
        var machine = Create(engine);
        await machine.ApplyAsync(new LogEntry(1, 2, "different-history"u8), default);
        Assert.Throws<InvalidDataException>(() => new RaftNode(options, log, new NoTransport(), machine));
    }
}
