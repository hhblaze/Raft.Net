using System.Collections.Immutable;
using DBreeze;
using Raft.Storage;

namespace Raft.Tests;

public sealed class IntegrityStorageTests
{
    private static RaftOptions Options => new() { NodeId = "n1", Members = ["n1", "n2", "n3"] };
    private static DBreezeEngine MemoryEngine() => new(new DBreezeConfiguration { Storage = DBreezeConfiguration.eStorage.MEMORY });

    [Fact]
    public void Castagnoli_vectors_hardware_portable_and_streaming_agree()
    {
        Assert.Equal(0u, Crc32C.Compute([]));
        Assert.Equal(0xE3069283u, Crc32C.Compute("123456789"u8));
        var random = new Random(20261112);
        for (var length = 0; length < 1024; length += 7)
        {
            var bytes = new byte[length];
            random.NextBytes(bytes);
            Assert.Equal(~Crc32C.UpdatePortable(uint.MaxValue, bytes), Crc32C.Compute(bytes));
            var split = length / 2;
            Assert.Equal(Crc32C.Compute(bytes), ~Crc32C.Update(Crc32C.Update(uint.MaxValue, bytes.AsSpan(0, split)), bytes.AsSpan(split)));
        }
    }

    [Fact]
    public void Every_record_byte_is_protected_and_key_swap_is_detected()
    {
        var bytes = StorageCodecV2.Entry(new LogEntry(5, 3, "payload"u8));
        for (var i = 0; i < bytes.Length; i++)
        {
            bytes[i] ^= 1;
            Assert.Throws<InvalidDataException>(() => StorageCodecV2.Entry(5, bytes, 1024));
            bytes[i] ^= 1;
        }
        Assert.Throws<InvalidDataException>(() => StorageCodecV2.Entry(6, bytes, 1024));
    }

    [Fact]
    public void Hard_state_entries_and_frontier_share_one_commit_and_recover()
    {
        using var engine = MemoryEngine();
        using (var store = new DBreezeLogStorage(engine, Options))
        {
            var before = store.Metrics.WriteTransactions;
            store.Write(new LogWrite.Batch([
                new LogWrite.HardStateUpdate(new(5, "n2")),
                new LogWrite.Append([new LogEntry(1, 1, "a"u8), new LogEntry(2, 3, "bb"u8), new LogEntry(3, 5, "ccc"u8)]),
                new LogWrite.HardStateUpdate(new(5, "n2", 2))
            ]));
            Assert.Equal(before + 1, store.Metrics.WriteTransactions);
            Assert.Equal(24, store.Recover().UncommittedBytes);
        }
        using var reopened = new DBreezeLogStorage(engine, Options);
        Assert.Equal(new HardState(5, "n2", 2), reopened.HardState);
        Assert.Equal(3, reopened.LastIndex);
        Assert.Equal("ccc"u8.ToArray(), reopened.Read(3, 1, 1024)[0].Command.ToArray());
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public void Corrupt_promised_records_fail_even_if_locally_uncommitted(int index)
    {
        using var engine = MemoryEngine();
        Seed(engine);
        using (var tx = engine.GetTransaction())
        {
            tx.SynchronizeTables(DBreezeLogStorage.LogTable);
            var bytes = tx.Select<long, byte[]>(DBreezeLogStorage.LogTable, index).Value;
            bytes[^1] ^= 0x40;
            tx.Insert<long, byte[]>(DBreezeLogStorage.LogTable, index, bytes);
            tx.Commit();
        }
        Assert.Throws<InvalidDataException>(() => new DBreezeLogStorage(engine, Options));
    }

    [Fact]
    public void Missing_interior_record_fails_during_startup()
    {
        using var engine = MemoryEngine();
        Seed(engine);
        using (var tx = engine.GetTransaction()) { tx.RemoveKey<long>(DBreezeLogStorage.LogTable, 2); tx.Commit(); }
        Assert.Throws<InvalidDataException>(() => new DBreezeLogStorage(engine, Options));
    }

    [Fact]
    public void Only_tail_beyond_the_durable_frontier_is_salvaged()
    {
        using var engine = MemoryEngine();
        Seed(engine);
        using (var tx = engine.GetTransaction())
        {
            tx.SynchronizeTables(DBreezeLogStorage.LogTable, DBreezeLogStorage.ConfigurationTable);
            tx.Insert<long, byte[]>(DBreezeLogStorage.LogTable, 4, [1, 2, 3]);
            tx.Insert<long, byte[]>(DBreezeLogStorage.ConfigurationTable, 4, [9]);
            tx.Commit();
        }
        using var recovered = new DBreezeLogStorage(engine, Options);
        Assert.Equal(3, recovered.LastIndex);
        Assert.Equal(2, recovered.HardState.CommitIndex);
        using var check = engine.GetTransaction();
        Assert.False(check.Select<long, byte[]>(DBreezeLogStorage.LogTable, 4).Exists);
        Assert.False(check.Select<long, byte[]>(DBreezeLogStorage.ConfigurationTable, 4).Exists);
    }

    [Fact]
    public void Active_configuration_is_recovered_and_rolls_back_with_a_conflicting_suffix()
    {
        using var engine = MemoryEngine();
        var config = new ClusterConfiguration(["n1", "n2", "n3"], ["n4"]);
        using (var store = new DBreezeLogStorage(engine, Options))
        {
            store.SaveHardState(new(3));
            store.Append([new LogEntry(1, 1, "safe"u8), new LogEntry(2, 3, config.Encode().AsSpan(), LogEntryKind.Configuration)]);
            store.SaveHardState(new(3, null, 1));
            Assert.Equal(new[] { "n4" }, store.Recover().Configuration!.Learners.ToArray());
        }
        using (var reopened = new DBreezeLogStorage(engine, Options))
        {
            Assert.Equal(2, reopened.Recover().Configuration!.Index);
            reopened.ReplaceSuffix(2, [new LogEntry(2, 3, "replacement"u8)]);
            Assert.Empty(reopened.Recover().Configuration!.Learners);
        }
        using var final = new DBreezeLogStorage(engine, Options);
        Assert.Empty(final.Recover().Configuration!.Learners);
    }

    [Theory]
    [InlineData(DBreezeLogLayout.SeparateTablesV2)]
    [InlineData(DBreezeLogLayout.UnifiedTableV3)]
    public void Snapshot_boundary_retains_a_tail_and_survives_reopen(DBreezeLogLayout layout)
    {
        using var engine = MemoryEngine();
        using (var store = new DBreezeLogStorage(engine, Options, layout))
        {
            store.SaveHardState(new(3));
            store.Append(Enumerable.Range(1, 5).Select(i => new LogEntry(i, 2, "x"u8)).ToArray());
            store.SaveHardState(new(3, null, 4));
            store.Write(new LogWrite.PublishSnapshot(new(4, 2, ["n1", "n2", "n3"], [], "snapshot_4", new byte[32].ToImmutableArray()))
                { RetainEntries = 2 });
            Assert.Equal(3, store.Recover().FirstIndex);
        }
        using var reopened = new DBreezeLogStorage(engine, Options, layout);
        Assert.Equal(4, reopened.Recover().Snapshot!.LastIncludedIndex);
        Assert.Equal(2, reopened.GetTerm(4));
        Assert.Equal(new long[] { 3, 4, 5 }, reopened.Read(3, 16, 1024).Select(e => e.Index));
        Assert.Throws<ArgumentOutOfRangeException>(() => reopened.Read(1, 1, 1024));
    }

    [Fact]
    public void V1_data_requires_copy_migration_instead_of_silent_empty_v2_initialization()
    {
        using var engine = MemoryEngine();
        using (var original = new DBreezeRaftStorage(engine, Options)) original.SaveHardState(new(7, "n2"));
        Assert.Throws<InvalidDataException>(() => new DBreezeLogStorage(engine, Options));
    }

    [Fact]
    public void Persisted_identity_cannot_be_bypassed_by_a_different_runtime_configuration()
    {
        using var storage = DBreezeRaftStorage.CreateInMemory(Options);
        Assert.Throws<InvalidDataException>(() => new RaftCore(Options with { GroupId = "wrong" }, storage));
        Assert.Throws<InvalidDataException>(() => new RaftCore(Options with { Members = ["n1", "n4", "n5"] }, storage));
    }

    [Fact]
    public void V2_recovered_configuration_is_authoritative_over_changed_bootstrap_options()
    {
        using var engine = MemoryEngine();
        using (var initial = new DBreezeLogStorage(engine, Options)) initial.SaveHardState(new(1));
        using var recovered = new DBreezeLogStorage(engine, Options with { Members = ["n1", "n4", "n5"] });
        Assert.Equal(Options.Members, recovered.Recover().Configuration!.Voters);
    }

    private static void Seed(DBreezeEngine engine)
    {
        using var store = new DBreezeLogStorage(engine, Options);
        store.SaveHardState(new(3));
        store.Append(Enumerable.Range(1, 3).Select(i => new LogEntry(i, i, "entry"u8)).ToArray());
        store.SaveHardState(new(3, null, 2));
    }
}
