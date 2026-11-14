using Raft.Storage;

namespace Raft.Tests;

public sealed class StorageProgressTests
{
    [Theory]
    [InlineData(DBreezeLogLayout.SeparateTablesV2)]
    [InlineData(DBreezeLogLayout.UnifiedTableV3)]
    [InlineData(DBreezeLogLayout.UnifiedTableV4)]
    public void Cached_frontier_matches_durable_suffix_after_commit_rollback_and_reopen(DBreezeLogLayout layout)
    {
        var options = new RaftOptions { NodeId = "n1", Members = ["n1"],
            WireVersion = layout == DBreezeLogLayout.UnifiedTableV4 ? 3 : 2,
            InitialPeers = [new("n1", "127.0.0.1", 7001)] };
        using var engine = new DBreeze.DBreezeEngine(new DBreeze.DBreezeConfiguration { Storage = DBreeze.DBreezeConfiguration.eStorage.MEMORY });
        using (var store = new DBreezeLogStorage(engine, options, layout))
        {
            store.Write(new LogWrite.Batch([new LogWrite.HardStateUpdate(new(1)),
                new LogWrite.Append([new(1, 1, new byte[17]), new(2, 1, new byte[33])])]));
            Assert.Equal(92, store.UncommittedBytes);
            store.SaveHardState(new(1, CommitIndex: 1));
            Assert.Equal(54, store.UncommittedBytes);
            store.ReplaceSuffix(2, [new(2, 1, new byte[7]), new(3, 1, new byte[11])]);
            Assert.Equal(60, store.UncommittedBytes);
            Assert.Equal(store.Recover().UncommittedBytes, ((ILogStoreProgress)store).UncommittedBytes);
            store.SaveHardState(new(1, CommitIndex: 3));
            Assert.Equal(0, store.UncommittedBytes);
        }
        using var reopened = new DBreezeLogStorage(engine, options, layout);
        Assert.Equal(0, reopened.UncommittedBytes);
        Assert.Equal(3, reopened.LastIndex);
    }

    [Fact]
    public void Repeated_cached_frontier_reads_allocate_no_recovery_or_membership_objects()
    {
        var options = new RaftOptions { NodeId = "n1", Members = ["n1"], WireVersion = 3,
            InitialPeers = [new("n1", "127.0.0.1", 7001)] };
        using var store = DBreezeLogStorage.CreateInMemory(options, DBreezeLogLayout.UnifiedTableV4);
        store.Write(new LogWrite.Batch([new LogWrite.HardStateUpdate(new(1)), new LogWrite.Append([new(1, 1, new byte[17])])]));
        long sum = 0;
        for (var i = 0; i < 1000; i++) sum += store.UncommittedBytes;
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 10000; i++) sum += RaftStorageAdapter.Uncommitted(store);
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.Equal(418000, sum);
        Assert.Equal(0, allocated);
    }
}
