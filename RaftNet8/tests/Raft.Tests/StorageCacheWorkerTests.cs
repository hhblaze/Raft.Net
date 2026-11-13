using System.Collections.Immutable;
using Raft.Storage;

namespace Raft.Tests;

public sealed class StorageCacheWorkerTests
{
    [Theory]
    [InlineData(DBreezeLogLayout.SeparateTablesV2)]
    [InlineData(DBreezeLogLayout.UnifiedTableV3)]
    public async Task Published_cache_reads_complete_inline_but_never_block_on_or_observe_an_unpublished_write(DBreezeLogLayout layout)
    {
        var options = new RaftOptions { NodeId = "n1", Members = ["n1"] };
        using var entered = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        var armed = false;
        var store = DBreezeLogStorage.CreateInMemory(options, layout, point =>
        {
            if (armed && point == PersistenceBoundary.BeforeCommit)
            { entered.Set(); release.Wait(); }
        });
        await using var adapter = new RaftStorageAdapter(store);
        await adapter.WriteAsync(new LogWrite.Batch([new LogWrite.HardStateUpdate(new(1)),
            new LogWrite.Append([new LogEntry(1, 1, "published"u8)]), new LogWrite.HardStateUpdate(new(1, CommitIndex: 1))]), default);
        var transactions = store.Metrics.ReadTransactions;
        var cached = adapter.ReadAsync(new LogQuery.Replication(0, 8, 4096), default);
        Assert.True(cached.IsCompletedSuccessfully);
        Assert.Equal("published"u8.ToArray(), (await cached).Entries.Single().Command.ToArray());
        Assert.Equal(transactions, store.Metrics.ReadTransactions);
        armed = true;
        var write = adapter.WriteAsync(new LogWrite.Append([new LogEntry(2, 1, "pending"u8)]), default).AsTask();
        ValueTask<LogReadResult> pendingRead = default;
        try
        {
            Assert.True(entered.Wait(TimeSpan.FromSeconds(5)));
            var published = adapter.ReadAsync(new LogQuery.Entries(1, 1, 4096), default);
            Assert.True(published.IsCompletedSuccessfully, "A published committed entry must remain readable while a later append waits for disk.");
            Assert.Equal("published"u8.ToArray(), (await published).Entries.Single().Command.ToArray());
            // Return the ValueTask itself, without awaiting it. Blocking on the cache lock
            // would deadlock a consensus owner behind the paused durable write.
            var admitted = Task.Run(() => { pendingRead = adapter.ReadAsync(new LogQuery.Entries(2, 1, 4096), default); });
            await admitted.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.False(pendingRead.IsCompleted);
        }
        finally { release.Set(); }
        await write.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("pending"u8.ToArray(), (await pendingRead).Entries.Single().Command.ToArray());
    }

    [Theory]
    [InlineData(DBreezeLogLayout.SeparateTablesV2, false)]
    [InlineData(DBreezeLogLayout.UnifiedTableV3, false)]
    [InlineData(DBreezeLogLayout.SeparateTablesV2, true)]
    [InlineData(DBreezeLogLayout.UnifiedTableV3, true)]
    public async Task Suffix_and_compaction_publish_cache_and_metadata_together_after_commit(DBreezeLogLayout layout, bool compact)
    {
        var options = new RaftOptions { NodeId = "n1", Members = ["n1"] };
        using var entered = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        var armed = false;
        var store = DBreezeLogStorage.CreateInMemory(options, layout, point =>
        { if (armed && point == PersistenceBoundary.BeforeCommit) { entered.Set(); release.Wait(); } });
        await using var adapter = new RaftStorageAdapter(store);
        await adapter.WriteAsync(new LogWrite.Batch([new LogWrite.HardStateUpdate(new(1)),
            new LogWrite.Append([new LogEntry(1, 1, "one"u8), new LogEntry(2, 1, "old"u8)]),
            new LogWrite.HardStateUpdate(new(1, CommitIndex: 1))]), default);
        armed = true;
        LogWrite mutation = compact
            ? new LogWrite.PublishSnapshot(new(1, 1, ["n1"], [], "image1", new byte[32].ToImmutableArray())) { RetainEntries = 0 }
            : new LogWrite.Batch([new LogWrite.HardStateUpdate(new(2, CommitIndex: 1)),
                new LogWrite.ReplaceSuffix(2, [new LogEntry(2, 2, "new"u8)])]);
        var write = adapter.WriteAsync(mutation, default).AsTask();
        try
        {
            Assert.True(entered.Wait(TimeSpan.FromSeconds(5)));
            var prior = adapter.ReadAsync(new LogQuery.Replication(0, 8, 4096), default);
            Assert.True(prior.IsCompletedSuccessfully);
            var result = await prior;
            Assert.Equal(new long[] { 1, 1 }, result.Entries.Select(e => e.Term));
            Assert.Equal("old"u8.ToArray(), result.Entries[1].Command.ToArray());
            var snapshot = adapter.ReadAsync(new LogQuery.Snapshot(), default);
            Assert.True(snapshot.IsCompletedSuccessfully);
            Assert.Null((await snapshot).Snapshot);
        }
        finally { release.Set(); }
        await write.WaitAsync(TimeSpan.FromSeconds(5));
        if (compact)
        {
            Assert.Equal("image1", (await adapter.ReadAsync(new LogQuery.Snapshot(), default)).Snapshot!.StorageId);
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => adapter.ReadAsync(new LogQuery.Entries(1, 1, 4096), default).AsTask());
        }
        else
        {
            var current = await adapter.ReadAsync(new LogQuery.Entries(2, 1, 4096), default);
            Assert.Equal(2, current.Entries.Single().Term);
            Assert.Equal("new"u8.ToArray(), current.Entries.Single().Command.ToArray());
        }
    }

    [Theory]
    [InlineData(DBreezeLogLayout.SeparateTablesV2)]
    [InlineData(DBreezeLogLayout.UnifiedTableV3)]
    public async Task Concurrent_cached_replication_never_mixes_two_suffix_generations(DBreezeLogLayout layout)
    {
        var options = new RaftOptions { NodeId = "n1", Members = ["n1"] };
        await using var adapter = new RaftStorageAdapter(DBreezeLogStorage.CreateInMemory(options, layout));
        await adapter.WriteAsync(new LogWrite.Batch([new LogWrite.HardStateUpdate(new(1)),
            new LogWrite.Append([new LogEntry(1, 1, []), new LogEntry(2, 1, []), new LogEntry(3, 1, [])]),
            new LogWrite.HardStateUpdate(new(1, CommitIndex: 1))]), default);
        var writer = Task.Run(async () =>
        {
            for (var term = 2; term <= 80; term++)
                await adapter.WriteAsync(new LogWrite.Batch([new LogWrite.HardStateUpdate(new(term, CommitIndex: 1)),
                    new LogWrite.ReplaceSuffix(2, [new LogEntry(2, term, []), new LogEntry(3, term, [])])]), default);
        });
        for (var i = 0; i < 1000; i++)
        {
            var read = await adapter.ReadAsync(new LogQuery.Replication(2, 1, 4096), default);
            Assert.Equal(read.PreviousTerm, read.Entries.Single().Term);
            if (i % 32 == 0) await Task.Yield();
        }
        await writer.WaitAsync(TimeSpan.FromSeconds(10));
    }

    [Theory]
    [InlineData(DBreezeLogLayout.SeparateTablesV2)]
    [InlineData(DBreezeLogLayout.UnifiedTableV3)]
    public async Task Cache_never_masks_a_poisoned_store_and_observes_cancellation_and_disposal(DBreezeLogLayout layout)
    {
        var options = new RaftOptions { NodeId = "n1", Members = ["n1"] };
        var armed = false;
        var failure = new IOException("uncertain flush");
        var store = DBreezeLogStorage.CreateInMemory(options, layout, point =>
        { if (armed && point == PersistenceBoundary.AfterCommit) throw failure; });
        var adapter = new RaftStorageAdapter(store);
        try
        {
            await adapter.WriteAsync(new LogWrite.Batch([new LogWrite.HardStateUpdate(new(1)),
                new LogWrite.Append([new LogEntry(1, 1, "cached"u8)])]), default);
            using var cancel = new CancellationTokenSource();
            cancel.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => adapter.ReadAsync(new LogQuery.Term(1), cancel.Token).AsTask());
            armed = true;
            Assert.Same(failure, await Assert.ThrowsAsync<IOException>(() => adapter.WriteAsync(new LogWrite.HardStateUpdate(new(1, CommitIndex: 1)), default).AsTask()));
            var poisoned = await Assert.ThrowsAsync<InvalidOperationException>(() => adapter.ReadAsync(new LogQuery.Entries(1, 1, 4096), default).AsTask());
            Assert.Same(failure, poisoned.InnerException);
        }
        finally { await adapter.DisposeAsync(); }
        await Assert.ThrowsAsync<ObjectDisposedException>(() => adapter.ReadAsync(new LogQuery.Term(1), default).AsTask());
    }
}
