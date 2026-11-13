using System.Collections.Immutable;
using Raft.Storage;

namespace Raft.Tests;

public sealed class CacheQueryTests
{
    [Fact]
    public void Cached_term_ranges_use_no_database_reads_and_stay_correct_through_replace_compact_reopen()
    {
        var options = new RaftOptions { NodeId = "n1", Members = ["n1"] };
        using var engine = new DBreeze.DBreezeEngine(new DBreeze.DBreezeConfiguration { Storage = DBreeze.DBreezeConfiguration.eStorage.MEMORY });
        var store = new DBreezeLogStorage(engine, options);
        try
        {
            store.SaveHardState(new(2));
            store.Append(Enumerable.Range(1, 100).Select(i => new LogEntry(i, 1, "one"u8)).ToArray());
            var reads = store.Metrics.ReadTransactions;
            Assert.All(store.Query(new LogQuery.Terms(15, 50)).Terms, term => Assert.Equal(1, term));
            Assert.Equal(reads, store.Metrics.ReadTransactions);
            store.ReplaceSuffix(20, Enumerable.Range(20, 81).Select(i => new LogEntry(i, 2, "two"u8)).ToArray());
            reads = store.Metrics.ReadTransactions;
            Assert.Equal(Enumerable.Range(15, 50).Select(i => i < 20 ? 1L : 2L), store.Query(new LogQuery.Terms(15, 50)).Terms);
            Assert.Equal(reads, store.Metrics.ReadTransactions);
            store.SaveHardState(new(2, CommitIndex: 90));
            var snapshot = new SnapshotMetadata(80, 2, ["n1"], [], "image80", new byte[32].ToImmutableArray());
            store.Write(new LogWrite.PublishSnapshot(snapshot) { RetainEntries = 8 });
            Assert.Throws<ArgumentOutOfRangeException>(() => store.Query(new LogQuery.Terms(70, 10)));
            store.Dispose();
            store = new DBreezeLogStorage(engine, options with { LogCacheEntries = 0 });
            reads = store.Metrics.ReadTransactions;
            Assert.All(store.Query(new LogQuery.Terms(81, 10)).Terms, term => Assert.Equal(2, term));
            Assert.Equal(reads + 1, store.Metrics.ReadTransactions); // all misses share one transaction
            reads = store.Metrics.ReadTransactions;
            Assert.Equal(new[] { 2L }, store.Query(new LogQuery.Terms(80, 1)).Terms.ToArray()); // snapshot boundary is metadata
            Assert.Empty(store.Query(new LogQuery.Terms(101, 0)).Terms);
            Assert.Equal(reads, store.Metrics.ReadTransactions);
        }
        finally { store.Dispose(); }
    }
}
