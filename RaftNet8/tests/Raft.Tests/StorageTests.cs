using Raft.Core;
using Raft.Storage;

namespace Raft.Tests;

public sealed class StorageTests
{
    private static RaftOptions Options => new() { NodeId = "n1", Members = new[] { "n1", "n2", "n3" } };

    [Fact]
    public void Disk_restart_preserves_term_and_vote_without_any_log_entry()
    {
        using var directory = new TestDirectory();
        using (var store = new DBreezeRaftStorage(directory.Path, Options))
        {
            var node = new RaftCore(Options, store);
            Assert.True(((VoteResponse)node.HandleRequest("n2", new VoteRequest(42, 0, 0))).Granted);
        }
        using (var store = new DBreezeRaftStorage(directory.Path, Options))
        {
            var node = new RaftCore(Options, store);
            Assert.Equal(42, node.Term);
            Assert.Equal(0, node.LastIndex);
            Assert.False(((VoteResponse)node.HandleRequest("n3", new VoteRequest(42, 0, 0))).Granted);
            Assert.True(((VoteResponse)node.HandleRequest("n2", new VoteRequest(42, 0, 0))).Granted);
        }
    }

    [Fact]
    public void Observing_a_higher_term_is_durable_even_when_vote_is_denied()
    {
        using var directory = new TestDirectory();
        using (var store = new DBreezeRaftStorage(directory.Path, Options))
        {
            store.SaveHardState(new HardState(2));
            store.Append(new[] { new LogEntry(1, 2, "saved"u8) });
            var node = new RaftCore(Options, store);
            Assert.False(((VoteResponse)node.HandleRequest("n2", new VoteRequest(50, 0, 0))).Granted);
        }
        using var reopened = new DBreezeRaftStorage(directory.Path, Options);
        Assert.Equal(new HardState(50), reopened.HardState);
        Assert.Equal(2, reopened.LastTerm);
    }

    [Fact]
    public void Atomic_suffix_replacement_survives_reopen_and_does_not_retain_old_cache_entries()
    {
        using var directory = new TestDirectory();
        using (var store = new DBreezeRaftStorage(directory.Path, Options))
        {
            store.SaveHardState(new HardState(3));
            store.Append(Enumerable.Range(1, 20).Select(i => new LogEntry(i, 1, BitConverter.GetBytes(i).AsSpan())).ToArray());
            store.SaveHardState(new HardState(3, null, 5));
            store.ReplaceSuffix(6, new[] { new LogEntry(6, 3, "new"u8), new LogEntry(7, 3, "tail"u8) });
            Assert.Equal(7, store.LastIndex);
            Assert.Equal(3, store.GetTerm(6));
        }
        using var reopened = new DBreezeRaftStorage(directory.Path, Options);
        Assert.Equal(7, reopened.LastIndex);
        Assert.Equal(5, reopened.HardState.CommitIndex);
        Assert.Equal(new long[] { 1, 1, 1, 1, 1, 3, 3 }, reopened.Read(1, 100, 1024).Select(e => e.Term));
    }

    [Fact]
    public void Rejected_mutations_leave_persistent_state_unchanged()
    {
        using var store = DBreezeRaftStorage.CreateInMemory(Options);
        store.SaveHardState(new HardState(2, "n1"));
        store.Append(new[] { new LogEntry(1, 1, "a"u8), new LogEntry(2, 2, "b"u8) });
        store.SaveHardState(new HardState(2, "n1", 1));
        Assert.Throws<InvalidOperationException>(() => store.ReplaceSuffix(1, Array.Empty<LogEntry>()));
        Assert.Throws<InvalidOperationException>(() => store.SaveHardState(new HardState(1, null, 1)));
        Assert.Throws<InvalidOperationException>(() => store.SaveHardState(new HardState(2, "n2", 1)));
        Assert.Throws<InvalidOperationException>(() => store.Append(new[] { new LogEntry(4, 2, "gap"u8) }));
        Assert.Throws<InvalidOperationException>(() => store.ReplaceSuffix(2, new[] { new LogEntry(2, 2, "x"u8), new LogEntry(4, 2, "gap"u8) }));
        Assert.Equal(2, store.LastIndex);
        Assert.Equal("b"u8.ToArray(), store.Read(2, 1, 1024)[0].Command.ToArray());
    }

    [Fact]
    public void Membership_or_group_identity_cannot_be_changed_on_existing_tables()
    {
        using var directory = new TestDirectory();
        using (var store = new DBreezeRaftStorage(directory.Path, Options)) { }
        var differentMembers = Options with { Members = new[] { "n1", "n2", "n4" } };
        Assert.Throws<InvalidDataException>(() => new DBreezeRaftStorage(directory.Path, differentMembers));
        Assert.Throws<InvalidDataException>(() => new DBreezeRaftStorage(directory.Path, Options with { GroupId = "different" }));
        Assert.Throws<InvalidDataException>(() => new DBreezeRaftStorage(directory.Path, Options with { NodeId = "n2" }));
        using var reopened = new DBreezeRaftStorage(directory.Path, Options);
        Assert.Equal(0, reopened.LastIndex);
    }

    [Fact]
    public void Cache_is_bounded_by_both_count_and_bytes_and_can_be_disabled()
    {
        var options = Options with { LogCacheEntries = 3, LogCacheBytes = 150 };
        using var store = DBreezeRaftStorage.CreateInMemory(options);
        store.SaveHardState(new HardState(1));
        store.Append(Enumerable.Range(1, 100).Select(i => new LogEntry(i, 1, new byte[40].AsSpan())).ToArray());
        Assert.InRange(store.Metrics.CachedEntries, 0, 2);
        Assert.InRange(store.Metrics.CachedBytes, 0, 150);
        var result = store.Read(1, 100, 10000);
        Assert.Equal(100, result.Count);
        Assert.InRange(store.Metrics.CachedBytes, 0, 150);
        using var uncached = DBreezeRaftStorage.CreateInMemory(Options with { LogCacheEntries = 0 });
        uncached.SaveHardState(new HardState(1));
        uncached.Append(new[] { new LogEntry(1, 1, "a"u8) });
        Assert.Equal(0, uncached.Metrics.CachedEntries);
    }

    [Fact]
    public void Bounded_reads_do_not_scan_the_log_prefix_and_term_hints_are_correct()
    {
        using var store = DBreezeRaftStorage.CreateInMemory(Options with { LogCacheEntries = 0 });
        store.SaveHardState(new HardState(3));
        store.Append(Enumerable.Range(1, 100).Select(i => new LogEntry(i, i <= 60 ? 1 : 3, new byte[10].AsSpan())).ToArray());
        var reads = store.Metrics.ReadTransactions;
        Assert.Equal(new long[] { 90, 91 }, store.Read(90, 20, 62).Select(e => e.Index));
        Assert.Equal(reads + 1, store.Metrics.ReadTransactions);
        Assert.Equal(61, store.FirstIndexOfTerm(99));
        Assert.Equal(60, store.LastIndexOfTerm(1));
        Assert.Equal(0, store.LastIndexOfTerm(2));
    }

    [Fact]
    public void Unchanged_heartbeats_do_not_write_or_reread_the_cached_tail()
    {
        using var store = DBreezeRaftStorage.CreateInMemory(Options);
        var node = new RaftCore(Options, store);
        node.HandleRequest("n2", new AppendRequest(1, 1, 0, 0, 1, new[] { new LogEntry(1, 1, "a"u8) }));
        var before = store.Metrics;
        for (var i = 0; i < 1000; i++) node.HandleRequest("n2", new AppendRequest(1, i + 2, 1, 1, 1, Array.Empty<LogEntry>()));
        Assert.Equal(before.WriteTransactions, store.Metrics.WriteTransactions);
        Assert.Equal(before.ReadTransactions, store.Metrics.ReadTransactions);
    }

    [Fact]
    public void Caller_memory_is_copied_when_constructing_a_public_log_entry()
    {
        var data = new byte[] { 1, 2, 3 };
        var entry = new LogEntry(1, 1, data.AsSpan());
        data[0] = 99;
        Assert.Equal(1, entry.Command.Span[0]);
    }
}
