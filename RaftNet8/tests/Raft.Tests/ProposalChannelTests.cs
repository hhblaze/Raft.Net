using System.Collections.Concurrent;
using Raft.Storage;

namespace Raft.Tests;

public sealed class ProposalChannelTests
{
    private sealed class BoundedBatchMachine : IBatchStateMachine
    {
        private long _applied;
        public long LastApplied => Interlocked.Read(ref _applied);
        public readonly ConcurrentQueue<int> BatchBytes = new();
        public ValueTask ApplyAsync(LogEntry entry, CancellationToken token) => ApplyBatchAsync([entry], token);
        public ValueTask ApplyBatchAsync(IReadOnlyList<LogEntry> entries, CancellationToken token)
        {
            Assert.Equal(LastApplied + 1, entries[0].Index);
            BatchBytes.Enqueue(entries.Sum(e => e.Command.Length + LogEntry.EncodedOverhead));
            Interlocked.Exchange(ref _applied, entries[^1].Index);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class NoPeers : ITransport
    {
        public ValueTask<RaftRpc> SendAsync(string peer, RaftRpc request, CancellationToken token) =>
            throw new InvalidOperationException("Single-voter test has no peers.");
    }

    private sealed class PausedStore(ILogStore inner) : ILogStore, ILogStoreProgress
    {
        internal readonly TaskCompletionSource Entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal readonly TaskCompletionSource Release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal readonly ConcurrentQueue<int> Batches = new();
        internal int Pause;
        public long UncommittedBytes => ((ILogStoreProgress)inner).UncommittedBytes;
        public ValueTask<RecoveredLogState> RecoverAsync(CancellationToken token) => inner.RecoverAsync(token);
        public ValueTask<LogReadResult> ReadAsync(LogQuery query, CancellationToken token) => inner.ReadAsync(query, token);
        public async ValueTask WriteAsync(LogWrite write, CancellationToken token)
        {
            var count = Commands(write);
            if (count > 0)
            {
                Batches.Enqueue(count);
                if (Interlocked.Exchange(ref Pause, 0) != 0)
                {
                    Entered.TrySetResult();
                    await Release.Task.WaitAsync(token);
                }
            }
            await inner.WriteAsync(write, token);
        }
        private static int Commands(LogWrite write) => write switch
        {
            LogWrite.Append a => a.Entries.Count(e => !e.IsNoOp),
            LogWrite.ReplaceSuffix r => r.Entries.Count(e => !e.IsNoOp),
            LogWrite.Batch b => b.Writes.Sum(Commands),
            _ => 0
        };
        public ValueTask DisposeAsync() => inner.DisposeAsync();
    }

    [Theory]
    [InlineData(null, 31, 4, 0)]
    [InlineData(512, 3, 22, 0)]
    [InlineData(null, 31, 4, 2)]
    public async Task Local_durable_group_is_independent_of_replication_and_application_byte_bounds(int? localBytes, int expectedMax, int expectedBatches, int delayMs)
    {
        var options = new RaftOptions { NodeId = "n1", Members = ["n1"], TickInterval = TimeSpan.FromMilliseconds(10),
            ElectionMinTicks = 20, ElectionMaxTicks = 30, RpcTimeout = TimeSpan.FromMilliseconds(5),
            MaxAppendEntries = 128, MaxAppendBytes = 512, MaxCommandBytes = 128,
            MaxProposalBatchBytes = localBytes, ProposalBatchDelay = TimeSpan.FromMilliseconds(delayMs) };
        var store = new PausedStore(new RaftStorageAdapter(DBreezeLogStorage.CreateInMemory(options, DBreezeLogLayout.UnifiedTableV3)));
        var machine = new BoundedBatchMachine();
        await using var node = await RaftNode.CreateAsync(options, store, new NoPeers(), machine);
        await node.StartAsync();
        await RuntimeCluster.EventuallyAsync(() => node.Status.Role == RaftRole.Leader && node.Status.LastApplied == 1);
        Volatile.Write(ref store.Pause, 1);
        var writes = new List<Task<ProposalResult>> { node.ProposeAsync(new byte[128]) };
        await store.Entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        try { for (var i = 1; i < 64; i++) writes.Add(node.ProposeAsync(new byte[128])); }
        finally { store.Release.TrySetResult(); }
        await Task.WhenAll(writes).WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(65, machine.LastApplied);
        Assert.Equal(0, node.Metrics.PendingProposals);
        Assert.Equal(64, store.Batches.Sum());
        Assert.Equal(expectedBatches, store.Batches.Count);
        Assert.Equal(expectedMax, store.Batches.Max());
        Assert.All(machine.BatchBytes, bytes => Assert.InRange(bytes, 21, 512));
    }

    [Theory]
    [InlineData(null, 2, 64)]
    [InlineData(1048576, 3, 63)]
    public async Task Default_group_has_room_for_power_of_two_payloads_and_their_entry_metadata(int? localBytes, int expectedBatches, int expectedMax)
    {
        var options = new RaftOptions { NodeId = "n1", Members = ["n1"], TickInterval = TimeSpan.FromMilliseconds(10),
            ElectionMinTicks = 20, ElectionMaxTicks = 30, RpcTimeout = TimeSpan.FromMilliseconds(5),
            ProposalBatchDelay = TimeSpan.Zero, MaxProposalBatchBytes = localBytes };
        var store = new PausedStore(new RaftStorageAdapter(DBreezeLogStorage.CreateInMemory(options, DBreezeLogLayout.UnifiedTableV3)));
        var machine = new BoundedBatchMachine();
        await using var node = await RaftNode.CreateAsync(options, store, new NoPeers(), machine);
        await node.StartAsync();
        await RuntimeCluster.EventuallyAsync(() => node.Status.Role == RaftRole.Leader && node.Status.LastApplied == 1);
        Volatile.Write(ref store.Pause, 1);
        var writes = new List<Task<ProposalResult>> { node.ProposeAsync(new byte[1]) };
        await store.Entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        try { for (var i = 0; i < 64; i++) writes.Add(node.ProposeAsync(new byte[16 * 1024])); }
        finally { store.Release.TrySetResult(); }
        await Task.WhenAll(writes).WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(expectedBatches, store.Batches.Count);
        Assert.Equal(expectedMax, store.Batches.Max());
        Assert.Equal(65, store.Batches.Sum());
        Assert.Equal(66, machine.LastApplied);
        Assert.All(machine.BatchBytes, bytes => Assert.InRange(bytes, 21, options.MaxAppendBytes));
    }

    [Fact]
    public async Task A_multi_frame_local_group_commits_over_TCP_with_the_original_small_wire_limit()
    {
        await using var cluster = new RuntimeCluster(tcp: true, modern: true, configure: options => options with
        { MaxAppendBytes = 512, MaxCommandBytes = 128, ProposalBatchDelay = TimeSpan.FromMilliseconds(1) });
        await cluster.StartAsync();
        var leader = await cluster.LeaderAsync();
        await Task.WhenAll(Enumerable.Range(0, 128).Select(i => leader.ProposeAsync(Enumerable.Repeat((byte)i, 128).ToArray())))
            .WaitAsync(TimeSpan.FromSeconds(10));
        await RuntimeCluster.EventuallyAsync(() => cluster.Machines.Values.All(m => m.Entries.Count(e => !e.IsNoOp) == 128));
        Assert.All(cluster.Nodes.Values, n => Assert.Null(n.Status.Fault));
        Assert.All(cluster.Machines.Values, m => Assert.Equal(Enumerable.Range(0, 128).Select(i => (byte)i).Order(),
            m.Entries.Where(e => !e.IsNoOp).Select(e => e.Command.Span[0]).Order()));
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(0)]
    [InlineData(128)]
    [InlineData(4097)]
    [InlineData(67108865)]
    public void Invalid_local_group_limits_are_rejected_before_node_admission(int bytes)
    {
        var options = new RaftOptions { NodeId = "n1", Members = ["n1"], MaxCommandBytes = 128,
            MaxAppendBytes = 512, MaxUncommittedBytes = 4096, MaxProposalBatchBytes = bytes };
        Assert.Throws<ArgumentOutOfRangeException>(() => options.Freeze());
    }

    [Fact]
    public async Task Interleaved_controls_do_not_fragment_queued_proposals_or_lose_batch_wakeups()
    {
        var options = new RaftOptions { NodeId = "n1", Members = ["n1"], TickInterval = TimeSpan.FromMilliseconds(1),
            ElectionMinTicks = 20, ElectionMaxTicks = 30, RpcTimeout = TimeSpan.FromMilliseconds(5),
            MaxAppendEntries = 16, ProposalBatchDelay = TimeSpan.Zero };
        var store = new PausedStore(new RaftStorageAdapter(DBreezeLogStorage.CreateInMemory(options)));
        await using var node = await RaftNode.CreateAsync(options, store, new NoPeers(), new TestStateMachine());
        await node.StartAsync();
        await RuntimeCluster.EventuallyAsync(() => node.Status.Role == RaftRole.Leader && node.Status.LastApplied == 1);
        Volatile.Write(ref store.Pause, 1);
        var writes = new List<Task<ProposalResult>> { node.ProposeAsync(new byte[64]) };
        await store.Entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var reads = new List<Task<ReadIndexResult>>();
        try
        {
            for (var i = 1; i < 64; i++)
            {
                reads.Add(node.ReadIndexAsync());
                writes.Add(node.ProposeAsync(new byte[64]));
            }
        }
        finally { store.Release.TrySetResult(); }
        await Task.WhenAll(writes).WaitAsync(TimeSpan.FromSeconds(5));
        await Task.WhenAll(reads).WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(65, node.Status.LastApplied);
        Assert.Equal(0, node.Metrics.PendingProposals);
        Assert.Equal(64, store.Batches.Sum());
        Assert.Contains(16, store.Batches);
        Assert.Equal(5, store.Batches.Count); // blocked first + 16 + 16 + 16 + 15
    }

    [Fact]
    public void Automatic_bulk_collection_is_bounded_by_the_heartbeat_and_can_be_disabled()
    {
        var defaults = new RaftOptions { NodeId = "n1", Members = ["n1"] };
        Assert.Equal(TimeSpan.FromMilliseconds(8), defaults.Freeze().BulkProposalBatchDelayLimit);
        Assert.Equal(TimeSpan.Zero, (defaults with { BulkProposalBatchDelay = TimeSpan.Zero }).Freeze().BulkProposalBatchDelayLimit);
        Assert.Equal(TimeSpan.FromMilliseconds(5), (defaults with { TickInterval = TimeSpan.FromMilliseconds(5),
            ElectionMinTicks = 20, ElectionMaxTicks = 30, RpcTimeout = TimeSpan.FromMilliseconds(2) }).Freeze().BulkProposalBatchDelayLimit);
        Assert.Equal(TimeSpan.Zero, (defaults with { ProposalBatchDelay = TimeSpan.Zero }).Freeze().BulkProposalBatchDelayLimit);
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(26)]
    [InlineData(1001)]
    public void Invalid_bulk_collection_windows_are_rejected_before_admission(int milliseconds)
    {
        var options = new RaftOptions { NodeId = "n1", Members = ["n1"], BulkProposalBatchDelay = TimeSpan.FromMilliseconds(milliseconds) };
        Assert.Throws<ArgumentOutOfRangeException>(() => options.Freeze());
    }
}
