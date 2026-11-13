using System.Collections.Concurrent;
using System.Threading.Channels;
using Raft.Storage;

namespace Raft.Tests;

public sealed class FollowerBatchTests
{
    private sealed class Clock : IClock, ITicker
    {
        private readonly Channel<bool> _ticks = Channel.CreateBounded<bool>(1);
        public ITicker CreateTicker(TimeSpan interval) => this;
        public ValueTask<bool> WaitForNextTickAsync(CancellationToken token) => _ticks.Reader.ReadAsync(token);
        public ValueTask DisposeAsync() { _ticks.Writer.TryComplete(); return ValueTask.CompletedTask; }
    }
    private sealed class NoNetwork : ITransport
    {
        public ValueTask<RaftRpc> SendAsync(string peer, RaftRpc request, CancellationToken token) =>
            throw new InvalidOperationException("This controlled follower never sends requests.");
    }
    private sealed class Store(ILogStore inner, bool pauseSecond = false, Exception? failure = null) : ILogStore, ILogStoreProgress
    {
        internal readonly TaskCompletionSource FirstEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal readonly TaskCompletionSource FirstRelease = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal readonly TaskCompletionSource SecondEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal readonly TaskCompletionSource SecondRelease = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal readonly ConcurrentQueue<int> Batches = new();
        private int _writes;
        public long UncommittedBytes => ((ILogStoreProgress)inner).UncommittedBytes;
        public ValueTask<RecoveredLogState> RecoverAsync(CancellationToken token) => inner.RecoverAsync(token);
        public ValueTask<LogReadResult> ReadAsync(LogQuery query, CancellationToken token) => inner.ReadAsync(query, token);
        public async ValueTask WriteAsync(LogWrite write, CancellationToken token)
        {
            var count = Count(write);
            if (count > 0)
            {
                Batches.Enqueue(count);
                var ordinal = ++_writes;
                if (ordinal == 1) { FirstEntered.TrySetResult(); await FirstRelease.Task.WaitAsync(token); }
                if (ordinal == 2)
                {
                    SecondEntered.TrySetResult();
                    if (pauseSecond) await SecondRelease.Task.WaitAsync(token);
                    if (failure is not null) throw failure;
                }
            }
            await inner.WriteAsync(write, token);
        }
        private static int Count(LogWrite write) => write switch
        {
            LogWrite.Append append => append.Entries.Length,
            LogWrite.ReplaceSuffix suffix => suffix.Entries.Length,
            LogWrite.Batch batch => batch.Writes.Sum(Count),
            _ => 0
        };
        public ValueTask DisposeAsync() => inner.DisposeAsync();
    }

    private static RaftOptions Options => new() { NodeId = "n2", Members = ["n1", "n2", "n3"], KnownPeers = ["n4"],
        MaxAppendBytes = 149, MaxReplicationBytes = 149, MaxCommandBytes = 128 };
    private static AppendRequest Append(int index) => new(1, index, index - 1, index == 1 ? 0 : 1, 4,
        [new LogEntry(index, 1, Enumerable.Repeat((byte)index, 128).ToArray())]);

    [Theory]
    [InlineData(DBreezeLogLayout.SeparateTablesV2)]
    [InlineData(DBreezeLogLayout.UnifiedTableV3)]
    public async Task Queued_fresh_frames_share_one_durable_group_but_keep_individual_replies_and_wire_limits(DBreezeLogLayout layout)
    {
        var options = Options;
        var backend = DBreezeLogStorage.CreateInMemory(options, layout);
        var store = new Store(new RaftStorageAdapter(backend), pauseSecond: true);
        var machine = new TestStateMachine();
        await using var node = await RaftNode.CreateAsync(options, store, new NoNetwork(), machine, clock: new Clock());
        await node.StartAsync();
        var calls = new List<Task<RaftRpc>> { node.HandleAsync("n1", Append(1)).AsTask() };
        await store.FirstEntered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        try
        {
            for (var i = 2; i <= 4; i++) calls.Add(node.HandleAsync("n1", Append(i)).AsTask());
            store.FirstRelease.TrySetResult();
            await store.SecondEntered.Task.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.All(calls.Skip(1), call => Assert.False(call.IsCompleted));
            Assert.Equal(1, backend.LastIndex);
            Assert.Equal(new[] { 1, 3 }, store.Batches);
        }
        finally
        {
            store.FirstRelease.TrySetResult(); store.SecondRelease.TrySetResult();
            await Task.WhenAll(calls).WaitAsync(TimeSpan.FromSeconds(5));
        }
        for (var i = 0; i < calls.Count; i++)
        {
            var reply = Assert.IsType<AppendResponse>(await calls[i]);
            Assert.True(reply.Success);
            Assert.Equal(i + 1, reply.RequestId);
            Assert.Equal(i + 1, reply.MatchIndex);
        }
        await RuntimeCluster.EventuallyAsync(() => machine.LastApplied == 4);
        Assert.Equal(4, backend.HardState.CommitIndex);
        var writes = store.Batches.Count;
        var oversizedWire = new AppendRequest(1, 9, 4, 1, 4, [Append(5).Entries[0], Append(6).Entries[0], Append(7).Entries[0]]);
        await Assert.ThrowsAsync<ArgumentException>(() => node.HandleAsync("n1", oversizedWire).AsTask());
        Assert.Equal(writes, store.Batches.Count);
        Assert.False(node.Completion.IsCompleted);
    }

    [Theory]
    [InlineData(DBreezeLogLayout.SeparateTablesV2, false)]
    [InlineData(DBreezeLogLayout.UnifiedTableV3, false)]
    [InlineData(DBreezeLogLayout.SeparateTablesV2, true)]
    [InlineData(DBreezeLogLayout.UnifiedTableV3, true)]
    public async Task Local_groups_respect_both_entry_and_encoded_byte_budgets(DBreezeLogLayout layout, bool byteBound)
    {
        var options = byteBound ? Options with { MaxProposalBatchBytes = 298 } : Options with { MaxAppendEntries = 2 };
        var backend = DBreezeLogStorage.CreateInMemory(options, layout);
        var store = new Store(new RaftStorageAdapter(backend));
        await using var node = await RaftNode.CreateAsync(options, store, new NoNetwork(), new TestStateMachine(), clock: new Clock());
        await node.StartAsync();
        var calls = new List<Task<RaftRpc>> { node.HandleAsync("n1", Append(1)).AsTask() };
        await store.FirstEntered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        try { for (var i = 2; i <= 4; i++) calls.Add(node.HandleAsync("n1", Append(i)).AsTask()); }
        finally { store.FirstRelease.TrySetResult(); }
        var replies = await Task.WhenAll(calls).WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(new[] { 1, 2, 1 }, store.Batches);
        for (var i = 0; i < replies.Length; i++)
        {
            var reply = Assert.IsType<AppendResponse>(replies[i]);
            Assert.True(reply.Success); Assert.Equal(i + 1, reply.RequestId); Assert.Equal(i + 1, reply.MatchIndex);
        }
        Assert.Equal(4, backend.LastIndex);
    }

    [Theory]
    [InlineData(DBreezeLogLayout.SeparateTablesV2, "vote")]
    [InlineData(DBreezeLogLayout.UnifiedTableV3, "vote")]
    [InlineData(DBreezeLogLayout.SeparateTablesV2, "read")]
    [InlineData(DBreezeLogLayout.UnifiedTableV3, "read")]
    [InlineData(DBreezeLogLayout.SeparateTablesV2, "configuration")]
    [InlineData(DBreezeLogLayout.UnifiedTableV3, "configuration")]
    public async Task Coalescing_never_crosses_term_read_context_or_configuration_boundaries(DBreezeLogLayout layout, string boundary)
    {
        var options = Options;
        var backend = DBreezeLogStorage.CreateInMemory(options, layout);
        var store = new Store(new RaftStorageAdapter(backend));
        await using var node = await RaftNode.CreateAsync(options, store, new NoNetwork(), new TestStateMachine(), clock: new Clock());
        await node.StartAsync();
        var first = node.HandleAsync("n1", Append(1)).AsTask();
        await store.FirstEntered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Task<RaftRpc> second, barrier, last;
        try
        {
            second = node.HandleAsync("n1", Append(2)).AsTask();
            barrier = boundary switch
            {
                "vote" => node.HandleAsync("n3", new VoteRequest(2, 2, 1)).AsTask(),
                "read" => node.HandleAsync("n1", Append(3) with { ReadContext = 19 }).AsTask(),
                _ => node.HandleAsync("n1", new AppendRequest(1, 3, 2, 1, 4,
                    [new LogEntry(3, 1, new ClusterConfiguration(["n1", "n2", "n3"], ["n4"]).Encode(), LogEntryKind.Configuration)])).AsTask()
            };
            last = node.HandleAsync("n1", Append(4)).AsTask();
        }
        finally { store.FirstRelease.TrySetResult(); }
        await Task.WhenAll(first, second, barrier, last).WaitAsync(TimeSpan.FromSeconds(5));
        Assert.True(Assert.IsType<AppendResponse>(await second).Success);
        if (boundary == "vote")
        {
            Assert.True(Assert.IsType<VoteResponse>(await barrier).Granted);
            Assert.False(Assert.IsType<AppendResponse>(await last).Success);
            Assert.Equal(2, backend.LastIndex);
            Assert.Equal(new[] { 1, 1 }, store.Batches);
        }
        else
        {
            Assert.Equal(new[] { 1, 1, 1, 1 }, store.Batches);
            if (boundary == "read") Assert.Equal(19, Assert.IsType<AppendResponse>(await barrier).ReadContext);
            else Assert.Contains("n4", backend.Recover().Configuration!.Learners);
        }
    }

    [Theory]
    [InlineData(DBreezeLogLayout.SeparateTablesV2)]
    [InlineData(DBreezeLogLayout.UnifiedTableV3)]
    public async Task A_group_failure_reaches_every_removed_request_and_node_completion_unchanged(DBreezeLogLayout layout)
    {
        var failure = new IOException("durable group failed");
        var store = new Store(new RaftStorageAdapter(DBreezeLogStorage.CreateInMemory(Options, layout)), failure: failure);
        await using var node = await RaftNode.CreateAsync(Options, store, new NoNetwork(), new TestStateMachine(), clock: new Clock());
        await node.StartAsync();
        var first = node.HandleAsync("n1", Append(1)).AsTask();
        await store.FirstEntered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var rest = new List<Task<RaftRpc>>();
        try { for (var i = 2; i <= 4; i++) rest.Add(node.HandleAsync("n1", Append(i)).AsTask()); }
        finally { store.FirstRelease.TrySetResult(); }
        Assert.True(Assert.IsType<AppendResponse>(await first).Success);
        foreach (var call in rest) Assert.Same(failure, await Assert.ThrowsAsync<IOException>(() => call.WaitAsync(TimeSpan.FromSeconds(5))));
        Assert.Same(failure, await Assert.ThrowsAsync<IOException>(() => node.Completion.WaitAsync(TimeSpan.FromSeconds(5))));
    }
}
