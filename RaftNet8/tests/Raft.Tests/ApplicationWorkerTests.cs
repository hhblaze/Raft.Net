using System.Collections.Concurrent;
using Raft.Storage;

namespace Raft.Tests;

public sealed class ApplicationWorkerTests
{
    private sealed class NoPeers : ITransport
    {
        public ValueTask<RaftRpc> SendAsync(string peer, RaftRpc request, CancellationToken token) =>
            throw new InvalidOperationException("No peers in a single-voter fixture.");
    }
    private class AsyncMachine : IStateMachine
    {
        private long _applied;
        public long LastApplied => Interlocked.Read(ref _applied);
        internal readonly ConcurrentQueue<(int Id, bool Pool, bool Background)> Threads = new();
        public ValueTask ApplyAsync(LogEntry entry, CancellationToken token) => Apply([entry], token);
        protected async ValueTask Apply(IReadOnlyList<LogEntry> entries, CancellationToken token)
        {
            Threads.Enqueue((Environment.CurrentManagedThreadId, Thread.CurrentThread.IsThreadPoolThread, Thread.CurrentThread.IsBackground));
            await Task.Yield(); // Genuine asynchronous callback must remain supported without capturing the worker.
            token.ThrowIfCancellationRequested();
            var expected = LastApplied + 1;
            foreach (var entry in entries) Assert.Equal(expected++, entry.Index);
            Interlocked.Exchange(ref _applied, entries[^1].Index);
        }
    }
    private sealed class AsyncBatchMachine : AsyncMachine, IBatchStateMachine
    {
        public ValueTask ApplyBatchAsync(IReadOnlyList<LogEntry> entries, CancellationToken token) => Apply(entries, token);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Application_invocations_start_on_one_dedicated_thread_and_async_callbacks_complete_in_order(bool batch)
    {
        var options = new RaftOptions { NodeId = "n1", Members = ["n1"], TickInterval = TimeSpan.FromMilliseconds(1),
            ElectionMinTicks = 20, ElectionMaxTicks = 30, RpcTimeout = TimeSpan.FromMilliseconds(5), MaxAppendEntries = 8,
            ProposalBatchDelay = TimeSpan.Zero };
        AsyncMachine machine = batch ? new AsyncBatchMachine() : new AsyncMachine();
        await using var log = new RaftStorageAdapter(DBreezeLogStorage.CreateInMemory(options, DBreezeLogLayout.UnifiedTableV3));
        await using var node = await RaftNode.CreateAsync(options, log, new NoPeers(), machine);
        await node.StartAsync();
        await RuntimeCluster.EventuallyAsync(() => node.Status.Role == RaftRole.Leader && machine.LastApplied == 1);
        await Task.WhenAll(Enumerable.Range(0, 64).Select(i => node.ProposeAsync(BitConverter.GetBytes(i))))
            .WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(65, machine.LastApplied);
        Assert.Equal(0, node.Metrics.PendingProposals);
        Assert.Single(machine.Threads.Select(t => t.Id).Distinct());
        Assert.All(machine.Threads, t => { Assert.False(t.Pool); Assert.True(t.Background); });
        Assert.Null(node.Status.Fault);
    }
}
