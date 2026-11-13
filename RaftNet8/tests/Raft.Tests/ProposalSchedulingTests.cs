using System.Collections.Concurrent;
using System.Threading.Channels;
using Raft.Storage;

namespace Raft.Tests;

public sealed class ProposalSchedulingTests
{
    private sealed class Clock : IClock, ITicker
    {
        private readonly Channel<bool> _ticks = Channel.CreateBounded<bool>(new BoundedChannelOptions(1)
        { FullMode = BoundedChannelFullMode.DropWrite });
        public ITicker CreateTicker(TimeSpan interval) => this;
        public ValueTask<bool> WaitForNextTickAsync(CancellationToken token) => _ticks.Reader.ReadAsync(token);
        public void Pulse() => _ticks.Writer.TryWrite(true);
        public ValueTask DisposeAsync() { _ticks.Writer.TryComplete(); return ValueTask.CompletedTask; }
    }
    private sealed class Network
    {
        internal readonly ConcurrentDictionary<string, RaftNode> Nodes = new();
        internal volatile bool HoldData;
        internal readonly TaskCompletionSource ReleaseData = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal sealed class Transport(string from, Network network) : ITransport
        {
            public async ValueTask<RaftRpc> SendAsync(string peer, RaftRpc request, CancellationToken token)
            {
                if (from == "n1" && network.HoldData && request is AppendRequest append && append.Entries.Any(e => !e.IsNoOp))
                    await network.ReleaseData.Task.WaitAsync(token);
                return await network.Nodes[peer].HandleAsync(from, request, token);
            }
        }
    }

    [Theory]
    [InlineData(DBreezeLogLayout.SeparateTablesV2)]
    [InlineData(DBreezeLogLayout.UnifiedTableV3)]
    public async Task An_inflight_uncommitted_proposal_keeps_the_feedback_window_without_blocking_a_vote(DBreezeLogLayout layout)
    {
        var network = new Network();
        var stores = new Dictionary<string, DBreezeLogStorage>();
        var clocks = new Dictionary<string, Clock>();
        Task<ProposalResult>? first = null, second = null;
        try
        {
            foreach (var id in new[] { "n1", "n2", "n3" })
            {
                var options = new RaftOptions { NodeId = id, Members = ["n1", "n2", "n3"],
                    TickInterval = TimeSpan.FromSeconds(1), HeartbeatTicks = 2,
                    ElectionMinTicks = 20, ElectionMaxTicks = 30, RpcTimeout = TimeSpan.FromSeconds(1),
                    MaxCommandBytes = 128, MaxAppendBytes = 149, MaxReplicationBytes = 596, MaxAppendEntries = 8,
                    ProposalBatchDelay = TimeSpan.FromMilliseconds(200), BulkProposalBatchDelay = TimeSpan.FromSeconds(1) };
                var clock = clocks[id] = new Clock();
                var store = stores[id] = DBreezeLogStorage.CreateInMemory(options, layout);
                network.Nodes[id] = await RaftNode.CreateAsync(options, new RaftStorageAdapter(store),
                    new Network.Transport(id, network), new TestStateMachine(), clock: clock);
            }
            foreach (var node in network.Nodes.Values) await node.StartAsync();
            var leader = network.Nodes["n1"];
            for (var attempt = 0; attempt < 1000 && leader.Status.Role != RaftRole.Leader; attempt++)
            { clocks["n1"].Pulse(); await Task.Delay(1); }
            await RuntimeCluster.EventuallyAsync(() => leader.Status.Role == RaftRole.Leader && leader.Status.CommitIndex >= 1 && leader.Status.LastApplied >= 1);
            var committed = leader.Status.CommitIndex;
            network.HoldData = true;
            first = leader.ProposeAsync(new byte[128]);
            await RuntimeCluster.EventuallyAsync(() => stores["n1"].LastIndex == committed + 1);
            var fence = stores["n1"].LastIndex;
            var term = leader.Status.Term;
            second = leader.ProposeAsync(new byte[128]);
            await Task.Delay(300); // exceeds the 200ms small window, within the 1s bulk deadline
            Assert.Equal(fence, stores["n1"].LastIndex);
            Assert.Equal(committed, stores["n1"].HardState.CommitIndex);
            var vote = Assert.IsType<VoteResponse>(await leader.HandleAsync("n2", new VoteRequest(term + 1, fence, stores["n1"].GetTerm(fence))));
            Assert.True(vote.Granted);
            await Assert.ThrowsAsync<NotLeaderException>(() => first.WaitAsync(TimeSpan.FromSeconds(5)));
            await Assert.ThrowsAsync<NotLeaderException>(() => second.WaitAsync(TimeSpan.FromSeconds(5)));
            Assert.Equal(0, leader.Metrics.PendingProposals);
        }
        finally
        {
            network.ReleaseData.TrySetResult();
            foreach (var node in network.Nodes.Values) await node.DisposeAsync();
            foreach (var task in new[] { first, second })
                if (task is not null) { try { await task; } catch (Exception) { /* Observe cleanup after an earlier assertion. */ } }
        }
    }

    [Fact]
    public async Task A_higher_term_vote_is_processed_before_a_waiting_proposal_is_written()
    {
        var network = new Network();
        var stores = new Dictionary<string, DBreezeLogStorage>();
        var clocks = new Dictionary<string, Clock>();
        try
        {
            foreach (var id in new[] { "n1", "n2", "n3" })
            {
                var options = new RaftOptions { NodeId = id, Members = ["n1", "n2", "n3"],
                    TickInterval = TimeSpan.FromSeconds(1), HeartbeatTicks = 2,
                    ElectionMinTicks = 20, ElectionMaxTicks = 30, RpcTimeout = TimeSpan.FromSeconds(1),
                    ProposalBatchDelay = TimeSpan.FromSeconds(1) };
                var clock = clocks[id] = new Clock();
                var store = stores[id] = DBreezeLogStorage.CreateInMemory(options, DBreezeLogLayout.UnifiedTableV3);
                network.Nodes[id] = await RaftNode.CreateAsync(options, new RaftStorageAdapter(store),
                    new Network.Transport(id, network), new TestStateMachine(), clock: clock);
            }
            foreach (var node in network.Nodes.Values) await node.StartAsync();
            var leader = network.Nodes["n1"];
            for (var attempt = 0; attempt < 1000 && leader.Status.Role != RaftRole.Leader; attempt++)
            { clocks["n1"].Pulse(); await Task.Delay(1); }
            await RuntimeCluster.EventuallyAsync(() => leader.Status.Role == RaftRole.Leader && leader.Status.CommitIndex >= 1 && leader.Status.LastApplied >= 1);
            var fence = leader.Status.CommitIndex;
            var term = leader.Status.Term;
            var fenceTerm = stores["n1"].GetTerm(fence);
            var proposal = leader.ProposeAsync(new byte[] { 7 });
            var vote = Assert.IsType<VoteResponse>(await leader.HandleAsync("n2", new VoteRequest(term + 1, fence, fenceTerm)));
            Assert.True(vote.Granted, "Collection must not append the waiting command before processing a queued higher-term vote.");
            Assert.Equal(fence, stores["n1"].LastIndex);
            Assert.Equal(new HardState(term + 1, "n2", fence), stores["n1"].HardState);
            await Assert.ThrowsAsync<NotLeaderException>(() => proposal.WaitAsync(TimeSpan.FromSeconds(5)));
            Assert.Equal(0, leader.Metrics.PendingProposals);
        }
        finally
        {
            foreach (var node in network.Nodes.Values) await node.DisposeAsync();
        }
    }
}
