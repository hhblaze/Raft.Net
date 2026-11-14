using System.Collections.Concurrent;
using System.Threading.Channels;
using Raft.Storage;

namespace Raft.Tests;

public sealed class CommitNotificationTests
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
    private sealed class Cluster : IAsyncDisposable
    {
        internal readonly ConcurrentDictionary<string, RaftNode> Nodes = new();
        internal readonly Dictionary<string, DBreezeLogStorage> Stores = new();
        internal readonly Dictionary<string, TestStateMachine> Machines = new();
        internal readonly Dictionary<string, Clock> Clocks = new();
        internal readonly ConcurrentDictionary<string, bool> Offline = new();
        internal readonly ConcurrentQueue<(string From, string To, AppendRequest Request)> Frames = new();
        private sealed class Transport(string from, Cluster cluster) : ITransport
        {
            public ValueTask<RaftRpc> SendAsync(string peer, RaftRpc request, CancellationToken token)
            {
                if (cluster.Offline.ContainsKey(peer)) throw new IOException("Peer is stopped.");
                if (request is AppendRequest append) cluster.Frames.Enqueue((from, peer, append));
                return cluster.Nodes[peer].HandleAsync(from, request, token);
            }
        }
        internal RaftNode Leader => Nodes["n1"];
        internal async Task StartAsync(DBreezeLogLayout layout, bool coalesce = true)
        {
            foreach (var id in new[] { "n1", "n2", "n3" })
            {
                var options = new RaftOptions { NodeId = id, Members = ["n1", "n2", "n3"],
                    TickInterval = TimeSpan.FromSeconds(1), HeartbeatTicks = 1,
                    ElectionMinTicks = 20, ElectionMaxTicks = 30, RpcTimeout = TimeSpan.FromSeconds(1),
                    CoalesceCommitNotifications = coalesce };
                var store = Stores[id] = DBreezeLogStorage.CreateInMemory(options, layout);
                var machine = Machines[id] = new TestStateMachine();
                var clock = Clocks[id] = new Clock();
                Nodes[id] = await RaftNode.CreateAsync(options, new RaftStorageAdapter(store), new Transport(id, this), machine, clock);
            }
            foreach (var node in Nodes.Values) await node.StartAsync();
            await DriveStartupAsync(() => Leader.Status.Role == RaftRole.Leader && Leader.Status.CommitIndex >= 1 && Leader.Status.LastApplied >= 1);
            var fence = Leader.ReadIndexAsync();
            await DriveStartupAsync(() => fence.IsCompleted);
            await fence;
            await DriveStartupAsync(() => Nodes.Values.All(n => n.Status.LastApplied >= 1) &&
                Leader.Status.Peers.All(p => p.InFlightMessages == 0));
            Frames.Clear();
        }
        private async Task DriveStartupAsync(Func<bool> ready)
        {
            var started = System.Diagnostics.Stopwatch.StartNew();
            while (!ready())
            {
                foreach (var node in Nodes.Values) Assert.Null(node.Status.Fault);
                if (started.Elapsed > TimeSpan.FromSeconds(10)) throw new TimeoutException("Manual-clock startup did not reach its fence.");
                Clocks["n1"].Pulse();
                // Keep startup retry/heartbeat delivery alive after the role changes. A single
                // delayed RPC can time out under the full suite; stopped logical time cannot retry it.
                await Task.Delay(Leader.Status.Role == RaftRole.Leader ? 100 : 1);
            }
        }
        internal async Task<ProposalResult> ProposeAsync(bool allCopies = true)
        {
            var result = await Leader.ProposeAsync(new byte[] { 7 }).WaitAsync(TimeSpan.FromSeconds(5));
            await RuntimeCluster.EventuallyAsync(() => (allCopies ? Stores.Values.All(s => s.LastIndex >= result.Index) : Stores["n2"].LastIndex >= result.Index) &&
                Leader.Status.Peers.All(p => p.InFlightMessages == 0));
            return result;
        }
        internal void AssertDeferred(long index, bool allCopies = true)
        {
            Assert.Equal(index, Stores["n1"].HardState.CommitIndex);
            Assert.Equal(index, Machines["n1"].LastApplied);
            foreach (var id in new[] { "n2", "n3" })
            {
                Assert.Equal(allCopies || id == "n2" ? index : index - 1, Stores[id].LastIndex);
                Assert.Equal(index - 1, Stores[id].HardState.CommitIndex);
                Assert.Equal(index - 1, Machines[id].LastApplied);
            }
            Assert.DoesNotContain(Frames, f => f.From == "n1" && f.Request.Entries.Count == 0 && f.Request.LeaderCommit >= index);
        }
        public async ValueTask DisposeAsync()
        {
            foreach (var node in Nodes.Values) await node.DisposeAsync();
        }
    }

    [Theory]
    [InlineData(DBreezeLogLayout.SeparateTablesV2, "tick")]
    [InlineData(DBreezeLogLayout.UnifiedTableV3, "tick")]
    [InlineData(DBreezeLogLayout.SeparateTablesV2, "data")]
    [InlineData(DBreezeLogLayout.UnifiedTableV3, "data")]
    [InlineData(DBreezeLogLayout.SeparateTablesV2, "read")]
    [InlineData(DBreezeLogLayout.UnifiedTableV3, "read")]
    public async Task Durable_quorum_and_leader_apply_do_not_require_an_extra_idle_commit_RPC(DBreezeLogLayout layout, string next)
    {
        await using var cluster = new Cluster();
        await cluster.StartAsync(layout);
        var result = await cluster.ProposeAsync();
        cluster.AssertDeferred(result.Index);
        if (next == "tick") cluster.Clocks["n1"].Pulse();
        else if (next == "data") await cluster.Leader.ProposeAsync(new byte[] { 8 }).WaitAsync(TimeSpan.FromSeconds(5));
        else await cluster.Leader.ReadIndexAsync().WaitAsync(TimeSpan.FromSeconds(5));
        await RuntimeCluster.EventuallyAsync(() => cluster.Nodes.Values.All(n => n.Status.LastApplied >= result.Index));
        foreach (var store in cluster.Stores.Values) Assert.True(store.HardState.CommitIndex >= result.Index);
        if (next == "data") Assert.Contains(cluster.Frames, f => f.Request.Entries.Count > 0 && f.Request.LeaderCommit >= result.Index);
    }

    [Theory]
    [InlineData(DBreezeLogLayout.SeparateTablesV2)]
    [InlineData(DBreezeLogLayout.UnifiedTableV3)]
    public async Task Opting_out_retains_immediate_idle_commit_notifications(DBreezeLogLayout layout)
    {
        await using var cluster = new Cluster();
        await cluster.StartAsync(layout, coalesce: false);
        var result = await cluster.ProposeAsync();
        // A peer with an older RPC still in flight can await the next heartbeat
        // even in the original mode; the caught-up peer gets an immediate RPC.
        await RuntimeCluster.EventuallyAsync(() => cluster.Nodes.Where(p => p.Key != "n1").Any(p => p.Value.Status.LastApplied >= result.Index));
        Assert.Contains(cluster.Frames, f => f.Request.Entries.Count == 0 && f.Request.LeaderCommit >= result.Index);
    }

    [Theory]
    [InlineData(DBreezeLogLayout.SeparateTablesV2, false)]
    [InlineData(DBreezeLogLayout.UnifiedTableV3, false)]
    [InlineData(DBreezeLogLayout.SeparateTablesV2, true)]
    [InlineData(DBreezeLogLayout.UnifiedTableV3, true)]
    public async Task An_acknowledged_command_survives_leader_death_before_its_commit_notification(DBreezeLogLayout layout, bool laggingThird)
    {
        await using var cluster = new Cluster();
        await cluster.StartAsync(layout);
        if (laggingThird) cluster.Offline["n3"] = true;
        var result = await cluster.ProposeAsync(allCopies: !laggingThird);
        cluster.AssertDeferred(result.Index, allCopies: !laggingThird);
        var term = cluster.Leader.Status.Term;
        cluster.Offline["n1"] = true;
        await cluster.Leader.DisposeAsync();
        cluster.Offline.TryRemove("n3", out _);
        // A delayed, valid TimeoutNow from the previous leader starts a controlled
        // election without adding unrelated logical heartbeats to the test.
        var next = cluster.Nodes["n2"];
        var reply = Assert.IsType<TimeoutNowResponse>(await next.HandleAsync("n1", new TimeoutNowRequest(term, 99)));
        Assert.True(reply.Accepted);
        await RuntimeCluster.EventuallyAsync(() => next.Status.Role == RaftRole.Leader && next.Status.CommitIndex > result.Index && next.Status.LastApplied > result.Index);
        await next.ReadIndexAsync().WaitAsync(TimeSpan.FromSeconds(5));
        await RuntimeCluster.EventuallyAsync(() => cluster.Nodes["n3"].Status.LastApplied > result.Index);
        foreach (var id in new[] { "n2", "n3" })
        {
            var command = Assert.Single(cluster.Machines[id].Entries, e => !e.IsNoOp);
            Assert.Equal(result.Index, command.Index); Assert.Equal(new byte[] { 7 }, command.Command.ToArray());
        }
    }
}
