using Raft.Core;
using Raft.Storage;

namespace Raft.Tests;

public sealed class MembershipTests
{
    private sealed class Cluster : IDisposable
    {
        public readonly Dictionary<string, RaftCore> Nodes = new();
        public readonly Dictionary<string, DBreezeLogStorage> Stores = new();
        public readonly Dictionary<string, RaftOptions> Options = new();
        public Cluster()
        {
            foreach (var id in new[] { "n1", "n2", "n3", "n4", "n5" })
            {
                var options = new RaftOptions { NodeId = id, Members = ["n1", "n2", "n3"],
                    Learners = id is "n4" or "n5" ? [id] : [], KnownPeers = ["n4", "n5"] };
                Options.Add(id, options);
                Stores.Add(id, DBreezeLogStorage.CreateInMemory(options));
                Nodes.Add(id, new(options, Stores[id], new Random(7)));
            }
        }
        public RaftCore Elect()
        {
            Nodes["n1"].Campaign();
            Pump();
            Assert.Equal(RaftRole.Leader, Nodes["n1"].Role);
            Assert.Equal(1, Nodes["n1"].CommitIndex);
            return Nodes["n1"];
        }
        public void Pump()
        {
            for (var step = 0; step < 10000; step++)
            {
                var sent = false;
                foreach (var (id, node) in Nodes)
                    while (node.TryDequeue(out var rpc))
                    {
                        sent = true;
                        if (Nodes.TryGetValue(rpc.PeerId, out var target))
                            node.HandleResponse(rpc.PeerId, target.HandleRequest(id, rpc.Request));
                    }
                if (!sent) return;
            }
            throw new InvalidOperationException("Membership pump did not converge.");
        }
        public void Dispose() { foreach (var store in Stores.Values) store.Dispose(); }
    }

    private static ControlResult.Configuration ConfigResult(RaftCore node)
    {
        while (node.TryDequeueControl(out var control))
            if (control is ControlResult.Configuration config) return config;
        throw new InvalidOperationException("No configuration result.");
    }

    [Fact]
    public async Task Changes_are_active_on_append_and_only_one_can_be_uncommitted()
    {
        using var cluster = new Cluster();
        var leader = cluster.Elect();
        await leader.ChangeConfigurationAsync(1, ["n1", "n2", "n3"], ["n4"]);
        var change = ConfigResult(leader);
        Assert.Null(change.Error);
        Assert.Equal(2, leader.State.Configuration.Index);
        Assert.Equal(1, leader.CommitIndex);
        Assert.Contains("n4", leader.State.Configuration.Learners);
        await leader.ChangeConfigurationAsync(2, ["n1", "n2", "n3"], ["n4", "n5"]);
        Assert.Equal("Busy", ConfigResult(leader).Error!.Type);
        Assert.Equal(2, leader.LastIndex);
        cluster.Pump();
        Assert.Equal(2, leader.CommitIndex);
        Assert.Equal(RaftRole.Learner, cluster.Nodes["n4"].Role);
        Assert.Equal(2, cluster.Stores["n4"].LastIndex);
        await leader.ChangeConfigurationAsync(3, ["n1", "n2", "n3", "n4"], []);
        Assert.Null(ConfigResult(leader).Error);
        Assert.Equal(3, leader.State.Configuration.Quorum);
        cluster.Pump();
        Assert.Equal(3, leader.CommitIndex);
        Assert.Equal(RaftRole.Follower, cluster.Nodes["n4"].Role);
    }

    [Fact]
    public async Task Current_term_commit_gate_prevents_the_2015_cross_term_configuration_counterexample()
    {
        using var cluster = new Cluster();
        var leader = cluster.Nodes["n1"];
        leader.Campaign();
        leader.HandleResponse("n2", new VoteResponse(1, true));
        Assert.Equal(RaftRole.Leader, leader.Role);
        Assert.Equal(0, leader.CommitIndex);
        await leader.ChangeConfigurationAsync(1, ["n1", "n2"], []);
        Assert.Equal("Busy", ConfigResult(leader).Error!.Type);
        Assert.Equal(1, leader.LastIndex); // only the election no-op
        cluster.Pump();
        await leader.ChangeConfigurationAsync(2, ["n1", "n2"], []);
        Assert.Null(ConfigResult(leader).Error);
    }

    [Fact]
    public async Task Uncaught_up_learner_and_multi_server_changes_are_rejected_without_writes()
    {
        using var cluster = new Cluster();
        var leader = cluster.Elect();
        await leader.ChangeConfigurationAsync(1, ["n1", "n2", "n3"], ["n4"]);
        ConfigResult(leader);
        // Commit with n2 only, leaving n4 at index 0.
        while (leader.TryDequeue(out var rpc))
            if (rpc.PeerId == "n2") leader.HandleResponse("n2", cluster.Nodes["n2"].HandleRequest("n1", rpc.Request));
        Assert.Equal(2, leader.CommitIndex);
        var writes = cluster.Stores["n1"].Metrics.WriteTransactions;
        await leader.ChangeConfigurationAsync(2, ["n1", "n2", "n3", "n4"], []);
        Assert.Equal("Busy", ConfigResult(leader).Error!.Type);
        await leader.ChangeConfigurationAsync(3, ["n1"], ["n4"]);
        Assert.Equal("Argument", ConfigResult(leader).Error!.Type);
        Assert.Equal(writes, cluster.Stores["n1"].Metrics.WriteTransactions);
    }

    [Fact]
    public void Replacing_an_uncommitted_configuration_restores_previous_quorum_and_survives_recovery()
    {
        using var cluster = new Cluster();
        var follower = cluster.Nodes["n2"];
        var config = new ClusterConfiguration(["n1", "n2"], []);
        follower.HandleRequest("n1", new AppendRequest(1, 1, 0, 0, 1,
            [new LogEntry(1, 1, [], true), new LogEntry(2, 1, config.Encode(), LogEntryKind.Configuration)]));
        Assert.Equal(2, follower.State.Configuration.Voters.Length);
        follower = new(cluster.Options["n2"], cluster.Stores["n2"]);
        Assert.Equal(2, follower.State.Configuration.Index);
        // n3 was removed by the uncommitted entry, but can be the next legitimate leader.
        var reply = Assert.IsType<AppendResponse>(follower.HandleRequest("n3", new AppendRequest(2, 2, 1, 1, 2,
            [new LogEntry(2, 2, [], true)])));
        Assert.True(reply.Success);
        Assert.Equal(3, follower.State.Configuration.Voters.Length);
        Assert.Equal(0, follower.State.Configuration.Index);
        var recovered = new RaftCore(cluster.Options["n2"], cluster.Stores["n2"]);
        Assert.Equal(3, recovered.State.Configuration.Voters.Length);
        Assert.Equal(2, recovered.CommitIndex);
    }

    [Fact]
    public async Task Removed_leader_stays_until_new_configuration_commits_and_does_not_count_itself()
    {
        using var cluster = new Cluster();
        var leader = cluster.Elect();
        await leader.ChangeConfigurationAsync(1, ["n2", "n3"], []);
        Assert.Null(ConfigResult(leader).Error);
        Assert.Equal(RaftRole.Leader, leader.Role);
        var requests = new List<OutboundRpc>();
        while (leader.TryDequeue(out var rpc)) requests.Add(rpc);
        var first = requests.Single(r => r.PeerId == "n2");
        leader.HandleResponse("n2", cluster.Nodes["n2"].HandleRequest("n1", first.Request));
        Assert.Equal(1, leader.CommitIndex);
        Assert.Equal(RaftRole.Leader, leader.Role);
        var second = requests.Single(r => r.PeerId == "n3");
        leader.HandleResponse("n3", cluster.Nodes["n3"].HandleRequest("n1", second.Request));
        Assert.Equal(2, leader.CommitIndex);
        Assert.Equal(RaftRole.Removed, leader.Role);
        cluster.Pump();
        Assert.Equal(2, cluster.Nodes["n2"].CommitIndex);
        Assert.Equal(2, cluster.Nodes["n3"].CommitIndex);
        for (var i = 0; i < 100; i++) leader.Tick();
        Assert.Equal(RaftRole.Removed, leader.Role);
        cluster.Nodes["n2"].Campaign();
        cluster.Pump();
        Assert.Equal(RaftRole.Leader, cluster.Nodes["n2"].Role);
    }

    [Theory]
    [InlineData(3)]
    [InlineData(5)]
    public async Task Real_TCP_disk_cluster_adds_promotes_transfers_removes_and_recovers_configuration(int count)
    {
        await using var cluster = new RuntimeCluster(count, disk: true, tcp: true, modern: true, spares: 1);
        await cluster.StartAsync();
        var leader = await cluster.LeaderAsync();
        var spare = $"n{count + 1}";
        await leader.ProposeAsync("before"u8.ToArray());
        await leader.AddLearnerAsync(spare).WaitAsync(TimeSpan.FromSeconds(10));
        await RuntimeCluster.EventuallyAsync(() => leader.Status.Peers.Any(p => p.NodeId == spare && p.MatchIndex == leader.Status.LastLogIndex));
        await leader.PromoteToVoterAsync(spare).WaitAsync(TimeSpan.FromSeconds(10));
        await leader.TransferLeadershipAsync(spare).WaitAsync(TimeSpan.FromSeconds(10));
        var promoted = cluster.Nodes[spare];
        await promoted.ReadIndexAsync(); // establish that its current-term no-op is committed
        await promoted.RemoveNodeAsync(leader.Status.NodeId).WaitAsync(TimeSpan.FromSeconds(10));
        var written = await promoted.ProposeAsync("after"u8.ToArray());
        await RuntimeCluster.EventuallyAsync(() => leader.Status.Role == RaftRole.Removed);
        await RuntimeCluster.EventuallyAsync(() => cluster.Nodes.Values.Where(n => n != leader).All(n => n.Status.LastApplied >= written.Index));
        await cluster.RestartAsync(leader.Status.NodeId);
        Assert.Equal(RaftRole.Removed, cluster.Nodes[leader.Status.NodeId].Status.Role);
        Assert.All(cluster.Nodes.Values, n => Assert.Null(n.Status.Fault));
    }
}
