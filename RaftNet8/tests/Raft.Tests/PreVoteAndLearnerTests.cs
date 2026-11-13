using Raft.Core;
using Raft.Storage;

namespace Raft.Tests;

public sealed class PreVoteAndLearnerTests
{
    [Fact]
    public void Isolated_node_rejoining_does_not_increment_term_or_depose_a_healthy_leader()
    {
        using var cluster = new SimCluster(preVote: true);
        cluster.Tick(80);
        var leader = cluster.Leader;
        var isolated = cluster.Nodes.Keys.First(id => id != leader);
        var rest = cluster.Nodes.Keys.Where(id => id != isolated).ToArray();
        var term = cluster.Nodes[leader].Term;
        cluster.Partition(rest, [isolated]);
        cluster.Tick(150);
        Assert.Equal(term, cluster.Nodes[isolated].Term);
        Assert.Equal(term, cluster.Nodes[leader].Term);
        Assert.Equal(RaftRole.Leader, cluster.Nodes[leader].Role);
        cluster.Heal();
        cluster.Tick(20);
        Assert.Equal(leader, cluster.Leader);
        Assert.All(cluster.Nodes.Values, node => Assert.Equal(term, node.Term));
    }

    [Fact]
    public void Prevote_changes_no_persistent_receiver_state_and_recent_leader_suppresses_it()
    {
        var options = new RaftOptions { NodeId = "n1", Members = ["n1", "n2", "n3"] };
        using var store = DBreezeRaftStorage.CreateInMemory(options);
        var node = new RaftCore(options, store, new Random(7));
        var before = store.Metrics.WriteTransactions;
        var reply = (PreVoteResponse)node.HandleRequest("n2", new PreVoteRequest(100, 1, 0, 0));
        Assert.True(reply.Granted);
        Assert.Equal(0, node.Term);
        Assert.Equal(before, store.Metrics.WriteTransactions);
        node.HandleRequest("n3", new AppendRequest(1, 1, 0, 0, 0, []));
        before = store.Metrics.WriteTransactions;
        reply = (PreVoteResponse)node.HandleRequest("n2", new PreVoteRequest(100, 2, 0, 0));
        Assert.False(reply.Granted);
        Assert.Equal(1, node.Term);
        Assert.Equal(before, store.Metrics.WriteTransactions);
    }

    [Fact]
    public void Prevote_needs_fresh_log_and_distinct_votes_from_its_own_round()
    {
        using var cluster = new SimCluster(5, preVote: true);
        var node = cluster.Nodes["n1"];
        for (var i = 0; i < 30 && node.Role != RaftRole.PreCandidate; i++) node.Tick();
        Assert.Equal(RaftRole.PreCandidate, node.Role);
        var requests = new List<PreVoteRequest>();
        while (node.TryDequeue(out var outbound)) if (outbound.Request is PreVoteRequest request) requests.Add(request);
        var pre = Assert.Single(requests.Distinct());
        node.HandleResponse("n2", new PreVoteResponse(0, pre.Round - 1, 1, true));
        node.HandleResponse("n2", new PreVoteResponse(0, pre.Round, 99, true));
        for (var i = 0; i < 8; i++) node.HandleResponse("n2", new PreVoteResponse(0, pre.Round, 1, true));
        Assert.Equal(0, node.Term);
        node.HandleResponse("n3", new PreVoteResponse(0, pre.Round, 1, true));
        Assert.Equal(1, node.Term);
        Assert.Equal(RaftRole.Candidate, node.Role);

        var options = new RaftOptions { NodeId = "n1", Members = ["n1", "n2", "n3"] };
        using var store = DBreezeRaftStorage.CreateInMemory(options);
        store.SaveHardState(new(3));
        store.Append([new LogEntry(1, 3, "newer"u8)]);
        var voter = new RaftCore(options, store);
        Assert.False(((PreVoteResponse)voter.HandleRequest("n2", new PreVoteRequest(4, 1, 100, 2))).Granted);
        Assert.Equal(3, voter.Term);
    }

    [Fact]
    public void Learners_never_campaign_vote_or_form_a_commit_quorum()
    {
        var voters = new[] { "n1", "n2", "n3" };
        var leaderOptions = new RaftOptions { NodeId = "n1", Members = voters, Learners = ["n4"] };
        var learnerOptions = leaderOptions with { NodeId = "n4" };
        using var leaderStore = DBreezeLogStorage.CreateInMemory(leaderOptions);
        using var learnerStore = DBreezeLogStorage.CreateInMemory(learnerOptions);
        var leader = new RaftCore(leaderOptions, leaderStore);
        var learner = new RaftCore(learnerOptions, learnerStore);
        for (var i = 0; i < 100; i++) learner.Tick();
        Assert.Equal(RaftRole.Learner, learner.Role);
        Assert.Equal(0, learner.Term);
        Assert.False(learner.TryDequeue(out _));
        Assert.False(((VoteResponse)learner.HandleRequest("n1", new VoteRequest(1, 0, 0))).Granted);
        leader.Campaign();
        leader.HandleResponse("n4", new VoteResponse(1, true));
        Assert.Equal(RaftRole.Candidate, leader.Role);
        leader.HandleResponse("n2", new VoteResponse(1, true));
        AppendRequest? toLearner = null, toVoter = null;
        while (leader.TryDequeue(out var rpc))
        {
            if (rpc.PeerId == "n4" && rpc.Request is AppendRequest append) toLearner = append;
            if (rpc.PeerId == "n2" && rpc.Request is AppendRequest voteAppend) toVoter = voteAppend;
        }
        Assert.NotNull(toLearner);
        Assert.NotNull(toVoter);
        leader.HandleResponse("n4", learner.HandleRequest("n1", toLearner));
        Assert.Equal(0, leader.CommitIndex);
        leader.HandleResponse("n2", new AppendResponse(1, toVoter.RequestId, true, 1));
        Assert.Equal(1, leader.CommitIndex);
    }

    [Fact]
    public void Removed_node_recovers_without_becoming_a_voter_from_bootstrap_options()
    {
        var options = new RaftOptions { NodeId = "n1", Members = ["n1", "n2", "n3"] };
        using var store = DBreezeLogStorage.CreateInMemory(options);
        store.SaveHardState(new(2));
        store.Append([new LogEntry(1, 2, new ClusterConfiguration(["n2", "n3"], []).Encode().AsSpan(), LogEntryKind.Configuration)]);
        store.SaveHardState(new(2, null, 1));
        var removed = new RaftCore(options, store);
        for (var i = 0; i < 100; i++) removed.Tick();
        Assert.Equal(RaftRole.Removed, removed.Role);
        Assert.Equal(2, removed.Term);
        Assert.False(removed.TryDequeue(out _));
    }
}
