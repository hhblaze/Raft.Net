using Raft.Core;
using Raft.Storage;

namespace Raft.Tests;

public sealed class ElectionTests
{
    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(5)]
    public void Elects_one_leader_and_commits_its_noop(int size)
    {
        using var cluster = new SimCluster(size);
        cluster.Tick(80);
        var leader = cluster.Leader;
        Assert.Equal(1, cluster.Nodes[leader].CommitIndex);
        Assert.True(cluster.Stores[leader].Read(1, 1, 1024)[0].IsNoOp);
    }

    [Fact]
    public void Duplicate_votes_cannot_form_a_quorum()
    {
        using var cluster = new SimCluster(5);
        var node = cluster.Nodes["n1"];
        node.Campaign();
        for (var i = 0; i < 20; i++) node.HandleResponse("n2", new VoteResponse(node.Term, true));
        node.HandleResponse("outsider", new VoteResponse(node.Term + 10, true));
        Assert.Equal(RaftRole.Candidate, node.Role);
        Assert.Equal(1, node.Term);
        node.HandleResponse("n3", new VoteResponse(node.Term, true));
        Assert.Equal(RaftRole.Leader, node.Role);
    }

    [Fact]
    public void Stale_vote_and_append_responses_do_not_elect_a_candidate()
    {
        using var cluster = new SimCluster();
        var node = cluster.Nodes["n1"];
        node.Campaign();
        node.Campaign();
        node.HandleResponse("n2", new VoteResponse(1, true));
        Assert.Equal(RaftRole.Candidate, node.Role);
        node.HandleResponse("n2", new VoteResponse(3, false));
        Assert.Equal(RaftRole.Follower, node.Role);
        Assert.Equal(3, node.Term);
    }

    [Fact]
    public void Repeated_vote_for_same_candidate_is_granted_but_other_candidate_is_rejected()
    {
        using var cluster = new SimCluster();
        var voter = cluster.Nodes["n1"];
        Assert.True(((VoteResponse)voter.HandleRequest("n2", new VoteRequest(4, 0, 0))).Granted);
        Assert.True(((VoteResponse)voter.HandleRequest("n2", new VoteRequest(4, 0, 0))).Granted);
        Assert.False(((VoteResponse)voter.HandleRequest("n3", new VoteRequest(4, 0, 0))).Granted);
        cluster.Restart("n1");
        Assert.False(((VoteResponse)cluster.Nodes["n1"].HandleRequest("n3", new VoteRequest(4, 0, 0))).Granted);
        Assert.Equal("n2", cluster.Stores["n1"].HardState.VotedFor);
    }

    [Theory]
    [InlineData(100, 1, false)]
    [InlineData(1, 2, false)]
    [InlineData(2, 2, true)]
    [InlineData(1, 3, true)]
    public void Voting_compares_last_term_before_log_length(long index, long term, bool expected)
    {
        using var cluster = new SimCluster();
        var store = cluster.Stores["n1"];
        store.SaveHardState(new HardState(3));
        store.Append(new[] { new LogEntry(1, 1, "a"u8), new LogEntry(2, 2, "b"u8) });
        cluster.Restart("n1");
        var vote = (VoteResponse)cluster.Nodes["n1"].HandleRequest("n2", new VoteRequest(4, index, term));
        Assert.Equal(expected, vote.Granted);
    }

    [Fact]
    public void Old_heartbeats_do_not_suppress_elections_or_replace_known_leader()
    {
        using var cluster = new SimCluster();
        var node = cluster.Nodes["n1"];
        node.HandleRequest("n2", new AppendRequest(8, 1, 0, 0, 0, Array.Empty<LogEntry>()));
        for (var tick = 0; tick < 30; tick++)
        {
            var reply = (AppendResponse)node.HandleRequest("n3", new AppendRequest(7, 2, 0, 0, 0, Array.Empty<LogEntry>()));
            Assert.False(reply.Success);
            node.Tick();
        }
        Assert.Equal(RaftRole.Candidate, node.Role);
        Assert.True(node.Term > 8);
        Assert.Null(node.LeaderId);
    }

    [Fact]
    public void Split_vote_is_resolved_by_later_randomized_election()
    {
        using var cluster = new SimCluster(4);
        cluster.Nodes["n1"].Campaign();
        cluster.Nodes["n2"].Campaign();
        cluster.Nodes["n3"].HandleRequest("n1", new VoteRequest(1, 0, 0));
        cluster.Nodes["n4"].HandleRequest("n2", new VoteRequest(1, 0, 0));
        cluster.Pump();
        Assert.Empty(cluster.Leaders);
        cluster.Tick(100);
        Assert.Single(cluster.Leaders);
    }

    [Fact]
    public void Isolated_leader_steps_down_without_a_quorum()
    {
        using var cluster = new SimCluster();
        cluster.Tick(50);
        var leader = cluster.Leader;
        var others = cluster.Nodes.Keys.Where(id => id != leader).ToArray();
        cluster.Partition(new[] { leader }, others);
        var index = cluster.Propose(leader, "minority"u8.ToArray());
        cluster.Tick(60);
        Assert.NotEqual(RaftRole.Leader, cluster.Nodes[leader].Role);
        Assert.True(cluster.Nodes[leader].CommitIndex < index);
        Assert.Contains(cluster.Leaders, id => id != leader);
    }
}
