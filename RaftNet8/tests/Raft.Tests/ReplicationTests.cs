using Raft.Core;
using Raft.Storage;

namespace Raft.Tests;

public sealed class ReplicationTests
{
    [Fact]
    public void Replicates_a_batched_ordered_log_and_propagates_commit()
    {
        using var cluster = new SimCluster(maxAppendEntries: 8);
        cluster.Tick(50);
        var leader = cluster.Leader;
        var commands = Enumerable.Range(0, 8).Select(i => new ProposedCommand(BitConverter.GetBytes(i))).ToArray();
        var start = cluster.Nodes[leader].Propose(commands);
        cluster.Tick(5);
        foreach (var id in cluster.Nodes.Keys)
        {
            Assert.Equal(start + 7, cluster.Nodes[id].CommitIndex);
            var entries = cluster.Stores[id].Read(start, 8, 1024);
            Assert.Equal(Enumerable.Range(0, 8), entries.Select(e => BitConverter.ToInt32(e.Command.Span)));
        }
    }

    [Fact]
    public void Heartbeat_commit_is_bounded_by_the_prefix_matched_in_that_rpc()
    {
        using var cluster = new SimCluster();
        var store = cluster.Stores["n1"];
        store.SaveHardState(new HardState(3));
        store.Append(new[] { new LogEntry(1, 1, "a"u8), new LogEntry(2, 2, "uncommitted"u8) });
        var reply = (AppendResponse)cluster.Nodes["n1"].HandleRequest("n2",
            new AppendRequest(3, 1, 1, 1, 2, Array.Empty<LogEntry>()));
        Assert.True(reply.Success);
        Assert.Equal(1, reply.MatchIndex);
        Assert.Equal(1, store.HardState.CommitIndex);
        Assert.Equal(2, store.LastIndex);
    }

    [Fact]
    public void Matching_shorter_batch_keeps_existing_suffix()
    {
        using var cluster = new SimCluster();
        var node = cluster.Nodes["n1"];
        node.HandleRequest("n2", new AppendRequest(2, 1, 0, 0, 0,
            new[] { new LogEntry(1, 1, "a"u8), new LogEntry(2, 2, "b"u8) }));
        node.HandleRequest("n2", new AppendRequest(2, 2, 0, 0, 0, new[] { new LogEntry(1, 1, "a"u8) }));
        Assert.Equal(2, node.LastIndex);
    }

    [Fact]
    public void Conflict_replaces_only_the_uncommitted_suffix()
    {
        using var cluster = new SimCluster();
        var node = cluster.Nodes["n1"];
        node.HandleRequest("n2", new AppendRequest(2, 1, 0, 0, 1,
            new[] { new LogEntry(1, 1, "safe"u8), new LogEntry(2, 2, "old"u8), new LogEntry(3, 2, "old-tail"u8) }));
        var response = (AppendResponse)node.HandleRequest("n3", new AppendRequest(3, 2, 1, 1, 2,
            new[] { new LogEntry(2, 3, "replacement"u8) }));
        Assert.True(response.Success);
        Assert.Equal(2, node.LastIndex);
        Assert.Equal("replacement"u8.ToArray(), cluster.Stores["n1"].Read(2, 1, 1024)[0].Command.ToArray());
        Assert.Equal(2, node.CommitIndex);
    }

    [Fact]
    public void Committed_entries_cannot_be_replaced()
    {
        using var cluster = new SimCluster();
        var node = cluster.Nodes["n1"];
        node.HandleRequest("n2", new AppendRequest(2, 1, 0, 0, 1, new[] { new LogEntry(1, 2, "safe"u8) }));
        var response = (AppendResponse)node.HandleRequest("n3", new AppendRequest(3, 2, 0, 0, 1,
            new[] { new LogEntry(1, 3, "invalid"u8) }));
        Assert.False(response.Success);
        Assert.Equal(2, cluster.Stores["n1"].GetTerm(1));
        Assert.Equal(1, node.CommitIndex);
    }

    [Fact]
    public void Figure8_prior_term_majority_does_not_commit_without_a_current_term_entry()
    {
        using var cluster = new SimCluster(5, maxAppendEntries: 1);
        var store = cluster.Stores["n1"];
        store.SaveHardState(new HardState(3));
        store.Append(new[] { new LogEntry(1, 1, "a"u8), new LogEntry(2, 2, "old-term"u8) });
        cluster.Restart("n1");
        var node = cluster.Nodes["n1"];
        node.Campaign(); // term 4, then the election no-op at index 3
        node.HandleResponse("n2", new VoteResponse(4, true));
        node.HandleResponse("n3", new VoteResponse(4, true));
        var initial = DrainAppends(node);
        var noOps = new Dictionary<string, AppendRequest>();
        foreach (var id in new[] { "n2", "n3" })
        {
            node.HandleResponse(id, new AppendResponse(4, initial[id].RequestId, false, 0, 2));
            var repair = DrainAppends(node)[id];
            Assert.Equal(2, repair.Entries[^1].Index);
            node.HandleResponse(id, new AppendResponse(4, repair.RequestId, true, 2));
            noOps[id] = DrainAppends(node)[id];
        }
        Assert.Equal(0, node.CommitIndex);
        node.HandleResponse("n2", new AppendResponse(4, noOps["n2"].RequestId, true, 3));
        Assert.Equal(0, node.CommitIndex);
        node.HandleResponse("n3", new AppendResponse(4, noOps["n3"].RequestId, true, 3));
        Assert.Equal(3, node.CommitIndex);
        Assert.True(store.Read(3, 1, 1024)[0].IsNoOp);
    }

    [Fact]
    public void Delayed_duplicate_append_response_cannot_regress_peer_progress()
    {
        using var cluster = new SimCluster();
        var node = cluster.Nodes["n1"];
        node.Campaign();
        node.HandleResponse("n2", new VoteResponse(1, true));
        var initial = DrainAppends(node)["n2"];
        node.HandleResponse("n2", new AppendResponse(1, initial.RequestId, true, 1));
        var next = DrainAppends(node)["n2"];
        node.HandleResponse("n2", new AppendResponse(1, initial.RequestId, false, 0, 1));
        node.HandleResponse("n2", new AppendResponse(1, next.RequestId, true, next.PreviousIndex + next.Entries.Count));
        cluster.Propose("n1", "next"u8.ToArray());
        var append = DrainAppends(node)["n2"];
        Assert.Equal(1, append.PreviousIndex);
        Assert.Equal(2, append.Entries[0].Index);
    }

    [Fact]
    public void Lagging_follower_catches_up_using_bounded_batches()
    {
        using var cluster = new SimCluster(maxAppendEntries: 4);
        cluster.Tick(50);
        var leader = cluster.Leader;
        var follower = cluster.Nodes.Keys.First(id => id != leader);
        cluster.Offline.Add(follower);
        for (var i = 0; i < 100; i++) { cluster.Propose(leader, BitConverter.GetBytes(i)); cluster.Tick(2); }
        cluster.Restart(follower);
        cluster.Tick(100);
        var committed = cluster.Nodes[cluster.Leader].CommitIndex;
        foreach (var node in cluster.Nodes.Values) Assert.Equal(committed, node.CommitIndex);
    }

    [Fact]
    public void Partition_failover_replaces_orphaned_entries_and_preserves_committed_prefix()
    {
        using var cluster = new SimCluster(5);
        cluster.Tick(60);
        var old = cluster.Leader;
        cluster.Propose(old, "before"u8.ToArray());
        cluster.Tick(3);
        var minority = new[] { old, cluster.Nodes.Keys.First(id => id != old) };
        var majority = cluster.Nodes.Keys.Except(minority).ToArray();
        cluster.Partition(minority, majority);
        cluster.Propose(old, "orphan"u8.ToArray());
        cluster.Tick(80);
        var leader = Assert.Single(cluster.Leaders, majority.Contains);
        cluster.Propose(leader, "after"u8.ToArray());
        cluster.Tick(5);
        cluster.Heal();
        cluster.Tick(100);
        foreach (var (id, store) in cluster.Stores)
        {
            var commands = store.Read(1, 100, 1024 * 1024).Where(e => !e.IsNoOp).Select(e => System.Text.Encoding.UTF8.GetString(e.Command.Span));
            Assert.Equal(new[] { "before", "after" }, commands);
            Assert.Equal(cluster.Nodes[cluster.Leader].CommitIndex, cluster.Nodes[id].CommitIndex);
        }
    }

    private static Dictionary<string, AppendRequest> DrainAppends(RaftCore core)
    {
        var result = new Dictionary<string, AppendRequest>();
        while (core.TryDequeue(out var rpc)) if (rpc.Request is AppendRequest append) result[rpc.PeerId] = append;
        return result;
    }
}
