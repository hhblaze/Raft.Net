using Raft.Core;
using Raft.Storage;

namespace Raft.Tests;

public sealed class ReadIndexAndTransferTests
{
    [Fact]
    public async Task Delayed_rejection_of_old_TimeoutNow_cannot_abort_transfer_after_the_target_started_its_election()
    {
        using var cluster = new SimCluster();
        cluster.Tick(80);
        var oldId = cluster.Leader;
        var old = cluster.Nodes[oldId];
        var targetId = cluster.Nodes.Keys.First(id => id != oldId);
        var target = cluster.Nodes[targetId];
        await old.TransferAsync(42, targetId);
        TimeoutNowRequest? timeout = null;
        while (old.TryDequeue(out var rpc)) if (rpc.Request is TimeoutNowRequest request) timeout = request;
        Assert.NotNull(timeout);
        Assert.True(Assert.IsType<TimeoutNowResponse>(target.HandleRequest(oldId, timeout)).Accepted);
        // A duplicate request reaches the target after it has already incremented its term.
        var delayedRejection = target.HandleRequest(oldId, timeout);
        Assert.False(Assert.IsType<TimeoutNowResponse>(delayedRejection).Accepted);
        old.HandleRequest(targetId, new VoteRequest(target.Term, target.LastIndex, target.State.LastTerm));
        old.HandleResponse(targetId, delayedRejection);
        Assert.False(old.TryDequeueControl(out _));
        cluster.Pump();
        Assert.Equal(targetId, cluster.Leader);
        Assert.True(old.TryDequeueControl(out var result));
        var complete = Assert.IsType<ControlResult.Transfer>(result);
        Assert.Equal(42, complete.ClientId);
        Assert.Null(complete.Error);
    }

    [Fact]
    public async Task Concurrent_reads_use_fresh_quorums_without_adding_log_entries_or_writes()
    {
        await using var cluster = new RuntimeCluster(modern: true);
        await cluster.StartAsync();
        var leader = await cluster.LeaderAsync();
        var write = await leader.ProposeAsync("value"u8.ToArray());
        var store = Assert.IsType<DBreezeLogStorage>(cluster.Stores[leader.Status.NodeId]);
        var last = store.LastIndex;
        var transactions = store.Metrics.WriteTransactions;
        var results = await Task.WhenAll(Enumerable.Range(0, 256).Select(_ => leader.ReadIndexAsync()))
            .WaitAsync(TimeSpan.FromSeconds(10));
        Assert.All(results, read => Assert.True(read.Index >= write.Index && read.Term == leader.Status.Term));
        Assert.Equal(last, store.LastIndex);
        Assert.Equal(transactions, store.Metrics.WriteTransactions);
        Assert.Null(leader.Status.Fault);
    }

    [Fact]
    public async Task Read_fence_waits_for_local_application_after_a_fresh_quorum()
    {
        await using var cluster = new RuntimeCluster(modern: true);
        await cluster.StartAsync();
        var leader = await cluster.LeaderAsync();
        await leader.ProposeAsync("initial"u8.ToArray());
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        cluster.Machines[leader.Status.NodeId].BeforeApply = async (entry, token) =>
        {
            if (entry.IsNoOp) return;
            entered.TrySetResult();
            await release.Task.WaitAsync(token);
        };
        var write = leader.ProposeAsync("later"u8.ToArray());
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var read = leader.ReadIndexAsync();
        try
        {
            await Task.Delay(50);
            Assert.False(read.IsCompleted);
        }
        finally { release.TrySetResult(); }
        var written = await write.WaitAsync(TimeSpan.FromSeconds(5));
        var fence = await read.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.True(fence.Index >= written.Index);
        Assert.True(leader.Status.LastApplied >= fence.Index);
    }

    [Fact]
    public async Task Old_or_missing_read_context_cannot_satisfy_a_new_read()
    {
        using var cluster = new SimCluster();
        cluster.Tick(80);
        var id = cluster.Leader;
        var leader = cluster.Nodes[id];
        await leader.ReadIndexAsync([1]);
        var sent = new List<OutboundRpc>();
        while (leader.TryDequeue(out var rpc)) sent.Add(rpc);
        var request = Assert.IsType<AppendRequest>(sent[0].Request);
        Assert.True(request.ReadContext > 0);
        leader.HandleResponse(sent[0].PeerId, new AppendResponse(leader.Term, request.RequestId, true,
            request.PreviousIndex + request.Entries.Count) { ReadContext = 0 });
        Assert.False(leader.TryDequeueControl(out _));
        leader.Tick();
        var fresh = new List<OutboundRpc>();
        while (leader.TryDequeue(out var rpc)) fresh.Add(rpc);
        var newRequest = fresh.First(r => r.PeerId == sent[0].PeerId);
        leader.HandleResponse(newRequest.PeerId, cluster.Nodes[newRequest.PeerId].HandleRequest(id, newRequest.Request));
        Assert.True(leader.TryDequeueControl(out var result));
        Assert.Equal(1, Assert.IsType<ControlResult.Read>(result).ClientId);
    }

    [Fact]
    public async Task A_new_leader_cannot_serve_reads_before_its_current_term_entry_commits()
    {
        using var cluster = new SimCluster();
        var leader = cluster.Nodes["n1"];
        leader.Campaign();
        leader.HandleResponse("n2", new VoteResponse(1, true));
        await leader.ReadIndexAsync([7]);
        Assert.Equal(0, leader.CommitIndex);
        Assert.False(leader.TryDequeueControl(out _));
        cluster.Pump();
        Assert.Equal(1, leader.CommitIndex);
        Assert.True(leader.TryDequeueControl(out var result));
        Assert.Equal(7, Assert.IsType<ControlResult.Read>(result).ClientId);
    }

    [Fact]
    public async Task Isolated_old_leader_cannot_complete_ReadIndex()
    {
        await using var cluster = new RuntimeCluster(modern: true);
        await cluster.StartAsync();
        var leader = await cluster.LeaderAsync();
        await leader.ProposeAsync("known"u8.ToArray());
        cluster.Isolate(leader.Status.NodeId);
        var error = await Record.ExceptionAsync(() => leader.ReadIndexAsync().WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.True(error is NotLeaderException or TimeoutException, error?.ToString());
        Assert.Null(leader.Status.Fault);
    }

    [Theory]
    [InlineData(3)]
    [InlineData(5)]
    public async Task Leadership_transfer_over_real_TCP_and_disk_elects_the_requested_voter(int count)
    {
        await using var cluster = new RuntimeCluster(count, disk: true, tcp: true, modern: true);
        await cluster.StartAsync();
        var leader = await cluster.LeaderAsync();
        var target = cluster.Nodes.Keys.First(id => id != leader.Status.NodeId);
        await leader.ProposeAsync("before-transfer"u8.ToArray());
        var term = leader.Status.Term;
        await leader.TransferLeadershipAsync(target).WaitAsync(TimeSpan.FromSeconds(10));
        var replacement = cluster.Nodes[target];
        await RuntimeCluster.EventuallyAsync(() => replacement.Status.Role == RaftRole.Leader && replacement.Status.Term > term);
        var write = await replacement.ProposeAsync("after-transfer"u8.ToArray());
        await RuntimeCluster.EventuallyAsync(() => cluster.Nodes.Values.All(n => n.Status.LastApplied >= write.Index));
        Assert.All(cluster.Nodes.Values, node => Assert.Null(node.Status.Fault));
    }

    [Fact]
    public async Task Failed_transfer_times_out_reopens_proposals_and_does_not_fault_the_node()
    {
        await using var cluster = new RuntimeCluster(modern: true);
        await cluster.StartAsync();
        var leader = await cluster.LeaderAsync();
        var target = cluster.Nodes.Keys.First(id => id != leader.Status.NodeId);
        cluster.Isolate(target);
        await leader.ProposeAsync("not-on-target"u8.ToArray());
        var transfer = leader.TransferLeadershipAsync(target);
        await RuntimeCluster.EventuallyAsync(() => leader.Status.LeadershipTransferTarget == target);
        await Assert.ThrowsAsync<LeadershipTransferInProgressException>(() => leader.ProposeAsync("blocked"u8.ToArray()));
        await Assert.ThrowsAsync<TimeoutException>(() => transfer.WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.Null(leader.Status.LeadershipTransferTarget);
        Assert.Null(leader.Status.Fault);
        Assert.Equal(RaftRole.Leader, leader.Status.Role);
        await leader.ProposeAsync("resumed"u8.ToArray());
    }

    [Fact]
    public async Task Invalid_transfer_target_is_a_client_error_and_does_not_stop_consensus()
    {
        await using var cluster = new RuntimeCluster(modern: true);
        await cluster.StartAsync();
        var leader = await cluster.LeaderAsync();
        await Assert.ThrowsAsync<ArgumentException>(() => leader.TransferLeadershipAsync("unknown"));
        Assert.Null(leader.Status.Fault);
        await leader.ProposeAsync("still-usable"u8.ToArray());
    }
}
