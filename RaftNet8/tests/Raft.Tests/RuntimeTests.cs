using Raft.Storage;

namespace Raft.Tests;

public sealed class RuntimeTests
{
    [Fact]
    public async Task Followers_reject_proposals_and_report_the_known_leader()
    {
        await using var cluster = new RuntimeCluster();
        await cluster.StartAsync();
        var leader = await cluster.LeaderAsync();
        var follower = cluster.Nodes.Values.First(node => node != leader);
        await RuntimeCluster.EventuallyAsync(() => follower.Status.LeaderId == leader.Status.NodeId);
        var error = await Assert.ThrowsAsync<NotLeaderException>(() => follower.ProposeAsync("test"u8.ToArray()));
        Assert.Equal(leader.Status.NodeId, error.LeaderId);
    }

    [Fact]
    public async Task Proposal_completes_only_after_local_application_without_blocking_heartbeats()
    {
        await using var cluster = new RuntimeCluster();
        await cluster.StartAsync();
        var leader = await cluster.LeaderAsync();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        cluster.Machines[leader.Status.NodeId].BeforeApply = async (entry, token) =>
        {
            if (!entry.IsNoOp) { entered.TrySetResult(); await release.Task.WaitAsync(token); }
        };
        var proposal = leader.ProposeAsync("blocked-application"u8.ToArray());
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.False(proposal.IsCompleted);
        var term = leader.Status.Term;
        await Task.Delay(500);
        Assert.Equal(RaftRole.Leader, leader.Status.Role);
        Assert.Equal(term, leader.Status.Term);
        release.SetResult();
        var result = await proposal.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(result.Index, leader.Status.LastApplied);
    }

    [Fact]
    public async Task Cancellation_after_append_does_not_undo_committed_command_and_keeps_admission_bounded()
    {
        await using var cluster = new RuntimeCluster(1, pending: 1);
        await cluster.StartAsync();
        var leader = await cluster.LeaderAsync();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        cluster.Machines["n1"].BeforeApply = async (entry, token) =>
        {
            if (!entry.IsNoOp) { entered.TrySetResult(); await release.Task.WaitAsync(token); }
        };
        using var cancel = new CancellationTokenSource();
        var first = leader.ProposeAsync("durable"u8.ToArray(), cancel.Token);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        cancel.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => first);
        Assert.Equal(1, leader.Metrics.PendingProposals);
        var before = leader.Status.LastLogIndex;
        using var secondCancel = new CancellationTokenSource(100);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => leader.ProposeAsync("never-admitted"u8.ToArray(), secondCancel.Token));
        Assert.Equal(before, leader.Status.LastLogIndex);
        release.SetResult();
        await RuntimeCluster.EventuallyAsync(() => leader.Metrics.PendingProposals == 0);
        Assert.Single(cluster.Machines["n1"].Entries, entry => !entry.IsNoOp);
    }

    [Fact]
    public async Task Concurrent_load_applies_identical_ordered_commands_with_bounded_pending_work()
    {
        await using var cluster = new RuntimeCluster(pending: 64);
        await cluster.StartAsync();
        var leader = await cluster.LeaderAsync();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var tasks = Enumerable.Range(0, 2000).Select(i => leader.ProposeAsync(BitConverter.GetBytes(i), timeout.Token)).ToArray();
        Assert.InRange(leader.Metrics.PendingProposals, 0, 64);
        var results = await Task.WhenAll(tasks);
        Assert.Equal(2000, results.Select(r => r.Index).Distinct().Count());
        var barrier = await leader.ReadBarrierAsync(timeout.Token);
        await RuntimeCluster.EventuallyAsync(() => cluster.Nodes.Values.All(node => node.Status.LastApplied >= barrier.Index));
        var expected = cluster.Machines[leader.Status.NodeId].Entries.Where(e => !e.IsNoOp).Select(e => BitConverter.ToInt32(e.Command.Span)).ToArray();
        Assert.Equal(2000, expected.Length);
        foreach (var machine in cluster.Machines.Values)
            Assert.Equal(expected, machine.Entries.Where(e => !e.IsNoOp).Select(e => BitConverter.ToInt32(e.Command.Span)));
        Assert.Equal(0, leader.Metrics.PendingProposals);
        Assert.InRange(leader.Metrics.InFlightRpcs, 0, 2);
    }

    [Fact]
    public async Task Minority_proposal_fails_when_check_quorum_removes_leadership()
    {
        await using var cluster = new RuntimeCluster();
        await cluster.StartAsync();
        var old = await cluster.LeaderAsync();
        cluster.Isolate(old.Status.NodeId);
        var proposal = old.ProposeAsync("cannot-commit"u8.ToArray());
        await Assert.ThrowsAsync<NotLeaderException>(() => proposal.WaitAsync(TimeSpan.FromSeconds(5)));
        var replacement = await cluster.LeaderAsync(old.Status.NodeId);
        Assert.NotEqual(old.Status.NodeId, replacement.Status.NodeId);
        cluster.Heal();
        var barrier = await replacement.ReadBarrierAsync();
        await RuntimeCluster.EventuallyAsync(() => cluster.Nodes.Values.All(node => node.Status.LastApplied >= barrier.Index));
        Assert.All(cluster.Machines.Values, machine => Assert.DoesNotContain(machine.Entries, e => e.Command.Span.SequenceEqual("cannot-commit"u8)));
    }

    [Fact]
    public async Task Application_failure_faults_the_node_and_pending_proposal()
    {
        await using var cluster = new RuntimeCluster(1);
        await cluster.StartAsync();
        var leader = await cluster.LeaderAsync();
        cluster.Machines["n1"].BeforeApply = (entry, _) => entry.IsNoOp ? ValueTask.CompletedTask : throw new InvalidOperationException("apply failed");
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => leader.ProposeAsync("fail"u8.ToArray()));
        Assert.Equal("apply failed", error.Message);
        await Assert.ThrowsAsync<InvalidOperationException>(() => leader.Completion);
        Assert.NotNull(leader.Status.Fault);
        Assert.Equal(0, leader.Metrics.PendingProposals);
    }

    [Fact]
    public async Task Fresh_application_replays_the_committed_disk_log_after_node_restart()
    {
        await using var cluster = new RuntimeCluster(1, disk: true);
        await cluster.StartAsync();
        var leader = await cluster.LeaderAsync();
        await leader.ProposeAsync("one"u8.ToArray());
        await leader.ProposeAsync("two"u8.ToArray());
        await cluster.RestartAsync("n1");
        leader = await cluster.LeaderAsync();
        await leader.ReadBarrierAsync();
        Assert.Equal(new[] { "one", "two" }, cluster.Machines["n1"].Entries.Where(e => !e.IsNoOp)
            .Select(e => System.Text.Encoding.UTF8.GetString(e.Command.Span)));
    }

    [Fact]
    public async Task Malformed_inbound_requests_are_rejected_without_faulting_the_node()
    {
        await using var cluster = new RuntimeCluster();
        await cluster.StartAsync();
        var node = cluster.Nodes["n1"];
        await Assert.ThrowsAsync<ArgumentException>(() => node.HandleAsync("outsider", new VoteRequest(999, 0, 0)).AsTask());
        await Assert.ThrowsAsync<ArgumentException>(() => node.HandleAsync("n2", new AppendRequest(999, 1, 0, 0, 0,
            new[] { new LogEntry(2, 1, "gap"u8) })).AsTask());
        Assert.True(node.Status.Term < 999);
        Assert.Null(node.Status.Fault);
        await cluster.LeaderAsync();
    }

    [Fact]
    public async Task Shutdown_unblocks_pending_application_and_admission_waiters()
    {
        await using var cluster = new RuntimeCluster(1, pending: 1);
        await cluster.StartAsync();
        var leader = await cluster.LeaderAsync();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        cluster.Machines["n1"].BeforeApply = async (entry, token) =>
        {
            if (!entry.IsNoOp) { entered.TrySetResult(); await Task.Delay(Timeout.Infinite, token); }
        };
        var first = leader.ProposeAsync("one"u8.ToArray());
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var second = leader.ProposeAsync("two"u8.ToArray());
        await leader.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.ThrowsAsync<ObjectDisposedException>(() => first);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => second);
        await leader.Completion;
    }
}
