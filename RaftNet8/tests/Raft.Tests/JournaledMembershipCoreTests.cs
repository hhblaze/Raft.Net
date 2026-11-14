using Raft.Core;
using Raft.Storage;

namespace Raft.Tests;

public sealed class JournaledMembershipCoreTests
{
    private sealed class Cluster : IDisposable
    {
        public readonly Dictionary<string, RaftCore> Nodes = new();
        public readonly Dictionary<string, DBreezeLogStorage> Stores = new();
        public readonly Dictionary<string, RaftOptions> Options = new();
        public readonly HashSet<string> Isolated = new(StringComparer.Ordinal);
        public bool RefuseTransfers;
        public Cluster(int voters = 3, int maxCommandBytes = 64 * 1024)
        {
            var members = Enumerable.Range(1, voters).Select(i => "n" + i).ToArray();
            foreach (var id in Enumerable.Range(1, voters + 2).Select(i => "n" + i))
            {
                var options = new RaftOptions { NodeId = id, WireVersion = 3, Members = members, MaxCommandBytes = maxCommandBytes,
                    JoiningNode = !members.Contains(id),
                    InitialPeers = members.Select((peer, index) => new PeerDescriptor(peer, "127.0.0.1", 7301 + index)).ToArray() };
                Options[id] = options;
                Stores[id] = DBreezeLogStorage.CreateInMemory(options, DBreezeLogLayout.UnifiedTableV4);
                Nodes[id] = new(options, Stores[id], new Random(7));
            }
        }
        public RaftCore Elect(string id = "n1")
        {
            Nodes[id].Campaign();
            Pump();
            Assert.True(Nodes[id].Role == RaftRole.Leader, id + ": " + string.Join("; ", Nodes.Select(pair =>
                $"{pair.Key}:{pair.Value.Role},term={pair.Value.State.HardState.Term},vote={pair.Value.State.HardState.VotedFor},tail={pair.Value.LastIndex},cfg={string.Join(',', pair.Value.State.Configuration.Voters)},commit={pair.Value.CommitIndex}")));
            Assert.True(Nodes[id].State.HasCommittedCurrentTerm);
            return Nodes[id];
        }
        public void Pump(Func<bool>? until = null)
        {
            for (var step = 0; step < 10000; step++)
            {
                if (until?.Invoke() == true) return;
                var sent = false;
                foreach (var (id, node) in Nodes)
                    while (node.TryDequeue(out var rpc))
                    {
                        sent = true;
                        if (!Isolated.Contains(id) && !Isolated.Contains(rpc.PeerId) && Nodes.TryGetValue(rpc.PeerId, out var target))
                        {
                            if (RefuseTransfers && rpc.Request is TimeoutNowRequest) node.ReportUnreachable(rpc.PeerId, rpc.Request);
                            else node.HandleResponse(rpc.PeerId, target.HandleRequest(id, rpc.Request));
                        }
                        else node.ReportUnreachable(rpc.PeerId, rpc.Request);
                        if (until?.Invoke() == true) return;
                    }
                if (!sent) return;
            }
            throw new InvalidOperationException("Membership pump did not converge.");
        }
        public void Tick()
        {
            foreach (var (id, node) in Nodes) if (!Isolated.Contains(id)) node.Tick();
            Pump();
        }
        public void Dispose() { foreach (var store in Stores.Values) store.Dispose(); }
    }

    private static MembershipOperationStatus Add(string id = "n4") => new(Guid.NewGuid(), MembershipOperationKind.AddNode, id,
        new(id, "127.0.0.1", id == "n4" ? 7304 : 7305), MembershipOperationStage.Accepted, 0, 0);
    private static ControlResult.Membership Result(RaftCore node, long client)
    {
        while (node.TryDequeueControl(out var control))
            if (control is ControlResult.Membership membership && membership.ClientId == client) return membership;
        throw new InvalidOperationException("Missing membership result.");
    }

    [Fact]
    public async Task Service_and_configuration_payloads_are_independent_of_the_business_command_size_limit()
    {
        using var cluster = new Cluster(maxCommandBytes: 1);
        var leader = cluster.Elect();
        var request = Add();
        await leader.StartMembershipAsync(1, request, default);
        Assert.Null(Result(leader, 1).Error);
        cluster.Pump();
        Assert.Contains("n4", leader.State.Configuration.Voters);
        Assert.Equal(MembershipOperationStage.Completed, cluster.Stores["n1"].Query(new LogQuery.MembershipOperation(request.OperationId)).MembershipOperation!.Stage);
        Assert.Throws<ArgumentException>(() => leader.HandleRequest("n2",
            new AppendRequest(2, 1000, 0, 0, 0, [new LogEntry(1, 2, "bad JSON"u8, LogEntryKind.Membership)])));
        Assert.Null(leader.State.Fault);
    }

    [Theory]
    [InlineData("uncommitted intent")]
    [InlineData("accepted")]
    [InlineData("learner configuration")]
    [InlineData("catching up")]
    [InlineData("promoting")]
    [InlineData("voter configuration")]
    [InlineData("finalizing")]
    public async Task Add_recovers_from_leader_crash_at_each_durable_boundary(string boundary)
    {
        using var cluster = new Cluster();
        var old = cluster.Elect();
        var request = Add();
        await old.StartMembershipAsync(1, request, default);
        Result(old, 1);
        bool Reached() => boundary switch
        {
            "uncommitted intent" => old.State.Membership!.PendingOperationId == request.OperationId,
            "accepted" => old.State.Membership!.Committed.ActiveOperation?.Stage == MembershipOperationStage.Accepted,
            "learner configuration" => old.State.Configuration.Learners.Contains("n4") && old.State.Configuration.Index > old.CommitIndex,
            "catching up" => old.State.Membership!.Committed.ActiveOperation?.Stage == MembershipOperationStage.CatchingUp,
            "promoting" => old.State.Membership!.Committed.ActiveOperation?.Stage == MembershipOperationStage.Promoting,
            "voter configuration" => old.State.Configuration.IsVoter("n4") && old.State.Configuration.Index > old.CommitIndex,
            "finalizing" => old.State.Configuration.IsVoter("n4") && old.State.Configuration.Index <= old.CommitIndex &&
                old.State.Membership!.Committed.ActiveOperation is not null,
            _ => false
        };
        cluster.Pump(Reached);
        Assert.True(Reached(), boundary);
        cluster.Isolated.Add("n1");
        // Recreate the killed node from its durable store; all volatile continuations/outbound messages are lost.
        cluster.Nodes["n1"] = new(cluster.Options["n1"], cluster.Stores["n1"], new Random(8));
        var successor = cluster.Nodes.Values.Where(node => node.State.Options.NodeId != "n1" &&
                node.State.Configuration.IsVoter(node.State.Options.NodeId))
            .OrderByDescending(node => node.State.LastTerm).ThenByDescending(node => node.LastIndex)
            .ThenBy(node => node.State.Options.NodeId, StringComparer.Ordinal).First().State.Options.NodeId;
        var next = cluster.Elect(successor);
        await next.StartMembershipAsync(2, request, default); // reusing the ID is safe, including an uncommitted lost intent
        Assert.Null(Result(next, 2).Error);
        cluster.Pump();
        for (var i = 0; i < 100 && next.State.Membership!.Committed.ActiveOperation is not null; i++) cluster.Tick();
        var result = cluster.Stores[successor].Query(new LogQuery.MembershipOperation(request.OperationId)).MembershipOperation!;
        Assert.Equal(MembershipOperationStage.Completed, result.Stage);
        Assert.True(next.State.Configuration.IsVoter("n4"));
        cluster.Isolated.Remove("n1");
        for (var i = 0; i < 20; i++) cluster.Tick();
        Assert.Equal(next.State.Configuration.Voters.Order(), cluster.Nodes["n1"].State.Configuration.Voters.Order());
        Assert.Null(cluster.Nodes["n1"].State.Membership!.Committed.ActiveOperation);
    }

    [Theory]
    [InlineData("intent")]
    [InlineData("accepted")]
    [InlineData("transferring")]
    [InlineData("removing")]
    [InlineData("configuration")]
    [InlineData("finalizing")]
    public async Task Remove_recovers_leader_crash_including_both_sides_of_transfer(string boundary)
    {
        using var cluster = new Cluster(5);
        var old = cluster.Elect();
        var request = new MembershipOperationStatus(Guid.NewGuid(), MembershipOperationKind.RemoveNode, "n1", null,
            MembershipOperationStage.Accepted, 0, 0);
        await old.StartMembershipAsync(1, request, default);
        Result(old, 1);
        RaftCore? Current() => cluster.Nodes.Values.Where(node => node.Role == RaftRole.Leader).OrderByDescending(node => node.Term).FirstOrDefault();
        bool Reached()
        {
            var node = Current();
            if (node is null) return false;
            var active = node.State.Membership!.Committed.ActiveOperation;
            return boundary switch
            {
                "intent" => node.State.Membership.PendingOperationId == request.OperationId,
                "accepted" => active?.Stage == MembershipOperationStage.Accepted,
                "transferring" => active?.Stage == MembershipOperationStage.Transferring && node.State.Transfer is not null,
                "removing" => active?.Stage == MembershipOperationStage.Removing,
                "configuration" => !node.State.Configuration.Contains("n1") && node.State.Configuration.Index > node.CommitIndex,
                "finalizing" => !node.State.Configuration.Contains("n1") && node.State.Configuration.Index <= node.CommitIndex && active is not null,
                _ => false
            };
        }
        cluster.Pump(Reached);
        Assert.True(Reached(), boundary);
        var killed = Current()!.State.Options.NodeId;
        cluster.Isolated.Add(killed);
        cluster.Nodes[killed] = new(cluster.Options[killed], cluster.Stores[killed], new Random(8));
        var nextId = cluster.Nodes.Values.Where(node => node.State.Options.NodeId != killed &&
                node.State.Options.NodeId != "n1" && node.State.Configuration.IsVoter(node.State.Options.NodeId))
            .OrderByDescending(node => node.State.LastTerm).ThenByDescending(node => node.LastIndex)
            .ThenBy(node => node.State.Options.NodeId, StringComparer.Ordinal).First().State.Options.NodeId;
        var next = cluster.Elect(nextId);
        await next.StartMembershipAsync(2, request, default);
        Assert.Null(Result(next, 2).Error);
        cluster.Pump();
        for (var i = 0; i < 100 && next.State.Membership!.Committed.ActiveOperation is not null; i++) cluster.Tick();
        Assert.Equal(MembershipOperationStage.Completed, cluster.Stores[nextId].Query(new LogQuery.MembershipOperation(request.OperationId)).MembershipOperation!.Stage);
        Assert.DoesNotContain("n1", next.State.Configuration.Voters);
    }

    [Fact]
    public async Task Partitioned_former_leader_cannot_commit_a_competing_operation_and_its_suffix_reservation_rolls_back()
    {
        using var cluster = new Cluster();
        var old = cluster.Elect();
        cluster.Isolated.Add("n1");
        var lost = Add();
        await old.StartMembershipAsync(1, lost, default);
        Result(old, 1);
        var tail = old.LastIndex;
        await old.StartMembershipAsync(2, lost, default);
        Assert.Null(Result(old, 2).Error);
        Assert.Equal(tail, old.LastIndex);
        Assert.Null(cluster.Stores["n1"].Query(new LogQuery.MembershipOperation(lost.OperationId)).MembershipOperation);
        var next = cluster.Elect("n2");
        var winning = Add("n5");
        await next.StartMembershipAsync(3, winning, default);
        Result(next, 3);
        cluster.Pump();
        Assert.Equal(MembershipOperationStage.Completed, cluster.Stores["n2"].Query(new LogQuery.MembershipOperation(winning.OperationId)).MembershipOperation!.Stage);
        cluster.Isolated.Remove("n1");
        for (var i = 0; i < 30; i++) cluster.Tick();
        Assert.Null(cluster.Stores["n1"].Query(new LogQuery.MembershipOperation(lost.OperationId)).MembershipOperation);
        Assert.Null(old.State.Membership!.Committed.ActiveOperation);
        Assert.Null(old.State.Membership.PendingOperationId);
        Assert.Contains("n5", old.State.Configuration.Voters);
        await next.StartMembershipAsync(4, lost, default);
        Assert.Null(Result(next, 4).Error);
        cluster.Pump();
        Assert.Equal(MembershipOperationStage.Completed, cluster.Stores["n2"].Query(new LogQuery.MembershipOperation(lost.OperationId)).MembershipOperation!.Stage);
    }

    [Fact]
    public async Task Cancellation_after_final_configuration_append_is_too_late_and_does_not_undo_promotion()
    {
        using var cluster = new Cluster();
        var leader = cluster.Elect();
        var request = Add();
        await leader.StartMembershipAsync(1, request, default);
        Result(leader, 1);
        cluster.Pump(() => leader.State.Configuration.IsVoter("n4") && leader.State.Configuration.Index > leader.CommitIndex);
        Assert.True(leader.State.Configuration.IsVoter("n4"));
        await leader.CancelMembershipAsync(2, request.OperationId, default);
        var tooLate = Result(leader, 2);
        Assert.Equal("MembershipTooLate", tooLate.Error!.Type);
        Assert.Equal(request.OperationId, tooLate.Status!.OperationId);
        Assert.Equal(MembershipOperationStage.Promoting, tooLate.Status.Stage);
        cluster.Pump();
        Assert.Equal(MembershipOperationStage.Completed, cluster.Stores["n1"].Query(new LogQuery.MembershipOperation(request.OperationId)).MembershipOperation!.Stage);
    }

    [Fact]
    public async Task New_identity_is_registered_added_caught_up_and_promoted_without_bootstrap_provisioning()
    {
        using var cluster = new Cluster();
        var leader = cluster.Elect();
        Assert.DoesNotContain("n4", leader.State.Options.KnownPeers);
        var request = Add();
        await leader.StartMembershipAsync(1, request, default);
        Assert.Null(Result(leader, 1).Error);
        cluster.Pump();
        Assert.Contains("n4", leader.State.Configuration.Voters);
        Assert.Null(leader.State.Membership!.Committed.ActiveOperation);
        var result = cluster.Stores["n1"].Query(new LogQuery.MembershipOperation(request.OperationId)).MembershipOperation;
        Assert.Equal(MembershipOperationStage.Completed, result!.Stage);
        Assert.All(cluster.Nodes.Values.Where(node => node.State.Configuration.IsVoter(node.State.Options.NodeId)),
            node => Assert.Contains("n4", node.State.Options.KnownPeers));
        var tail = leader.LastIndex;
        await leader.StartMembershipAsync(2, request, default);
        Assert.Equal(MembershipOperationStage.Completed, Result(leader, 2).Status!.Stage);
        Assert.Equal(tail, leader.LastIndex);
        await leader.CancelMembershipAsync(3, request.OperationId, default);
        var completedCancellation = Result(leader, 3);
        Assert.Equal("MembershipTooLate", completedCancellation.Error!.Type);
        Assert.Equal(MembershipOperationStage.Completed, completedCancellation.Status!.Stage);
        Assert.Equal(tail, leader.LastIndex);
    }

    [Fact]
    public async Task Whole_operation_is_exclusive_and_cancel_removes_its_learner_before_releasing_lane()
    {
        using var cluster = new Cluster();
        var leader = cluster.Elect();
        cluster.Isolated.Add("n4");
        var request = Add();
        await leader.StartMembershipAsync(1, request, default);
        Result(leader, 1);
        cluster.Pump();
        Assert.Equal(MembershipOperationStage.CatchingUp, leader.State.Membership!.Committed.ActiveOperation!.Stage);
        var competing = Add("n5");
        await leader.StartMembershipAsync(2, competing, default);
        Assert.Equal("MembershipBusy", Result(leader, 2).Error!.Type);
        await leader.StartMembershipAsync(3, request with { Peer = new("n4", "localhost", 7304) }, default);
        Assert.Equal("MembershipConflict", Result(leader, 3).Error!.Type);
        await leader.CancelMembershipAsync(4, request.OperationId, default);
        Result(leader, 4);
        cluster.Pump();
        Assert.DoesNotContain("n4", leader.State.Configuration.AllMembers);
        Assert.Null(leader.State.Membership.Committed.ActiveOperation);
        Assert.Equal(MembershipOperationStage.Cancelled,
            cluster.Stores["n1"].Query(new LogQuery.MembershipOperation(request.OperationId)).MembershipOperation!.Stage);
        await leader.CancelMembershipAsync(40, request.OperationId, default);
        Assert.Equal(MembershipOperationStage.Cancelled, Result(leader, 40).Status!.Stage);
        await leader.StartMembershipAsync(5, competing, default);
        Assert.Null(Result(leader, 5).Error);
        cluster.Pump();
        Assert.Contains("n5", leader.State.Configuration.Voters);
    }

    [Fact]
    public async Task Elected_successor_resumes_durable_Add_without_restarting_or_resubmitting_the_operation()
    {
        using var cluster = new Cluster();
        var old = cluster.Elect();
        cluster.Isolated.Add("n4");
        var request = Add();
        await old.StartMembershipAsync(1, request, default);
        Result(old, 1);
        cluster.Pump();
        cluster.Isolated.Add("n1");
        var leader = cluster.Elect("n2");
        Assert.Equal(request.OperationId, leader.State.Membership!.Committed.ActiveOperation!.OperationId);
        cluster.Isolated.Remove("n4");
        for (var tick = 0; tick < 100 && !leader.State.Configuration.IsVoter("n4"); tick++) cluster.Tick();
        cluster.Pump();
        Assert.Contains("n4", leader.State.Configuration.Voters);
        Assert.Null(leader.State.Membership.Committed.ActiveOperation);
        Assert.Equal(MembershipOperationStage.Completed,
            cluster.Stores["n2"].Query(new LogQuery.MembershipOperation(request.OperationId)).MembershipOperation!.Stage);
    }

    [Fact]
    public async Task Removing_leader_transfers_first_and_successor_commits_the_same_operation()
    {
        using var cluster = new Cluster();
        var old = cluster.Elect();
        var request = new MembershipOperationStatus(Guid.NewGuid(), MembershipOperationKind.RemoveNode, "n1", null,
            MembershipOperationStage.Accepted, 0, 0);
        await old.StartMembershipAsync(1, request, default);
        Result(old, 1);
        cluster.Pump();
        var leader = cluster.Nodes.Values.Single(node => node.Role == RaftRole.Leader);
        Assert.NotSame(old, leader);
        Assert.Equal(RaftRole.Removed, old.Role);
        Assert.DoesNotContain("n1", leader.State.Configuration.Voters);
        Assert.Null(leader.State.Membership!.Committed.ActiveOperation);
        var result = cluster.Stores[leader.State.Options.NodeId].Query(new LogQuery.MembershipOperation(request.OperationId)).MembershipOperation!;
        Assert.Equal(MembershipOperationStage.Completed, result.Stage);
        Assert.Equal(request.OperationId, result.OperationId);
    }

    [Fact]
    public async Task Failed_transfer_records_failure_and_keeps_the_original_leader_in_the_configuration()
    {
        using var cluster = new Cluster { RefuseTransfers = true };
        var leader = cluster.Elect();
        var request = new MembershipOperationStatus(Guid.NewGuid(), MembershipOperationKind.RemoveNode, "n1", null,
            MembershipOperationStage.Accepted, 0, 0);
        await leader.StartMembershipAsync(1, request, default);
        Result(leader, 1);
        cluster.Pump();
        for (var tick = 0; tick < 100 && leader.State.Membership!.Committed.ActiveOperation is not null; tick++) cluster.Tick();
        Assert.Equal(RaftRole.Leader, leader.Role);
        Assert.Contains("n1", leader.State.Configuration.Voters);
        Assert.Null(leader.State.Membership!.Committed.ActiveOperation);
        var result = cluster.Stores["n1"].Query(new LogQuery.MembershipOperation(request.OperationId)).MembershipOperation!;
        Assert.Equal(MembershipOperationStage.Failed, result.Stage);
        Assert.NotNull(result.Error);
    }
}
