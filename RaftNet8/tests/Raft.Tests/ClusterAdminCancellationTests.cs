using Raft.Cluster;
using Raft.Cluster.Server;
using Raft.Storage;
using Raft.Transport;

namespace Raft.Tests;

[Collection("OS process clusters")]
public sealed class ClusterAdminCancellationTests
{
    [Fact]
    public async Task Pending_add_survives_leader_loss_and_cancel_commits_learner_and_route_cleanup()
    {
        await using var cluster = new ClientTestCluster(3) { ManagementEnabled = true };
        await cluster.StartAsync();
        var leader = await cluster.LeaderAsync();
        await using var bootstrap = cluster.Admin();
        var root = await bootstrap.BootstrapAsync(cluster.BootstrapSecret, Guid.NewGuid());
        await using var admin = cluster.Admin(root.Secret);
        using var candidateDirectory = new ClusterTestDirectory();
        var options = leader.Options with
        {
            NodeId = "n4", JoiningNode = true, SnapshotDirectory = Path.Combine(candidateDirectory.Path, "images")
        };
        using var ledger = new MessageLedger(Path.Combine(candidateDirectory.Path, "messages"), version: ClusterLedgerVersion.V2);
        await using var transport = new TcpRaftTransport(options, cluster.Peer("n4"));
        var log = new RaftStorageAdapter(new DBreezeLogStorage(Path.Combine(candidateDirectory.Path, "log"), options, DBreezeLogLayout.UnifiedTableV4));
        await using var candidate = await RaftNode.CreateAsync(options, log, transport, ledger);
        await candidate.StartAsync();
        // Management remains reachable while all incoming log/snapshot replication is partitioned.
        transport.Start((from, request, token) => request is AppendRequest or SnapshotChunkRequest
            ? ValueTask.FromException<RaftRpc>(new IOException("Injected replication partition."))
            : candidate.HandleAsync(from, request, token));
        await using var server = await RaftClusterServer.StartAsync(candidate, ledger, new()
        {
            GroupId = cluster.GroupId, Endpoint = cluster.Endpoint("n4"),
            Management = new() { Endpoint = cluster.ManagementEndpoint("n4").Address, LocalPeer = cluster.Peer("n4") }
        });
        var id = Guid.NewGuid();
        await admin.StartOperationAsync(new("add", Add: new(new("n4", cluster.Peer("n4").Host, cluster.Peer("n4").Port),
            cluster.Endpoint("n4"), cluster.ManagementEndpoint("n4"))), id);
        await RuntimeCluster.EventuallyAsync(() => leader.Node.Status.Membership?.ActiveOperation?.Stage == MembershipOperationStage.CatchingUp, 15_000);
        await cluster.StopAsync(leader.Node.Status.NodeId);
        var replacement = await cluster.LeaderAsync();
        Assert.Equal(id, replacement.Ledger.ActiveAdminOperation!.OperationId);
        var busy = await Assert.ThrowsAsync<ClusterRequestException>(() => admin.StartOperationAsync(new("snapshot"), Guid.NewGuid()));
        Assert.Equal("membership_busy", busy.Code);
        await admin.CancelMembershipOperationAsync(id);
        await RuntimeCluster.EventuallyAsync(() => replacement.Ledger.GetAdminOperation(id)?.State == "Cancelled", 20_000);
        Assert.Null(replacement.Ledger.ActiveAdminOperation);
        Assert.DoesNotContain(replacement.Ledger.GetEndpoints().Endpoints, endpoint => endpoint.NodeId == "n4");
        Assert.DoesNotContain(replacement.Ledger.GetManagementEndpoints(), endpoint => endpoint.NodeId == "n4");
        Assert.DoesNotContain("n4", replacement.Node.Status.Configuration!.Learners);
        Assert.Equal(MembershipOperationStage.Cancelled, (await replacement.Node.GetMembershipOperationAsync(id))!.Stage);
        var next = Guid.NewGuid();
        await admin.StartOperationAsync(new("snapshot"), next);
        await RuntimeCluster.EventuallyAsync(() => replacement.Ledger.GetAdminOperation(next)?.State == "Completed", 20_000);
    }
}
