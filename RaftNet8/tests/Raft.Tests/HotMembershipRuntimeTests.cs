using System.Net;
using System.Net.Sockets;
using Raft.Storage;
using Raft.Transport;

namespace Raft.Tests;

internal sealed class HotRuntimeCluster : IAsyncDisposable
{
    internal sealed record Host(RaftOptions Options, RaftNode Node, TcpRaftTransport Tcp,
        DBreezeGenerationStateMachine Application);
    private readonly TestDirectory _root = new();
    private readonly string _group = "hot_" + Guid.NewGuid().ToString("N");
    private readonly string[] _voters;
    private readonly PeerDescriptor[] _initial;
    private readonly Dictionary<string, PeerDescriptor> _descriptors = new(StringComparer.Ordinal);
    public Dictionary<string, Host> Hosts { get; } = new(StringComparer.Ordinal);
    public bool Snapshots { get; init; }

    public HotRuntimeCluster(int voters = 3)
    {
        _voters = Enumerable.Range(1, voters).Select(i => "n" + i).ToArray();
        var ports = ReservePorts(10);
        for (var i = 0; i < ports.Length; i++) _descriptors["n" + (i + 1)] = new("n" + (i + 1), "127.0.0.1", ports[i]);
        _initial = _voters.Select(id => _descriptors[id]).ToArray();
    }

    private static int[] ReservePorts(int count)
    {
        for (var attempt = 0; attempt < 100; attempt++)
        {
            var start = Random.Shared.Next(10000, 29000);
            var listeners = Enumerable.Range(start, count).Select(port => new TcpListener(IPAddress.Loopback, port)).ToArray();
            try
            {
                foreach (var listener in listeners) listener.Start();
                return Enumerable.Range(start, count).ToArray();
            }
            catch (SocketException) { }
            finally { foreach (var listener in listeners) listener.Stop(); }
        }
        throw new IOException("No free lab port block.");
    }

    public PeerDescriptor Peer(string id) => _descriptors[id];
    public async Task StartAsync() { foreach (var id in _voters) await StartNodeAsync(id); }
    public async Task<Host> StartNodeAsync(string id)
    {
        var path = Path.Combine(_root.Path, id);
        var options = new RaftOptions
        {
            NodeId = id, GroupId = _group, WireVersion = 3, Members = _voters,
            JoiningNode = !_voters.Contains(id), InitialPeers = _initial,
            TickInterval = TimeSpan.FromMilliseconds(25), ElectionMinTicks = 40, ElectionMaxTicks = 80,
            RpcTimeout = TimeSpan.FromMilliseconds(500),
            SnapshotDirectory = Snapshots ? Path.GetFullPath(Path.Combine(path, "images")) : null,
            SnapshotRetainEntries = 4
        };
        var app = new DBreezeGenerationStateMachine(Path.GetFullPath(Path.Combine(path, "app")), "hot-test-v1",
            "hot/checkpoint", ["hot/values"], (tx, entry) =>
            {
                var old = tx.Select<string, long>("hot/values", "count");
                tx.Insert<string, long>("hot/values", "count", (old.Exists ? old.Value : 0) + 1);
                tx.Insert<string, byte[]>("hot/values", "last", entry.Command.ToArray());
            });
        var log = new RaftStorageAdapter(new DBreezeLogStorage(Path.Combine(path, "log"), options, DBreezeLogLayout.UnifiedTableV4));
        var tcp = new TcpRaftTransport(options, Peer(id));
        RaftNode node;
        try { node = await RaftNode.CreateAsync(options, log, tcp, app); }
        catch { await tcp.DisposeAsync(); await log.DisposeAsync(); app.Dispose(); throw; }
        var host = new Host(options, node, tcp, app);
        Hosts.Add(id, host);
        await node.StartAsync();
        tcp.Start(node.HandleAsync);
        return host;
    }

    public async Task StopNodeAsync(string id)
    {
        var host = Hosts[id];
        Hosts.Remove(id);
        await host.Tcp.DisposeAsync();
        await host.Node.DisposeAsync();
        host.Application.Dispose();
    }

    public async Task<RaftNode> LeaderAsync()
    {
        RaftNode? leader = null;
        await RuntimeCluster.EventuallyAsync(() =>
        {
            foreach (var host in Hosts.Values) if (host.Node.Status.Fault is { } fault) throw new InvalidOperationException("Faulted hot node.", fault);
            var leaders = Hosts.Values.Where(host => host.Node.Status.Role == RaftRole.Leader && host.Node.Status.CommitIndex > 0).ToArray();
            leader = leaders.Length == 1 ? leaders[0].Node : null;
            return leader is not null;
        });
        return leader!;
    }

    public async Task<T> OnLeaderAsync<T>(Func<RaftNode, Task<T>> operation, CancellationToken token)
    {
        while (true)
        {
            token.ThrowIfCancellationRequested();
            var leader = await LeaderAsync();
            try { return await operation(leader); }
            catch (NotLeaderException) { await Task.Delay(25, token); }
        }
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var host in Hosts.Values) await host.Tcp.DisposeAsync();
        foreach (var host in Hosts.Values) await host.Node.DisposeAsync();
        foreach (var host in Hosts.Values) host.Application.Dispose();
        Hosts.Clear();
        _root.Dispose();
    }
}

[Collection("OS process clusters")]
public sealed class HotMembershipRuntimeTests
{
    [Fact]
    public async Task Offline_replica_learns_new_leaders_routes_after_both_original_contacts_are_retired()
    {
        await using var cluster = new HotRuntimeCluster { Snapshots = true };
        await cluster.StartAsync();
        await cluster.LeaderAsync();
        await cluster.StopNodeAsync("n3");
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        foreach (var pair in new[] { (Add: "n4", Remove: "n1"), (Add: "n5", Remove: "n2") })
        {
            await cluster.StartNodeAsync(pair.Add);
            var addId = Guid.NewGuid();
            await cluster.OnLeaderAsync(node => node.AddNodeAsync(cluster.Peer(pair.Add), addId, deadline.Token), deadline.Token);
            var removeId = Guid.NewGuid();
            await cluster.OnLeaderAsync(node => node.RemoveNodeAsync(pair.Remove, removeId, deadline.Token), deadline.Token);
            await cluster.StopNodeAsync(pair.Remove);
        }
        var leader = await cluster.LeaderAsync();
        for (var i = 0; i < 24; i++) await leader.ProposeAsync(BitConverter.GetBytes(i), deadline.Token);
        var snapshot = await leader.CreateSnapshotAsync(deadline.Token);
        Assert.DoesNotContain(leader.Status.NodeId, new[] { "n1", "n2", "n3" });
        var recovered = await cluster.StartNodeAsync("n3");
        await RuntimeCluster.EventuallyAsync(() => recovered.Node.Status.LastApplied >= snapshot.LastIncludedIndex);
        Assert.NotNull(recovered.Node.Status.Snapshot);
        Assert.Equal(24, recovered.Application.Read(tx => tx.Select<string, long>("hot/values", "count").Value));
        Assert.Contains(recovered.Node.PeerDirectory, peer => peer.NodeId == leader.Status.NodeId);
        Assert.Equal(new[] { "n3", "n4", "n5" }, recovered.Node.Status.Configuration!.Voters.Order().ToArray());
    }

    [Fact]
    public async Task Snapshot_catches_up_a_new_ID_preserves_results_and_recovers_an_active_Add()
    {
        await using var cluster = new HotRuntimeCluster { Snapshots = true };
        await cluster.StartAsync();
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(40));
        var completedId = Guid.NewGuid();
        await cluster.StartNodeAsync("n4");
        var completed = await cluster.OnLeaderAsync(node => node.AddNodeAsync(cluster.Peer("n4"), completedId, deadline.Token), deadline.Token);
        Assert.Equal(MembershipOperationStage.Completed, completed.Stage);
        var leader = await cluster.LeaderAsync();
        for (var i = 0; i < 16; i++) await leader.ProposeAsync(BitConverter.GetBytes(i), deadline.Token);
        var pendingId = Guid.NewGuid();
        using (var caller = new CancellationTokenSource(500))
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => leader.AddNodeAsync(cluster.Peer("n5"), pendingId, caller.Token));
        var snapshot = await leader.CreateSnapshotAsync(deadline.Token);
        Assert.Equal(pendingId, snapshot.Membership!.ActiveOperation!.OperationId);
        Assert.Contains(snapshot.Membership.Peers, peer => peer.NodeId == "n5");
        // Every possible successor must require snapshot catch-up. A new leader may supply its own image ID.
        foreach (var host in cluster.Hosts.Values)
        {
            if (host.Node == leader) continue;
            await RuntimeCluster.EventuallyAsync(() => host.Node.Status.LastApplied >= snapshot.LastIncludedIndex);
            await host.Node.CreateSnapshotAsync(deadline.Token);
        }
        var joined = await cluster.StartNodeAsync("n5");
        var added = await cluster.OnLeaderAsync(node => node.AddNodeAsync(cluster.Peer("n5"), pendingId, deadline.Token), deadline.Token);
        Assert.Equal(MembershipOperationStage.Completed, added.Stage);
        await RuntimeCluster.EventuallyAsync(() => joined.Node.Status.Snapshot?.LastIncludedIndex >= snapshot.LastIncludedIndex);
        Assert.Equal(16, joined.Application.Read(tx => tx.Select<string, long>("hot/values", "count").Value));
        var oldLeaderId = leader.Status.NodeId;
        await cluster.StopNodeAsync(oldLeaderId);
        leader = await cluster.LeaderAsync();
        Assert.Equal(completed, await leader.GetMembershipOperationAsync(completedId, deadline.Token));
        await cluster.StartNodeAsync(oldLeaderId);
        // Restart an installed generation, then elect it without depending on its old initial routes.
        await cluster.StopNodeAsync("n5");
        joined = await cluster.StartNodeAsync("n5");
        Assert.NotNull(joined.Node.Status.Membership);
        Assert.Equal(16, joined.Application.Read(tx => tx.Select<string, long>("hot/values", "count").Value));
        Assert.Equal(completedId, (await (await cluster.LeaderAsync()).GetMembershipOperationAsync(completedId, deadline.Token))!.OperationId);
    }

    [Theory]
    [InlineData(3)]
    [InlineData(5)]
    public async Task Real_TCP_and_disk_adds_a_new_ID_then_removes_it_without_restarting_existing_nodes(int voters)
    {
        await using var cluster = new HotRuntimeCluster(voters);
        await cluster.StartAsync();
        var originals = cluster.Hosts.ToDictionary(pair => pair.Key, pair => pair.Value.Node);
        var leader = await cluster.LeaderAsync();
        var id = "n" + (voters + 1);
        Assert.DoesNotContain(leader.PeerDirectory, peer => peer.NodeId == id);
        await cluster.StartNodeAsync(id);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var addId = Guid.NewGuid();
        var added = await cluster.OnLeaderAsync(node => node.AddNodeAsync(cluster.Peer(id), addId, timeout.Token), timeout.Token);
        Assert.Equal(MembershipOperationStage.Completed, added.Stage);
        leader = await cluster.LeaderAsync();
        Assert.Contains(id, leader.Status.Configuration!.Voters);
        foreach (var (nodeId, node) in originals) Assert.Same(node, cluster.Hosts[nodeId].Node);
        var written = await leader.ProposeAsync("hot data"u8.ToArray(), timeout.Token);
        await RuntimeCluster.EventuallyAsync(() => cluster.Hosts.Values.All(host => host.Application.LastApplied >= written.Index));
        Assert.All(cluster.Hosts.Values, host => Assert.Equal(1, host.Application.Read(tx => tx.Select<string, long>("hot/values", "count").Value)));
        var removeId = Guid.NewGuid();
        var removed = await cluster.OnLeaderAsync(node => node.RemoveNodeAsync(id, removeId, timeout.Token), timeout.Token);
        Assert.Equal(MembershipOperationStage.Completed, removed.Stage);
        Assert.Equal(removeId, (await (await cluster.LeaderAsync()).GetMembershipOperationAsync(removeId, timeout.Token))!.OperationId);
        await RuntimeCluster.EventuallyAsync(() => cluster.Hosts[id].Node.Status.Role == RaftRole.Removed);
    }

    [Fact]
    public async Task Client_timeout_does_not_cancel_Add_and_explicit_cancel_cleans_the_learner()
    {
        await using var cluster = new HotRuntimeCluster();
        await cluster.StartAsync();
        var leader = await cluster.LeaderAsync();
        var addId = Guid.NewGuid();
        using var caller = new CancellationTokenSource(TimeSpan.FromMilliseconds(800));
        var pending = leader.AddNodeAsync(cluster.Peer("n4"), addId, caller.Token);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending);
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var active = await leader.GetMembershipOperationAsync(addId, deadline.Token);
        Assert.NotNull(active);
        Assert.False(active.IsTerminal);
        var busy = await Assert.ThrowsAsync<MembershipChangeInProgressException>(() =>
            leader.AddNodeAsync(cluster.Peer("n5"), Guid.NewGuid(), deadline.Token));
        Assert.Equal(addId, busy.OperationId);
        var cancelled = await leader.CancelMembershipOperationAsync(addId, deadline.Token);
        Assert.Equal(MembershipOperationStage.Cancelled, cancelled.Stage);
        Assert.DoesNotContain("n4", leader.Status.Configuration!.AllMembers);
        Assert.Null(leader.Status.Membership!.ActiveOperation);
        await cluster.StartNodeAsync("n5");
        var next = await cluster.OnLeaderAsync(node => node.AddNodeAsync(cluster.Peer("n5"), Guid.NewGuid(), deadline.Token), deadline.Token);
        Assert.Equal(MembershipOperationStage.Completed, next.Stage);
    }

    [Fact]
    public async Task Removing_current_leader_is_finished_by_its_successor_using_the_original_ID()
    {
        await using var cluster = new HotRuntimeCluster();
        await cluster.StartAsync();
        var original = await cluster.LeaderAsync();
        var target = original.Status.NodeId;
        var operation = Guid.NewGuid();
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var removed = await cluster.OnLeaderAsync(node => node.RemoveNodeAsync(target, operation, deadline.Token), deadline.Token);
        Assert.Equal(MembershipOperationStage.Completed, removed.Stage);
        var leader = await cluster.LeaderAsync();
        Assert.NotEqual(target, leader.Status.NodeId);
        Assert.DoesNotContain(target, leader.Status.Configuration!.Voters);
        await RuntimeCluster.EventuallyAsync(() => original.Status.Role == RaftRole.Removed);
        Assert.Equal(operation, (await leader.GetMembershipOperationAsync(operation, deadline.Token))!.OperationId);
    }
}
