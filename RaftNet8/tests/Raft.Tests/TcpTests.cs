using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using Raft.Transport;

namespace Raft.Tests;

public sealed class TcpTests
{
    [Fact]
    public async Task Real_tcp_cluster_replicates_fails_over_and_recovers_a_restarted_disk_node()
    {
        await using var cluster = new RuntimeCluster(disk: true, tcp: true);
        await cluster.StartAsync();
        var leader = await cluster.LeaderAsync();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var first = await leader.ProposeAsync("before-failover"u8.ToArray(), timeout.Token);
        await RuntimeCluster.EventuallyAsync(() => cluster.Nodes.Values.All(n => n.Status.LastApplied >= first.Index));
        var oldId = leader.Status.NodeId;
        await cluster.StopAsync(oldId);
        var replacement = await cluster.LeaderAsync();
        await replacement.ProposeAsync("after-failover"u8.ToArray(), timeout.Token);
        await cluster.RestartAsync(oldId);
        var barrier = await replacement.ReadBarrierAsync(timeout.Token);
        await RuntimeCluster.EventuallyAsync(() => cluster.Nodes.Values.All(n => n.Status.LastApplied >= barrier.Index));
        foreach (var machine in cluster.Machines.Values)
            Assert.Equal(new[] { "before-failover", "after-failover" }, machine.Entries.Where(e => !e.IsNoOp)
                .Select(e => System.Text.Encoding.UTF8.GetString(e.Command.Span)));
    }

    [Fact]
    public async Task Oversized_tcp_frame_is_rejected_and_listener_remains_usable()
    {
        var options = new RaftOptions { NodeId = "n1", Members = new[] { "n1", "n2" } };
        var endpoints = new Dictionary<string, IPEndPoint>
        {
            ["n1"] = new(IPAddress.Loopback, 0), ["n2"] = new(IPAddress.Loopback, 0)
        };
        await using var server = new TcpRaftTransport(options, endpoints);
        server.Start((_, request, _) => ValueTask.FromResult<RaftRpc>(new VoteResponse(request.Term, true)));
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        using (var client = new TcpClient())
        {
            await client.ConnectAsync(server.LocalEndPoint, timeout.Token);
            var header = new byte[4];
            BinaryPrimitives.WriteInt32LittleEndian(header, int.MaxValue);
            await client.GetStream().WriteAsync(header, timeout.Token);
            Assert.Equal(0, await client.GetStream().ReadAsync(new byte[1], timeout.Token));
        }
        endpoints["n1"] = server.LocalEndPoint;
        await using var peer = new TcpRaftTransport(options with { NodeId = "n2" }, endpoints);
        var response = (VoteResponse)await peer.SendAsync("n1", new VoteRequest(1, 0, 0), timeout.Token);
        Assert.True(response.Granted);
        Assert.True(server.ConnectionErrors >= 1);
    }

    [Fact]
    public async Task Foreign_group_cannot_issue_requests_on_a_members_socket()
    {
        var options = new RaftOptions { NodeId = "n1", Members = new[] { "n1", "n2" } };
        var endpoints = new Dictionary<string, IPEndPoint> { ["n1"] = new(IPAddress.Loopback, 0), ["n2"] = new(IPAddress.Loopback, 0) };
        await using var server = new TcpRaftTransport(options, endpoints);
        var calls = 0;
        server.Start((_, request, _) => { Interlocked.Increment(ref calls); return ValueTask.FromResult<RaftRpc>(new VoteResponse(request.Term, true)); });
        endpoints["n1"] = server.LocalEndPoint;
        await using var foreign = new TcpRaftTransport(options with { NodeId = "n2", GroupId = "different" }, endpoints);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await Assert.ThrowsAsync<EndOfStreamException>(() => foreign.SendAsync("n1", new VoteRequest(1, 0, 0), timeout.Token).AsTask());
        Assert.Equal(0, calls);
    }
}
