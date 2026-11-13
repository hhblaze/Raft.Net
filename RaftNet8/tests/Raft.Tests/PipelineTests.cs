using Raft.Core;

namespace Raft.Tests;

public sealed class PipelineTests
{
    private static List<OutboundRpc> Drain(RaftCore node)
    {
        var list = new List<OutboundRpc>();
        while (node.TryDequeue(out var rpc)) list.Add(rpc);
        return list;
    }

    [Fact]
    public void Replicate_window_advances_optimistically_and_cumulative_reordered_ack_releases_older_flights()
    {
        using var cluster = new SimCluster(maxAppendEntries: 1);
        cluster.Tick(80);
        var id = cluster.Leader;
        var leader = cluster.Nodes[id];
        var start = leader.LastIndex;
        for (var i = 0; i < 8; i++) leader.Propose([new(BitConverter.GetBytes(i))]);
        var sent = Drain(leader);
        var peerId = sent[0].PeerId;
        var requests = sent.Where(r => r.PeerId == peerId).Select(r => Assert.IsType<AppendRequest>(r.Request)).ToArray();
        Assert.Equal(4, requests.Length);
        Assert.Equal(Enumerable.Range(0, 4).Select(i => start + i), requests.Select(r => r.PreviousIndex));
        Assert.Equal(start + 5, leader.State.Peers.Single(p => p.Id == peerId).NextIndex);
        var replies = requests.Select(r => cluster.Nodes[peerId].HandleRequest(id, r)).ToArray();
        leader.HandleResponse(peerId, replies[^1]);
        var progress = leader.State.Peers.Single(p => p.Id == peerId);
        Assert.Equal(start + 4, progress.MatchIndex);
        Assert.Equal(start + 4, leader.CommitIndex);
        Assert.Equal(4, progress.Flights.Length); // next window was filled immediately
        var nextIds = progress.Flights.Select(r => r.RequestId).ToArray();
        foreach (var stale in replies.Take(3).Reverse()) leader.HandleResponse(peerId, stale);
        Assert.Equal(nextIds, leader.State.Peers.Single(p => p.Id == peerId).Flights.Select(r => r.RequestId));
        cluster.Pump();
        cluster.AssertSafety();
    }

    [Fact]
    public void Reordered_rejection_collapses_window_to_probe_and_late_success_cannot_move_it()
    {
        using var cluster = new SimCluster(maxAppendEntries: 1);
        cluster.Tick(80);
        var id = cluster.Leader;
        var leader = cluster.Nodes[id];
        for (var i = 0; i < 4; i++) leader.Propose([new([1])]);
        var sent = Drain(leader);
        var peer = sent[0].PeerId;
        var requests = sent.Where(r => r.PeerId == peer).Select(r => (AppendRequest)r.Request).ToArray();
        var rejected = cluster.Nodes[peer].HandleRequest(id, requests[2]);
        Assert.False(Assert.IsType<AppendResponse>(rejected).Success);
        leader.HandleResponse(peer, rejected);
        var progress = leader.State.Peers.Single(p => p.Id == peer);
        Assert.Equal(ReplicationMode.Probe, progress.Mode);
        Assert.Single(progress.Flights);
        var probe = progress.InFlight;
        leader.HandleResponse(peer, cluster.Nodes[peer].HandleRequest(id, requests[0]));
        Assert.Same(probe, leader.State.Peers.Single(p => p.Id == peer).InFlight);
        cluster.Pump();
        cluster.AssertSafety();
    }

    [Theory]
    [InlineData(true, 2)]
    [InlineData(false, 1)]
    public void Byte_limit_and_pipeline_switch_bound_retained_requests(bool pipeline, int expected)
    {
        using var cluster = new SimCluster(maxAppendEntries: 1, configure: options => options with
        {
            EnablePipelining = pipeline, MaxInFlightMessages = 8, MaxInFlightBytes = 256,
            MaxCommandBytes = 32, MaxAppendBytes = 64
        });
        cluster.Tick(80);
        var leader = cluster.Nodes[cluster.Leader];
        for (var i = 0; i < 8; i++) leader.Propose([new(new byte[32])]);
        Assert.All(leader.State.Peers, peer =>
        {
            Assert.Equal(expected, peer.Flights.Length);
            Assert.InRange(peer.InFlightBytes, 1, 256);
        });
        var sent = Drain(leader);
        var failed = sent[0];
        leader.ReportUnreachable(failed.PeerId, failed.Request);
        Assert.Empty(leader.State.Peers.Single(p => p.Id == failed.PeerId).Flights);
        Assert.Equal(ReplicationMode.Probe, leader.State.Peers.Single(p => p.Id == failed.PeerId).Mode);
        leader.Tick();
        Assert.Single(leader.State.Peers.Single(p => p.Id == failed.PeerId).Flights);
    }

    [Fact]
    public async Task Uncommitted_byte_pressure_is_a_client_error_and_reopens_after_quorum_progress()
    {
        await using var cluster = new RuntimeCluster(modern: true, configure: options => options with
        {
            MaxAppendEntries = 1, MaxCommandBytes = 32, MaxAppendBytes = 64,
            MaxInFlightBytes = 256, MaxUncommittedBytes = 128
        });
        await cluster.StartAsync();
        var leader = await cluster.LeaderAsync();
        cluster.Isolate(leader.Status.NodeId);
        var start = leader.Status.LastLogIndex;
        var first = leader.ProposeAsync(new byte[32]);
        var second = leader.ProposeAsync(new byte[32]);
        await RuntimeCluster.EventuallyAsync(() => leader.Status.LastLogIndex == start + 2);
        await Assert.ThrowsAsync<UncommittedLogLimitException>(() => leader.ProposeAsync(new byte[32]));
        Assert.Null(leader.Status.Fault);
        cluster.Heal();
        await Task.WhenAll(first, second).WaitAsync(TimeSpan.FromSeconds(5));
        await leader.ProposeAsync(new byte[32]);
        Assert.Null(leader.Status.Fault);
    }
}
