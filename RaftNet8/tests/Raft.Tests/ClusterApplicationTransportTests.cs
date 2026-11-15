using System.Net;
using System.Net.Sockets;
using Raft.Cluster.Server;
using Raft.Transport;

namespace Raft.Tests;

public sealed class ClusterApplicationTransportTests
{
    [Theory]
    [InlineData(true, true)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    public async Task Application_preamble_rejects_old_and_new_mixed_sessions_before_any_RPC(bool serverV2, bool clientV2)
    {
        var endpoints = new Dictionary<string, IPEndPoint> { ["n1"] = new(IPAddress.Loopback, 0), ["n2"] = new(IPAddress.Loopback, 0) };
        RaftOptions Options(string id) => new() { NodeId = id, Members = ["n1", "n2"], TickInterval = TimeSpan.FromMilliseconds(100), RpcTimeout = TimeSpan.FromSeconds(1) };
        var calls = 0;
        await using var server = new TcpRaftTransport(Options("n1"), endpoints, serverV2 ? new ClusterApplicationTransport(null) : null);
        server.Start((_, message, _) =>
        {
            Interlocked.Increment(ref calls);
            var append = (AppendRequest)message;
            return ValueTask.FromResult<RaftRpc>(new AppendResponse(append.Term, append.RequestId, true, 0));
        });
        endpoints["n1"] = server.LocalEndPoint;
        await using var client = new TcpRaftTransport(Options("n2"), endpoints, clientV2 ? new ClusterApplicationTransport(null) : null);
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        var request = client.SendAsync("n1", new AppendRequest(1, 1, 0, 0, 0, []), deadline.Token).AsTask();
        if (serverV2 && clientV2) { Assert.IsType<AppendResponse>(await request); Assert.Equal(1, calls); }
        else { await Assert.ThrowsAnyAsync<Exception>(() => request); Assert.Equal(0, calls); }
    }
}
