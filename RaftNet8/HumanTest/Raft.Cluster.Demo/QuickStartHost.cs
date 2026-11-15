// book:start client-host
using Raft;
using Raft.Cluster;
using Raft.Cluster.Server;

namespace Raft.Cluster.Demo;

public static class QuickStartHost
{
    // Invoke in three different processes with n1/n2/n3 and the same bootstrap.
    public static Task<RaftClusterNode> StartAsync(string nodeId, string dataRoot, CancellationToken token = default)
    {
        PeerDescriptor[] peers = [new("n1", "127.0.0.1", 6101),
            new("n2", "127.0.0.1", 6102), new("n3", "127.0.0.1", 6103)];
        ClientEndpoint[] clients = [new("n1", new("http://127.0.0.1:7101/")),
            new("n2", new("http://127.0.0.1:7102/")), new("n3", new("http://127.0.0.1:7103/"))];
        return RaftClusterNode.StartAsync(new()
        {
            DataDirectory = Path.GetFullPath(Path.Combine(dataRoot, nodeId)),
            Peer = peers.Single(peer => peer.NodeId == nodeId),
            Raft = new()
            {
                NodeId = nodeId,
                GroupId = "movies",
                WireVersion = 3,
                Members = peers.Select(peer => peer.NodeId).ToArray(),
                InitialPeers = peers,
                // Sample disk/large-message profile; tune for your measured IO/network tails.
                TickInterval = TimeSpan.FromMilliseconds(25),
                ElectionMinTicks = 200,
                ElectionMaxTicks = 280,
                RpcTimeout = TimeSpan.FromSeconds(2)
            },
            ClientServer = new()
            {
                Endpoint = clients.Single(client => client.NodeId == nodeId),
                InitialEndpoints = clients
                // ConfigureHttpServer: HTTPS; Authorize: your application access policy.
            }
        }, token);
    }
}
// book:end client-host
