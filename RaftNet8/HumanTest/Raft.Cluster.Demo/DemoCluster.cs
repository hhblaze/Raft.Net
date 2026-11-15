using System.Net;
using System.Net.Sockets;
using Raft;
using Raft.Cluster;
using Raft.Cluster.Client;
using Raft.Cluster.Server;

namespace Raft.Cluster.Demo;

public sealed class DemoCluster : IAsyncDisposable
{
    private readonly string _root;
    private readonly string[] _voters;
    private readonly Dictionary<string, PeerDescriptor> _peers = new(StringComparer.Ordinal);
    private readonly Dictionary<string, ClientEndpoint> _clients = new(StringComparer.Ordinal);
    public string GroupId { get; }
    public int HttpConcurrency { get; init; } = 4;
    public Dictionary<string, RaftClusterNode> Nodes { get; } = new(StringComparer.Ordinal);
    public DemoCluster(string root, int voters = 3, bool fixedPorts = false, string groupId = "movies", int peerBasePort = 6101, int clientBasePort = 7101)
    {
        if (voters is < 1 or > 7) throw new ArgumentOutOfRangeException(nameof(voters));
        _root = Path.GetFullPath(root); GroupId = groupId;
        _voters = Enumerable.Range(1, voters).Select(index => "n" + index).ToArray();
        var listeners = Enumerable.Range(0, 18).Select(_ => new TcpListener(IPAddress.Loopback, 0)).ToArray();
        try
        {
            if (!fixedPorts) foreach (var listener in listeners) listener.Start();
            for (var index = 0; index < 9; index++)
            {
                var id = "n" + (index + 1);
                _peers[id] = new(id, "127.0.0.1", fixedPorts ? peerBasePort + index : ((IPEndPoint)listeners[index * 2].LocalEndpoint).Port);
                _clients[id] = new(id, new Uri($"http://127.0.0.1:{(fixedPorts ? clientBasePort + index : ((IPEndPoint)listeners[index * 2 + 1].LocalEndpoint).Port)}/"));
            }
        }
        finally { foreach (var listener in listeners) listener.Stop(); }
    }
    public PeerDescriptor Peer(string id) => _peers[id];
    public ClientEndpoint Endpoint(string id) => _clients[id];
    public async Task StartAsync(CancellationToken token) { foreach (var id in _voters) await StartNodeAsync(id, token); }
    public async Task<RaftClusterNode> StartNodeAsync(string id, CancellationToken token)
    {
        var host = await RaftClusterNode.StartAsync(new()
        {
            DataDirectory = Path.Combine(_root, id),
            Peer = _peers[id],
            Raft = new()
            {
                NodeId = id,
                GroupId = GroupId,
                Members = _voters,
                InitialPeers = _voters.Select(voter => _peers[voter]).ToArray(),
                WireVersion = 3,
                JoiningNode = !_voters.Contains(id),
                TickInterval = TimeSpan.FromMilliseconds(25),
                ElectionMinTicks = 200,
                ElectionMaxTicks = 280,
                RpcTimeout = TimeSpan.FromSeconds(2),
                SnapshotRetainEntries = 0,
                SnapshotBytesPerSecond = 128 * 1024 * 1024
            },
            ClientServer = new() { Endpoint = _clients[id], InitialEndpoints = _voters.Select(voter => _clients[voter]).ToArray(), MaxConcurrentRequests = HttpConcurrency }
        }, token);
        Nodes.Add(id, host); return host;
    }
    public RaftClusterClient Client(int concurrency = 4) => new(new()
    { GroupId = GroupId, SeedEndpoints = _voters.Select(id => _clients[id].Address).ToArray(), MaxConcurrentRequests = concurrency });
    public async Task<RaftClusterNode> LeaderAsync(CancellationToken token)
    {
        while (true)
        {
            token.ThrowIfCancellationRequested();
            foreach (var host in Nodes.Values) if (host.Node.Status.Fault is { } error) throw new InvalidOperationException("Node failed.", error);
            var leaders = Nodes.Values.Where(host => host.Node.Status.Role == RaftRole.Leader && host.Node.Status.CommitIndex > 0).ToArray();
            if (leaders.Length == 1) return leaders[0];
            await Task.Delay(25, token);
        }
    }
    public async Task StopAsync(string id) { var host = Nodes[id]; Nodes.Remove(id); await host.DisposeAsync(); }
    public async ValueTask DisposeAsync() { foreach (var host in Nodes.Values) await host.DisposeAsync(); Nodes.Clear(); }
}
