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
    private readonly Dictionary<string, ManagedNodeEndpoint> _management = new(StringComparer.Ordinal);
    public string GroupId { get; }
    public int HttpConcurrency { get; init; } = 4;
    public bool ManagementEnabled { get; init; }
    public bool RequireClientTokens { get; init; }
    public string? BootstrapSecret { get; init; }
    public string? ClientAccessToken { get; set; }
    public bool EnableTelemetry { get; init; } = true;
    public bool BatchObservationReads { get; init; } = true;
    public DemoSecurity? Security { get; init; }
    private HttpClient? _secureHttp;
    public Dictionary<string, RaftClusterNode> Nodes { get; } = new(StringComparer.Ordinal);
    public DemoCluster(string root, int voters = 3, bool fixedPorts = false, string groupId = "movies", int peerBasePort = 6101, int clientBasePort = 7101, int managementBasePort = 7201, bool secure = false)
    {
        if (voters is < 1 or > 7) throw new ArgumentOutOfRangeException(nameof(voters));
        _root = Path.GetFullPath(root); GroupId = groupId;
        _voters = Enumerable.Range(1, voters).Select(index => "n" + index).ToArray();
        var listeners = Enumerable.Range(0, 27).Select(_ => new TcpListener(IPAddress.Loopback, 0)).ToArray();
        try
        {
            if (!fixedPorts) foreach (var listener in listeners) listener.Start();
            for (var index = 0; index < 9; index++)
            {
                var id = "n" + (index + 1);
                _peers[id] = new(id, "127.0.0.1", fixedPorts ? peerBasePort + index : ((IPEndPoint)listeners[index * 2].LocalEndpoint).Port);
                _clients[id] = new(id, new Uri($"{(secure ? "https" : "http")}://127.0.0.1:{(fixedPorts ? clientBasePort + index : ((IPEndPoint)listeners[index * 2 + 1].LocalEndpoint).Port)}/"));
                _management[id] = new(id, new Uri($"{(secure ? "https" : "http")}://127.0.0.1:{(fixedPorts ? managementBasePort + index : ((IPEndPoint)listeners[index + 18].LocalEndpoint).Port)}/"));
            }
        }
        finally { foreach (var listener in listeners) listener.Stop(); }
    }
    public PeerDescriptor Peer(string id) => _peers[id];
    public ClientEndpoint Endpoint(string id) => _clients[id];
    public ManagedNodeEndpoint ManagementEndpoint(string id) => _management[id];
    public async Task StartAsync(CancellationToken token) { foreach (var id in _voters) await StartNodeAsync(id, token); }
    public async Task<RaftClusterNode> StartNodeAsync(string id, CancellationToken token)
    {
        if (Security is not null) _secureHttp ??= Security.HttpClient();
        var host = await RaftClusterNode.StartAsync(new()
        {
            DataDirectory = Path.Combine(_root, id),
            Peer = _peers[id],
            PeerSecurity = Security?.Peer(id),
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
            ClientServer = new()
            {
                Endpoint = _clients[id], InitialEndpoints = _voters.Select(voter => _clients[voter]).ToArray(), MaxConcurrentRequests = HttpConcurrency,
                Security = new() { RequireClientTokens = RequireClientTokens || Security is not null, SecureTransport = Security is not null },
                ConfigureHttpServer = Security is null ? null : builder => Security.Configure(builder, _clients[id].Address, false),
                Management = ManagementEnabled ? new()
                {
                    Endpoint = _management[id].Address, BootstrapSecret = BootstrapSecret,
                    EnableTelemetry = EnableTelemetry,
                    BatchObservationReads = BatchObservationReads,
                    HttpClient = _secureHttp,
                    DiagnosticCertificateThumbprints = Security is null ? [] : [Security.DiagnosticThumbprint],
                    ConfigureHttpServer = Security is null ? null : builder => Security.Configure(builder, _management[id].Address, true),
                    InitialEndpoints = _voters.Select(voter => _management[voter]).ToArray()
                } : null
            }
        }, token);
        Nodes.Add(id, host); return host;
    }
    public RaftClusterClient Client(int concurrency = 4) => new(new()
    {
        GroupId = GroupId, SeedEndpoints = _voters.Select(id => _clients[id].Address).ToArray(), MaxConcurrentRequests = concurrency,
        AccessTokenProvider = _ => ValueTask.FromResult(ClientAccessToken)
        , HttpClient = _secureHttp
    });
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
    public async ValueTask DisposeAsync() { foreach (var host in Nodes.Values) await host.DisposeAsync(); Nodes.Clear(); _secureHttp?.Dispose(); }
}
