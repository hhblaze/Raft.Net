using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text.Json;
using System.Security.Cryptography.X509Certificates;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Https;
using DBreeze;
using Raft.Cluster;
using Raft.Cluster.Client;
using Raft.Cluster.Server;

namespace Raft.Tests;

internal sealed class ClientTestCluster : IAsyncDisposable
{
    private readonly ClusterTestDirectory _directory = new();
    private readonly string[] _voters;
    private readonly PeerDescriptor[] _bootstrap;
    private readonly Dictionary<string, PeerDescriptor> _peers = new(StringComparer.Ordinal);
    private readonly Dictionary<string, ClientEndpoint> _clients = new(StringComparer.Ordinal);
    public X509Certificate2? Certificate { get; init; }
    public bool RequireToken { get; init; }
    public bool NativeTokens { get; init; }
    public bool ManagementEnabled { get; init; }
    public int MaxHttpRequests { get; init; } = 4;
    public ClusterLedgerVersion ApplicationVersion { get; init; } = ClusterLedgerVersion.V2;
    public string BootstrapSecret { get; } = Convert.ToBase64String(RandomNumberGenerator.GetBytes(32));
    private readonly Dictionary<string, Uri> _management = new(StringComparer.Ordinal);
    public string GroupId { get; } = "clients_" + Guid.NewGuid().ToString("N");
    public Dictionary<string, RaftClusterNode> Nodes { get; } = new(StringComparer.Ordinal);
    public ClientTestCluster(int count, bool https = false)
    {
        _voters = Enumerable.Range(1, count).Select(index => "n" + index).ToArray();
        var listeners = Enumerable.Range(0, 24).Select(_ => new TcpListener(IPAddress.Loopback, 0)).ToArray();
        try
        {
            foreach (var listener in listeners) listener.Start();
            for (var index = 0; index < 8; index++)
            {
                var id = "n" + (index + 1);
                _peers[id] = new(id, "127.0.0.1", ((IPEndPoint)listeners[index * 2].LocalEndpoint).Port);
                _clients[id] = new(id, new Uri($"{(https ? "https" : "http")}://127.0.0.1:{((IPEndPoint)listeners[index * 2 + 1].LocalEndpoint).Port}/"));
                _management[id] = new Uri($"http://127.0.0.1:{((IPEndPoint)listeners[index + 16].LocalEndpoint).Port}/");
            }
        }
        finally { foreach (var listener in listeners) listener.Stop(); }
        _bootstrap = _voters.Select(id => _peers[id]).ToArray();
    }
    public PeerDescriptor Peer(string id) => _peers[id];
    public ClientEndpoint Endpoint(string id) => _clients[id];
    public ManagedNodeEndpoint ManagementEndpoint(string id) => new(id, _management[id]);
    public string DataPath(string id) => Path.Combine(_directory.Path, id);
    public async Task StartAsync() { foreach (var id in _voters) await StartNodeAsync(id); }
    public async Task<RaftClusterNode> StartNodeAsync(string id)
    {
        if (ManagementEnabled && !_management.ContainsKey(id))
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            _management[id] = new Uri($"http://127.0.0.1:{((IPEndPoint)listener.LocalEndpoint).Port}/");
            listener.Stop();
        }
        var options = new RaftOptions
        {
            NodeId = id,
            GroupId = GroupId,
            Members = _voters,
            InitialPeers = _bootstrap,
            JoiningNode = !_voters.Contains(id),
            WireVersion = 3,
            SnapshotRetainEntries = 0,
            TickInterval = TimeSpan.FromMilliseconds(25),
            ElectionMinTicks = 200,
            ElectionMaxTicks = 280,
            RpcTimeout = TimeSpan.FromSeconds(2),
            SnapshotBytesPerSecond = 128 * 1024 * 1024
        };
        var host = await RaftClusterNode.StartAsync(new()
        {
            Raft = options,
            ApplicationVersion = ApplicationVersion,
            Peer = _peers[id],
            DataDirectory = DataPath(id),
            ClientServer = new()
            {
                Endpoint = _clients[id],
                MaxConcurrentRequests = MaxHttpRequests,
                Security = new() { RequireClientTokens = NativeTokens },
                Management = ManagementEnabled ? new()
                {
                    Endpoint = _management[id], BootstrapSecret = BootstrapSecret,
                    InitialEndpoints = _voters.Select(voter => new ManagedNodeEndpoint(voter, _management[voter])).ToArray()
                } : null,
                InitialEndpoints = _voters.Select(voter => _clients[voter]).ToArray(),
                Authorize = RequireToken ? (context, _) => ValueTask.FromResult(context.Request.Headers.Authorization == "Bearer test-secret") : null,
                ConfigureHttpServer = Certificate is null ? null : builder => builder.WebHost.ConfigureKestrel(kestrel =>
                    kestrel.Listen(IPAddress.Loopback, _clients[id].Address.Port, listen => listen.UseHttps(Certificate)))
            }
        });
        Nodes.Add(id, host);
        return host;
    }
    public async Task<RaftClusterNode> LeaderAsync()
    {
        RaftClusterNode? leader = null;
        await RuntimeCluster.EventuallyAsync(() =>
        {
            foreach (var host in Nodes.Values) if (host.Node.Status.Fault is { } error) throw new InvalidOperationException("Message node faulted.", error);
            var leaders = Nodes.Values.Where(host => host.Node.Status.Role == RaftRole.Leader && host.Node.Status.CommitIndex > 0).ToArray();
            leader = leaders.Length == 1 ? leaders[0] : null;
            return leader is not null;
        }, 20_000);
        return leader!;
    }
    public RaftClusterClient Client(string? onlySeed = null, HttpClient? http = null) => new(new()
    {
        GroupId = GroupId,
        SeedEndpoints = onlySeed is null ? _voters.Select(id => _clients[id].Address).ToArray() : [_clients[onlySeed].Address],
        PollInterval = TimeSpan.FromMilliseconds(25),
        TopologyRefreshInterval = TimeSpan.FromMilliseconds(150),
        HandlerRetryDelay = TimeSpan.FromMilliseconds(20),
        HttpClient = http
    });
    public RaftClusterAdminClient Admin(string? secret = null) => new(new()
    {
        GroupId = GroupId, SeedEndpoints = _management.Values.ToArray(),
        AccessTokenProvider = _ => ValueTask.FromResult(secret)
    });
    public RaftClusterClient AuthenticatedClient(string secret) => new(new()
    {
        GroupId = GroupId, SeedEndpoints = _voters.Select(id => _clients[id].Address).ToArray(),
        AccessTokenProvider = _ => ValueTask.FromResult<string?>(secret),
        TopologyRefreshInterval = TimeSpan.FromMilliseconds(150)
    });
    public async Task StopAsync(string id)
    {
        var node = Nodes[id]; Nodes.Remove(id); await node.DisposeAsync();
    }
    public async ValueTask DisposeAsync()
    {
        foreach (var node in Nodes.Values) await node.DisposeAsync();
        Nodes.Clear(); _directory.Dispose();
    }
}

[Collection("OS process clusters")]
public sealed class ClusterClientTests
{
    [Fact]
    public async Task Follower_seed_discovers_leader_and_concurrent_entities_have_independent_ids()
    {
        await using var cluster = new ClientTestCluster(3);
        await cluster.StartAsync();
        var leader = await cluster.LeaderAsync();
        var follower = cluster.Nodes.Keys.First(id => id != leader.Node.Status.NodeId);
        await using var client = cluster.Client(follower);
        var topology = await client.RefreshTopologyAsync();
        Assert.True(topology.IsAuthoritative);
        Assert.Equal(3, topology.Endpoints.Count);
        var ids = await Task.WhenAll(Enumerable.Range(0, 64).Select(index => client.AddLogEntryAsync(BitConverter.GetBytes(index), index % 2 == 0 ? "alpha" : "beta")));
        Assert.Equal(Enumerable.Range(1, 32).Select(index => (long)index), ids.Where((_, index) => index % 2 == 0).Order());
        Assert.Equal(Enumerable.Range(1, 32).Select(index => (long)index), ids.Where((_, index) => index % 2 == 1).Order());
        Assert.Equal(32, (await client.GetEntityAsync("alpha"))!.LastMessageId);
        Assert.Equal(32, (await client.GetEntityAsync("beta"))!.LastMessageId);
    }

    private sealed class DropFirstPublicationReply : DelegatingHandler
    {
        private int _dropped;
        public DropFirstPublicationReply() : base(new SocketsHttpHandler { AllowAutoRedirect = false }) { }
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken token)
        {
            var response = await base.SendAsync(request, token);
            if (request.RequestUri!.AbsolutePath.EndsWith("/publish", StringComparison.Ordinal) && Interlocked.Exchange(ref _dropped, 1) == 0)
            { response.Dispose(); throw new HttpRequestException("Simulated loss after quorum commit and leader application."); }
            return response;
        }
    }

    [Fact]
    public async Task Lost_reply_retry_and_leader_failover_do_not_duplicate_publication()
    {
        await using var cluster = new ClientTestCluster(3);
        await cluster.StartAsync();
        var leader = await cluster.LeaderAsync();
        using var http = new HttpClient(new DropFirstPublicationReply());
        await using var client = cluster.Client(http: http);
        var request = Guid.NewGuid();
        Assert.Equal(1, await client.AddLogEntryAsync(new byte[] { 1, 2, 3 }, "history", request));
        await cluster.StopAsync(leader.Node.Status.NodeId);
        await cluster.LeaderAsync();
        Assert.Equal(1, await client.AddLogEntryAsync(new byte[] { 1, 2, 3 }, "history", request));
        var error = await Assert.ThrowsAsync<ClusterRequestException>(() => client.AddLogEntryAsync(new byte[] { 4 }, "history", request));
        Assert.Equal("request_conflict", error.Code);
        Assert.Equal(1, (await client.GetEntityAsync("history"))!.LastMessageId);
        Assert.Equal(2, await client.AddLogEntryAsync(new byte[] { 5 }, "history"));
    }

    [Fact]
    public async Task Subscription_retries_false_and_exceptions_without_cluster_ack_writes()
    {
        await using var cluster = new ClientTestCluster(1);
        await cluster.StartAsync();
        var leader = await cluster.LeaderAsync();
        await using var client = cluster.Client();
        await client.AddLogEntryAsync(new byte[] { 1 }, "events");
        await client.AddLogEntryAsync(new byte[] { 2 }, "events");
        await RuntimeCluster.EventuallyAsync(() => leader.Ledger.GetEndpoints().Endpoints.Count == 1);
        var before = leader.Node.Status.LastLogIndex;
        var seen = new List<long>(); var failures = 0;
        client.SubscriptionCallbackFailed += _ => failures++;
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var subscription = client.SubscribeAsync("events", 0, (_, id, data) =>
        {
            Assert.Equal((byte)id, data.Span[0]); seen.Add(id);
            if (seen.Count == 1) return false;
            if (seen.Count == 2) throw new IOException("Application transaction failed before commit.");
            if (id == 2) deadline.Cancel();
            return true;
        }, deadline.Token);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => subscription);
        Assert.Equal(new long[] { 1, 1, 1, 2 }, seen);
        Assert.Equal(1, failures);
        Assert.Equal(before, leader.Node.Status.LastLogIndex);
    }

    [Fact]
    public async Task Application_transaction_cursor_survives_client_crash_before_callback_ack()
    {
        using var local = new ClusterTestDirectory();
        using var db = new DBreezeEngine(Path.Combine(local.Path, "subscriber"));
        await using var cluster = new ClientTestCluster(1);
        await cluster.StartAsync(); await cluster.LeaderAsync();
        await using var client = cluster.Client();
        for (var index = 1; index <= 4; index++) await client.AddLogEntryAsync(BitConverter.GetBytes(index), "events");
        using var first = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => client.SubscribeAsync("events", 0, (_, id, data) =>
        {
            using var tx = db.GetTransaction(); tx.SynchronizeTables("consumer");
            tx.Insert<long, byte[]>("consumer", id, data.ToArray()); tx.Insert<string, long>("consumer", "cursor", id); tx.Commit();
            if (id == 2) { first.Cancel(); throw new IOException("Crash after local commit, before acknowledgment."); }
            return true;
        }, first.Token));
        long saved;
        using (var tx = db.GetTransaction()) saved = tx.Select<string, long>("consumer", "cursor").Value;
        Assert.Equal(2, saved);
        await using var restarted = cluster.Client();
        var delivered = new List<long>();
        using var second = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => restarted.SubscribeIdsAsync("events", saved, (_, id) =>
        { delivered.Add(id); if (id == 4) second.Cancel(); return true; }, second.Token));
        Assert.Equal(new long[] { 3, 4 }, delivered);
    }

    [Theory]
    [InlineData(3)]
    [InlineData(5)]
    public async Task Sixteen_MB_stream_reaches_quorum_and_can_be_fetched_by_id(int voters)
    {
        await using var cluster = new ClientTestCluster(voters);
        await cluster.StartAsync();
        var leader = await cluster.LeaderAsync();
        await using var client = cluster.Client();
        var payload = GC.AllocateUninitializedArray<byte>(ClusterLimits.MaxPayloadBytes);
        RandomNumberGenerator.Fill(payload);
        using var input = new MemoryStream(payload, writable: false);
        Assert.Equal(1, await client.AddLogEntryAsync(input, payload.Length, "chunks"));
        var before = leader.Ledger.PayloadReads;
        var ids = await client.ReadBatchAsync("chunks", 0, idsOnly: true);
        Assert.True(ids.Messages.Single().Data.IsEmpty);
        Assert.Equal(before, leader.Ledger.PayloadReads);
        using var output = new MemoryStream();
        Assert.True(await client.CopyMessageToAsync("chunks", 1, output));
        Assert.Equal(SHA256.HashData(payload), SHA256.HashData(output.GetBuffer().AsSpan(0, (int)output.Length)));
        var fence = await leader.Node.ReadIndexAsync();
        await RuntimeCluster.EventuallyAsync(() => cluster.Nodes.Values.All(host => host.Node.Status.LastApplied >= fence.Index));
        Assert.All(cluster.Nodes.Values, host => Assert.Equal(payload.Length, host.Ledger.GetMessage("chunks", 1)!.PayloadLength));
    }

    [Fact]
    public async Task Hot_add_remove_and_empty_snapshot_join_refresh_existing_client_endpoints()
    {
        await using var cluster = new ClientTestCluster(3);
        await cluster.StartAsync();
        var leader = await cluster.LeaderAsync();
        var seed = leader.Node.Status.NodeId;
        await using var client = cluster.Client(seed);
        for (var index = 0; index < 100; index++) await client.AddLogEntryAsync(BitConverter.GetBytes(index), "events");
        var original = cluster.Nodes.ToDictionary(pair => pair.Key, pair => pair.Value);
        await leader.Node.CreateSnapshotAsync(); // Joining nodes can no longer depend on the full technical log.
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        foreach (var id in new[] { "n4", "n5" })
        {
            await cluster.StartNodeAsync(id);
            var added = await leader.Server.AddNodeAsync(cluster.Peer(id), cluster.Endpoint(id), Guid.NewGuid(), deadline.Token);
            Assert.Equal(MembershipOperationStage.Completed, added.Stage);
        }
        await RuntimeCluster.EventuallyAsync(() => client.KnownEndpoints.Count == 5, 15_000);
        Assert.Equal(1, cluster.Nodes["n5"].Ledger.ReadBatch("events", 0).Messages[0].MessageId);
        foreach (var pair in original) Assert.Same(pair.Value, cluster.Nodes[pair.Key]);
        var removeId = Guid.NewGuid();
        MembershipOperationStatus removed;
        while (true)
        {
            try { removed = await (await cluster.LeaderAsync()).Node.RemoveNodeAsync(seed, removeId, deadline.Token); break; }
            catch (NotLeaderException) { await Task.Delay(25, deadline.Token); }
        }
        Assert.Equal(MembershipOperationStage.Completed, removed.Stage);
        await RuntimeCluster.EventuallyAsync(() => client.KnownEndpoints.Count == 4 && client.KnownEndpoints.All(endpoint => endpoint.NodeId != seed), 20_000);
        await cluster.StopAsync(seed);
        Assert.Equal(101, await client.AddLogEntryAsync(new byte[] { 101 }, "events"));
        Assert.Equal(101, (await client.ReadBatchAsync("events", 0)).Messages.Count);
    }

    [Fact]
    public async Task Cache_subscription_returns_latest_and_create_rejects_mode_change()
    {
        await using var cluster = new ClientTestCluster(1);
        await cluster.StartAsync(); await cluster.LeaderAsync();
        await using var client = cluster.Client();
        await client.CreateEntityAsync("cache", EntityMode.HighSpeedCache);
        for (var index = 1; index <= 10; index++) Assert.Equal(index, await client.AddLogEntryAsync(BitConverter.GetBytes(index), "cache"));
        var batch = await client.ReadBatchAsync("cache", 2);
        Assert.Equal(10, batch.Messages.Single().MessageId);
        using var discarded = new MemoryStream();
        Assert.False(await client.CopyMessageToAsync("cache", 2, discarded));
        var conflict = await Assert.ThrowsAsync<ClusterRequestException>(() => client.CreateEntityAsync("cache", EntityMode.Standard));
        Assert.Equal("entity_mode_conflict", conflict.Code);
    }

    private sealed class WrongGroupHandler : HttpMessageHandler
    {
        public int Posts;
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken token)
        {
            if (request.Method == HttpMethod.Post) Interlocked.Increment(ref Posts);
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
            { Content = new StringContent(JsonSerializer.Serialize(new ClusterTopology("other", "n1", 1, 0, 0, true, [new("n1", new Uri("http://localhost:1234/"))]))) });
        }
    }
    [Fact]
    public async Task Wrong_group_is_rejected_before_any_publication_is_sent()
    {
        using var handler = new WrongGroupHandler(); using var http = new HttpClient(handler);
        await using var client = new RaftClusterClient(new() { SeedEndpoints = [new("http://localhost:1234/")], HttpClient = http });
        var error = await Assert.ThrowsAsync<ClusterRequestException>(() => client.AddLogEntryAsync(new byte[] { 1 }));
        Assert.Equal("wrong_group", error.Code); Assert.Equal(0, handler.Posts);
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => client.AddLogEntryAsync(Stream.Null, ClusterLimits.MaxPayloadBytes + 1L));
    }
}
