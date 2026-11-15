using System.Net;
using System.Net.Http.Headers;
using Raft.Cluster;
using Raft.Cluster.Client;

namespace Raft.Tests;

[Collection("OS process clusters")]
public sealed class ClusterAdminHttpTests
{
    [Fact]
    public async Task Admission_rejection_is_counted_while_an_accepted_publication_waits_for_its_body()
    {
        await using var cluster = new ClientTestCluster(1) { ManagementEnabled = true, MaxHttpRequests = 1 };
        await cluster.StartAsync();
        var leader = await cluster.LeaderAsync();
        await using var bootstrap = cluster.Admin();
        var root = await bootstrap.BootstrapAsync(cluster.BootstrapSecret, Guid.NewGuid());
        await using var admin = cluster.Admin(root.Secret);
        var management = cluster.ManagementEndpoint(leader.Node.Status.NodeId);
        var before = (await admin.GetDiagnosticsAsync(management)).FailedRequests;
        var endpoint = cluster.Endpoint(leader.Node.Status.NodeId).Address;
        using var stalled = new System.Net.Sockets.TcpClient();
        await stalled.ConnectAsync(endpoint.Host, endpoint.Port);
        var stream = stalled.GetStream();
        var headers = $"POST {ClusterLimits.ApiPath}/publish?entity=blocked HTTP/1.1\r\nHost: {endpoint.Authority}\r\nX-Raft-RequestId: {Guid.NewGuid()}\r\nContent-Length: 1\r\nExpect: 100-continue\r\nConnection: close\r\n\r\n";
        await stream.WriteAsync(System.Text.Encoding.ASCII.GetBytes(headers));
        using var reader = new StreamReader(stream, System.Text.Encoding.ASCII, false, 1024, true);
        // 100 Continue proves that the server admitted this request and started reading its body.
        Assert.Equal("HTTP/1.1 100 Continue", await reader.ReadLineAsync().WaitAsync(TimeSpan.FromSeconds(10)));
        using var raw = new HttpClient();
        using var rejected = await raw.GetAsync(new Uri(endpoint, ClusterLimits.ApiPath + "/topology"));
        Assert.Equal(HttpStatusCode.TooManyRequests, rejected.StatusCode);
        Assert.Equal(before + 1, (await admin.GetDiagnosticsAsync(management)).FailedRequests);
        Assert.Null(leader.Ledger.GetEntity("blocked"));
    }

    [Fact]
    public async Task Administrative_client_discovers_the_leader_from_one_authenticated_follower_seed()
    {
        await using var cluster = new ClientTestCluster(3) { ManagementEnabled = true };
        await cluster.StartAsync();
        var leader = await cluster.LeaderAsync();
        await using var bootstrap = cluster.Admin();
        var root = await bootstrap.BootstrapAsync(cluster.BootstrapSecret, Guid.NewGuid());
        await RuntimeCluster.EventuallyAsync(() => cluster.Nodes.Values.All(host => host.Ledger.SecurityInitialized));
        var follower = cluster.Nodes.Keys.First(id => id != leader.Node.Status.NodeId);
        await using var single = new RaftClusterAdminClient(new()
        {
            GroupId = cluster.GroupId, SeedEndpoints = [cluster.ManagementEndpoint(follower).Address],
            AccessTokenProvider = _ => ValueTask.FromResult(root.Secret), OperationTimeout = TimeSpan.FromSeconds(2)
        });
        Assert.Equal(leader.Node.Status.NodeId, (await single.GetStatusAsync()).NodeId);
    }
    [Fact]
    public async Task Administrative_add_3_to_5_and_current_leader_removal_survive_publication_load()
    {
        await using var cluster = new ClientTestCluster(3) { ManagementEnabled = true, NativeTokens = true };
        await cluster.StartAsync();
        await cluster.LeaderAsync();
        await using var bootstrap = cluster.Admin();
        var root = await bootstrap.BootstrapAsync(cluster.BootstrapSecret, Guid.NewGuid());
        await using var admin = cluster.Admin(root.Secret);
        await using var publisher = cluster.AuthenticatedClient(root.Secret!);
        var published = 0;
        using var loadStop = new CancellationTokenSource();
        var load = Task.Run(async () =>
        {
            while (!loadStop.IsCancellationRequested)
            {
                try { await publisher.AddLogEntryAsync(new byte[] { 1, 2, 3 }, "live", cancellationToken: loadStop.Token); Interlocked.Increment(ref published); }
                catch (OperationCanceledException) when (loadStop.IsCancellationRequested) { break; }
            }
        });
        try
        {
            foreach (var id in new[] { "n4", "n5" })
            {
                await cluster.StartNodeAsync(id);
                var operation = Guid.NewGuid();
                await admin.StartOperationAsync(new("add", Add: new(new(id, cluster.Peer(id).Host, cluster.Peer(id).Port),
                    cluster.Endpoint(id), cluster.ManagementEndpoint(id))), operation);
                await RuntimeCluster.EventuallyAsync(() => cluster.Nodes.Values.Any(node => node.Ledger.GetAdminOperation(operation)?.State == "Completed"), 30_000);
            }
            Assert.Equal(5, (await admin.GetStatusAsync()).Voters.Count);
            var oldLeader = await cluster.LeaderAsync();
            var remove = Guid.NewGuid();
            await admin.StartOperationAsync(new("remove", oldLeader.Node.Status.NodeId), remove);
            await RuntimeCluster.EventuallyAsync(() => cluster.Nodes.Values.Any(node => node.Node.Status.Role == RaftRole.Leader &&
                node.Ledger.GetAdminOperation(remove)?.State == "Completed"), 30_000);
            Assert.Equal(4, (await admin.GetStatusAsync()).Voters.Count);
            Assert.NotEqual(oldLeader.Node.Status.NodeId, (await admin.GetStatusAsync()).NodeId);
            Assert.True(Volatile.Read(ref published) > 0);
            Assert.All(cluster.Nodes.Values, node => Assert.Null(node.Node.Status.Fault));
        }
        finally { loadStop.Cancel(); await load; }
    }
    [Fact]
    public async Task Native_security_issues_tokens_checks_every_request_and_orders_revocation()
    {
        await using var cluster = new ClientTestCluster(3) { ManagementEnabled = true, NativeTokens = true };
        await cluster.StartAsync();
        var leader = await cluster.LeaderAsync();
        using var raw = new HttpClient();
        var denied = await raw.GetAsync(new Uri(cluster.Endpoint(leader.Node.Status.NodeId).Address, ClusterLimits.ApiPath + "/topology"));
        Assert.Equal(HttpStatusCode.Unauthorized, denied.StatusCode);
        await using var bootstrap = cluster.Admin();
        var rootId = Guid.NewGuid();
        var root = await bootstrap.BootstrapAsync(cluster.BootstrapSecret, rootId);
        Assert.NotNull(root.Secret);
        var repeated = await bootstrap.BootstrapAsync(cluster.BootstrapSecret, rootId);
        Assert.Null(repeated.Secret);
        await using var admin = cluster.Admin(root.Secret);
        Assert.Equal(3, (await admin.RefreshDirectoryAsync()).Count);
        var overview = await admin.GetOverviewAsync();
        Assert.Equal(cluster.GroupId, overview.Status.GroupId);
        Assert.Equal(3, overview.Directory.Count);
        Assert.NotNull(overview.LeaderDiagnostics);
        Assert.Equal(root.Token.Grant.ExpiresAt, overview.CredentialExpiresAt);
        var grant = new TokenGrant("publisher", ClusterPermission.Discover | ClusterPermission.Publish | ClusterPermission.ReadMetadata |
            ClusterPermission.CreateEntity, ["events"], DateTimeOffset.UtcNow.AddHours(1));
        var issueId = Guid.NewGuid();
        var issued = await admin.IssueClientTokenAsync(grant, issueId);
        Assert.NotNull(issued.Secret);
        Assert.Null((await admin.IssueClientTokenAsync(grant, issueId)).Secret);
        await using var client = cluster.AuthenticatedClient(issued.Secret!);
        Assert.Equal(1, await client.AddLogEntryAsync(new byte[] { 1, 2 }, "events"));
        Assert.Equal(2, (await client.GetEntityAsync("events"))!.RetainedBytes);
        Assert.Equal("AQI=", (await admin.PreviewMessageAsync("events", 1)).Base64);
        Assert.Contains("MaxPendingProposals", (await admin.GetConfigurationAsync()).Keys);
        await using (var restrictedAdmin = cluster.Admin(issued.Secret))
            await Assert.ThrowsAsync<ClusterRequestException>(() => restrictedAdmin.PreviewMessageAsync("events", 1));
        await admin.UpdateClientTokenAsync(issued.Token with { Grant = grant with { Permissions = grant.Permissions | ClusterPermission.ReadPayload | ClusterPermission.Observe } }, Guid.NewGuid());
        await using (var previewClient = cluster.Admin(issued.Secret))
            Assert.Equal("AQI=", (await previewClient.PreviewMessageAsync("events", 1)).Base64);
        await Assert.ThrowsAsync<ClusterRequestException>(() => client.AddLogEntryAsync(new byte[] { 1 }, "denied"));
        var entity = (await admin.GetEntitiesAsync()).Single();
        Assert.Equal("events", entity.Entity.Name);
        await admin.ConfigureEntityAsync("events", new(true), Guid.NewGuid());
        await Assert.ThrowsAsync<ClusterRequestException>(() => client.AddLogEntryAsync(new byte[] { 1 }, "events"));
        await admin.RevokeClientTokenAsync(issued.Token.TokenId, Guid.NewGuid());
        raw.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", issued.Secret);
        denied = await raw.GetAsync(new Uri(cluster.Endpoint(leader.Node.Status.NodeId).Address, ClusterLimits.ApiPath + "/entity?entity=events"));
        Assert.Equal(HttpStatusCode.Unauthorized, denied.StatusCode);
        foreach (var endpoint in admin.KnownEndpoints)
        {
            var diagnostics = await admin.GetDiagnosticsAsync(endpoint);
            Assert.Equal(cluster.GroupId, diagnostics.Status.GroupId);
            Assert.True(diagnostics.Resources.WorkingSetBytes > 0);
            Assert.NotEmpty(diagnostics.CoreMetrics);
        }
        Assert.NotEmpty(await admin.GetAuditAsync());
    }

    [Fact]
    public async Task Concurrent_HTTP_publications_cannot_overrun_a_committed_quota_and_pause_preserves_history()
    {
        await using var cluster = new ClientTestCluster(3) { ManagementEnabled = true, NativeTokens = true };
        await cluster.StartAsync();
        await cluster.LeaderAsync();
        await using var bootstrap = cluster.Admin();
        var root = await bootstrap.BootstrapAsync(cluster.BootstrapSecret, Guid.NewGuid());
        await using var admin = cluster.Admin(root.Secret);
        await using var client = cluster.AuthenticatedClient(root.Secret!);
        await admin.CreateEntityAsync("bounded", EntityMode.Standard, Guid.NewGuid());
        await admin.ConfigureEntityAsync("bounded", new(false, 4, 64), Guid.NewGuid());

        var ids = await Task.WhenAll(Enumerable.Range(0, 64).Select(async _ =>
        {
            try { return await client.AddLogEntryAsync(new byte[] { 1, 2, 3, 4 }, "bounded"); }
            catch (ClusterRequestException error) when (error.Code == "entity_quota") { return 0L; }
        }));
        Assert.Equal(Enumerable.Range(1, 16).Select(value => (long)value), ids.Where(id => id > 0).Order());
        Assert.Equal(48, ids.Count(id => id == 0));
        var full = (await client.GetEntityAsync("bounded"))!;
        Assert.Equal(64, full.RetainedBytes);
        Assert.Equal(16, full.RetainedMessages);

        await admin.ConfigureEntityAsync("bounded", new(true, 4, 64), Guid.NewGuid());
        var denied = await Assert.ThrowsAsync<ClusterRequestException>(() => client.AddLogEntryAsync(new byte[] { 9 }, "bounded"));
        Assert.Equal("entity_paused", denied.Code);
        await admin.ConfigureEntityAsync("bounded", new(false, 4, 128), Guid.NewGuid());
        Assert.Equal(17, await client.AddLogEntryAsync(new byte[] { 9 }, "bounded"));
        await RuntimeCluster.EventuallyAsync(() => cluster.Nodes.Values.All(host => host.Ledger.GetEntity("bounded")?.LastMessageId == 17));
        Assert.All(cluster.Nodes.Values, host => Assert.Equal(65, host.Ledger.GetEntity("bounded")!.RetainedBytes));
        Assert.Equal(Enumerable.Range(1, 17).Select(value => (long)value),
            (await client.ReadBatchAsync("bounded", 0)).Messages.Select(message => message.MessageId));
    }

    [Fact]
    public async Task Durable_snapshot_operation_finishes_and_can_be_queried_after_reconnect()
    {
        await using var cluster = new ClientTestCluster(3) { ManagementEnabled = true };
        await cluster.StartAsync();
        await cluster.LeaderAsync();
        await using var bootstrap = cluster.Admin();
        var root = await bootstrap.BootstrapAsync(cluster.BootstrapSecret, Guid.NewGuid());
        await using var admin = cluster.Admin(root.Secret);
        var id = Guid.NewGuid();
        await admin.StartOperationAsync(new("snapshot"), id);
        await RuntimeCluster.EventuallyAsync(() => cluster.Nodes.Values.Any(node => node.Ledger.GetAdminOperation(id)?.State == "Completed"), 20_000);
        var result = await admin.GetOperationAsync(id);
        Assert.Equal("Completed", result.Operation!.State);
        Assert.Contains((await admin.GetOperationsAsync()), operation => operation.OperationId == id);
        Assert.NotNull((await admin.GetStatusAsync()).SnapshotIndex);
    }
}
