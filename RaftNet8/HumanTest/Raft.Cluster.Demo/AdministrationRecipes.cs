using Raft.Cluster.Client;

namespace Raft.Cluster.Demo;

/// <summary>Compiled examples used by both offline human books.</summary>
public static class AdministrationRecipes
{
    public static RaftClusterClient ClientWithToken(string group, IReadOnlyList<Uri> seeds)
    {
        // book:start token-provider
        return new RaftClusterClient(new()
        {
            GroupId = group,
            SeedEndpoints = seeds,
            // Called for discovery and every HTTP attempt. A host may read a rotating secret vault here.
            AccessTokenProvider = cancellation => ValueTask.FromResult(
                Environment.GetEnvironmentVariable("RAFT_CLUSTER_ACCESS_TOKEN"))
        });
        // book:end token-provider
    }
    public static async Task<IssuedClientToken> IssuePublisherAsync(RaftClusterAdminClient admin, Guid operationId, CancellationToken token)
    {
        // book:start issue-token
        var issued = await admin.IssueClientTokenAsync(new TokenGrant(
            "orders publisher",
            ClusterPermission.Discover | ClusterPermission.Publish | ClusterPermission.ReadMetadata,
            ["orders"], DateTimeOffset.UtcNow.AddDays(7)), operationId, token);
        // Persist the secret in the application's secret store NOW. It is returned only once.
        // A retry after a lost response returns the same token metadata and Secret == null.
        return issued;
        // book:end issue-token
    }
    public static async Task ConfigureAsync(RaftClusterAdminClient admin, Guid operationId, CancellationToken token)
    {
        // book:start entity-policy
        await admin.ConfigureEntityAsync("orders", new EntitySettings(
            WritesPaused: false, MaxPayloadBytes: 64 * 1024,
            MaxRetainedBytes: 1024L * 1024 * 1024), operationId, token);
        // Policies and publications are applied in Raft log order, including concurrent publishers.
        // Pausing or lowering a quota keeps every already-retained message.
        // book:end entity-policy
    }
    public static async Task<AdminOperationView> AddAsync(RaftClusterAdminClient admin, Guid operationId, CancellationToken token)
    {
        // book:start admin-add
        // Start n4 separately, with the original bootstrap configuration and JoiningNode=true.
        await admin.StartOperationAsync(new("add", Add: new(
            new AdminPeer("n4", "127.0.0.1", 6104),
            new ClientEndpoint("n4", new("http://127.0.0.1:7104/")),
            new ManagedNodeEndpoint("n4", new("http://127.0.0.1:7204/")))), operationId, token);
        // Acceptance is durable; closing this client does not cancel the operation.
        return await admin.GetOperationAsync(operationId, token);
        // book:end admin-add
    }
}
