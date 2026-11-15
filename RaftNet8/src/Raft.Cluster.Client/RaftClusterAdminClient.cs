using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text.Json;

namespace Raft.Cluster.Client;

public sealed record RaftClusterAdminClientOptions
{
    public required IReadOnlyList<Uri> SeedEndpoints { get; init; }
    public string GroupId { get; init; } = "default";
    public Func<CancellationToken, ValueTask<string?>>? AccessTokenProvider { get; init; }
    public HttpClient? HttpClient { get; init; }
    public TimeSpan OperationTimeout { get; init; } = TimeSpan.FromSeconds(30);
    public int MaxConcurrentRequests { get; init; } = 4;
}

/// <summary>Non-voting management SDK. Caller operation IDs survive uncertain HTTP outcomes and leader changes.</summary>
public sealed class RaftClusterAdminClient : IAsyncDisposable
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);
    private readonly RaftClusterAdminClientOptions _options;
    private readonly HttpClient _http;
    private readonly bool _ownsHttp;
    private readonly SemaphoreSlim _requests;
    private readonly object _gate = new();
    private ManagedNodeEndpoint[] _endpoints;
    private int _preferred;
    private int _disposed;
    public RaftClusterAdminClient(RaftClusterAdminClientOptions options)
    {
        if (options.SeedEndpoints.Count is < 1 or > ClusterLimits.MaxEndpoints || options.MaxConcurrentRequests is < 1 or > 64 ||
            options.OperationTimeout < TimeSpan.FromSeconds(1) || options.OperationTimeout > TimeSpan.FromMinutes(10))
            throw new ArgumentException("Invalid administration client limits.");
        ClusterLimits.ValidateName(options.GroupId);
        _endpoints = options.SeedEndpoints.Select((uri, index) => new ManagedNodeEndpoint("seed_" + index,
            new ClientEndpoint("seed", uri).Validate().Address)).DistinctBy(endpoint => endpoint.Address).ToArray();
        _options = options with { SeedEndpoints = Array.AsReadOnly(options.SeedEndpoints.ToArray()) };
        _ownsHttp = options.HttpClient is null;
        _http = options.HttpClient ?? new(new SocketsHttpHandler
        {
            AllowAutoRedirect = false, AutomaticDecompression = DecompressionMethods.None, ConnectTimeout = TimeSpan.FromSeconds(2),
            MaxConnectionsPerServer = options.MaxConcurrentRequests + 1, PooledConnectionLifetime = TimeSpan.FromMinutes(5)
        }) { Timeout = Timeout.InfiniteTimeSpan };
        _requests = new(options.MaxConcurrentRequests);
    }
    public IReadOnlyList<ManagedNodeEndpoint> KnownEndpoints { get { lock (_gate) return Array.AsReadOnly(_endpoints.ToArray()); } }
    public async Task<AdminNodeStatus> GetStatusAsync(CancellationToken token = default)
    {
        var status = await SendAsync<AdminNodeStatus>(HttpMethod.Get, "/status", null, null, token).ConfigureAwait(false);
        if (status.GroupId != _options.GroupId || status.ApplicationVersion != Administration.ApplicationVersion)
            throw new ClusterRequestException("incompatible_cluster", "Management endpoint belongs to another group or application version.");
        return status;
    }
    public async Task<IReadOnlyList<ManagedNodeEndpoint>> RefreshDirectoryAsync(CancellationToken token = default)
    {
        await GetStatusAsync(token).ConfigureAwait(false);
        var directory = await SendAsync<ManagedNodeEndpoint[]>(HttpMethod.Get, "/directory", null, null, token).ConfigureAwait(false);
        AdoptDirectory(directory);
        return KnownEndpoints;
    }
    public async Task<AdminOverview> GetOverviewAsync(CancellationToken token = default)
    {
        var overview = await SendAsync<AdminOverview>(HttpMethod.Get, "/overview", null, null, token).ConfigureAwait(false);
        if (overview.Status.GroupId != _options.GroupId || overview.Status.ApplicationVersion != Administration.ApplicationVersion)
            throw new ClusterRequestException("incompatible_cluster", "Management endpoint belongs to another group or application version.");
        AdoptDirectory(overview.Directory.ToArray());
        return overview;
    }
    private void AdoptDirectory(ManagedNodeEndpoint[] directory)
    {
        if (directory.Length > ClusterLimits.MaxEndpoints) throw new InvalidDataException("Management directory exceeds its bound.");
        foreach (var endpoint in directory) new ClientEndpoint(endpoint.NodeId, endpoint.Address).Validate();
        lock (_gate)
        {
            if (directory.Length > 0)
            {
                var previous = _endpoints[_preferred % _endpoints.Length].Address;
                _endpoints = directory;
                _preferred = Math.Max(0, Array.FindIndex(directory, endpoint => endpoint.Address == previous));
            }
        }
    }
    public Task<AdminEntity[]> GetEntitiesAsync(CancellationToken token = default) => SendAsync<AdminEntity[]>(HttpMethod.Get, "/entities", null, null, token);
    public Task<ClientTokenInfo> GetCredentialInfoAsync(CancellationToken token = default) => SendAsync<ClientTokenInfo>(HttpMethod.Get, "/credential", null, null, token);
    public Task<Dictionary<string, string>> GetConfigurationAsync(CancellationToken token = default) =>
        SendAsync<Dictionary<string, string>>(HttpMethod.Get, "/configuration", null, null, token);
    public Task<MessagePreview> PreviewMessageAsync(string entity, long messageId, CancellationToken token = default)
    {
        ClusterLimits.ValidateName(entity);
        if (messageId < 1) throw new ArgumentOutOfRangeException(nameof(messageId));
        return SendAsync<MessagePreview>(HttpMethod.Get, "/preview?entity=" + Uri.EscapeDataString(entity) + "&id=" +
            messageId.ToString(System.Globalization.CultureInfo.InvariantCulture), null, null, token);
    }
    public Task<ClientTokenInfo[]> GetTokensAsync(Guid? after = null, int count = 100, CancellationToken token = default) =>
        SendAsync<ClientTokenInfo[]>(HttpMethod.Get, $"/tokens?count={count}&after={after}", null, null, token);
    public Task<AdminAuditEntry[]> GetAuditAsync(long afterIndex = 0, int count = 100, CancellationToken token = default) =>
        SendAsync<AdminAuditEntry[]>(HttpMethod.Get, $"/audit?after={afterIndex}&count={count}", null, null, token);
    public Task<AdminOperation[]> GetOperationsAsync(long afterIndex = 0, int count = 100, CancellationToken token = default) =>
        SendAsync<AdminOperation[]>(HttpMethod.Get, $"/operations?after={afterIndex}&count={count}", null, null, token);
    public Task<AdminOperationView> GetOperationAsync(Guid operationId, CancellationToken token = default) =>
        SendAsync<AdminOperationView>(HttpMethod.Get, "/operation?id=" + operationId, null, null, token);
    public Task<IssuedClientToken> IssueClientTokenAsync(TokenGrant grant, Guid operationId, CancellationToken token = default) =>
        SendAsync<IssuedClientToken>(HttpMethod.Post, "/tokens/issue", grant.Validate(), operationId, token);
    public Task RevokeClientTokenAsync(Guid tokenId, Guid operationId, CancellationToken token = default) =>
        SendAsync<JsonElement>(HttpMethod.Post, "/tokens/revoke?id=" + tokenId, null, operationId, token);
    public Task UpdateClientTokenAsync(ClientTokenInfo tokenInfo, Guid operationId, CancellationToken token = default) =>
        SendAsync<JsonElement>(HttpMethod.Post, "/tokens/update", tokenInfo with { Grant = tokenInfo.Grant.Validate() }, operationId, token);
    public Task ConfigureEntityAsync(string entity, EntitySettings settings, Guid operationId, CancellationToken token = default)
    {
        ClusterLimits.ValidateName(entity);
        return SendAsync<JsonElement>(HttpMethod.Post, "/entities/settings?entity=" + Uri.EscapeDataString(entity), settings.Validate(), operationId, token);
    }
    public Task CreateEntityAsync(string name, EntityMode mode, Guid operationId, CancellationToken token = default)
    {
        ClusterLimits.ValidateName(name);
        return SendAsync<JsonElement>(HttpMethod.Post, "/entities/create", new CreateManagedEntity(name, mode), operationId, token);
    }
    public Task StartOperationAsync(AdminOperationRequest operation, Guid operationId, CancellationToken token = default) =>
        SendAsync<JsonElement>(HttpMethod.Post, "/operations", operation, operationId, token);
    public Task CancelMembershipOperationAsync(Guid operationId, CancellationToken token = default) =>
        SendAsync<JsonElement>(HttpMethod.Post, "/operations/cancel?id=" + operationId, null, Guid.NewGuid(), token);

    /// <summary>Call from the node host using its protected one-time bootstrap secret. Lost responses return metadata with Secret=null.</summary>
    public Task<IssuedClientToken> BootstrapAsync(string localBootstrapSecret, Guid operationId, CancellationToken token = default) =>
        SendAsync<IssuedClientToken>(HttpMethod.Post, "/bootstrap", null, operationId, token, bootstrap: localBootstrapSecret);

    public async Task<NodeDiagnostics> GetDiagnosticsAsync(ManagedNodeEndpoint endpoint, CancellationToken token = default)
    {
        new ClientEndpoint(endpoint.NodeId, endpoint.Address).Validate();
        return await SendAsync<NodeDiagnostics>(HttpMethod.Get, "/diagnostics", null, null, token, direct: endpoint).ConfigureAwait(false);
    }
    private async Task<T> SendAsync<T>(HttpMethod method, string path, object? body, Guid? id, CancellationToken cancellationToken,
        ManagedNodeEndpoint? direct = null, string? bootstrap = null)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        if (id == Guid.Empty) throw new ArgumentException("Operation ID must not be empty.");
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(_options.OperationTimeout);
        var token = deadline.Token;
        await _requests.WaitAsync(token).ConfigureAwait(false);
        try
        {
            var payload = body is null ? null : JsonSerializer.SerializeToUtf8Bytes(body, Json);
            if (payload?.Length > 64 * 1024) throw new ArgumentException("Administrative body exceeds64KiB.");
            while (true)
            {
                ManagedNodeEndpoint[] endpoints;
                int preferred;
                lock (_gate) { endpoints = direct is null ? _endpoints : [direct]; preferred = direct is null ? _preferred : 0; }
                for (var index = 0; index < endpoints.Length; index++)
                {
                    token.ThrowIfCancellationRequested();
                    var target = (preferred + index) % endpoints.Length;
                    using var request = new HttpRequestMessage(method, new Uri(endpoints[target].Address, Administration.ApiPath + path));
                    request.Headers.Add("X-Raft-GroupId", _options.GroupId);
                    if (payload is not null) { request.Content = new ByteArrayContent(payload); request.Content.Headers.ContentType = new("application/json"); }
                    if (id.HasValue) request.Headers.Add("X-Raft-RequestId", id.Value.ToString("D"));
                    if (bootstrap is not null) request.Headers.Add("X-Raft-Bootstrap", bootstrap);
                    else if (_options.AccessTokenProvider is { } provider && await provider(token).ConfigureAwait(false) is { Length: > 0 } secret)
                        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", secret);
                    try
                    {
                        using var attempt = CancellationTokenSource.CreateLinkedTokenSource(token);
                        attempt.CancelAfter(TimeSpan.FromSeconds(3));
                        using var response = await _http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, attempt.Token).ConfigureAwait(false);
                        var bytes = await ReadBoundedAsync(response.Content, response.IsSuccessStatusCode ? 8 * 1024 * 1024 : 16 * 1024, attempt.Token).ConfigureAwait(false);
                        if (response.IsSuccessStatusCode)
                        {
                            if (direct is null) lock (_gate) _preferred = Array.FindIndex(_endpoints, endpoint => endpoint.Address == endpoints[target].Address) is var found && found >= 0 ? found : 0;
                            return JsonSerializer.Deserialize<T>(bytes, Json) ?? throw new InvalidDataException("Missing management response.");
                        }
                        var error = bytes.Length == 0 ? null : JsonSerializer.Deserialize<ClusterError>(bytes, Json);
                        if (direct is null && error is { Code: "not_leader", ManagementLeader: { } hint })
                        {
                            new ClientEndpoint(hint.NodeId, hint.Address).Validate();
                            if (endpoints[target].Address.Scheme == "https" && hint.Address.Scheme != "https")
                                throw new InvalidDataException("An HTTPS management route cannot redirect to plaintext.");
                            lock (_gate)
                            {
                                var found = Array.FindIndex(_endpoints, endpoint => endpoint.NodeId == hint.NodeId);
                                if (found >= 0) _endpoints[found] = hint;
                                else if (_endpoints.Length < ClusterLimits.MaxEndpoints) { found = _endpoints.Length; _endpoints = [.. _endpoints, hint]; }
                                if (found >= 0) _preferred = found;
                            }
                            break; // Retry using the newly learned leader, rather than cycling only the original seeds.
                        }
                        if (direct is null && (error?.Code == "not_leader" || response.StatusCode is HttpStatusCode.ServiceUnavailable or HttpStatusCode.GatewayTimeout)) continue;
                        throw new ClusterRequestException(error?.Code ?? "http_" + (int)response.StatusCode, error?.Message ?? "Management request failed.");
                    }
                    catch (HttpRequestException) when (direct is null) { }
                    catch (OperationCanceledException) when (!token.IsCancellationRequested && direct is null) { }
                }
                await Task.Delay(100, token).ConfigureAwait(false);
            }
        }
        finally { _requests.Release(); }
    }
    private static async Task<byte[]> ReadBoundedAsync(HttpContent content, int maximum, CancellationToken token)
    {
        if (content.Headers.ContentLength > maximum) throw new InvalidDataException("Oversized management response.");
        await using var stream = await content.ReadAsStreamAsync(token).ConfigureAwait(false);
        using var memory = new MemoryStream();
        var buffer = new byte[16 * 1024];
        while (true)
        {
            var count = await stream.ReadAsync(buffer, token).ConfigureAwait(false);
            if (count == 0) return memory.ToArray();
            if (memory.Length + count > maximum) throw new InvalidDataException("Oversized management response.");
            memory.Write(buffer, 0, count);
        }
    }
    public ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0) { if (_ownsHttp) _http.Dispose(); _requests.Dispose(); }
        return ValueTask.CompletedTask;
    }
}
