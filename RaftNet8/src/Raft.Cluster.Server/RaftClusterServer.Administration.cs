using System.Net;
using System.Net.Http.Json;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Server.Kestrel.Https;
using Microsoft.Extensions.Logging;

namespace Raft.Cluster.Server;

public sealed partial class RaftClusterServer
{
    private WebApplication? _management;
    private Task? _adminWorker;
    private Task? _cancelWorker;
    private NodeTelemetry? _telemetry;
    private ObservationReadBatch? _observationReads;
    private long _observationRequests, _observationReadCalls;
    internal (long Requests, long Calls) ObservationReadCounts => (Interlocked.Read(ref _observationRequests), Interlocked.Read(ref _observationReadCalls));
    private readonly HttpClient _readFences;
    private readonly bool _ownsReadFences;
    private readonly SemaphoreSlim _managementRequests = new(16, 16);
    private readonly SemaphoreSlim _diagnosticRequests = new(8, 8);
    private static int StatusCode(string code) => code switch
    { "unauthorized" => 401, "forbidden" => 403, "entity_not_found" or "token_not_found" => 404, _ => 409 };
    private static string? Bearer(HttpContext context)
    {
        var header = context.Request.Headers.Authorization.ToString();
        return header.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase) ? header[7..] : null;
    }
    private CommandAuthorization? AuthorizeData(HttpContext context)
    {
        if (!_options.Security.RequireClientTokens && !_ledger.RequiresClientTokens &&
            !(_ledger.SecurityInitialized && context.Request.Headers.Authorization.ToString().StartsWith("Bearer rc2.", StringComparison.OrdinalIgnoreCase))) return null;
        if (!_ledger.SecurityInitialized) throw new ClusterRequestException("unauthorized", "Cluster security has not been initialized.");
        var path = context.Request.Path.Value ?? "";
        var permission = path.EndsWith("/topology", StringComparison.Ordinal) ? ClusterPermission.Discover :
            path.EndsWith("/publish", StringComparison.Ordinal) ? ClusterPermission.Publish :
            path.EndsWith("/create", StringComparison.Ordinal) ? ClusterPermission.CreateEntity :
            path.EndsWith("/message", StringComparison.Ordinal) || path.EndsWith("/messages", StringComparison.Ordinal) &&
                context.Request.Query["idsOnly"] != "True" && context.Request.Query["idsOnly"] != "true" ? ClusterPermission.ReadPayload : ClusterPermission.ReadMetadata;
        return _ledger.Authenticate(Bearer(context), permission, context.Request.Query["entity"].ToString(), DateTimeOffset.UtcNow);
    }
    private async Task StartManagementAsync(CancellationToken token)
    {
        if (_options.Security.SecureTransport && _options.Endpoint.Address.Scheme != "https")
            throw new ArgumentException("Secure transport requires an HTTPS client endpoint.");
        if (_options.Security.RequireClientTokens && _ledger.Version != ClusterLedgerVersion.V2)
            throw new ArgumentException("Native token security requires cluster-v2.");
        if (_options.Management is not { } options) return;
        if (options.SampleIntervalSeconds is < 1 or > 60 || options.FileScanIntervalSeconds is < 1 or > 3600)
            throw new ArgumentException("Invalid resource collection intervals.");
        if (_ledger.Version != ClusterLedgerVersion.V2) throw new ArgumentException("Administration requires cluster-v2; migrate the stopped cluster.");
        _ = new ClientEndpoint(_node.Status.NodeId, options.Endpoint).Validate();
        if (_options.Security.SecureTransport && options.Endpoint.Scheme != "https")
            throw new ArgumentException("Secure transport requires an HTTPS management endpoint.");
        if (options.Endpoint.Scheme == "http" && !options.Endpoint.IsLoopback && !options.AllowInsecureRemote)
            throw new ArgumentException("Remote HTTP administration must explicitly opt into a trusted network.");
        if (options.BootstrapSecret is { Length: < 32 }) throw new ArgumentException("Bootstrap secrets need at least32 random characters.");
        if (options.InitialEndpoints.Count > ClusterLimits.MaxEndpoints) throw new ArgumentException("Too many management endpoints.");
        foreach (var endpoint in options.InitialEndpoints) _ = new ClientEndpoint(endpoint.NodeId, endpoint.Address).Validate();
        var builder = WebApplication.CreateSlimBuilder(new WebApplicationOptions { Args = [] });
        builder.Logging.ClearProviders();
        builder.WebHost.UseUrls(options.Endpoint.AbsoluteUri);
        builder.WebHost.ConfigureKestrel(server =>
        {
            server.Limits.MaxRequestBodySize = 64 * 1024;
            server.Limits.MaxRequestHeadersTotalSize = 16 * 1024;
            server.Limits.MaxConcurrentConnections = 128;
            server.ConfigureHttpsDefaults(https => https.ClientCertificateMode = ClientCertificateMode.AllowCertificate);
        });
        options.ConfigureHttpServer?.Invoke(builder);
        _management = builder.Build();
        _management.Run(HandleManagementAsync);
        if (options.BatchObservationReads) _observationReads = new(InvokeObservationRead, _stop.Token, _options.RequestTimeout);
        if (options.EnableTelemetry) _telemetry = new(_ledger, options, _options.GroupId, _node.Status.NodeId);
        await _management.StartAsync(token).ConfigureAwait(false);
        _adminWorker = AdministrationLoopAsync();
        _cancelWorker = CancellationLoopAsync();
    }
    public AdminNodeStatus GetAdminStatus()
    {
        var status = _node.Status;
        return new(status.NodeId, _options.GroupId, _ledger.Version == ClusterLedgerVersion.V2 ? Administration.ApplicationVersion : "cluster-v1",
            status.Role.ToString(), status.Term, status.LeaderId, status.CommitIndex, status.LastLogIndex, status.LastApplied,
            status.Configuration?.Index ?? 0, status.Configuration?.Voters.ToArray() ?? [], status.Configuration?.Learners.ToArray() ?? [],
            status.Peers.Select(peer => new AdminPeerStatus(peer.NodeId, peer.IsVoter, peer.MatchIndex, peer.NextIndex,
                peer.Mode.ToString(), peer.InFlightMessages, peer.InFlightBytes, peer.SnapshotOffset)).ToArray(),
            status.UncommittedBytes, status.Snapshot?.LastIncludedIndex, status.Fault is null ? null : "Node failed; inspect the host logs.");
    }
    private CommandAuthorization AuthorizeAdmin(HttpContext context, ClusterPermission permission, string entity = "") =>
        _ledger.Authenticate(Bearer(context), permission, entity, DateTimeOffset.UtcNow);
    private async Task AuthorizeDiagnosticAsync(HttpContext context, CancellationToken token)
    {
        var options = _options.Management!;
        var certificate = context.Connection.ClientCertificate;
        if (context.Request.IsHttps && certificate is not null &&
            options.DiagnosticCertificateThumbprints.Contains(certificate.Thumbprint, StringComparer.OrdinalIgnoreCase)) return;
        _ = AuthorizeAdmin(context, ClusterPermission.Observe);
        if (_node.Status.Role == RaftRole.Leader) await ObservationFenceAsync(token).ConfigureAwait(false);
        else
        {
            var leader = _node.Status.LeaderId ?? throw new NotLeaderException(null);
            var endpoint = ManagementDirectory().FirstOrDefault(item => item.NodeId == leader)
                ?? throw new NotLeaderException(leader);
            using var request = new HttpRequestMessage(HttpMethod.Get, new Uri(endpoint.Address, Administration.ApiPath + "/barrier"));
            request.Headers.Add("X-Raft-GroupId", _options.GroupId);
            request.Headers.Authorization = new("Bearer", Bearer(context));
            using var response = await _readFences.SendAsync(request, token).ConfigureAwait(false);
            if (!response.IsSuccessStatusCode) throw new ClusterRequestException("diagnostics_unavailable", "Cannot obtain the leader authorization barrier.");
            var fence = await response.Content.ReadFromJsonAsync<long>(cancellationToken: token).ConfigureAwait(false);
            while (_ledger.LastApplied < fence) await Task.Delay(10, token).ConfigureAwait(false);
        }
        _ = AuthorizeAdmin(context, ClusterPermission.Observe);
    }
    private async Task HandleManagementAsync(HttpContext context)
    {
        var admission = context.Request.Path == Administration.ApiPath + "/diagnostics" ? _diagnosticRequests : _managementRequests;
        if (!await admission.WaitAsync(0, context.RequestAborted).ConfigureAwait(false))
        { context.Response.StatusCode = 429; return; }
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(context.RequestAborted, _stop.Token);
        deadline.CancelAfter(_options.RequestTimeout);
        var token = deadline.Token;
        var executing = false;
        try
        {
            var path = context.Request.Path.Value ?? "";
            var api = Administration.ApiPath;
            if (context.Request.Headers["X-Raft-GroupId"] != _options.GroupId)
                throw new ClusterRequestException("group_mismatch", "Management endpoint belongs to another group.");
            if (context.Request.Method == "GET" && path == api + "/capabilities")
            {
                var peer = _options.Management!.LocalPeer;
                await context.Response.WriteAsJsonAsync(new NodeJoinCapability(_node.Status.NodeId, _options.GroupId,
                    _ledger.Version == ClusterLedgerVersion.V2 ? Administration.ApplicationVersion : "cluster-v1",
                    _options.Security.RequireClientTokens, _options.Security.SecureTransport,
                    _node.Status.Configuration?.Contains(_node.Status.NodeId) == true,
                    peer is null ? null : new(peer.NodeId, peer.Host, peer.Port), _options.Endpoint,
                    new(_node.Status.NodeId, _options.Management.Endpoint)), token).ConfigureAwait(false);
                return;
            }
            if (context.Request.Method == "GET" && path == api + "/diagnostics")
            {
                executing = true;
                await AuthorizeDiagnosticAsync(context, token).ConfigureAwait(false);
                if (_telemetry is null) throw new ClusterRequestException("telemetry_disabled", "Telemetry is disabled.");
                await context.Response.WriteAsJsonAsync(_telemetry.Read(GetAdminStatus()), token).ConfigureAwait(false);
                return;
            }
            if (_node.Status.Role != RaftRole.Leader) throw new NotLeaderException(_node.Status.LeaderId);
            if (path == api + "/bootstrap" && context.Request.Method == "POST")
            {
                if (context.Connection.RemoteIpAddress is not { } remote || !IPAddress.IsLoopback(remote))
                    throw new ClusterRequestException("forbidden", "Bootstrap is local only.");
                var expected = _options.Management!.BootstrapSecret;
                var supplied = context.Request.Headers["X-Raft-Bootstrap"].ToString();
                if (expected is null || supplied.Length > 256 || !CryptographicOperations.FixedTimeEquals(
                    SHA256.HashData(Encoding.UTF8.GetBytes(expected)), SHA256.HashData(Encoding.UTF8.GetBytes(supplied))))
                    throw new ClusterRequestException("unauthorized", "Invalid bootstrap credential.");
                var id = RequestGuid(context);
                executing = true;
                await _node.ReadIndexAsync(token).ConfigureAwait(false);
                if (_ledger.SecurityInitialized)
                {
                    var existing = _ledger.GetToken(id) ?? throw new ClusterRequestException("already_initialized", "Security is already initialized.");
                    await context.Response.WriteAsJsonAsync(new IssuedClientToken(existing, null), token).ConfigureAwait(false);
                    return;
                }
                foreach (var voter in _node.Status.Configuration!.Voters.Where(id => id != _node.Status.NodeId))
                {
                    var endpoint = ManagementDirectory().FirstOrDefault(item => item.NodeId == voter)
                        ?? throw new ClusterRequestException("missing_management_endpoint", "Configure management endpoints for every voter before activating cluster-v2 administration.");
                    _ = await ReadCapabilitiesAsync(endpoint, token).ConfigureAwait(false);
                }
                var grant = new TokenGrant("bootstrap administrator", ClusterPermission.All, ["*"], DateTimeOffset.UtcNow.AddDays(30));
                var (stored, secret) = NewCredential(id, grant);
                await ExecuteAsync(new(MessageOperation.BootstrapSecurity, id, "security", EntityMode.Standard,
                    JsonSerializer.SerializeToUtf8Bytes(new BootstrapSecurity(stored, _options.Security.RequireClientTokens))), token).ConfigureAwait(false);
                await context.Response.WriteAsJsonAsync(new IssuedClientToken(_ledger.GetToken(id)!, secret), token).ConfigureAwait(false);
                return;
            }
            var permission = path.Contains("/tokens", StringComparison.Ordinal) ? ClusterPermission.ManageTokens :
                path == api + "/preview" ? ClusterPermission.ReadPayload :
                context.Request.Method == "GET" ? ClusterPermission.Observe :
                path == api + "/entities/settings" || path == api + "/entities/create" ? ClusterPermission.ManageEntities : ClusterPermission.ManageNodes;
            var entity = path == api + "/entities/settings" || path == api + "/preview" ? context.Request.Query["entity"].ToString() : "";
            var authorization = AuthorizeAdmin(context, permission, entity);
            if (context.Request.Method == "GET")
            {
                long previewId = 0;
                if (path == api + "/preview")
                {
                    ClusterLimits.ValidateName(entity);
                    if (!long.TryParse(context.Request.Query["id"], out previewId) || previewId < 1) throw new ArgumentException("Message ID must be positive.");
                }
                Guid operationId = default;
                if (path == api + "/operation") operationId = Guid.Parse(context.Request.Query["id"].ToString());
                executing = true;
                var fence = await ObservationFenceAsync(token).ConfigureAwait(false);
                _ = AuthorizeAdmin(context, permission, entity);
                object? value;
                if (path == api + "/status") value = GetAdminStatus();
                else if (path == api + "/overview")
                {
                    var status = GetAdminStatus();
                    value = new AdminOverview(status, ManagementDirectory(), _ledger.GetAdminEntities(),
                        await ReadOperationsAsync(0, Administration.MaxPageSize, token).ConfigureAwait(false),
                        _ledger.GetToken(authorization.TokenId)?.Grant.ExpiresAt, _telemetry?.Read(status));
                }
                else if (path == api + "/credential") value = _ledger.GetToken(authorization.TokenId);
                else if (path == api + "/configuration") value = _options.Management!.EffectiveConfiguration;
                else if (path == api + "/preview")
                {
                    value = _ledger.PreviewMessage(entity, previewId);
                    if (value is null) { context.Response.StatusCode = 404; return; }
                }
                else if (path == api + "/barrier") value = fence.Index;
                else if (path == api + "/directory") value = ManagementDirectory();
                else if (path == api + "/entities") value = _ledger.GetAdminEntities();
                else if (path == api + "/tokens") value = _ledger.GetTokens(PageCount(context),
                    Guid.TryParse(context.Request.Query["after"], out var after) ? after : null);
                else if (path == api + "/audit") value = AfterIndex(context) == 0 ? _ledger.GetRecentAudit(PageCount(context)) : _ledger.GetAudit(AfterIndex(context), PageCount(context));
                else if (path == api + "/operations")
                {
                    value = await ReadOperationsAsync(AfterIndex(context), PageCount(context), token).ConfigureAwait(false);
                }
                else if (path == api + "/operation")
                {
                    var id = operationId;
                    var operation = _ledger.GetAdminOperation(id);
                    value = new { Operation = operation, Membership = operation?.Request.Kind is "add" or "remove"
                        ? await _node.GetMembershipOperationAsync(id, token).ConfigureAwait(false) : null };
                }
                else { context.Response.StatusCode = 404; return; }
                await context.Response.WriteAsJsonAsync(value, token).ConfigureAwait(false);
                return;
            }
            if (context.Request.Method != "POST") { context.Response.StatusCode = 405; return; }
            var requestId = RequestGuid(context);
            if (path == api + "/tokens/issue")
            {
                var grant = (await context.Request.ReadFromJsonAsync<TokenGrant>(token).ConfigureAwait(false)
                    ?? throw new ArgumentException("Missing grant.")).Validate();
                if (grant.ExpiresAt <= DateTimeOffset.UtcNow) throw new ArgumentException("Token expiry must be in the future.");
                executing = true;
                await _node.ReadIndexAsync(token).ConfigureAwait(false);
                _ = AuthorizeAdmin(context, permission);
                if (_ledger.GetToken(requestId) is { } prior)
                {
                    if (!JsonSerializer.SerializeToUtf8Bytes(prior.Grant).AsSpan().SequenceEqual(JsonSerializer.SerializeToUtf8Bytes(grant)))
                        throw new ClusterRequestException("request_conflict", "This ID belongs to another token grant.");
                    await context.Response.WriteAsJsonAsync(new IssuedClientToken(prior, null), token).ConfigureAwait(false);
                    return;
                }
                var (stored, secret) = NewCredential(requestId, grant);
                await ExecuteAsync(new(MessageOperation.IssueToken, requestId, requestId.ToString("N"), EntityMode.Standard,
                    JsonSerializer.SerializeToUtf8Bytes(stored), authorization), token).ConfigureAwait(false);
                await context.Response.WriteAsJsonAsync(new IssuedClientToken(_ledger.GetToken(requestId)!, secret), token).ConfigureAwait(false);
                return;
            }
            MessageCommand command;
            if (path == api + "/tokens/revoke")
            {
                var id = Guid.Parse(context.Request.Query["id"].ToString());
                command = new(MessageOperation.RevokeToken, requestId, id.ToString("N"), EntityMode.Standard, ReadOnlyMemory<byte>.Empty, authorization);
            }
            else if (path == api + "/tokens/update")
            {
                var update = await context.Request.ReadFromJsonAsync<ClientTokenInfo>(token).ConfigureAwait(false) ?? throw new ArgumentException("Missing token.");
                update.Grant.Validate();
                command = new(MessageOperation.UpdateToken, requestId, update.TokenId.ToString("N"), EntityMode.Standard,
                    JsonSerializer.SerializeToUtf8Bytes(update), authorization);
            }
            else if (path == api + "/entities/create")
            {
                var create = await context.Request.ReadFromJsonAsync<CreateManagedEntity>(token).ConfigureAwait(false) ?? throw new ArgumentException("Missing entity.");
                ClusterLimits.ValidateName(create.Name);
                if (!Enum.IsDefined(create.Mode)) throw new ArgumentException("Invalid entity mode.");
                command = new(MessageOperation.Create, requestId, create.Name, create.Mode, ReadOnlyMemory<byte>.Empty, authorization);
            }
            else if (path == api + "/entities/settings")
            {
                ClusterLimits.ValidateName(entity);
                var settings = (await context.Request.ReadFromJsonAsync<EntitySettings>(token).ConfigureAwait(false) ?? throw new ArgumentException("Missing settings.")).Validate();
                command = new(MessageOperation.ConfigureEntity, requestId, entity, EntityMode.Standard, JsonSerializer.SerializeToUtf8Bytes(settings), authorization);
            }
            else if (path == api + "/operations")
            {
                var request = await context.Request.ReadFromJsonAsync<AdminOperationRequest>(token).ConfigureAwait(false) ?? throw new ArgumentException("Missing operation.");
                ValidateOperation(request);
                if (request.Add is { } add)
                {
                    executing = true;
                    await _node.ReadIndexAsync(token).ConfigureAwait(false);
                    if (_ledger.GetAdminOperation(requestId) is null)
                    {
                        if (_node.Status.Configuration?.Contains(add.Peer.NodeId) == true)
                            throw new ClusterRequestException("already_member", "This node already has a cluster role.");
                        var capability = await ReadCapabilitiesAsync(add.ManagementEndpoint, token).ConfigureAwait(false);
                        if (capability.HasVotingRole || capability.Peer != add.Peer || capability.ClientEndpoint != add.ClientEndpoint)
                            throw new ClusterRequestException("invalid_joining_node", "Candidate identity, advertised routes or joining state do not match the Add request.");
                    }
                }
                command = new(MessageOperation.AdminIntent, requestId, request.NodeId ?? request.Add?.Peer.NodeId ?? "snapshot",
                    EntityMode.Standard, JsonSerializer.SerializeToUtf8Bytes(request), authorization);
            }
            else if (path == api + "/operations/cancel")
            {
                var id = Guid.Parse(context.Request.Query["id"].ToString());
                executing = true;
                await ExecuteAsync(new(MessageOperation.CancelAdmin, requestId, id.ToString("N"), EntityMode.Standard,
                    JsonSerializer.SerializeToUtf8Bytes(id), authorization), token).ConfigureAwait(false);
                while (true)
                {
                    var operation = _ledger.GetAdminOperation(id);
                    if (operation?.CancelError is { } cancelError) throw new ClusterRequestException("too_late", cancelError);
                    if (operation?.State != "Accepted")
                    {
                        if (operation?.State != "Cancelled") throw new ClusterRequestException("too_late", "Operation has already reached " + operation?.State + ".");
                        await context.Response.WriteAsJsonAsync(operation, token).ConfigureAwait(false); break;
                    }
                    if (_node.Status.Role != RaftRole.Leader) throw new NotLeaderException(_node.Status.LeaderId);
                    await Task.Delay(25, token).ConfigureAwait(false);
                }
                return;
            }
            else { context.Response.StatusCode = 404; return; }
            executing = true;
            await ExecuteAsync(command, token).ConfigureAwait(false);
            context.Response.StatusCode = path == api + "/operations" ? 202 : 200;
            await context.Response.WriteAsJsonAsync(new { OperationId = requestId }, token).ConfigureAwait(false);
        }
        catch (NotLeaderException error)
        {
            ManagedNodeEndpoint? hint = null;
            try
            {
                _ = AuthorizeAdmin(context, ClusterPermission.Observe);
                hint = ManagementDirectory().FirstOrDefault(endpoint => endpoint.NodeId == error.LeaderId);
            }
            catch (ClusterRequestException) { } // Anonymous/denied callers receive no additional address.
            if (context.Response.HasStarted) { context.Abort(); return; }
            context.Response.StatusCode = 409;
            await context.Response.WriteAsJsonAsync(new ClusterError("not_leader", error.Message, error.LeaderId) { ManagementLeader = hint }, context.RequestAborted).ConfigureAwait(false);
        }
        catch (ClusterRequestException error) { await ErrorAsync(context, StatusCode(error.Code), error.Code, error.Message).ConfigureAwait(false); }
        catch (MembershipCancellationTooLateException error) { await ErrorAsync(context, 409, "too_late", error.Message).ConfigureAwait(false); }
        catch (OperationCanceledException) when (token.IsCancellationRequested) { if (!context.Response.HasStarted) context.Response.StatusCode = 504; else context.Abort(); }
        catch (Exception error) when (!executing && error is ArgumentException or FormatException or JsonException or BadHttpRequestException)
        { await ErrorAsync(context, 400, "invalid_request", error.Message).ConfigureAwait(false); }
        catch (Exception) { await ErrorAsync(context, 503, "unavailable", "Administration is unavailable; inspect the node host.").ConfigureAwait(false); }
        finally { admission.Release(); }
    }
    private static Guid RequestGuid(HttpContext context)
    {
        var id = Guid.Parse(context.Request.Headers["X-Raft-RequestId"].ToString());
        if (id == Guid.Empty) throw new ArgumentException("Operation ID must not be empty.");
        return id;
    }
    private Task<ReadIndexResult> ObservationFenceAsync(CancellationToken token)
    {
        Interlocked.Increment(ref _observationRequests);
        return _observationReads?.ReadAsync(token) ?? InvokeObservationRead(token);
    }
    private Task<ReadIndexResult> InvokeObservationRead(CancellationToken token)
    {
        Interlocked.Increment(ref _observationReadCalls);
        return _node.ReadIndexAsync(token);
    }
    private async Task<IReadOnlyList<AdminOperation>> ReadOperationsAsync(long after, int count, CancellationToken token)
    {
        var operations = after == 0 ? _ledger.GetRecentAdminOperations(count) : _ledger.GetAdminOperations(after, count);
        if (_ledger.ActiveAdminOperation is { Request.Kind: "add" or "remove" } active)
        {
            var membership = await _node.GetMembershipOperationAsync(active.OperationId, token).ConfigureAwait(false);
            operations = operations.Select(operation => operation.OperationId == active.OperationId
                ? operation with { MembershipStage = membership?.Stage.ToString() } : operation).ToArray();
        }
        return operations;
    }
    private async Task<NodeJoinCapability> ReadCapabilitiesAsync(ManagedNodeEndpoint endpoint, CancellationToken token)
    {
        if (_options.Security.SecureTransport && endpoint.Address.Scheme != "https")
            throw new ClusterRequestException("insecure_endpoint", "Secure groups require HTTPS management routes.");
        using var request = new HttpRequestMessage(HttpMethod.Get, new Uri(endpoint.Address, Administration.ApiPath + "/capabilities"));
        request.Headers.Add("X-Raft-GroupId", _options.GroupId);
        using var response = await _readFences.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, token).ConfigureAwait(false);
        if (!response.IsSuccessStatusCode || response.Content.Headers.ContentLength > 16 * 1024)
            throw new ClusterRequestException("incompatible_node", "Candidate does not expose a compatible cluster-v2 capability endpoint.");
        await using var stream = await response.Content.ReadAsStreamAsync(token).ConfigureAwait(false);
        using var memory = new MemoryStream();
        var buffer = new byte[4096];
        while (true)
        {
            var count = await stream.ReadAsync(buffer, token).ConfigureAwait(false);
            if (count == 0) break;
            if (memory.Length + count > 16 * 1024) throw new InvalidDataException("Capability response exceeds16KiB.");
            memory.Write(buffer, 0, count);
        }
        var capability = JsonSerializer.Deserialize<NodeJoinCapability>(memory.GetBuffer().AsSpan(0, (int)memory.Length),
            new JsonSerializerOptions(JsonSerializerDefaults.Web)) ?? throw new InvalidDataException("Missing capability response.");
        if (capability.NodeId != endpoint.NodeId || capability.GroupId != _options.GroupId ||
            capability.ApplicationVersion != Administration.ApplicationVersion ||
            capability.RequireClientTokens != _options.Security.RequireClientTokens || capability.SecureTransport != _options.Security.SecureTransport)
            throw new ClusterRequestException("incompatible_node", "Node ID, group, application version or security profile differs.");
        return capability;
    }
    private static int PageCount(HttpContext context) => int.TryParse(context.Request.Query["count"], out var count) && count is >= 1 and <= Administration.MaxPageSize
        ? count : Administration.MaxPageSize;
    private static long AfterIndex(HttpContext context) => long.TryParse(context.Request.Query["after"], out var index) && index >= 0 ? index : 0;
    private IReadOnlyList<ManagedNodeEndpoint> ManagementDirectory() => _ledger.GetManagementEndpoints()
        .Where(endpoint => !_options.Security.SecureTransport || endpoint.Address.Scheme == "https")
        .Concat(_options.Management!.InitialEndpoints).Append(new(_node.Status.NodeId, _options.Management.Endpoint))
        .DistinctBy(item => item.NodeId).Where(item => _node.Status.Configuration?.Contains(item.NodeId) == true).ToArray();
    private static (StoredClientToken stored, string secret) NewCredential(Guid id, TokenGrant grant)
    {
        var bytes = RandomNumberGenerator.GetBytes(32);
        return (new(new(id, grant, false, 0), SHA256.HashData(bytes)), "rc2." + id.ToString("N") + "." + Convert.ToBase64String(bytes));
    }
    private static void ValidateOperation(AdminOperationRequest request)
    {
        if (request.Kind == "add" && request.Add is { } add)
        {
            add.Peer.Validate();
            add.ClientEndpoint.Validate();
            _ = new ClientEndpoint(add.ManagementEndpoint.NodeId, add.ManagementEndpoint.Address).Validate();
            if (add.ClientEndpoint.NodeId != add.Peer.NodeId || add.ManagementEndpoint.NodeId != add.Peer.NodeId)
                throw new ArgumentException("All node identities must agree.");
        }
        else if (request.Kind is "remove" or "transfer" && request.NodeId is { } id) _ = new ClientEndpoint(id, new Uri("http://127.0.0.1/")).Validate();
        else if (request.Kind != "snapshot") throw new ArgumentException("Invalid administrative action.");
    }
    private async Task AdministrationLoopAsync()
    {
        long registeredTerm = -1;
        while (!_stop.IsCancellationRequested)
        {
            try
            {
                if (_node.Status.Role == RaftRole.Leader)
                {
                    if (registeredTerm != _node.Status.Term)
                    {
                        var term = _node.Status.Term;
                        var registered = _ledger.GetManagementEndpoints().Select(item => item.NodeId).ToHashSet(StringComparer.Ordinal);
                        foreach (var endpoint in _options.Management!.InitialEndpoints.Append(new(_node.Status.NodeId, _options.Management.Endpoint)))
                            if (!registered.Contains(endpoint.NodeId))
                                await ExecuteAsync(new(MessageOperation.RegisterManagementEndpoint, Guid.NewGuid(), endpoint.NodeId,
                                    EntityMode.Standard, JsonSerializer.SerializeToUtf8Bytes(endpoint)), _stop.Token).ConfigureAwait(false);
                        registeredTerm = term;
                    }
                    if (_ledger.ActiveAdminOperation is { } operation) await RunAdminOperationAsync(operation, _stop.Token).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (_stop.IsCancellationRequested) { break; }
            catch (NotLeaderException) { }
            catch (LeadershipTransferInProgressException) { }
            catch (Exception error) { Volatile.Write(ref _registrationError, error); }
            try { await Task.Delay(250, _stop.Token).ConfigureAwait(false); }
            catch (OperationCanceledException) when (_stop.IsCancellationRequested) { break; }
        }
    }
    private async Task RunAdminOperationAsync(AdminOperation operation, CancellationToken token)
    {
        string? result = null, failure = null, finalState = null;
        try
        {
            switch (operation.Request.Kind)
            {
                case "add":
                    var add = operation.Request.Add!;
                    if (_ledger.ActiveAdminOperation?.CancelRequested == true &&
                        await _node.GetMembershipOperationAsync(operation.OperationId, token).ConfigureAwait(false) is null)
                    { finalState = "Cancelled"; result = "Cancelled before learner registration."; break; }
                    await ExecuteAsync(new(MessageOperation.RegisterManagementEndpoint, DerivedId(operation.OperationId, "management"),
                        add.Peer.NodeId, EntityMode.Standard, JsonSerializer.SerializeToUtf8Bytes(add.ManagementEndpoint)), token).ConfigureAwait(false);
                    await RegisterClientEndpointAsync(add.ClientEndpoint, DerivedId(operation.OperationId, "client"), token).ConfigureAwait(false);
                    var added = await _node.AddNodeAsync(new(add.Peer.NodeId, add.Peer.Host, add.Peer.Port), operation.OperationId, token).ConfigureAwait(false);
                    result = JsonSerializer.Serialize(added); finalState = added.Stage.ToString(); failure = added.Error;
                    break;
                case "remove":
                    var removed = await _node.RemoveNodeAsync(operation.Request.NodeId!, operation.OperationId, token).ConfigureAwait(false);
                    result = JsonSerializer.Serialize(removed); finalState = removed.Stage.ToString(); failure = removed.Error; break;
                case "transfer":
                    if (_node.Status.NodeId != operation.Request.NodeId) await _node.TransferLeadershipAsync(operation.Request.NodeId!, token).ConfigureAwait(false);
                    result = "Leadership transferred.";
                    break;
                case "snapshot":
                    await _node.CreateSnapshotAsync(token).ConfigureAwait(false); result = "Snapshot created."; break;
            }
        }
        catch (NotLeaderException) { throw; }
        catch (OperationCanceledException) { throw; }
        catch (LeadershipTransferInProgressException) { throw; }
        catch (Exception error) when (error is ArgumentException or MembershipChangeInProgressException or MembershipOperationConflictException)
        { failure = error.Message; }
        await ExecuteAsync(new(MessageOperation.AdminResult, DerivedId(operation.OperationId, "result"), operation.OperationId.ToString("N"),
            EntityMode.Standard, JsonSerializer.SerializeToUtf8Bytes(new AdminResult(operation.OperationId, result, failure, finalState))), token).ConfigureAwait(false);
    }
    private async Task CancellationLoopAsync()
    {
        while (!_stop.IsCancellationRequested)
        {
            try
            {
                if (_node.Status.Role == RaftRole.Leader && _ledger.ActiveAdminOperation is { CancelRequested: true } operation &&
                    await _node.GetMembershipOperationAsync(operation.OperationId, _stop.Token).ConfigureAwait(false) is not null)
                {
                    try { await _node.CancelMembershipOperationAsync(operation.OperationId, _stop.Token).ConfigureAwait(false); }
                    catch (MembershipCancellationTooLateException)
                    {
                        await ExecuteAsync(new(MessageOperation.CancelResult, DerivedId(operation.OperationId, "cancel-too-late"), operation.OperationId.ToString("N"),
                            EntityMode.Standard, JsonSerializer.SerializeToUtf8Bytes(new AdminCancelResult(operation.OperationId, "Voter promotion has already started."))), _stop.Token).ConfigureAwait(false);
                    }
                }
            }
            catch (OperationCanceledException) when (_stop.IsCancellationRequested) { break; }
            catch (NotLeaderException) { }
            catch (LeadershipTransferInProgressException) { }
            catch (Exception error) { Volatile.Write(ref _registrationError, error); }
            try { await Task.Delay(100, _stop.Token).ConfigureAwait(false); }
            catch (OperationCanceledException) when (_stop.IsCancellationRequested) { break; }
        }
    }
    private static Guid DerivedId(Guid operation, string phase) => new(SHA256.HashData(Encoding.UTF8.GetBytes(operation.ToString("N") + ":" + phase)).AsSpan(0, 16));
    private async ValueTask StopManagementAsync()
    {
        if (_adminWorker is not null) await _adminWorker.ConfigureAwait(false);
        if (_cancelWorker is not null) await _cancelWorker.ConfigureAwait(false);
        if (_telemetry is not null) await _telemetry.DisposeAsync().ConfigureAwait(false);
        if (_management is not null) { await _management.StopAsync().ConfigureAwait(false); await _management.DisposeAsync().ConfigureAwait(false); }
        if (_ownsReadFences) _readFences.Dispose();
    }
}
