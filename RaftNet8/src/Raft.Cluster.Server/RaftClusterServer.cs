using System.Buffers.Binary;
using System.Net;
using System.Security.Cryptography;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;

namespace Raft.Cluster.Server;

public sealed record ClusterServerOptions
{
    public required ClientEndpoint Endpoint { get; init; }
    public string GroupId { get; init; } = "default";
    public IReadOnlyList<ClientEndpoint> InitialEndpoints { get; init; } = Array.Empty<ClientEndpoint>();
    public int MaxConcurrentRequests { get; init; } = 4;
    public int MaxConnections { get; init; } = 256;
    public TimeSpan RequestTimeout { get; init; } = TimeSpan.FromSeconds(30);
    /// <summary>Configure HTTPS certificates, logging or other host services. Peer mTLS is configured separately.</summary>
    public Action<WebApplicationBuilder>? ConfigureHttpServer { get; init; }
    /// <summary>Null means trusted-network access. The host may validate bearer tokens or client certificates here.</summary>
    public Func<HttpContext, CancellationToken, ValueTask<bool>>? Authorize { get; init; }
}

/// <summary>HTTP(S) gateway over an existing node and its MessageLedger. Neither dependency is owned.</summary>
public sealed class RaftClusterServer : IAsyncDisposable
{
    private readonly RaftNode _node;
    private readonly MessageLedger _ledger;
    private readonly ClusterServerOptions _options;
    private readonly SemaphoreSlim _requests;
    private readonly CancellationTokenSource _stop = new();
    private readonly WebApplication _http;
    private Task? _registration;
    private int _disposed;
    private Exception? _registrationError;

    private RaftClusterServer(RaftNode node, MessageLedger ledger, ClusterServerOptions options, WebApplication http)
    {
        _node = node; _ledger = ledger; _options = options; _http = http;
        _requests = new(options.MaxConcurrentRequests);
    }
    public Exception? RegistrationError => Volatile.Read(ref _registrationError);

    public static async Task<RaftClusterServer> StartAsync(RaftNode node, MessageLedger ledger,
        ClusterServerOptions options, CancellationToken token = default)
    {
        options.Endpoint.Validate();
        if (options.Endpoint.NodeId != node.Status.NodeId || options.MaxConcurrentRequests is < 1 or > 64 ||
            options.MaxConnections is < 1 or > 10_000 || options.RequestTimeout < TimeSpan.FromSeconds(1) ||
            options.RequestTimeout > TimeSpan.FromMinutes(10) || options.InitialEndpoints.Count > ClusterLimits.MaxEndpoints)
            throw new ArgumentException("Invalid cluster server options.");
        var frozen = options with { InitialEndpoints = Array.AsReadOnly(options.InitialEndpoints.Select(endpoint => endpoint.Validate()).ToArray()) };
        var builder = WebApplication.CreateSlimBuilder(new WebApplicationOptions { Args = [] });
        builder.Logging.ClearProviders();
        builder.WebHost.UseUrls(options.Endpoint.Address.AbsoluteUri);
        builder.WebHost.ConfigureKestrel(kestrel =>
        {
            kestrel.Limits.MaxRequestBodySize = ClusterLimits.MaxPayloadBytes;
            kestrel.Limits.MaxRequestHeadersTotalSize = 16 * 1024;
            kestrel.Limits.MaxConcurrentConnections = options.MaxConnections;
            kestrel.Limits.KeepAliveTimeout = TimeSpan.FromMinutes(2);
        });
        options.ConfigureHttpServer?.Invoke(builder);
        var http = builder.Build();
        var server = new RaftClusterServer(node, ledger, frozen, http);
        http.Run(server.HandleAsync);
        try
        {
            await http.StartAsync(token).ConfigureAwait(false);
            server._registration = server.RegisterBootstrapAsync();
            return server;
        }
        catch { await server.DisposeAsync().ConfigureAwait(false); throw; }
    }

    public async Task RegisterClientEndpointAsync(ClientEndpoint endpoint, Guid requestId, CancellationToken token = default)
    {
        endpoint.Validate();
        var registered = _ledger.GetEndpoints().Endpoints;
        if (registered.Count >= ClusterLimits.MaxEndpoints && !registered.Any(item => item.NodeId == endpoint.NodeId))
            throw new ArgumentException("Client endpoint directory is full.");
        await ExecuteAsync(new(MessageOperation.RegisterEndpoint, requestId, endpoint.NodeId, EntityMode.Standard,
            JsonSerializer.SerializeToUtf8Bytes(endpoint)), token).ConfigureAwait(false);
    }

    /// <summary>Persist the new client origin before starting the journaled Add; it is advertised only once the node has a role.</summary>
    public async Task<MembershipOperationStatus> AddNodeAsync(PeerDescriptor peer, ClientEndpoint endpoint,
        Guid operationId, CancellationToken token = default)
    {
        if (endpoint.NodeId != peer.NodeId) throw new ArgumentException("Peer and client identities differ.");
        await RegisterClientEndpointAsync(endpoint, operationId, token).ConfigureAwait(false);
        return await _node.AddNodeAsync(peer, operationId, token).ConfigureAwait(false);
    }

    private async Task RegisterBootstrapAsync()
    {
        var endpoints = _options.InitialEndpoints.Append(_options.Endpoint).DistinctBy(endpoint => endpoint.NodeId).ToArray();
        long registeredTerm = -1;
        while (!_stop.IsCancellationRequested)
        {
            try
            {
                var status = _node.Status;
                if (status.Role == RaftRole.Leader && status.Term != registeredTerm)
                {
                    var registered = _ledger.GetEndpoints().Endpoints.ToDictionary(endpoint => endpoint.NodeId);
                    foreach (var endpoint in endpoints)
                    {
                        var peer = _node.Status.Membership?.Peers.FirstOrDefault(item => item.NodeId == endpoint.NodeId);
                        if (peer is not null && !peer.IsRetired && !registered.ContainsKey(endpoint.NodeId))
                            await RegisterClientEndpointAsync(endpoint, Guid.NewGuid(), _stop.Token).ConfigureAwait(false);
                    }
                    registeredTerm = status.Term;
                }
                Volatile.Write(ref _registrationError, null);
            }
            catch (OperationCanceledException) when (_stop.IsCancellationRequested) { break; }
            catch (NotLeaderException) { }
            catch (LeadershipTransferInProgressException) { }
            catch (Exception error) { Volatile.Write(ref _registrationError, error); }
            try { await Task.Delay(TimeSpan.FromSeconds(1), _stop.Token).ConfigureAwait(false); }
            catch (OperationCanceledException) when (_stop.IsCancellationRequested) { break; }
        }
    }

    private async Task<RequestOutcome> ExecuteAsync(MessageCommand command, CancellationToken token)
    {
        if (_node.Status.Role != RaftRole.Leader) throw new NotLeaderException(_node.Status.LeaderId);
        var encoded = command.Encode();
        // Fresh random IDs must not require an extra disk transaction before admission.
        // Older evicted duplicates can consume a log entry, but deterministic application still deduplicates them.
        var existing = _ledger.CachedOutcome(command.RequestId);
        if (existing is null)
        {
            await _node.ProposeAsync(encoded, token).ConfigureAwait(false);
            existing = _ledger.Outcome(command.RequestId) ?? throw new InvalidDataException("Applied publication has no durable result.");
        }
        else await _node.ReadIndexAsync(token).ConfigureAwait(false);
        if (!CryptographicOperations.FixedTimeEquals(existing.Hash, MessageCommand.Hash(encoded)))
            throw new ClusterRequestException("request_conflict", "The request ID belongs to a different command.");
        if (existing.Error is { } error) throw new ClusterRequestException(error, error);
        return existing;
    }

    public ClusterTopology GetTopology(bool authoritative = false)
    {
        var status = _node.Status;
        var stored = _ledger.GetEndpoints();
        var known = stored.Endpoints.Concat(_options.InitialEndpoints).Append(_options.Endpoint)
            .DistinctBy(endpoint => endpoint.NodeId)
            .Where(endpoint => status.Configuration?.Contains(endpoint.NodeId) == true)
            .OrderBy(endpoint => endpoint.NodeId, StringComparer.Ordinal).ToArray();
        return new(_options.GroupId, status.Role == RaftRole.Leader ? status.NodeId : status.LeaderId,
            status.Term, status.Configuration?.Index ?? 0, stored.Index,
            authoritative && status.Role == RaftRole.Leader, Array.AsReadOnly(known));
    }

    private async Task HandleAsync(HttpContext context)
    {
        if (!_requests.Wait(0))
        {
            context.Response.StatusCode = 429;
            context.Response.Headers.RetryAfter = "1";
            return;
        }
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(context.RequestAborted, _stop.Token);
        deadline.CancelAfter(_options.RequestTimeout);
        var token = deadline.Token;
        var executing = false;
        try
        {
            if (_options.Authorize is { } authorize && !await authorize(context, token).ConfigureAwait(false))
            { context.Response.StatusCode = 401; return; }
            var path = context.Request.Path.Value;
            if (path == ClusterLimits.ApiPath + "/topology" && context.Request.Method == "GET")
            {
                var leader = _node.Status.Role == RaftRole.Leader;
                executing = true;
                if (leader) await _node.ReadIndexAsync(token).ConfigureAwait(false);
                await context.Response.WriteAsJsonAsync(GetTopology(leader), token).ConfigureAwait(false);
                return;
            }
            if (_node.Status.Role != RaftRole.Leader) throw new NotLeaderException(_node.Status.LeaderId);
            var entity = context.Request.Query["entity"].ToString();
            ClusterLimits.ValidateName(entity);
            if (context.Request.Method == "POST" && path == ClusterLimits.ApiPath + "/publish")
            {
                var requestId = Guid.Parse(context.Request.Headers["X-Raft-RequestId"].ToString());
                if (requestId == Guid.Empty) throw new ArgumentException("Request ID must not be empty.");
                var length = context.Request.ContentLength ?? throw new ClusterRequestException("length_required", "Content-Length is required.");
                ClusterLimits.ValidatePayloadLength(length);
                var bytes = GC.AllocateUninitializedArray<byte>((int)length);
                await context.Request.Body.ReadExactlyAsync(bytes, token).ConfigureAwait(false);
                executing = true;
                var result = await ExecuteAsync(new(MessageOperation.Append, requestId, entity, EntityMode.Standard, bytes), token).ConfigureAwait(false);
                await context.Response.WriteAsJsonAsync(result.MessageId, token).ConfigureAwait(false);
            }
            else if (context.Request.Method == "POST" && path == ClusterLimits.ApiPath + "/create")
            {
                var requestId = Guid.Parse(context.Request.Headers["X-Raft-RequestId"].ToString());
                var mode = (EntityMode)int.Parse(context.Request.Query["mode"].ToString(), System.Globalization.CultureInfo.InvariantCulture);
                if (!Enum.IsDefined(mode)) throw new ArgumentException("Invalid entity mode.");
                if (requestId == Guid.Empty) throw new ArgumentException("Request ID must not be empty.");
                executing = true;
                await ExecuteAsync(new(MessageOperation.Create, requestId, entity, mode, ReadOnlyMemory<byte>.Empty), token).ConfigureAwait(false);
                await context.Response.WriteAsJsonAsync(_ledger.GetEntity(entity), token).ConfigureAwait(false);
            }
            else if (context.Request.Method == "GET" && path == ClusterLimits.ApiPath + "/entity")
            {
                executing = true;
                await _node.ReadIndexAsync(token).ConfigureAwait(false);
                var info = _ledger.GetEntity(entity);
                if (info is null) context.Response.StatusCode = 404;
                else await context.Response.WriteAsJsonAsync(info, token).ConfigureAwait(false);
            }
            else if (context.Request.Method == "GET" && path == ClusterLimits.ApiPath + "/messages")
            {
                var after = long.Parse(context.Request.Query["after"].ToString(), System.Globalization.CultureInfo.InvariantCulture);
                var idsOnly = bool.Parse(context.Request.Query["idsOnly"].ToString());
                var count = int.Parse(context.Request.Query["count"].ToString(), System.Globalization.CultureInfo.InvariantCulture);
                var byteLimit = int.Parse(context.Request.Query["bytes"].ToString(), System.Globalization.CultureInfo.InvariantCulture);
                if (after < 0 || count is < 1 or > ClusterLimits.MaxBatchCount ||
                    byteLimit is < ClusterLimits.MaxPayloadBytes or > ClusterLimits.MaxBatchBytes) throw new ArgumentException("Invalid batch limits.");
                executing = true;
                await _node.ReadIndexAsync(token).ConfigureAwait(false);
                var batch = _ledger.ReadBatch(entity, after, idsOnly, count, byteLimit);
                context.Response.ContentType = "application/vnd.raft-cluster.batch-v1";
                context.Response.ContentLength = 9L + batch.Messages.Sum(message => 16L + (idsOnly ? 0 : message.PayloadLength));
                var header = new byte[16];
                BinaryPrimitives.WriteInt32LittleEndian(header, ClusterLimits.BatchMagic);
                BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(4), batch.Messages.Count);
                header[8] = (byte)batch.Mode;
                await context.Response.Body.WriteAsync(header.AsMemory(0, 9), token).ConfigureAwait(false);
                foreach (var message in batch.Messages)
                {
                    BinaryPrimitives.WriteInt64LittleEndian(header, message.MessageId);
                    BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(8), message.PayloadLength);
                    BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(12), idsOnly ? 0 : message.PayloadLength);
                    await context.Response.Body.WriteAsync(header, token).ConfigureAwait(false);
                    if (!idsOnly) await context.Response.Body.WriteAsync(message.Data, token).ConfigureAwait(false);
                }
            }
            else if (context.Request.Method == "GET" && path == ClusterLimits.ApiPath + "/message")
            {
                var id = long.Parse(context.Request.Query["id"].ToString(), System.Globalization.CultureInfo.InvariantCulture);
                if (id <= 0) throw new ArgumentException("Message ID must be positive.");
                executing = true;
                await _node.ReadIndexAsync(token).ConfigureAwait(false);
                var message = _ledger.GetMessage(entity, id);
                if (message is null) { context.Response.StatusCode = 404; return; }
                context.Response.ContentType = "application/octet-stream";
                context.Response.ContentLength = message.PayloadLength;
                await context.Response.Body.WriteAsync(message.Data, token).ConfigureAwait(false);
            }
            else context.Response.StatusCode = 404;
        }
        catch (NotLeaderException error) { await ErrorAsync(context, 409, "not_leader", error.Message, error.LeaderId).ConfigureAwait(false); }
        catch (LeadershipTransferInProgressException error) { await ErrorAsync(context, 503, "transferring", error.Message).ConfigureAwait(false); }
        catch (UncommittedLogLimitException error) { await ErrorAsync(context, 429, "busy", error.Message).ConfigureAwait(false); }
        catch (ClusterRequestException error) { await ErrorAsync(context, error.Code == "entity_not_found" ? 404 : 409, error.Code, error.Message).ConfigureAwait(false); }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        { if (!context.Response.HasStarted) context.Response.StatusCode = 504; else context.Abort(); }
        catch (Exception error) when (!executing && error is (ArgumentException or FormatException or OverflowException or EndOfStreamException or BadHttpRequestException))
        { await ErrorAsync(context, 400, "invalid_request", error.Message).ConfigureAwait(false); }
        catch (Exception)
        { await ErrorAsync(context, 503, "unavailable", "The node is unavailable. Observe RaftNode.Completion for its original failure.").ConfigureAwait(false); }
        finally { _requests.Release(); }
    }
    private static async Task ErrorAsync(HttpContext context, int status, string code, string message, string? leader = null)
    {
        if (context.Response.HasStarted) { context.Abort(); return; }
        context.Response.ContentLength = null;
        context.Response.StatusCode = status;
        try { await context.Response.WriteAsJsonAsync(new ClusterError(code, message, leader), context.RequestAborted).ConfigureAwait(false); }
        catch (OperationCanceledException) { }
    }
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        _stop.Cancel();
        if (_registration is not null) await _registration.ConfigureAwait(false);
        await _http.StopAsync().ConfigureAwait(false);
        await _http.DisposeAsync().ConfigureAwait(false);
        _stop.Dispose(); _requests.Dispose();
    }
}
