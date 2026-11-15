using System.Buffers;
using System.Buffers.Binary;
using System.Net;
using System.Net.Http.Headers;
using System.Text.Json;

namespace Raft.Cluster.Client;

public sealed record RaftClusterClientOptions
{
    public required IReadOnlyList<Uri> SeedEndpoints { get; init; }
    public string GroupId { get; init; } = "default";
    public int MaxConcurrentRequests { get; init; } = 4;
    public int MaxSubscriptions { get; init; } = 8;
    public TimeSpan OperationTimeout { get; init; } = TimeSpan.FromSeconds(30);
    public TimeSpan TopologyRefreshInterval { get; init; } = TimeSpan.FromSeconds(1);
    public TimeSpan PollInterval { get; init; } = TimeSpan.FromMilliseconds(100);
    public TimeSpan HandlerRetryDelay { get; init; } = TimeSpan.FromMilliseconds(500);
    /// <summary>A caller-owned HttpClient may supply authentication, mTLS and normal certificate validation.</summary>
    public HttpClient? HttpClient { get; init; }
}

/// <summary>External non-voting client. Durable subscription cursors belong to the application.</summary>
public sealed class RaftClusterClient : IAsyncDisposable
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);
    private readonly RaftClusterClientOptions _options;
    private readonly HttpClient _http;
    private readonly bool _ownsHttp;
    private readonly CancellationTokenSource _stop = new();
    private readonly SemaphoreSlim _requests, _subscriptions, _discovery = new(1, 1);
    private readonly object _gate = new();
    private readonly Uri[] _seeds;
    private ClusterTopology? _topology;
    private string? _leaderHint;
    private readonly Task _watch;
    private int _disposed;

    public RaftClusterClient(RaftClusterClientOptions options)
    {
        if (options.SeedEndpoints.Count is < 1 or > ClusterLimits.MaxEndpoints || options.MaxConcurrentRequests is < 1 or > 64 || options.MaxSubscriptions is < 1 or > 256 ||
            options.OperationTimeout < TimeSpan.FromSeconds(1) || options.OperationTimeout > TimeSpan.FromMinutes(10) ||
            options.TopologyRefreshInterval < TimeSpan.FromMilliseconds(100) || options.TopologyRefreshInterval > TimeSpan.FromMinutes(1) ||
            options.PollInterval < TimeSpan.FromMilliseconds(10) || options.PollInterval > TimeSpan.FromMinutes(1) ||
            options.HandlerRetryDelay < TimeSpan.FromMilliseconds(10) || options.HandlerRetryDelay > TimeSpan.FromMinutes(1))
            throw new ArgumentException("Invalid cluster client options.");
        ClusterLimits.ValidateName(options.GroupId);
        _seeds = options.SeedEndpoints.Select(uri => new ClientEndpoint("seed", uri).Validate().Address).Distinct().ToArray();
        _options = options with { SeedEndpoints = Array.AsReadOnly(_seeds) };
        _requests = new(options.MaxConcurrentRequests);
        _subscriptions = new(options.MaxSubscriptions);
        _ownsHttp = options.HttpClient is null;
        _http = options.HttpClient ?? new HttpClient(new SocketsHttpHandler
        {
            AllowAutoRedirect = false,
            AutomaticDecompression = DecompressionMethods.None,
            ConnectTimeout = TimeSpan.FromSeconds(2),
            MaxConnectionsPerServer = options.MaxConcurrentRequests + 1,
            PooledConnectionLifetime = TimeSpan.FromMinutes(5)
        })
        { Timeout = Timeout.InfiniteTimeSpan };
        _watch = WatchTopologyAsync();
    }

    public ClusterTopology? Topology { get { lock (_gate) return _topology; } }
    public IReadOnlyList<ClientEndpoint> KnownEndpoints => Topology?.Endpoints ?? Array.Empty<ClientEndpoint>();
    public event Action<ClusterTopology>? TopologyChanged;
    public event Action<Exception>? SubscriptionCallbackFailed;
    public Exception? DiscoveryError { get; private set; }

    private CancellationTokenSource Deadline(CancellationToken token)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        var source = CancellationTokenSource.CreateLinkedTokenSource(token, _stop.Token);
        source.CancelAfter(_options.OperationTimeout);
        return source;
    }

    public async Task<long> AddLogEntryAsync(ReadOnlyMemory<byte> data, string entityName = "default",
        Guid? requestId = null, CancellationToken cancellationToken = default)
    {
        ClusterLimits.ValidateName(entityName); ClusterLimits.ValidatePayloadLength(data.Length);
        var id = RequestId(requestId);
        using var deadline = Deadline(cancellationToken);
        await _requests.WaitAsync(deadline.Token).ConfigureAwait(false);
        try { return await PublishAsync(data, entityName, id, deadline.Token).ConfigureAwait(false); }
        finally { _requests.Release(); }
    }

    /// <summary>Buffers at most one message for repeatable network retries. The caller retains ownership of the source.</summary>
    public async Task<long> AddLogEntryAsync(Stream source, long length, string entityName = "default",
        Guid? requestId = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        ClusterLimits.ValidateName(entityName); ClusterLimits.ValidatePayloadLength(length);
        var id = RequestId(requestId);
        using var deadline = Deadline(cancellationToken);
        await _requests.WaitAsync(deadline.Token).ConfigureAwait(false);
        try
        {
            var data = GC.AllocateUninitializedArray<byte>((int)length);
            await source.ReadExactlyAsync(data, deadline.Token).ConfigureAwait(false);
            return await PublishAsync(data, entityName, id, deadline.Token).ConfigureAwait(false);
        }
        finally { _requests.Release(); }
    }

    /// <summary>Read to EOF with a hard per-message ceiling. Supply length to avoid the growable buffer.</summary>
    public async Task<long> AddLogEntryAsync(Stream source, string entityName = "default", Guid? requestId = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        if (source.CanSeek) return await AddLogEntryAsync(source, source.Length - source.Position, entityName, requestId, cancellationToken).ConfigureAwait(false);
        ClusterLimits.ValidateName(entityName); var id = RequestId(requestId);
        using var deadline = Deadline(cancellationToken);
        await _requests.WaitAsync(deadline.Token).ConfigureAwait(false);
        var buffer = ArrayPool<byte>.Shared.Rent(64 * 1024);
        try
        {
            using var memory = new MemoryStream();
            while (true)
            {
                var read = await source.ReadAsync(buffer, deadline.Token).ConfigureAwait(false);
                if (read == 0) break;
                if (memory.Length + read > ClusterLimits.MaxPayloadBytes) throw new ArgumentOutOfRangeException(nameof(source), "Stream exceeds the message ceiling.");
                memory.Write(buffer, 0, read);
            }
            return await PublishAsync(memory.GetBuffer().AsMemory(0, (int)memory.Length), entityName, id, deadline.Token).ConfigureAwait(false);
        }
        finally { ArrayPool<byte>.Shared.Return(buffer); _requests.Release(); }
    }

    private async Task<long> PublishAsync(ReadOnlyMemory<byte> data, string name, Guid id, CancellationToken token)
    {
        return await RetryBufferedResponseAsync(async () =>
        {
            using var response = await SendAsync(HttpMethod.Post, "/publish?entity=" + Uri.EscapeDataString(name),
                () => new ReadOnlyMemoryContent(data), id, false, token).ConfigureAwait(false);
            var value = await JsonAsync<long>(response, token).ConfigureAwait(false);
            if (value <= 0) throw new InvalidDataException("Invalid committed message ID.");
            return value;
        }, token).ConfigureAwait(false);
    }

    public async Task<EntityInfo> CreateEntityAsync(string name, EntityMode mode = EntityMode.Standard,
        Guid? requestId = null, CancellationToken cancellationToken = default)
    {
        ClusterLimits.ValidateName(name);
        if (!Enum.IsDefined(mode)) throw new ArgumentOutOfRangeException(nameof(mode));
        var id = RequestId(requestId);
        using var deadline = Deadline(cancellationToken);
        await _requests.WaitAsync(deadline.Token).ConfigureAwait(false);
        try
        {
            return await RetryBufferedResponseAsync(async () =>
            {
                using var response = await SendAsync(HttpMethod.Post, $"/create?entity={Uri.EscapeDataString(name)}&mode={(int)mode}", null, id, false, deadline.Token).ConfigureAwait(false);
                return await JsonAsync<EntityInfo>(response, deadline.Token).ConfigureAwait(false);
            }, deadline.Token).ConfigureAwait(false);
        }
        finally { _requests.Release(); }
    }

    public async Task<EntityInfo?> GetEntityAsync(string name, CancellationToken cancellationToken = default)
    {
        ClusterLimits.ValidateName(name);
        using var deadline = Deadline(cancellationToken);
        await _requests.WaitAsync(deadline.Token).ConfigureAwait(false);
        try
        {
            return await RetryBufferedResponseAsync<EntityInfo?>(async () =>
            {
                using var response = await SendAsync(HttpMethod.Get, "/entity?entity=" + Uri.EscapeDataString(name), null, null, true, deadline.Token).ConfigureAwait(false);
                return response.StatusCode == HttpStatusCode.NotFound ? null : await JsonAsync<EntityInfo>(response, deadline.Token).ConfigureAwait(false);
            }, deadline.Token).ConfigureAwait(false);
        }
        finally { _requests.Release(); }
    }

    public async Task<MessageBatch> ReadBatchAsync(string entityName, long afterMessageId, bool idsOnly = false,
        int maxCount = ClusterLimits.MaxBatchCount, int maxBytes = ClusterLimits.MaxBatchBytes, CancellationToken cancellationToken = default)
    {
        ClusterLimits.ValidateName(entityName);
        if (afterMessageId < 0 || maxCount is < 1 or > ClusterLimits.MaxBatchCount ||
            maxBytes is < ClusterLimits.MaxPayloadBytes or > ClusterLimits.MaxBatchBytes) throw new ArgumentOutOfRangeException(nameof(afterMessageId));
        using var deadline = Deadline(cancellationToken);
        await _requests.WaitAsync(deadline.Token).ConfigureAwait(false);
        try
        {
            return await RetryBufferedResponseAsync<MessageBatch>(async () =>
            {
                var path = FormattableString.Invariant($"/messages?entity={Uri.EscapeDataString(entityName)}&after={afterMessageId}&idsOnly={idsOnly}&count={maxCount}&bytes={maxBytes}");
                using var response = await SendAsync(HttpMethod.Get, path, null, null, false, deadline.Token).ConfigureAwait(false);
                if (response.Content.Headers.ContentLength is { } bytes && (bytes < 9 || bytes > maxBytes + 9L + 16L * maxCount))
                    throw new InvalidDataException("Invalid batch frame length.");
                await using var stream = await response.Content.ReadAsStreamAsync(deadline.Token).ConfigureAwait(false);
                var header = new byte[16];
                await stream.ReadExactlyAsync(header.AsMemory(0, 9), deadline.Token).ConfigureAwait(false);
                var count = BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(4));
                var mode = (EntityMode)header[8];
                if (BinaryPrimitives.ReadInt32LittleEndian(header) != ClusterLimits.BatchMagic || count < 0 || count > maxCount || !Enum.IsDefined(mode))
                    throw new InvalidDataException("Invalid batch header/version.");
                var messages = new List<ClusterMessage>(count);
                long total = 0, previous = afterMessageId;
                for (var i = 0; i < count; i++)
                {
                    await stream.ReadExactlyAsync(header, deadline.Token).ConfigureAwait(false);
                    var id = BinaryPrimitives.ReadInt64LittleEndian(header);
                    var length = BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(8));
                    var included = BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(12));
                    if (id <= previous || length is < 0 or > ClusterLimits.MaxPayloadBytes || included != (idsOnly ? 0 : length) ||
                        total + included > maxBytes || (mode == EntityMode.Standard && id != previous + 1))
                        throw new InvalidDataException("Invalid/non-monotonic batch record.");
                    var data = GC.AllocateUninitializedArray<byte>(included);
                    await stream.ReadExactlyAsync(data, deadline.Token).ConfigureAwait(false);
                    messages.Add(new(id, length, data)); total += included; previous = id;
                }
                if (await stream.ReadAsync(header.AsMemory(0, 1), deadline.Token).ConfigureAwait(false) != 0)
                    throw new InvalidDataException("Trailing batch data.");
                return new MessageBatch(mode, messages.AsReadOnly());
            }, deadline.Token).ConfigureAwait(false);
        }
        finally { _requests.Release(); }
    }

    /// <summary>Streams one stored message. On a broken download discard/rewind the partial destination before retrying.</summary>
    public async Task<bool> CopyMessageToAsync(string entityName, long messageId, Stream destination, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(destination); ClusterLimits.ValidateName(entityName);
        if (messageId <= 0) throw new ArgumentOutOfRangeException(nameof(messageId));
        using var deadline = Deadline(cancellationToken);
        await _requests.WaitAsync(deadline.Token).ConfigureAwait(false);
        var buffer = ArrayPool<byte>.Shared.Rent(64 * 1024);
        try
        {
            using var response = await SendAsync(HttpMethod.Get, FormattableString.Invariant($"/message?entity={Uri.EscapeDataString(entityName)}&id={messageId}"), null, null, true, deadline.Token).ConfigureAwait(false);
            if (response.StatusCode == HttpStatusCode.NotFound) return false;
            var length = response.Content.Headers.ContentLength ?? throw new InvalidDataException("Message length missing.");
            if (length is < 0 or > ClusterLimits.MaxPayloadBytes) throw new InvalidDataException("Invalid message length.");
            await using var stream = await response.Content.ReadAsStreamAsync(deadline.Token).ConfigureAwait(false);
            long written = 0;
            while (written < length)
            {
                var read = await stream.ReadAsync(buffer.AsMemory(0, (int)Math.Min(buffer.Length, length - written)), deadline.Token).ConfigureAwait(false);
                if (read == 0) throw new EndOfStreamException("Truncated message.");
                await destination.WriteAsync(buffer.AsMemory(0, read), deadline.Token).ConfigureAwait(false); written += read;
            }
            if (await stream.ReadAsync(buffer.AsMemory(0, 1), deadline.Token).ConfigureAwait(false) != 0) throw new InvalidDataException("Trailing message data.");
            return true;
        }
        finally { ArrayPool<byte>.Shared.Return(buffer); _requests.Release(); }
    }

    public Task SubscribeAsync(string entityName, long afterMessageId, Func<string, long, ReadOnlyMemory<byte>, bool> handler,
        CancellationToken cancellationToken = default)
    { ArgumentNullException.ThrowIfNull(handler); return SubscribeAsync(entityName, afterMessageId, (name, id, data) => new ValueTask<bool>(handler(name, id, data)), cancellationToken); }
    public Task SubscribeAsync(string entityName, long afterMessageId, Func<string, long, ReadOnlyMemory<byte>, ValueTask<bool>> handler,
        CancellationToken cancellationToken = default) => SubscribeCoreAsync(entityName, afterMessageId, handler, false, cancellationToken);
    public Task SubscribeIdsAsync(string entityName, long afterMessageId, Func<string, long, bool> handler,
        CancellationToken cancellationToken = default)
    { ArgumentNullException.ThrowIfNull(handler); return SubscribeIdsAsync(entityName, afterMessageId, (name, id) => new ValueTask<bool>(handler(name, id)), cancellationToken); }
    public Task SubscribeIdsAsync(string entityName, long afterMessageId, Func<string, long, ValueTask<bool>> handler,
        CancellationToken cancellationToken = default)
    { ArgumentNullException.ThrowIfNull(handler); return SubscribeCoreAsync(entityName, afterMessageId, (name, id, _) => handler(name, id), true, cancellationToken); }

    private async Task SubscribeCoreAsync(string entityName, long afterMessageId,
        Func<string, long, ReadOnlyMemory<byte>, ValueTask<bool>> handler, bool idsOnly, CancellationToken token)
    {
        ArgumentNullException.ThrowIfNull(handler); ClusterLimits.ValidateName(entityName);
        if (afterMessageId < 0) throw new ArgumentOutOfRangeException(nameof(afterMessageId));
        using var lifetime = CancellationTokenSource.CreateLinkedTokenSource(token, _stop.Token);
        await _subscriptions.WaitAsync(lifetime.Token).ConfigureAwait(false);
        try
        {
            var cursor = afterMessageId;
            while (true)
            {
                lifetime.Token.ThrowIfCancellationRequested();
                MessageBatch batch;
                try { batch = await ReadBatchAsync(entityName, cursor, idsOnly, cancellationToken: lifetime.Token).ConfigureAwait(false); }
                catch (ClusterRequestException error) when (error.Code == "entity_not_found")
                { await Task.Delay(_options.PollInterval, lifetime.Token).ConfigureAwait(false); continue; }
                catch (OperationCanceledException) when (!lifetime.IsCancellationRequested)
                { await Task.Delay(_options.PollInterval, lifetime.Token).ConfigureAwait(false); continue; }
                catch (Exception error) when (IsRecoverableReadFailure(error))
                { await Task.Delay(_options.PollInterval, lifetime.Token).ConfigureAwait(false); continue; }
                foreach (var message in batch.Messages)
                {
                    while (true)
                    {
                        lifetime.Token.ThrowIfCancellationRequested();
                        try { if (await handler(entityName, message.MessageId, message.Data).ConfigureAwait(false)) break; }
                        catch (OperationCanceledException) when (lifetime.IsCancellationRequested) { throw; }
                        catch (Exception error) { Notify(SubscriptionCallbackFailed, error); }
                        await Task.Delay(_options.HandlerRetryDelay, lifetime.Token).ConfigureAwait(false);
                    }
                    cursor = message.MessageId;
                }
                if (batch.Messages.Count == 0) await Task.Delay(_options.PollInterval, lifetime.Token).ConfigureAwait(false);
            }
        }
        finally { _subscriptions.Release(); }
    }

    public async Task<ClusterTopology> RefreshTopologyAsync(CancellationToken cancellationToken = default)
    {
        using var deadline = Deadline(cancellationToken);
        await _discovery.WaitAsync(deadline.Token).ConfigureAwait(false);
        try
        {
            var candidates = Candidates().ToList();
            for (var index = 0; index < candidates.Count; index++)
            {
                var endpoint = candidates[index];
                using var attempt = CancellationTokenSource.CreateLinkedTokenSource(deadline.Token);
                attempt.CancelAfter(TimeSpan.FromSeconds(3));
                try
                {
                    using var request = new HttpRequestMessage(HttpMethod.Get, Url(endpoint, "/topology"));
                    using var response = await _http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, attempt.Token).ConfigureAwait(false);
                    if (response.StatusCode is HttpStatusCode.Unauthorized or HttpStatusCode.Forbidden)
                        throw new ClusterRequestException("unauthorized", "Cluster access denied.");
                    if (!response.IsSuccessStatusCode) continue;
                    var topology = await JsonAsync<ClusterTopology>(response, attempt.Token).ConfigureAwait(false);
                    AcceptTopology(topology);
                    if (topology.IsAuthoritative) return Topology!;
                    foreach (var learned in Candidates())
                        if (candidates.Count < ClusterLimits.MaxEndpoints && !candidates.Contains(learned)) candidates.Add(learned);
                }
                catch (HttpRequestException) { }
                catch (OperationCanceledException) when (!deadline.IsCancellationRequested) { }
            }
            return Topology ?? throw new HttpRequestException("No cluster endpoint supplied a topology.");
        }
        finally { _discovery.Release(); }
    }

    private Uri[] Candidates()
    {
        lock (_gate)
        {
            var leader = _topology?.Endpoints.FirstOrDefault(endpoint => endpoint.NodeId == (_leaderHint ?? _topology.LeaderId))?.Address;
            return (leader is null ? Enumerable.Empty<Uri>() : [leader]).Concat(_topology?.Endpoints.Select(endpoint => endpoint.Address) ?? [])
                .Concat(_seeds).Distinct().Take(ClusterLimits.MaxEndpoints).ToArray();
        }
    }
    private void AcceptTopology(ClusterTopology value)
    {
        if (value.GroupId != _options.GroupId) throw new ClusterRequestException("wrong_group", "Seed belongs to a different Raft group.");
        if (value.Term < 0 || value.ConfigurationIndex < 0 || value.DirectoryIndex < 0 || value.Endpoints.Count > ClusterLimits.MaxEndpoints ||
            value.Endpoints.Select(endpoint => endpoint.NodeId).Distinct(StringComparer.Ordinal).Count() != value.Endpoints.Count)
            throw new InvalidDataException("Invalid topology.");
        foreach (var endpoint in value.Endpoints) endpoint.Validate();
        ClusterTopology? publish = null;
        lock (_gate)
        {
            if (_topology is { IsAuthoritative: true } prior && (!value.IsAuthoritative || value.Term < prior.Term ||
                (value.Term == prior.Term && (value.ConfigurationIndex < prior.ConfigurationIndex || value.DirectoryIndex < prior.DirectoryIndex)))) return;
            var frozen = value with { Endpoints = Array.AsReadOnly(value.Endpoints.ToArray()) };
            if (_topology is null || _topology.Term != value.Term || _topology.LeaderId != value.LeaderId ||
                _topology.ConfigurationIndex != value.ConfigurationIndex || _topology.DirectoryIndex != value.DirectoryIndex ||
                !_topology.Endpoints.SequenceEqual(value.Endpoints)) publish = frozen;
            _topology = frozen; _leaderHint = value.LeaderId;
        }
        if (publish is not null) Notify(TopologyChanged, publish);
    }

    private async Task<HttpResponseMessage> SendAsync(HttpMethod method, string path, Func<HttpContent>? content,
        Guid? requestId, bool allowNotFound, CancellationToken token)
    {
        while (Topology?.IsAuthoritative != true)
        {
            try { await RefreshTopologyAsync(token).ConfigureAwait(false); }
            catch (HttpRequestException) { }
            if (Topology?.IsAuthoritative != true) await Task.Delay(_options.PollInterval, token).ConfigureAwait(false);
        }
        var round = 0;
        while (true)
        {
            token.ThrowIfCancellationRequested();
            var candidates = Candidates();
            var endpoint = candidates[round % candidates.Length];
            using var request = new HttpRequestMessage(method, Url(endpoint, path));
            if (content is not null) { request.Content = content(); request.Content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream"); }
            if (requestId is { } id) request.Headers.Add("X-Raft-RequestId", id.ToString("D"));
            HttpResponseMessage? response = null;
            try
            {
                response = await _http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, token).ConfigureAwait(false);
                if (response.IsSuccessStatusCode || (allowNotFound && response.StatusCode == HttpStatusCode.NotFound)) return response;
                if (response.StatusCode is HttpStatusCode.Unauthorized or HttpStatusCode.Forbidden)
                    throw new ClusterRequestException("unauthorized", "Cluster access denied.");
                if (response.StatusCode is HttpStatusCode.TooManyRequests or HttpStatusCode.ServiceUnavailable or HttpStatusCode.GatewayTimeout)
                { response.Dispose(); response = null; }
                else
                {
                    var error = await JsonAsync<ClusterError>(response, token).ConfigureAwait(false);
                    if (error.Code != "not_leader") throw new ClusterRequestException(error.Code, error.Message);
                    lock (_gate) _leaderHint = error.LeaderId;
                    response.Dispose(); response = null;
                }
            }
            catch (HttpRequestException) { response?.Dispose(); }
            catch { response?.Dispose(); throw; }
            round++;
            if (round % candidates.Length == 0)
            {
                try { await RefreshTopologyAsync(token).ConfigureAwait(false); }
                catch (HttpRequestException) { }
                await Task.Delay(_options.PollInterval, token).ConfigureAwait(false);
            }
        }
    }
    private static Uri Url(Uri origin, string suffix) => new(origin, ClusterLimits.ApiPath + suffix);
    private static bool IsRecoverableReadFailure(Exception error) =>
        error is HttpRequestException || (error is IOException && error is not InvalidDataException);
    private async Task<T> RetryBufferedResponseAsync<T>(Func<Task<T>> read, CancellationToken token)
    {
        while (true)
        {
            try { return await read().ConfigureAwait(false); }
            catch (Exception error) when (!token.IsCancellationRequested && IsRecoverableReadFailure(error))
            { await Task.Delay(_options.PollInterval, token).ConfigureAwait(false); }
        }
    }
    private static Guid RequestId(Guid? value)
    {
        var id = value ?? Guid.NewGuid();
        if (id == Guid.Empty) throw new ArgumentException("Request ID must not be empty.", nameof(value));
        return id;
    }
    private static async Task<T> JsonAsync<T>(HttpResponseMessage response, CancellationToken token)
    {
        const int limit = 1024 * 1024;
        if (response.Content.Headers.ContentLength > limit) throw new InvalidDataException("Oversized JSON response.");
        await using var source = await response.Content.ReadAsStreamAsync(token).ConfigureAwait(false);
        using var data = new MemoryStream();
        var buffer = ArrayPool<byte>.Shared.Rent(16 * 1024);
        try
        {
            while (true)
            {
                var read = await source.ReadAsync(buffer, token).ConfigureAwait(false);
                if (read == 0) break;
                if (data.Length + read > limit) throw new InvalidDataException("Oversized JSON response.");
                data.Write(buffer, 0, read);
            }
            return JsonSerializer.Deserialize<T>(data.GetBuffer().AsSpan(0, (int)data.Length), Json) ?? throw new InvalidDataException("Missing JSON response.");
        }
        finally { ArrayPool<byte>.Shared.Return(buffer); }
    }
    private async Task WatchTopologyAsync()
    {
        while (!_stop.IsCancellationRequested)
        {
            try { await RefreshTopologyAsync(_stop.Token).ConfigureAwait(false); DiscoveryError = null; }
            catch (OperationCanceledException) when (_stop.IsCancellationRequested) { return; }
            catch (Exception error) { DiscoveryError = error; }
            try { await Task.Delay(_options.TopologyRefreshInterval, _stop.Token).ConfigureAwait(false); }
            catch (OperationCanceledException) when (_stop.IsCancellationRequested) { return; }
        }
    }
    private static void Notify<T>(Action<T>? callbacks, T value)
    {
        if (callbacks is null) return;
        foreach (var callback in callbacks.GetInvocationList().Cast<Action<T>>())
            try { callback(value); } catch { /* Diagnostics do not break delivery/discovery. */ }
    }
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        _stop.Cancel(); await _watch.ConfigureAwait(false);
        if (_ownsHttp) _http.Dispose();
        // Active callers/subscriptions observe cancellation before returning their own permits.
        _stop.Dispose();
    }
}
