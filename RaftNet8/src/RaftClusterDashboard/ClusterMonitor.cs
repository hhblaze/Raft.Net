using System.Collections.Concurrent;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Channels;
using Raft.Cluster;
using Raft.Cluster.Client;

namespace RaftClusterDashboard;

public sealed record ClusterPoint(DateTimeOffset At, double? MessagesPerSecond, double? BytesPerSecond,
    double? P99Milliseconds, long? WorkingSetBytes, long? PhysicalStorageBytes);
public sealed record ClusterObservation(string Id, string Name, string GroupId, DateTimeOffset? ObservedAt,
    string? Error, AdminNodeStatus? Status, IReadOnlyList<NodeDiagnostics> Nodes,
    IReadOnlyList<AdminEntity> Entities, IReadOnlyList<AdminOperation> Operations, IReadOnlyList<ClusterPoint> History)
{
    public DateTimeOffset? CredentialExpiresAt { get; init; }
    public int StaleAfterSeconds { get; init; } = 5;
}
public sealed record DiagnosticCertificateSettings(string Path, string? Password, string? TrustedRootPath = null);

public sealed class ClusterMonitor(DashboardStore store, DashboardSettings settings, ILogger<ClusterMonitor> logger) : BackgroundService
{
    private sealed class Connection(RaftClusterAdminClient client, HttpClient http, X509Certificate2? certificate, string identity) : IAsyncDisposable
    {
        public RaftClusterAdminClient Client { get; } = client;
        public string Identity { get; } = identity;
        public Queue<ClusterPoint> History { get; } = new();
        public Dictionary<string, NodeDiagnostics> Nodes { get; } = new(StringComparer.Ordinal);
        public AdminNodeStatus? Status;
        public AdminEntity[] Entities = [];
        public DateTimeOffset? CatalogAt;
        public readonly Dictionary<string, long> EntityCounts = new(StringComparer.Ordinal);
        public AdminOperation[] Operations = [];
        public DateTimeOffset? At;
        public DateTimeOffset? CredentialExpiresAt;
        public string? Error;
        public long LastMessages, LastBytes;
        public DateTimeOffset? LastRateAt;
        public DateTimeOffset LastDirectory;
        public DateTimeOffset? MinuteAt;
        public long MinuteMessages, MinuteBytes;
        public long[]? MinuteCounts;
        public double[]? MinuteBounds;
        public readonly Dictionary<string, (string Epoch, long[] Counts)> Histograms = new();
        public async ValueTask DisposeAsync() { await Client.DisposeAsync(); http.Dispose(); certificate?.Dispose(); }
    }
    private readonly Dictionary<string, Connection> _connections = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, ClusterObservation> _observations = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<Guid, Channel<bool>> _subscribers = new();
    private readonly object _subscriberGate = new();
    private readonly SemaphoreSlim _sampling = new(1, 1);
    private readonly SemaphoreSlim _diagnostics = new(8, 8);
    public IReadOnlyList<ClusterObservation> Observations => _observations.Values.OrderBy(item => item.Name, StringComparer.Ordinal).ToArray();
    public ClusterObservation? Get(string id) => _observations.GetValueOrDefault(id);
    public (Guid Id, ChannelReader<bool> Reader) Subscribe()
    {
        lock (_subscriberGate)
        {
            if (_subscribers.Count >= settings.MaxSseConnections) throw new InvalidOperationException("Dashboard stream capacity reached.");
            var channel = Channel.CreateBounded<bool>(new BoundedChannelOptions(1) { FullMode = BoundedChannelFullMode.DropOldest, SingleReader = true, SingleWriter = false });
            var id = Guid.NewGuid();
            _subscribers[id] = channel;
            channel.Writer.TryWrite(true);
            return (id, channel.Reader);
        }
    }
    public void Unsubscribe(Guid id) { if (_subscribers.TryRemove(id, out var channel)) channel.Writer.TryComplete(); }
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(settings.PollIntervalSeconds));
        try
        {
            do
            {
                await SampleAsync(stoppingToken);
                foreach (var channel in _subscribers.Values) channel.Writer.TryWrite(true);
            } while (await timer.WaitForNextTickAsync(stoppingToken));
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
        finally
        {
            foreach (var connection in _connections.Values) await connection.DisposeAsync();
            foreach (var id in _subscribers.Keys) Unsubscribe(id);
        }
    }
    private async Task SampleAsync(CancellationToken token)
    {
        await _sampling.WaitAsync(token);
        try
        {
            var profiles = store.Profiles();
            foreach (var id in _connections.Keys.Except(profiles.Select(profile => profile.Id)).ToArray())
            { await _connections[id].DisposeAsync(); _connections.Remove(id); _observations.TryRemove(id, out _); }
            // Only this sampling owner changes connections; each connection has one concurrent sampler.
            foreach (var profile in profiles)
                if (!_connections.TryGetValue(profile.Id, out var existing) || existing.Identity != Identity(profile))
                {
                    if (existing is not null) await existing.DisposeAsync();
                    _connections.Remove(profile.Id);
                }
            await Parallel.ForEachAsync(profiles, new ParallelOptions { MaxDegreeOfParallelism = 4, CancellationToken = token }, async (profile, token) =>
            {
                try
                {
                    Connection? connection;
                    lock (_connections) _connections.TryGetValue(profile.Id, out connection);
                    if (connection is null)
                    {
                        connection = Connect(profile);
                        lock (_connections) _connections[profile.Id] = connection;
                    }
                    using var timeout = CancellationTokenSource.CreateLinkedTokenSource(token);
                    timeout.CancelAfter(TimeSpan.FromSeconds(3));
                    var cancellation = timeout.Token;
                    NodeDiagnostics? overviewDiagnostic = null;
                    if (DateTimeOffset.UtcNow - connection.LastDirectory >= TimeSpan.FromSeconds(settings.DirectoryRefreshSeconds))
                    {
                        try
                        {
                            var overview = await connection.Client.GetOverviewAsync(cancellation);
                            connection.Status = overview.Status;
                            var entities = overview.Entities;
                            var catalogAt = DateTimeOffset.UtcNow;
                            var catalogSeconds = connection.CatalogAt is { } previous ? (catalogAt - previous).TotalSeconds : 0;
                            connection.Entities = entities.Select(item => item with { MessagesPerSecond = catalogSeconds > 0 &&
                                connection.EntityCounts.TryGetValue(item.Entity.Name, out var count) && item.Entity.LastMessageId >= count
                                    ? (item.Entity.LastMessageId - count) / catalogSeconds : null }).ToArray();
                            connection.EntityCounts.Clear();
                            foreach (var item in entities) connection.EntityCounts[item.Entity.Name] = item.Entity.LastMessageId;
                            connection.CatalogAt = catalogAt;
                            connection.Operations = overview.Operations.ToArray();
                            connection.CredentialExpiresAt = overview.CredentialExpiresAt;
                            overviewDiagnostic = overview.LeaderDiagnostics;
                            connection.Error = null;
                        }
                        catch (Exception error) when (error is HttpRequestException or OperationCanceledException or ClusterRequestException)
                        { connection.Error = "Cluster read barrier unavailable"; }
                        connection.LastDirectory = DateTimeOffset.UtcNow;
                    }
                    // A quorum failure must not prevent certificate-only diagnostics on individual nodes.
                    var endpoints = connection.Client.KnownEndpoints;
                    if (endpoints.Count > settings.MaxObservedNodes) throw new InvalidDataException("Cluster exceeds configured observation node bound.");
                    var failures = new ConcurrentBag<string>();
                    var samples = new ConcurrentBag<NodeDiagnostics>();
                    if (overviewDiagnostic is not null) samples.Add(overviewDiagnostic);
                    await Parallel.ForEachAsync(endpoints, new ParallelOptions { MaxDegreeOfParallelism = 8, CancellationToken = token }, async (endpoint, token) =>
                    {
                        if (overviewDiagnostic?.Status.NodeId == endpoint.NodeId) return;
                        await _diagnostics.WaitAsync(token);
                        try
                        {
                            using var diagnosticTimeout = CancellationTokenSource.CreateLinkedTokenSource(token);
                            diagnosticTimeout.CancelAfter(TimeSpan.FromSeconds(2));
                            var diagnostic = await connection.Client.GetDiagnosticsAsync(endpoint, diagnosticTimeout.Token);
                            samples.Add(diagnostic);
                        }
                        catch (Exception error) when (error is HttpRequestException or OperationCanceledException or ClusterRequestException)
                        { failures.Add(endpoint.NodeId); }
                        finally { _diagnostics.Release(); }
                    });
                    var knownIds = endpoints.Select(endpoint => endpoint.NodeId).ToHashSet(StringComparer.Ordinal);
                    foreach (var id in connection.Nodes.Keys.Where(id => !knownIds.Contains(id)).ToArray()) connection.Nodes.Remove(id);
                    foreach (var sample in samples) connection.Nodes[sample.Status.NodeId] = sample;
                    connection.At = DateTimeOffset.UtcNow;
                    if (failures.Count != 0) connection.Error = "Diagnostics unavailable: " + string.Join(", ", failures);
                    var minute = RecordPoint(connection);
                    if (minute is not null) store.SaveHistory(profile.Id, minute, settings.HistoryDays, Math.Max(1, settings.MaxHistoryRows / Math.Max(1, profiles.Count)));
                    _observations[profile.Id] = Observe(profile, connection);
                }
                catch (Exception error) when (error is HttpRequestException or OperationCanceledException or ClusterRequestException or System.Security.Cryptography.CryptographicException or InvalidDataException)
                {
                    Connection? connection;
                    lock (_connections) _connections.TryGetValue(profile.Id, out connection);
                    if (connection is not null)
                    {
                        connection.Error = error is ClusterRequestException failure ? failure.Code : error.GetType().Name;
                        _observations[profile.Id] = Observe(profile, connection);
                    }
                    else _observations[profile.Id] = new(profile.Id, profile.Name, profile.GroupId, null, "Cannot connect", null, [], [], [], []);
                    logger.LogDebug("Cluster observation unavailable for {ClusterId}: {ErrorType}", profile.Id, error.GetType().Name);
                }
            });
        }
        finally { _sampling.Release(); }
    }
    private ClusterObservation Observe(ClusterProfile profile, Connection connection) =>
        new(profile.Id, profile.Name, profile.GroupId, connection.At, connection.Error, connection.Status,
            connection.Nodes.Values.ToArray(), connection.Entities, connection.Operations, connection.History.ToArray())
            { CredentialExpiresAt = connection.CredentialExpiresAt, StaleAfterSeconds = Math.Max(5, settings.PollIntervalSeconds * 2) };
    private Connection Connect(ClusterProfile profile)
    {
        var handler = new SocketsHttpHandler { AllowAutoRedirect = false, ConnectTimeout = TimeSpan.FromSeconds(2), MaxConnectionsPerServer = 8 };
        X509Certificate2? certificate = null;
        if (profile.DiagnosticCertificatePath is { } alias)
        {
            if (!settings.DiagnosticCertificates.TryGetValue(alias, out var configuration)) throw new InvalidDataException("Unknown diagnostic certificate profile.");
            certificate = new X509Certificate2(configuration.Path, configuration.Password);
            handler.SslOptions.ClientCertificates = new X509CertificateCollection { certificate };
            if (configuration.TrustedRootPath is { } path)
            {
                var rootBytes = File.ReadAllBytes(path);
                handler.SslOptions.RemoteCertificateValidationCallback = (_, remote, _, errors) =>
                {
                    if (remote is null || (errors & ~SslPolicyErrors.RemoteCertificateChainErrors) != SslPolicyErrors.None) return false;
                    using var root = new X509Certificate2(rootBytes);
                    using var leaf = new X509Certificate2(remote);
                    using var chain = new X509Chain();
                    chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
                    chain.ChainPolicy.CustomTrustStore.Add(root);
                    chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
                    chain.ChainPolicy.ApplicationPolicy.Add(new("1.3.6.1.5.5.7.3.1"));
                    return chain.Build(leaf);
                };
            }
        }
        var http = new HttpClient(handler) { Timeout = Timeout.InfiniteTimeSpan };
        var secret = store.Unprotect(profile.ProtectedCredential);
        var client = new RaftClusterAdminClient(new()
        {
            GroupId = profile.GroupId, SeedEndpoints = profile.ManagementEndpoints.Select(endpoint => new Uri(endpoint)).ToArray(),
            AccessTokenProvider = _ => ValueTask.FromResult<string?>(secret), HttpClient = http, OperationTimeout = TimeSpan.FromSeconds(3),
            MaxConcurrentRequests = 8 // Match the diagnostic lane: a five-node poll must not split into4+1 waves.
        });
        return new(client, http, certificate, Identity(profile));
    }
    private static string Identity(ClusterProfile profile) => profile.GroupId + "\n" + string.Join("\n", profile.ManagementEndpoints) + profile.ProtectedCredential + profile.DiagnosticCertificatePath;
    private ClusterPoint? RecordPoint(Connection connection)
    {
        var now = DateTimeOffset.UtcNow;
        var live = connection.Nodes.Values.Where(node => now - node.Resources.ObservedAt < TimeSpan.FromSeconds(Math.Max(5, node.Resources.SampleIntervalSeconds * 2))).ToArray();
        long? processBytes = live.Length == 0 ? null : live.GroupBy(node => node.Resources.HostId + "|" +
            (node.Resources.ProcessId > 0 ? "pid:" + node.Resources.ProcessId : "node:" + node.Status.NodeId))
            .Sum(group => group.Max(node => node.Resources.WorkingSetBytes));
        var leader = live.FirstOrDefault(node => node.Status.Role == "Leader" && node.Status.NodeId == connection.Status?.LeaderId);
        double? rate = null, bytesRate = null, p99 = null;
        if (leader is not null)
        {
            var seconds = connection.LastRateAt is { } previous ? (now - previous).TotalSeconds : 0;
            if (seconds > 0 && leader.SuccessfulPublications >= connection.LastMessages && leader.PublishedBytes >= connection.LastBytes)
            {
                rate = (leader.SuccessfulPublications - connection.LastMessages) / seconds;
                bytesRate = (leader.PublishedBytes - connection.LastBytes) / seconds;
                connection.MinuteMessages += leader.SuccessfulPublications - connection.LastMessages;
                connection.MinuteBytes += leader.PublishedBytes - connection.LastBytes;
            }
            connection.LastMessages = leader.SuccessfulPublications; connection.LastBytes = leader.PublishedBytes; connection.LastRateAt = now;
            var histogram = leader.PublishLatency;
            var key = leader.Status.NodeId;
            if (connection.Histograms.TryGetValue(key, out var last) && last.Epoch == leader.Epoch && last.Counts.Length == histogram.Counts.Count)
            {
                var counts = histogram.Counts.Select((value, index) => Math.Max(0, value - last.Counts[index])).ToArray();
                connection.MinuteCounts ??= new long[counts.Length];
                connection.MinuteBounds ??= histogram.UpperBoundsMilliseconds.ToArray();
                if (connection.MinuteCounts.Length == counts.Length)
                    for (var index = 0; index < counts.Length; index++) connection.MinuteCounts[index] += counts[index];
                var target = (long)Math.Ceiling(counts.Sum() * .99);
                long seen = 0;
                if (target > 0)
                    for (var index = 0; index < counts.Length; index++)
                    { seen += counts[index]; if (seen >= target) { p99 = histogram.UpperBoundsMilliseconds[index]; break; } }
            }
            connection.Histograms[key] = (leader.Epoch, histogram.Counts.ToArray());
        }
        else connection.LastRateAt = null;
        long? storage = live.Length > 0 && live.All(node => node.Resources.LogBytes.HasValue && node.Resources.MessageBytes.HasValue && node.Resources.ImageBytes.HasValue)
            ? live.Sum(node => node.Resources.LogBytes!.Value + node.Resources.MessageBytes!.Value + node.Resources.ImageBytes!.Value) : null;
        connection.History.Enqueue(new(now, rate, bytesRate, p99, processBytes, storage));
        while (connection.History.Count > 0 && (connection.History.Count > settings.DetailedHistorySeconds || now - connection.History.Peek().At > TimeSpan.FromSeconds(settings.DetailedHistorySeconds))) connection.History.Dequeue();
        connection.MinuteAt ??= now;
        if (now - connection.MinuteAt < TimeSpan.FromMinutes(1)) return null;
        double? minuteP99 = null;
        if (connection.MinuteCounts is { } minuteCounts && connection.MinuteBounds is { } bounds)
        {
            var target = (long)Math.Ceiling(minuteCounts.Sum() * .99);
            long seen = 0;
            if (target > 0) for (var index = 0; index < minuteCounts.Length; index++)
            { seen += minuteCounts[index]; if (seen >= target) { minuteP99 = bounds[index]; break; } }
            Array.Clear(minuteCounts);
        }
        var duration = (now - connection.MinuteAt.Value).TotalSeconds;
        var aggregate = new ClusterPoint(now, leader is null ? null : connection.MinuteMessages / duration,
            leader is null ? null : connection.MinuteBytes / duration, minuteP99, processBytes, storage);
        connection.MinuteAt = now; connection.MinuteMessages = 0; connection.MinuteBytes = 0;
        return aggregate;
    }
    public async Task<T> WithClientAsync<T>(string profileId, Func<RaftClusterAdminClient, Task<T>> action)
    {
        var profile = store.Profile(profileId) ?? throw new KeyNotFoundException("Unknown cluster.");
        await using var connection = Connect(profile);
        await connection.Client.GetStatusAsync();
        return await action(connection.Client);
    }
}
