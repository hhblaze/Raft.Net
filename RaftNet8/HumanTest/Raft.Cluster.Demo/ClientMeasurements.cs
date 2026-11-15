using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text.Json;
using Raft.Cluster.Client;
using Raft.Cluster.Server;

namespace Raft.Cluster.Demo;

public static class ClientMeasurements
{
    public sealed record Round(int Voters, int PayloadBytes, int RoundNumber, int Requests, int Concurrency,
        double CommandsPerSecond, double P50Milliseconds, double P99Milliseconds, double AllocatedBytesPerCommand,
        double[] LatencyMilliseconds);
    public static async Task RunAsync(string parent, int rounds, int requests, CancellationToken token,
        bool management = false, bool nativeTokens = false, string? dashboardAssembly = null, bool telemetry = true, bool tls = false,
        int onlyVoters = 0, int onlyPayload = 0, bool diagnose = false, bool batchObservationReads = true)
    {
        if (rounds is < 1 or > 20 || requests is < 128 or > 100_000) throw new ArgumentOutOfRangeException(nameof(rounds));
        if (onlyVoters is not (0 or 3 or 5) || onlyPayload is not (0 or 64 or 16384)) throw new ArgumentException("Invalid case filter.");
        var run = Path.Combine(parent, "client-bench-" + DateTime.UtcNow.ToString("yyyyMMdd-HHmmss") + "-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(run);
        if (tls && dashboardAssembly is not null) throw new ArgumentException("Measure TLS separately from the dashboard collector.");
        var pki = Path.Combine(run, "pki");
        if (tls) DemoSecurity.Generate(pki);
        using var security = tls ? new DemoSecurity(pki) : null;
        using var http = security?.HttpClient();
        var results = new List<Round>();
        var diagnostics = new List<object>();
        var observations = new List<object>();
        const int concurrency = 64;
        for (var round = 1; round <= rounds; round++)
        {
            // Reverse order every round to reduce ordering/host-load bias. Every sample uses a new disk cluster.
            var cases = new[] { (3, 64), (3, 16 * 1024), (5, 64), (5, 16 * 1024) };
            if (onlyVoters != 0 || onlyPayload != 0)
                cases = cases.Where(item => (onlyVoters == 0 || item.Item1 == onlyVoters) && (onlyPayload == 0 || item.Item2 == onlyPayload)).ToArray();
            if (round % 2 == 0) Array.Reverse(cases);
            foreach (var (voters, size) in cases)
            {
                var caseRoot = Path.Combine(run, $"r{round}-{voters}n-{size}B");
                var bootstrapSecret = Convert.ToBase64String(RandomNumberGenerator.GetBytes(32));
                await using var cluster = new DemoCluster(caseRoot, voters, secure: tls)
                {
                    HttpConcurrency = concurrency, ManagementEnabled = management || nativeTokens || tls || dashboardAssembly is not null,
                    RequireClientTokens = nativeTokens || tls, BootstrapSecret = bootstrapSecret, EnableTelemetry = telemetry, Security = security,
                    BatchObservationReads = batchObservationReads
                };
                await cluster.StartAsync(token); var leader = await cluster.LeaderAsync(token);
                string? rootSecret = null;
                if (cluster.ManagementEnabled)
                {
                    await using var admin = new RaftClusterAdminClient(new()
                    {
                        GroupId = cluster.GroupId, SeedEndpoints = Enumerable.Range(1, voters).Select(index => cluster.ManagementEndpoint("n" + index).Address).ToArray(), HttpClient = http
                    });
                    rootSecret = (await admin.BootstrapAsync(bootstrapSecret, Guid.NewGuid(), token)).Secret!;
                    if (nativeTokens || tls) cluster.ClientAccessToken = rootSecret;
                }
                await using var dashboard = dashboardAssembly is null ? null : await MeasurementDashboard.StartAsync(
                    dashboardAssembly, caseRoot, cluster.GroupId, Enumerable.Range(1, voters).Select(index => cluster.ManagementEndpoint("n" + index).Address.AbsoluteUri).ToArray(),
                    rootSecret!, token);
                await using var client = cluster.Client(concurrency);
                await client.RefreshTopologyAsync(token);
                var data = RandomNumberGenerator.GetBytes(size);
                await Run(client, data, new double[128], token);
                var latencies = new double[requests];
                var readRequests = cluster.Nodes.Values.Sum(host => host.Server.ObservationReadCounts.Requests);
                var readCalls = cluster.Nodes.Values.Sum(host => host.Server.ObservationReadCounts.Calls);
                var pause = diagnose ? GC.GetTotalPauseDuration() : default;
                var gen0 = diagnose ? GC.CollectionCount(0) : 0;
                var gen1 = diagnose ? GC.CollectionCount(1) : 0;
                var gen2 = diagnose ? GC.CollectionCount(2) : 0;
                var workers = diagnose ? ThreadPool.ThreadCount : 0;
                using var process = diagnose ? Process.GetCurrentProcess() : null;
                var cpu = process?.TotalProcessorTime ?? default;
                var allocated = GC.GetTotalAllocatedBytes(precise: true);
                var started = Stopwatch.GetTimestamp();
                if (diagnose) MeasurementEvents.Log.Start(voters, size, started);
                await Run(client, data, latencies, token);
                var seconds = Stopwatch.GetElapsedTime(started).TotalSeconds;
                if (diagnose) MeasurementEvents.Log.Stop(voters, size, Stopwatch.GetTimestamp());
                allocated = GC.GetTotalAllocatedBytes(precise: true) - allocated;
                observations.Add(new { Voters = voters, PayloadBytes = size,
                    Requests = cluster.Nodes.Values.Sum(host => host.Server.ObservationReadCounts.Requests) - readRequests,
                    ReadIndexCalls = cluster.Nodes.Values.Sum(host => host.Server.ObservationReadCounts.Calls) - readCalls });
                if (diagnose) diagnostics.Add(new
                {
                    Voters = voters, PayloadBytes = size, Round = round, ElapsedSeconds = seconds,
                    GcPauseMilliseconds = (GC.GetTotalPauseDuration() - pause).TotalMilliseconds,
                    Gen0 = GC.CollectionCount(0) - gen0, Gen1 = GC.CollectionCount(1) - gen1, Gen2 = GC.CollectionCount(2) - gen2,
                    ThreadPoolBefore = workers, ThreadPoolAfter = ThreadPool.ThreadCount, PendingAfter = ThreadPool.PendingWorkItemCount,
                    CpuMilliseconds = (process!.TotalProcessorTime - cpu).TotalMilliseconds
                });
                var sorted = latencies.Order().ToArray();
                var result = new Round(voters, size, round, requests, concurrency, requests / seconds,
                    sorted[(int)Math.Ceiling(sorted.Length * .50) - 1], sorted[(int)Math.Ceiling(sorted.Length * .99) - 1],
                    (double)allocated / requests, latencies);
                var fence = await leader.Node.ReadIndexAsync(token);
                while (cluster.Nodes.Values.Any(host => host.Node.Status.LastApplied < fence.Index)) await Task.Delay(25, token);
                foreach (var host in cluster.Nodes.Values)
                    if (host.Ledger.GetEntity("benchmark")?.LastMessageId != requests + 128) throw new InvalidDataException("Replica history count differs.");
                results.Add(result);
                await Save(false);
                Console.WriteLine(JsonSerializer.Serialize(result with { LatencyMilliseconds = [] }));
            }
        }
        await Save(true);
        Console.WriteLine($"BENCHMARK {Path.Combine(run, "results.json")}");

        static Task Run(RaftClusterClient client, byte[] data, double[] latency, CancellationToken token) =>
            Parallel.ForEachAsync(Enumerable.Range(0, latency.Length), new ParallelOptions { MaxDegreeOfParallelism = concurrency, CancellationToken = token }, async (index, cancellation) =>
            {
                var started = Stopwatch.GetTimestamp();
                await client.AddLogEntryAsync(data, "benchmark", cancellationToken: cancellation);
                latency[index] = Stopwatch.GetElapsedTime(started).TotalMilliseconds;
            });
        async Task Save(bool completed)
        {
            var report = new
            {
                Completed = completed,
                Workload = "External HTTP client; full durable Standard message history; wire3/raft8-v4; loopback " + (tls ? "HTTPS/mTLS" : "HTTP/TCP") + "; all nodes and client in one process",
                WarmupRequests = 128,
                Rounds = rounds,
                Requests = requests,
                Concurrency = concurrency,
                Runtime = RuntimeInformation.FrameworkDescription,
                OS = RuntimeInformation.OSDescription,
                DataRoot = run,
                TimestampUtc = DateTime.UtcNow,
                CoreSha256 = Hash(typeof(RaftNode).Assembly.Location),
                ClientSha256 = Hash(typeof(RaftClusterClient).Assembly.Location),
                ServerSha256 = Hash(typeof(RaftClusterServer).Assembly.Location),
                Comparison = "Different application/transport from the accepted embedded-core baseline; do not calculate a performance delta between them.",
                Results = results,
                Management = management || nativeTokens || dashboardAssembly is not null, NativeTokens = nativeTokens,
                Telemetry = telemetry && (management || nativeTokens || dashboardAssembly is not null), DashboardAssembly = dashboardAssembly,
                Tls = tls,
                MeasurementBoundary = "Process-wide managed allocations include client and all Raft replicas. A separate dashboard process, if configured, is excluded from this allocation counter.",
                Diagnose = diagnose, OnlyVoters = onlyVoters, OnlyPayload = onlyPayload, Diagnostics = diagnostics,
                BatchObservationReads = batchObservationReads, Observations = observations
            };
            await File.WriteAllTextAsync(Path.Combine(run, "results.json"), JsonSerializer.Serialize(report, new JsonSerializerOptions { WriteIndented = true }), token);
        }
        static string Hash(string path) => Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(path)));
    }
}
