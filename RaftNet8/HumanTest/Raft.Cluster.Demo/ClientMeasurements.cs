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
    public static async Task RunAsync(string parent, int rounds, int requests, CancellationToken token)
    {
        if (rounds is < 1 or > 20 || requests is < 128 or > 100_000) throw new ArgumentOutOfRangeException(nameof(rounds));
        var run = Path.Combine(parent, "client-bench-" + DateTime.UtcNow.ToString("yyyyMMdd-HHmmss") + "-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(run);
        var results = new List<Round>();
        const int concurrency = 64;
        for (var round = 1; round <= rounds; round++)
        {
            // Reverse order every round to reduce ordering/host-load bias. Every sample uses a new disk cluster.
            var cases = new[] { (3, 64), (3, 16 * 1024), (5, 64), (5, 16 * 1024) };
            if (round % 2 == 0) Array.Reverse(cases);
            foreach (var (voters, size) in cases)
            {
                await using var cluster = new DemoCluster(Path.Combine(run, $"r{round}-{voters}n-{size}B"), voters) { HttpConcurrency = concurrency };
                await cluster.StartAsync(token); var leader = await cluster.LeaderAsync(token);
                await using var client = cluster.Client(concurrency);
                await client.RefreshTopologyAsync(token);
                var data = RandomNumberGenerator.GetBytes(size);
                await Run(client, data, new double[128], token);
                var latencies = new double[requests];
                var allocated = GC.GetTotalAllocatedBytes(precise: true);
                var started = Stopwatch.GetTimestamp();
                await Run(client, data, latencies, token);
                var seconds = Stopwatch.GetElapsedTime(started).TotalSeconds;
                allocated = GC.GetTotalAllocatedBytes(precise: true) - allocated;
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
                Workload = "External HTTP client; full durable Standard message history; wire3/raft8-v4; loopback TCP; all nodes and client in one process",
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
                Results = results
            };
            await File.WriteAllTextAsync(Path.Combine(run, "results.json"), JsonSerializer.Serialize(report, new JsonSerializerOptions { WriteIndented = true }), token);
        }
        static string Hash(string path) => Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(path)));
    }
}
