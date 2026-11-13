using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Security.Cryptography;

namespace Raft.Benchmarks;

internal static class LatencyRunner
{
    private sealed record Result(ClusterCase Case, int Operations, int Concurrency, double Seconds,
        double ProposalsPerSecond, double P50Milliseconds, double P99Milliseconds, double MaxMilliseconds,
        long AllocatedBytesPerProposal, double[] LatencyMilliseconds);
    public static async Task<int> RunAsync(string[] args)
    {
        string Option(string name, string fallback)
        {
            var index = Array.IndexOf(args, name);
            return index < 0 ? fallback : args[index + 1];
        }
        var operations = int.Parse(Option("--operations", "512"), System.Globalization.CultureInfo.InvariantCulture);
        var concurrency = int.Parse(Option("--concurrency", "64"), System.Globalization.CultureInfo.InvariantCulture);
        var repeats = int.Parse(Option("--repeats", "3"), System.Globalization.CultureInfo.InvariantCulture);
        var warmup = int.Parse(Option("--warmup", "128"), System.Globalization.CultureInfo.InvariantCulture);
        if (operations < 100 || concurrency < 1 || repeats < 1 || warmup < 0) throw new ArgumentOutOfRangeException(nameof(args));
        var filter = Option("--case", "");
        var output = Path.GetFullPath(Option("--out", ".artifacts/autonomous/benchmarks/latency.json"));
        Directory.CreateDirectory(Path.GetDirectoryName(output)!);
        var results = new List<Result>();
        var cases = ClusterCase.All().Where(c => c.ToString().Contains(filter, StringComparison.OrdinalIgnoreCase)).ToArray();
        if (cases.Length == 0) throw new ArgumentException("No benchmark case matches --case.");
        var started = DateTimeOffset.UtcNow;
        var currentHash = Convert.ToHexString(SHA256.HashData(await File.ReadAllBytesAsync(typeof(RaftNode).Assembly.Location)));
        var baselineHash = Convert.ToHexString(SHA256.HashData(await File.ReadAllBytesAsync(Path.Combine(AppContext.BaseDirectory, "Raft.Baseline.Net8.dll"))));
        // Interleave versions in each topology/payload. Do not run clusters concurrently.
        foreach (var configuration in cases)
        for (var round = 0; round < repeats; round++)
        {
            await using var cluster = new ComparisonCluster(configuration);
            await cluster.StartAsync();
            await cluster.RunAsync(warmup, concurrency);
            await cluster.ValidateAsync();
            var latencies = new double[operations];
            var allocated = GC.GetTotalAllocatedBytes(precise: true);
            var timer = Stopwatch.StartNew();
            await cluster.RunAsync(operations, concurrency, latencies);
            timer.Stop();
            allocated = GC.GetTotalAllocatedBytes(precise: true) - allocated;
            await cluster.ValidateAsync();
            var sorted = latencies.Order().ToArray();
            var result = new Result(configuration, operations, concurrency, timer.Elapsed.TotalSeconds,
                operations / timer.Elapsed.TotalSeconds, sorted[(int)Math.Ceiling(operations * 0.50) - 1],
                sorted[(int)Math.Ceiling(operations * 0.99) - 1], sorted[^1], allocated / operations, latencies);
            results.Add(result);
            Console.WriteLine($"{configuration} round={round + 1}: {result.ProposalsPerSecond:F0}/s p50={result.P50Milliseconds:F3}ms p99={result.P99Milliseconds:F3}ms alloc={result.AllocatedBytesPerProposal}B/op");
            await File.WriteAllTextAsync(output, JsonSerializer.Serialize(new { Runtime = RuntimeInformation.FrameworkDescription,
                StartedAtUtc = started, ExpectedMeasurements = cases.Length * repeats,
                Complete = results.Count == cases.Length * repeats, WarmupOperationsPerRound = warmup,
                CurrentLibrarySha256 = currentHash, BaselineLibrarySha256 = baselineHash,
                OS = RuntimeInformation.OSDescription, ProcessorCount = Environment.ProcessorCount, Baseline = "8b991bd",
                DataRoot = Path.GetFullPath(Environment.GetEnvironmentVariable("RAFT_BENCHMARK_ROOT") ?? ".artifacts/autonomous/benchmarks/data"),
                Timing = "25ms tick, 1 heartbeat tick, 200..240 election ticks, 2s RPC deadline; identical in both versions",
                Storage = "DBreeze 1.142.2026.1016-beta DISK; atomic application writes and CRC index/term checkpoint on every node",
                Results = results }, new JsonSerializerOptions { WriteIndented = true, Converters = { new JsonStringEnumConverter() } }));
        }
        return 0;
    }
}
