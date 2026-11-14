using System.Diagnostics;
using System.Runtime;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text.Json;
using System.Text.Json.Serialization;
using DBreeze;
using DotNext.Net.Cluster.Consensus.Raft;
using Raft.Storage;

namespace Raft.Benchmarks;

internal sealed record BenchmarkCase(string Implementation, int Nodes, int PayloadBytes, ClusterMode Mode)
{
    public override string ToString() => $"{Implementation}-{Nodes}n-{PayloadBytes}B-{Mode}";
    public static IEnumerable<BenchmarkCase> All()
    {
        foreach (var nodes in new[] { 1, 3, 5 })
        foreach (var bytes in new[] { 64, 16384 })
        {
            yield return new("Baseline", nodes, bytes, ClusterMode.Batched);
            yield return new("Current", nodes, bytes, ClusterMode.Batched);
            yield return new("Current", nodes, bytes, ClusterMode.Pipeline);
            yield return new("DotNext", nodes, bytes, ClusterMode.Batched);
            yield return new("Baseline", nodes, bytes, ClusterMode.Unbatched);
            yield return new("Current", nodes, bytes, ClusterMode.Unbatched);
            yield return new("Current", nodes, bytes, ClusterMode.UnbatchedPipeline);
            yield return new("DotNext", nodes, bytes, ClusterMode.Unbatched);
        }
    }
}

internal static class ThreeWayRunner
{
    private sealed record Result(BenchmarkCase Case, int Round, int Operations, int Concurrency, double Seconds,
        double ProposalsPerSecond, double P50Milliseconds, double P99Milliseconds, double MaxMilliseconds,
        long AllocatedBytesPerProposal, double[] LatencyMilliseconds, object? Diagnostics);
    private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = true, Converters = { new JsonStringEnumConverter() } };

    public static async Task<int> RunAsync(string[] args)
    {
        string Option(string key, string fallback)
        {
            var position = Array.IndexOf(args, key);
            return position < 0 ? fallback : position + 1 < args.Length ? args[position + 1] : throw new ArgumentException($"Missing value for {key}");
        }
        int Count(string key, int fallback) => int.Parse(Option(key, fallback.ToString(System.Globalization.CultureInfo.InvariantCulture)),
            System.Globalization.CultureInfo.InvariantCulture);
        var operations = Count("--operations", 512);
        var warmup = Count("--warmup", 128);
        var repeats = Count("--repeats", 3);
        var concurrency = Count("--concurrency", 64);
        var seconds = Count("--timeout-seconds", 180);
        var output = Path.GetFullPath(Option("--out", ".artifacts/dotnext-comparison/short.json"));
        var parent = Path.GetFullPath(Environment.GetEnvironmentVariable("RAFT_BENCHMARK_ROOT") ?? ".artifacts/dotnext-comparison/data");
        var filter = Option("--case", "");
        if (operations < 1 || warmup < 1 || repeats < 1 || concurrency < 1 || concurrency > 1024 || seconds < 5)
            throw new ArgumentException("Invalid benchmark counts.");
        var cases = BenchmarkCase.All().Where(c => c.ToString().Contains(filter, StringComparison.OrdinalIgnoreCase))
            .Where(c => !args.Contains("--batched-only") || c.Mode is ClusterMode.Batched or ClusterMode.Pipeline).ToArray();
        if (cases.Length == 0) throw new ArgumentException("No benchmark case matches.");
        if (args.Contains("--list"))
        {
            foreach (var configuration in cases) Console.WriteLine(configuration);
            return 0;
        }
        Environment.SetEnvironmentVariable("RAFT_BENCHMARK_ROOT", parent);
        Directory.CreateDirectory(parent);
        Directory.CreateDirectory(Path.GetDirectoryName(output)!);
        var results = new List<Result>();
        var trace = MeasurementTrace.Log; // initialize outside warmup/timed/allocation fences
        var started = DateTimeOffset.UtcNow;
        var hashes = new Dictionary<string, string>();
        foreach (var name in new[] { "Raft.Net8.dll", "Raft.Baseline.Net8.dll", "DotNext.Net.Cluster.dll", "Raft.Comparison.dll" })
            hashes[name] = Convert.ToHexString(SHA256.HashData(await File.ReadAllBytesAsync(Path.Combine(AppContext.BaseDirectory, name))));
        var lockFile = Path.Combine("benchmarks", "Raft.Comparison", "packages.lock.json");
        var lockHash = File.Exists(lockFile) ? Convert.ToHexString(SHA256.HashData(await File.ReadAllBytesAsync(lockFile))) : null;
        var currentSource = SourceHash(Path.Combine("src", "Raft"));
        var harnessSource = SourceHash(Path.Combine("benchmarks", "Raft.Comparison"));
        async Task SaveAsync(string? failure = null) => await File.WriteAllTextAsync(output, JsonSerializer.Serialize(new
        {
            Complete = failure is null && results.Count == cases.Length * repeats, Failure = failure,
            StartedAtUtc = started, UpdatedAtUtc = DateTimeOffset.UtcNow, ExpectedMeasurements = cases.Length * repeats,
            WarmupOperationsPerRound = warmup, RequestedOperations = operations, Concurrency = concurrency, Repeats = repeats,
            Baseline = "8b991bd", DotNextPackage = "5.26.2", DotNextCommit = "39ff4fe882e68a5ea90818b41adb97cdc3ec60d8",
            LatestDotNextAtInspection = "6.12.1 (net10.0 only; not executed)", Runtime = RuntimeInformation.FrameworkDescription,
            OS = RuntimeInformation.OSDescription, Architecture = RuntimeInformation.ProcessArchitecture.ToString(),
            ProcessorCount = Environment.ProcessorCount, ServerGC = GCSettings.IsServerGC,
            DataRoot = parent, BinarySha256 = hashes, PackageLockSha256 = lockHash,
            CurrentSourceSha256 = currentSource, HarnessSourceSha256 = harnessSource,
            CurrentLayout = args.Contains("--hot-membership") || args.Contains("--hot-add") ? "UnifiedTableV4 (wire3)" :
                args.Contains("--unified") ? "UnifiedTableV3 (explicit)" : "SeparateTablesV2",
            HotMembership = args.Contains("--hot-membership") || args.Contains("--hot-add"),
            HotAddDuringWorkload = args.Contains("--hot-add"),
            CurrentLayoutSchema = args.Contains("--hot-membership") || args.Contains("--hot-add")
                ? "raft8/v4: ToIndex prefixes 1=meta,2=log,3=config,4=service index,5=results; RF4M manifest; checked entries"
                : args.Contains("--unified") ? "raft8/v3: ToIndex prefixes 1=meta,2=log,3=config; RF3M packed state/frontier; checked v2 entries" : "raft8/v2/meta,log,config",
            CurrentProposalBatchBytes = args.Contains("--proposal-batch-bytes") ? Count("--proposal-batch-bytes", 262144) : (int?)null,
            CurrentDefaultBatchedGroupBytes = 1_051_264,
            CurrentReplicationBatchBytes = 1_051_264,
            CurrentApplicationBatchBytes = 262_144,
            CurrentCacheReadPolicy = "published entries readable during unrelated flush; atomic short-gate publication",
            CurrentBatchedProposalDelayMilliseconds = 2,
            CurrentSingleVoterCollectionDelayMilliseconds = 1,
            CurrentBulkProposalDelayMilliseconds = 8,
            CurrentProposalCollector = "bounded change signal; owner drains admitted commands without waiting",
            CurrentFollowerAppendGrouping = "already-queued plain same-term tail extensions; original per-frame replies after one bounded durable write",
            CurrentCommitNotificationPolicy = "ordinary idle commit notifications piggyback next data/heartbeat; read/config/transfer immediate",
            CurrentProposalFlushPolicy = "off-owner in-flight feedback deadline; bounded 2ms count-limited,8ms byte-limited with other admitted work,1ms single-voter collection",
            CommonApplicationSourceSha256 = Convert.ToHexString(SHA256.HashData(
                await File.ReadAllBytesAsync(Path.Combine("benchmarks", "Raft.Benchmarks", "ComparisonCluster.cs")))),
            Timing = "Raft.Net tick25ms,heartbeat1,election[5000,6000)ms,RPC2s; DotNext election5000..6000ms,heartbeat threshold0.005 (25..30ms),RPC2s",
            Storage = "Raft.Net: disk DBreeze log+application/engine/node. DotNext: native persistent WAL AutoFlush+IntegrityCheck, DBreeze application/engine/node.",
            Application = "same ComparisonMachine: payload ring256,count,CRC(index,term) atomic; validate all replicas + close/reopen every successful round",
            Batch = "count128, all application256KiB(21B accounting); Current wire1MiB+2688B metadata headroom, baseline wire256KiB, native DotNext wire aggregation has its own limits; Current durable proposal group default4x256KiB+128x21B metadata headroom (1051264 encoded bytes, bounded), override via --proposal-batch-bytes; baseline/DotNext proposal256KiB; Current off-owner collection up to2ms small,8ms byte-limited with other admitted work,1ms single-voter; Current followers coalesce only already-queued fresh plain contiguous frames within the local budget, preserving individual replies and configured wire bounds; DotNext1ms; baseline drains queued",
            Order = "one cluster at a time; round outermost, topology/payload groups, rotate case order by round",
            Results = results
        }, JsonOptions));
        await SaveAsync();
        try
        {
            for (var round = 0; round < repeats; round++)
            foreach (var group in cases.GroupBy(c => (c.Nodes, c.PayloadBytes)))
            {
                var block = group.ToArray();
                foreach (var configuration in block.Skip(round % block.Length).Concat(block.Take(round % block.Length)))
                {
                    using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(seconds));
                    ComparisonCluster? ours = null;
                    DotNextCluster? dotnext = null;
                    string? root = null;
                    var success = false;
                    object? diagnostics = null;
                    using var profile = args.Contains("--diagnose") ? new ComparativeDiagnostics() : null;
                    try
                    {
                        if (configuration.Implementation == "DotNext")
                        {
                            dotnext = new(configuration.Nodes, configuration.PayloadBytes, configuration.Mode == ClusterMode.Batched, parent);
                            root = dotnext.DataDirectory;
                            await dotnext.StartAsync(deadline.Token);
                        }
                        else
                        {
                            ours = new(new(Enum.Parse<ClusterVersion>(configuration.Implementation), configuration.Nodes,
                                configuration.PayloadBytes, configuration.Mode))
                            {
                                RetainDataForValidation = true,
                                UseUnifiedLogLayout = args.Contains("--unified"),
                                UseHotMembership = args.Contains("--hot-membership") || args.Contains("--hot-add"),
                                CurrentProposalBatchBytes = args.Contains("--proposal-batch-bytes") ? Count("--proposal-batch-bytes", 262144) : null,
                                DecorateLogStore = profile is null ? null : profile.Wrap,
                                DecorateTransport = profile is null ? null : profile.WrapTransport,
                                AppliedBatch = profile is null ? null : profile.Applied
                            };
                            root = ours.DataDirectory;
                            await ours.StartAsync();
                        }
                        Task Propose(CancellationToken token) => dotnext is null ? ours!.ProposeAsync(token) : dotnext.ProposeAsync(token);
                        Task Validate() => dotnext is null ? ours!.ValidateAsync() : dotnext.ValidateAsync(deadline.Token);
                        await WorkloadAsync(Propose, warmup, concurrency, null, deadline.Token);
                        await Validate();
                        if (args.Contains("--hot-add"))
                        {
                            if (ours is null || configuration.Implementation != "Current")
                                throw new ArgumentException("--hot-add requires a Current case.");
                            await ours.PrepareJoiningPeerAsync();
                        }
                        profile?.Begin();
                        var latencies = new double[operations];
                        var runtimeBefore = profile is null ? null : RuntimeSample.Capture();
                        var allocated = GC.GetTotalAllocatedBytes(precise: true);
                        var timer = Stopwatch.StartNew();
                        if (trace.IsEnabled()) trace.Start(configuration.ToString(), round + 1, Stopwatch.GetTimestamp());
                        var membership = args.Contains("--hot-add") ? TimedAddAsync(ours!, deadline.Token) : null;
                        await WorkloadAsync(Propose, operations, concurrency, latencies, deadline.Token);
                        var commandMilliseconds = timer.Elapsed.TotalMilliseconds;
                        var addFinishedDuringCommands = membership?.IsCompletedSuccessfully;
                        if (membership is not null)
                        {
                            var added = await membership;
                            if (added.Status.Stage != MembershipOperationStage.Completed) throw new InvalidOperationException("Benchmark Add did not complete.");
                            diagnostics = new { Membership = added.Status, AddMilliseconds = added.Milliseconds,
                                CommandMilliseconds = commandMilliseconds, AddFinishedDuringCommands = addFinishedDuringCommands,
                                IncludesWaitingForFullAdd = true };
                        }
                        timer.Stop();
                        if (trace.IsEnabled()) trace.Stop(Stopwatch.GetTimestamp());
                        allocated = GC.GetTotalAllocatedBytes(precise: true) - allocated;
                        profile?.End();
                        var runtimeAfter = profile is null ? null : RuntimeSample.Capture();
                        await Validate();
                        if (dotnext is not null) diagnostics = dotnext.Diagnostics;
                        else if (profile is not null) diagnostics = new { Counters = profile.Snapshot(), RuntimeBefore = runtimeBefore, RuntimeAfter = runtimeAfter };
                        var sorted = latencies.Order().ToArray();
                        var result = new Result(configuration, round + 1, operations, concurrency, timer.Elapsed.TotalSeconds,
                            operations / timer.Elapsed.TotalSeconds, sorted[(int)Math.Ceiling(operations * .5) - 1],
                            sorted[(int)Math.Ceiling(operations * .99) - 1], sorted[^1],
                            allocated / operations, latencies, diagnostics);
                        // A measurement is published only after close/reopen validation succeeds below.
                        if (ours is not null) { await ours.DisposeAsync(); ours = null; }
                        if (dotnext is not null) { await dotnext.DisposeAsync(); dotnext = null; }
                        VerifyBusinessOnDisk(root, configuration.Nodes + (args.Contains("--hot-add") ? 1 : 0), warmup + operations, configuration.PayloadBytes);
                        results.Add(result);
                        await SaveAsync();
                        Console.WriteLine($"{configuration} round={round + 1}: {result.ProposalsPerSecond:F0}/s p50={result.P50Milliseconds:F3}ms p99={result.P99Milliseconds:F3}ms alloc={result.AllocatedBytesPerProposal}B/op reopened=OK");
                        success = true;
                    }
                    finally
                    {
                        if (ours is not null) await ours.DisposeAsync();
                        if (dotnext is not null) await dotnext.DisposeAsync();
                        if (success && root is not null) DeleteOwnedRoot(parent, root);
                    }
                }
            }
        }
        catch (Exception error) { await SaveAsync(error.ToString()); throw; }
        return 0;
    }

    private static async Task<(MembershipOperationStatus Status, double Milliseconds)> TimedAddAsync(ComparisonCluster cluster, CancellationToken token)
    {
        var start = Stopwatch.GetTimestamp();
        var status = await cluster.AddJoiningPeerAsync(token);
        return (status, Stopwatch.GetElapsedTime(start).TotalMilliseconds);
    }

    private static async Task WorkloadAsync(Func<CancellationToken, Task> propose, int operations, int concurrency,
        double[]? latencies, CancellationToken token)
    {
        var issued = -1;
        var workers = Enumerable.Range(0, Math.Min(operations, concurrency)).Select(async _ =>
        {
            while (true)
            {
                var index = Interlocked.Increment(ref issued);
                if (index >= operations) return;
                var start = latencies is null ? 0 : Stopwatch.GetTimestamp();
                await propose(token);
                if (latencies is not null) latencies[index] = Stopwatch.GetElapsedTime(start).TotalMilliseconds;
            }
        });
        await Task.WhenAll(workers);
    }

    private static void VerifyBusinessOnDisk(string root, int nodes, int expected, int payloadBytes)
    {
        AppliedCheckpoint? previous = null;
        for (var i = 1; i <= nodes; i++)
        {
            using var engine = new DBreezeEngine(Path.Combine(root, "n" + i));
            using var tx = engine.GetTransaction();
            var checkpoint = DBreezeStateMachine.DecodeCheckpoint(tx.Select<string, byte[]>("bench/checkpoint", "applied").Value);
            if (checkpoint.Term < 1 || checkpoint.Index < expected ||
                tx.Select<string, long>("bench/checkpoint", "commands").Value != expected ||
                previous is { } p && p != checkpoint) throw new InvalidDataException("Reopened checkpoint/count/replicas differ.");
            var keys = 0;
            foreach (var row in tx.SelectForward<long, byte[]>("bench/values"))
            {
                if (row.Value.Length != payloadBytes || row.Value.Any(value => value != 0))
                    throw new InvalidDataException("Reopened payload differs.");
                keys++;
            }
            if (keys != Math.Min(expected, 256)) throw new InvalidDataException("Reopened payload ring size differs.");
            previous = checkpoint;
        }
    }

    private static void DeleteOwnedRoot(string parent, string root)
    {
        var directory = new DirectoryInfo(Path.GetFullPath(root));
        if (directory.Parent?.FullName != Path.GetFullPath(parent).TrimEnd(Path.DirectorySeparatorChar) ||
            !Guid.TryParseExact(directory.Name, "N", out _) || directory.LinkTarget is not null)
            throw new InvalidOperationException("Refusing to delete a non-owned benchmark directory.");
        if (directory.Exists) directory.Delete(recursive: true);
    }

    private static string SourceHash(string directory)
    {
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        foreach (var file in Directory.EnumerateFiles(directory, "*.cs", SearchOption.AllDirectories)
                     .Where(file => !Path.GetRelativePath(directory, file).Split(Path.DirectorySeparatorChar).Any(p => p is "bin" or "obj"))
                     .Order(StringComparer.Ordinal))
        {
            hash.AppendData(System.Text.Encoding.UTF8.GetBytes(Path.GetRelativePath(directory, file).Replace('\\', '/') + "\n"));
            hash.AppendData(File.ReadAllBytes(file));
        }
        return Convert.ToHexString(hash.GetHashAndReset());
    }
}
