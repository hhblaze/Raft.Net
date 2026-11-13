using System.Diagnostics;
using System.Collections.Concurrent;
using System.Text.Json;
using Raft.Simulation;

try
{
    var flags = new Dictionary<string, string>(StringComparer.Ordinal);
    for (var i = 0; i < args.Length; i++)
    {
        if (args[i] is "--help" or "-h")
        {
            Console.WriteLine("Raft.Simulator [--seed 1 | --start-seed 1 --seeds 3000] [--scenario all|Mixed|Elections|Partitions|CrashRecovery|Reads|Membership|Snapshots] [--steps 3000] [--voters 3,5] [--parallel 4] [--out report.json]");
            return 0;
        }
        if (i + 1 >= args.Length || !args[i].StartsWith("--", StringComparison.Ordinal)) throw new ArgumentException("Use --name value options.");
        flags.Add(args[i], args[++i]);
    }
    var allowed = new[] { "--seed", "--start-seed", "--seeds", "--scenario", "--steps", "--voters", "--parallel", "--out" };
    if (flags.Keys.Any(key => !allowed.Contains(key, StringComparer.Ordinal))) throw new ArgumentException("Unknown simulation option.");
    var start = int.Parse(flags.GetValueOrDefault("--seed", flags.GetValueOrDefault("--start-seed", "1")));
    var count = int.Parse(flags.GetValueOrDefault("--seeds", flags.ContainsKey("--seed") ? "1" : "64"));
    var steps = int.Parse(flags.GetValueOrDefault("--steps", "3000"));
    if (count is < 1 or > 1_000_000) throw new ArgumentException("Seed count must be 1..1000000.");
    var selected = flags.GetValueOrDefault("--scenario", "Mixed");
    var scenarios = selected.Equals("all", StringComparison.OrdinalIgnoreCase) ? Enum.GetValues<SimulationScenario>() :
        [Enum.Parse<SimulationScenario>(selected, ignoreCase: true)];
    var sizes = flags.GetValueOrDefault("--voters", "3").Split(',').Select(int.Parse).ToArray();
    var parallelism = int.Parse(flags.GetValueOrDefault("--parallel", "1"));
    if (parallelism is < 1 or > 32) throw new ArgumentException("Parallelism must be 1..32 independent seeds.");
    var watch = Stopwatch.StartNew();
    var completed = new ConcurrentDictionary<(int Voters, SimulationScenario Scenario), int>();
    var collected = new ConcurrentBag<SimulationResult>();
    var cases = sizes.SelectMany(voters => scenarios.SelectMany(scenario => Enumerable.Range(0, count)
        .Select(i => (Voters: voters, Scenario: scenario, Seed: checked(start + i)))));
    Exception? failed = null;
    Parallel.ForEach(cases, new ParallelOptions { MaxDegreeOfParallelism = parallelism }, (item, loop) =>
    {
        try
        {
            var result = SimulationRunner.Run(item.Seed, item.Scenario, steps, item.Voters);
            collected.Add(result);
            var total = completed.AddOrUpdate((item.Voters, item.Scenario), 1, (_, old) => old + 1);
            if (count == 1 || total % 100 == 0 || total == count)
                Console.WriteLine($"{item.Scenario} voters={item.Voters} seeds={total}/{count}, last={result.Seed}, events={result.Events}, digest={result.Digest}");
        }
        catch (Exception error)
        {
            Interlocked.CompareExchange(ref failed, error, null);
            loop.Stop();
        }
    });
    if (failed is not null) throw failed;
    var results = collected.OrderBy(r => r.Nodes).ThenBy(r => r.Scenario).ThenBy(r => r.Seed).ToArray();
    Console.WriteLine($"PASS runs={results.Length}, events={results.Sum(r => r.Events)}, writes={results.Sum(r => r.SuccessfulWrites)}, reads={results.Sum(r => r.SuccessfulReads)}, crashes={results.Sum(r => r.Crashes)}, torn={results.Sum(r => r.TornTails)}, diskFailures={results.Sum(r => r.StorageFailures)}, snapshots={results.Sum(r => r.SnapshotInstalls)}, elapsed={watch.Elapsed}");
    if (flags.TryGetValue("--out", out var output))
    {
        var path = Path.GetFullPath(output);
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        File.WriteAllBytes(path, JsonSerializer.SerializeToUtf8Bytes(new { StartedAtUtc = DateTimeOffset.UtcNow - watch.Elapsed, Elapsed = watch.Elapsed, Results = results },
            new JsonSerializerOptions { WriteIndented = true }));
    }
    return 0;
}
catch (Exception error) { Console.Error.WriteLine(error); return 1; }
