using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;

namespace Raft.Benchmarks;

/// <summary>Real disk/TCP consensus plus atomic durable business writes, normalized per proposal.</summary>
[MemoryDiagnoser]
[Config(typeof(OneInvocation))]
public class DurableClusterBenchmarks
{
    public sealed class OneInvocation : ManualConfig
    {
        public OneInvocation()
        {
            AddJob(Job.Default.WithWarmupCount(3).WithIterationCount(8).WithInvocationCount(1).WithUnrollFactor(1).AsDefault());
            SummaryStyle = BenchmarkDotNet.Reports.SummaryStyle.Default.WithMaxParameterColumnWidth(60);
        }
    }
    [ParamsSource(nameof(Cases))] public ClusterCase Case { get; set; } = null!;
    public IEnumerable<ClusterCase> Cases => ClusterCase.All();
    private ComparisonCluster _cluster = null!;
    [GlobalSetup] public void Setup() => StartAsync().GetAwaiter().GetResult();
    private async Task StartAsync()
    {
        _cluster = new(Case) { UseUnifiedLogLayout = true };
        await _cluster.StartAsync();
        await _cluster.RunAsync(128, 64); // warm JIT/connections/tables; fixed iteration count bounds the retained log
    }
    [Benchmark(OperationsPerInvoke = 128)] public Task Replicate128() => _cluster.RunAsync(128, 64);
    [GlobalCleanup] public void Cleanup() => StopAsync().GetAwaiter().GetResult();
    private async Task StopAsync()
    {
        try { await _cluster.ValidateAsync(); }
        finally { await _cluster.DisposeAsync(); }
    }
}
