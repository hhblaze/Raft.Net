using BenchmarkDotNet.Attributes;
using Raft.Storage;

namespace Raft.Benchmarks;

/// <summary>Mean is per durable batch transaction. Each iteration has a bounded, preallocated workload.</summary>
[MemoryDiagnoser]
public class AppendBenchmarks
{
    [Params(1, 32, 128)] public int BatchSize { get; set; }
    [Params(false, true)] public bool Disk { get; set; }
    private BenchmarkStorage _scope = null!;
    private LogEntry[][] _batches = null!;

    [IterationSetup]
    public void Setup()
    {
        var options = new RaftOptions { NodeId = "n1", Members = new[] { "n1" } };
        _scope = new BenchmarkStorage(Disk, options);
        _scope.Store.SaveHardState(new HardState(1));
        var payload = new byte[256];
        _scope.Store.Append(Enumerable.Range(1, 1024).Select(i => new LogEntry(i, 1, payload, false)).ToArray());
        _batches = Enumerable.Range(0, 64).Select(batch => Enumerable.Range(0, BatchSize)
            .Select(i => new LogEntry(1025 + batch * BatchSize + i, 1, payload, false)).ToArray()).ToArray();
    }

    [Benchmark(OperationsPerInvoke = 64)]
    public long AppendBatch()
    {
        foreach (var batch in _batches) _scope.Store.Append(batch);
        return _scope.Store.LastIndex;
    }

    [IterationCleanup] public void Cleanup() => _scope.Dispose();
}
