using BenchmarkDotNet.Attributes;
using Raft.Storage;

namespace Raft.Benchmarks;

[MemoryDiagnoser]
public class ReadBenchmarks
{
    [Params(false, true)] public bool Disk { get; set; }
    [Params(0, 1024)] public int CacheEntries { get; set; }
    private BenchmarkStorage _scope = null!;
    private int _cursor = 1;

    [GlobalSetup]
    public void Setup()
    {
        _scope = new BenchmarkStorage(Disk, new RaftOptions { NodeId = "n1", Members = new[] { "n1" }, LogCacheEntries = CacheEntries });
        _scope.Store.SaveHardState(new HardState(1));
        var payload = new byte[256];
        _scope.Store.Append(Enumerable.Range(1, 10000).Select(i => new LogEntry(i, 1, payload, false)).ToArray());
    }

    [Benchmark] public IReadOnlyList<LogEntry> ReadTail() => _scope.Store.Read(10000 - 127, 128, 256 * 1024);
    [Benchmark] public long HistoricalTerm() => _scope.Store.GetTerm(5000);

    [Benchmark]
    public IReadOnlyList<LogEntry> CatchUpWindow()
    {
        var entries = _scope.Store.Read(_cursor, 128, 256 * 1024);
        _cursor = _cursor + entries.Count > 10000 ? 1 : _cursor + entries.Count;
        return entries;
    }

    [GlobalCleanup] public void Cleanup() => _scope.Dispose();
}
