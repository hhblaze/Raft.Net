using Raft.Storage;

namespace Raft.Benchmarks;

internal sealed class BenchmarkStorage : IDisposable
{
    private readonly string? _directory;
    public DBreezeLogStorage Store { get; }
    public BenchmarkStorage(bool disk, RaftOptions options)
    {
        if (disk)
        {
            _directory = Path.Combine(Path.GetTempPath(), "raftnet8-benchmarks", Guid.NewGuid().ToString("N"));
            Store = new DBreezeLogStorage(_directory, options);
        }
        else Store = DBreezeLogStorage.CreateInMemory(options);
    }
    public void Dispose()
    {
        Store.Dispose();
        if (_directory is null) return;
        var parent = Path.GetFullPath(Path.Combine(Path.GetTempPath(), "raftnet8-benchmarks")) + Path.DirectorySeparatorChar;
        var target = Path.GetFullPath(_directory);
        if (!target.StartsWith(parent, StringComparison.OrdinalIgnoreCase)) throw new InvalidOperationException("Unexpected benchmark directory.");
        Directory.Delete(target, true);
    }
}
