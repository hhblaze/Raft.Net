using System.Diagnostics;
using System.Runtime.InteropServices;

namespace Raft.Cluster.Server;

internal sealed class NodeTelemetry : IAsyncDisposable
{
    private readonly MessageLedger _ledger;
    private readonly ClusterManagementOptions _options;
    private readonly CancellationTokenSource _stop = new();
    private readonly Task _sampling;
    private readonly CoreTelemetry _core;
    private readonly long[] _counts = new long[49];
    private readonly double[] _bounds = Enumerable.Range(0, 49).Select(i => Math.Pow(1.5, i) / 100).ToArray();
    private readonly object _gate = new();
    private NodeResources _resources;
    private long _failed, _publications, _bytes;
    private DateTimeOffset? _filesAt;
    private long? _log, _messages, _images;
    public string Epoch { get; } = Guid.NewGuid().ToString("N");
    public NodeTelemetry(MessageLedger ledger, ClusterManagementOptions options, string groupId, string nodeId)
    {
        _ledger = ledger; _options = options;
        _resources = Sample();
        _core = new(groupId, nodeId);
        _sampling = RunAsync();
    }
    public void Published(int bytes, double milliseconds)
    {
        _ = bytes;
        var bucket = Array.BinarySearch(_bounds, milliseconds);
        if (bucket < 0) bucket = ~bucket;
        lock (_gate) _counts[Math.Min(bucket, _counts.Length - 1)]++;
    }
    public void Failed() => Interlocked.Increment(ref _failed);
    public NodeDiagnostics Read(AdminNodeStatus status)
    {
        var core = _core.Read();
        lock (_gate) return new(status, _resources, _publications, _bytes, Interlocked.Read(ref _failed),
            new(_bounds.ToArray(), _counts.ToArray()), Epoch) { CoreMetrics = core };
    }
    private async Task RunAsync()
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(_options.SampleIntervalSeconds));
        try
        {
            while (await timer.WaitForNextTickAsync(_stop.Token).ConfigureAwait(false))
            {
                try
                {
                    var sample = Sample();
                    var entities = _ledger.GetEntities();
                    lock (_gate)
                    {
                        _resources = sample;
                        _publications = entities.Sum(entity => entity.LastMessageId);
                        _bytes = entities.Sum(entity => entity.PublishedBytes);
                    }
                }
                catch (Exception error) when (error is IOException or UnauthorizedAccessException or InvalidOperationException)
                { /* Keep the previous timestamp so collection failures are visible as stale data. */ }
            }
        }
        catch (OperationCanceledException) when (_stop.IsCancellationRequested) { }
    }
    private NodeResources Sample()
    {
        using var process = Process.GetCurrentProcess();
        var now = DateTimeOffset.UtcNow;
        long? total = null, available = null, diskTotal = null, diskFree = null;
        string? volume = null;
        if (OperatingSystem.IsWindows())
        {
            var memory = new MemoryStatus { Length = (uint)Marshal.SizeOf<MemoryStatus>() };
            if (GlobalMemoryStatusEx(ref memory)) { total = checked((long)memory.TotalPhysical); available = checked((long)memory.AvailablePhysical); }
        }
        else if (OperatingSystem.IsLinux())
        {
            var lines = File.ReadAllLines("/proc/meminfo");
            total = MemoryField(lines, "MemTotal:"); available = MemoryField(lines, "MemAvailable:");
        }
        if (_options.DataDirectory is { } root)
        {
            var absolute = Path.GetFullPath(root);
            var drive = DriveInfo.GetDrives().Where(drive => absolute.StartsWith(drive.RootDirectory.FullName, OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal))
                .OrderByDescending(drive => drive.RootDirectory.FullName.Length).First();
            if (drive.IsReady) { volume = drive.Name; diskTotal = drive.TotalSize; diskFree = drive.AvailableFreeSpace; }
            if (_filesAt is null || now - _filesAt >= TimeSpan.FromSeconds(_options.FileScanIntervalSeconds))
            {
                _log = Size(Path.Combine(root, "log")); _messages = Size(Path.Combine(root, "messages")); _images = Size(Path.Combine(root, "images"));
                _filesAt = now;
            }
        }
        return new(_options.HostId, volume, now, process.WorkingSet64, process.PrivateMemorySize64,
            GC.GetTotalMemory(false), _ledger.CachedPayloadBytes, total, available, diskTotal, diskFree, _log, _messages, _images, _filesAt, Environment.ProcessId, _options.SampleIntervalSeconds);
    }
    private static long? MemoryField(string[] lines, string key)
    {
        var line = lines.FirstOrDefault(item => item.StartsWith(key, StringComparison.Ordinal));
        return line is null ? null : long.Parse(line[key.Length..].Trim().Split(' ', StringSplitOptions.RemoveEmptyEntries)[0],
            System.Globalization.CultureInfo.InvariantCulture) * 1024;
    }
    private static long? Size(string root)
    {
        if (!Directory.Exists(root)) return 0;
        long size = 0;
        var count = 0;
        var options = new EnumerationOptions { RecurseSubdirectories = true, AttributesToSkip = FileAttributes.ReparsePoint, IgnoreInaccessible = false };
        try
        {
            foreach (var file in Directory.EnumerateFiles(root, "*", options))
            {
                if (++count > 100_000) return null;
                size = checked(size + new FileInfo(file).Length);
            }
            return size;
        }
        catch (Exception error) when (error is IOException or UnauthorizedAccessException) { return null; }
    }
    [StructLayout(LayoutKind.Sequential)]
    private struct MemoryStatus
    {
        public uint Length, MemoryLoad;
        public ulong TotalPhysical, AvailablePhysical, TotalPageFile, AvailablePageFile, TotalVirtual, AvailableVirtual, AvailableExtendedVirtual;
    }
    [DllImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool GlobalMemoryStatusEx(ref MemoryStatus status);
    public async ValueTask DisposeAsync() { _stop.Cancel(); await _sampling.ConfigureAwait(false); _core.Dispose(); _stop.Dispose(); }
}
