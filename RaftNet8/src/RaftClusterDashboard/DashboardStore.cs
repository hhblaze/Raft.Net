using System.Text.Json;
using DBreeze;
using DBreeze.Transactions;
using DBreeze.Utils;
using Microsoft.AspNetCore.DataProtection;

namespace RaftClusterDashboard;

public sealed record DashboardSettings
{
    public string DataDirectory { get; init; } = Path.Combine(AppContext.BaseDirectory, "dashboard-data");
    public bool SecureProfile { get; init; }
    public bool RequirePrivilegedMfa { get; init; }
    public int DetailedHistorySeconds { get; init; } = 600;
    public int HistoryDays { get; init; } = 7;
    public int MaxHistoryRows { get; init; } = 400_000;
    public int MaxClusters { get; init; } = 32;
    public int MaxObservedNodes { get; init; } = 128;
    public int MaxSseConnections { get; init; } = 64;
    public int PollIntervalSeconds { get; init; } = 1;
    public int DirectoryRefreshSeconds { get; init; } = 5;
    public Dictionary<string, DiagnosticCertificateSettings> DiagnosticCertificates { get; init; } = new(StringComparer.Ordinal);
}
public sealed record ClusterProfile(string Id, string Name, string GroupId, string[] ManagementEndpoints,
    string ProtectedCredential, string? DiagnosticCertificatePath = null);
public sealed record DashboardAudit(DateTimeOffset At, string UserId, string Action, string Target, bool Success);

/// <summary>One long-lived DBreeze engine; all records use typed prefixes in one physical table.</summary>
public sealed class DashboardStore : IDisposable
{
    private const string Table = "dashboard-v1";
    private readonly DBreezeEngine _engine;
    private readonly object _gate = new();
    private readonly IDataProtector _protector;
    private readonly DashboardSettings _settings;
    public DashboardStore(DashboardSettings settings, IDataProtectionProvider protection)
    {
        _settings = settings;
        Directory.CreateDirectory(settings.DataDirectory);
        _engine = new(Path.Combine(settings.DataDirectory, "database"));
        _protector = protection.CreateProtector("RaftClusterDashboard.Store.v1");
    }
    public string Protect(string value) => _protector.Protect(value);
    public string Unprotect(string value) => _protector.Unprotect(value);
    internal T? Get<T>(byte[] key) => Read(tx => Get<T>(tx, key));
    internal static T? Get<T>(Transaction tx, byte[] key)
    {
        var row = tx.Select<byte[], byte[]>(Table, key);
        return row.Exists ? JsonSerializer.Deserialize<T>(row.Value) : default;
    }
    internal static void Put<T>(Transaction tx, byte[] key, T value) =>
        tx.Insert<byte[], byte[]>(Table, key, JsonSerializer.SerializeToUtf8Bytes(value));
    internal static void Remove(Transaction tx, byte[] key) => tx.RemoveKey<byte[]>(Table, key);
    internal T Read<T>(Func<Transaction, T> read)
    {
        lock (_gate) { using var tx = _engine.GetTransaction(); return read(tx); }
    }
    internal T Write<T>(Func<Transaction, T> write)
    {
        lock (_gate)
        {
            using var tx = _engine.GetTransaction();
            tx.SynchronizeTables(Table);
            var value = write(tx);
            tx.Commit();
            return value;
        }
    }
    internal IReadOnlyList<T> List<T>(int prefix, int maximum) => Read(tx => Range<T>(tx, prefix, maximum));
    internal static IReadOnlyList<T> Range<T>(Transaction tx, int type, int maximum)
    {
        var prefix = type.ToIndex();
        var result = new List<T>();
        foreach (var row in tx.SelectForwardStartFrom<byte[], byte[]>(Table, prefix, true))
        {
            if (!row.Key.AsSpan().StartsWith(prefix)) break;
            if (result.Count == maximum) break;
            result.Add(JsonSerializer.Deserialize<T>(row.Value) ?? throw new InvalidDataException("Invalid dashboard record."));
        }
        return result;
    }
    public IReadOnlyList<ClusterProfile> Profiles() => List<ClusterProfile>(3, 32);
    public ClusterProfile? Profile(string id) => Get<ClusterProfile>(3.ToIndex(id));
    public void SaveProfile(ClusterProfile profile) => Write(tx =>
    {
        if (Get<ClusterProfile>(tx, 3.ToIndex(profile.Id)) is null && Range<ClusterProfile>(tx, 3, _settings.MaxClusters).Count >= _settings.MaxClusters)
            throw new InvalidOperationException("Dashboard cluster profile limit reached.");
        Put(tx, 3.ToIndex(profile.Id), profile); return true;
    });
    public void Audit(string userId, string action, string target, bool success) => Write(tx =>
    {
        Put(tx, 5.ToIndex(DateTimeOffset.UtcNow.UtcTicks, Guid.NewGuid().ToByteArray()), new DashboardAudit(DateTimeOffset.UtcNow, userId, action, target, success));
        var countKey = 6.ToIndex("audit-count");
        var count = Get<long>(tx, countKey) + 1;
        var prefix = 5.ToIndex();
        foreach (var row in tx.SelectForwardStartFrom<byte[], byte[]>(Table, prefix, true, AsReadVisibilityScope: true))
        {
            if (count <= 10_000 || !row.Key.AsSpan().StartsWith(prefix)) break;
            Remove(tx, row.Key); count--;
        }
        Put(tx, countKey, count);
        return true;
    });
    public void SaveHistory(string clusterId, ClusterPoint point, int days, int maximum) => Write(tx =>
    {
        var key = 4.ToIndex(clusterId, point.At.UtcTicks);
        if (tx.Select<byte[], byte[]>(Table, key).Exists) return false;
        Put(tx, key, point);
        var countKey = 6.ToIndex("history:" + clusterId);
        var count = Get<long>(tx, countKey) + 1;
        var prefix = 4.ToIndex(clusterId);
        var cutoff = point.At.AddDays(-days);
        foreach (var row in tx.SelectForwardStartFrom<byte[], byte[]>(Table, prefix, true, AsReadVisibilityScope: true))
        {
            if (!row.Key.AsSpan().StartsWith(prefix)) break;
            var old = JsonSerializer.Deserialize<ClusterPoint>(row.Value) ?? throw new InvalidDataException("Invalid history point.");
            if (count <= maximum && old.At >= cutoff) break;
            Remove(tx, row.Key); count--;
        }
        Put(tx, countKey, count);
        return true;
    });
    public IReadOnlyList<ClusterPoint> History(string clusterId, DateTimeOffset after, int maximum = 10080) => Read(tx =>
    {
        var prefix = 4.ToIndex(clusterId);
        var result = new List<ClusterPoint>();
        foreach (var row in tx.SelectBackwardStartFrom<byte[], byte[]>(Table, 4.ToIndex(clusterId, long.MaxValue), true))
        {
            if (!row.Key.AsSpan().StartsWith(prefix) || result.Count == maximum) break;
            var point = JsonSerializer.Deserialize<ClusterPoint>(row.Value) ?? throw new InvalidDataException("Invalid history point.");
            if (point.At < after) break;
            result.Add(point);
        }
        result.Reverse(); // A bounded page must show recent points, including when retention exceeds one week.
        return result;
    });
    public void Dispose() => _engine.Dispose();
}
