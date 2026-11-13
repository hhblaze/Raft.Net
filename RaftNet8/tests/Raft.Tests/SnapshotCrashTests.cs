using System.Diagnostics;
using System.Text.Json;
using Raft.Storage;

namespace Raft.Tests;

public sealed class SnapshotCrashTests
{
    private sealed class OfflineTransport : ITransport
    {
        public ValueTask<RaftRpc> SendAsync(string peerId, RaftRpc request, CancellationToken cancellationToken) =>
            ValueTask.FromException<RaftRpc>(new IOException("Offline fixture."));
    }

    public static IEnumerable<object[]> Boundaries()
    {
        foreach (var layout in Enum.GetValues<DBreezeLogLayout>())
        foreach (var row in InstallBoundaries())
            yield return [layout, .. row];
    }

    private static IEnumerable<object[]> InstallBoundaries()
    {
        foreach (var point in Enum.GetValues<PersistenceBoundary>())
        {
            yield return ["snapshot-intent-" + point, point is PersistenceBoundary.AfterCommit or PersistenceBoundary.AfterCachePublication, false];
            yield return ["snapshot-publish-" + point, true, point is PersistenceBoundary.AfterCommit or PersistenceBoundary.AfterCachePublication];
        }
        foreach (var point in new[] { SnapshotPersistenceBoundary.BeforeGenerationSwitch, SnapshotPersistenceBoundary.AfterGenerationPointer, SnapshotPersistenceBoundary.AfterGenerationPublication })
            yield return ["snapshot-generation-" + point, true, false];
        yield return ["snapshot-image-BeforeImagePublication", false, false];
        yield return ["snapshot-image-AfterImagePublication", false, false];
    }

    [Theory]
    [MemberData(nameof(Boundaries))]
    public async Task Process_death_at_each_install_boundary_recovers_application_and_log_as_one_generation(DBreezeLogLayout layout, string mode, bool installedAfterRecovery, bool logPublished)
    {
        using var directory = new TestDirectory();
        var options = new RaftOptions { NodeId = "n1", Members = ["n1", "n2", "n3"], SnapshotDirectory = Path.Combine(directory.Path, "snapshots") };
        await CreateFixtureAsync(directory.Path, options, layout);
        using var child = Start(directory.Path, layout == DBreezeLogLayout.UnifiedTableV3 ? "v3-" + mode : mode);
        try
        {
            var ready = await child.StandardOutput.ReadLineAsync().WaitAsync(TimeSpan.FromSeconds(20));
            if (ready != "READY") throw new InvalidOperationException($"Child failed before {mode}: {ready}\n{await child.StandardError.ReadToEndAsync()}");
        }
        finally
        {
            if (!child.HasExited) child.Kill(entireProcessTree: true);
            await child.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(10));
        }
        using (var raw = new DBreezeLogStorage(Path.Combine(directory.Path, "log"), options, layout))
        {
            Assert.Equal(logPublished ? 10 : 2, raw.HardState.CommitIndex);
            Assert.Equal(logPublished, raw.Recover().Snapshot is not null);
            Assert.Equal(installedAfterRecovery && !logPublished, raw.Recover().PendingSnapshotInstall is not null);
        }
        using var machine = SnapshotStorageTests.Machine(Path.Combine(directory.Path, "application"));
        await using var log = new RaftStorageAdapter(new DBreezeLogStorage(Path.Combine(directory.Path, "log"), options, layout));
        await using var node = await RaftNode.CreateAsync(options, log, new OfflineTransport(), machine);
        var expected = installedAfterRecovery ? 10 : 2;
        Assert.Equal(new(expected, 2), machine.Checkpoint);
        Assert.Equal(expected, SnapshotStorageTests.Value(machine));
        Assert.Equal(expected, node.Status.CommitIndex);
        Assert.Equal(expected, node.Status.LastApplied);
        Assert.Null((await log.RecoverAsync(default)).PendingSnapshotInstall);
        Assert.Null(node.Status.Fault);
    }

    private static async Task CreateFixtureAsync(string root, RaftOptions options, DBreezeLogLayout layout)
    {
        using var sourceLog = DBreezeLogStorage.CreateInMemory(options, layout);
        sourceLog.SaveHardState(new(2));
        var entries = Enumerable.Range(1, 10).Select(i => new LogEntry(i, 2, BitConverter.GetBytes(1))).ToArray();
        sourceLog.Append(entries);
        sourceLog.SaveHardState(new(2, CommitIndex: 10));
        using var source = SnapshotStorageTests.Machine(Path.Combine(root, "source-app"));
        await source.ApplyBatchAsync(entries, default);
        await using var repository = new SnapshotRepository(options);
        var context = new SnapshotBuildContext(new(10, 2), sourceLog.Recover().Configuration!, null, options,
            (from, count, bytes, _) => ValueTask.FromResult(sourceLog.Query(new LogQuery.Entries(from, count, bytes))));
        var image = await repository.CreateAsync(source, context, default);
        await File.WriteAllBytesAsync(Path.Combine(root, "intent.json"), JsonSerializer.SerializeToUtf8Bytes(new SnapshotInstallIntent(image.Metadata, false)));
        using var targetLog = new DBreezeLogStorage(Path.Combine(root, "log"), options, layout);
        targetLog.SaveHardState(new(2));
        targetLog.Append(entries.Take(2).ToArray());
        targetLog.SaveHardState(new(2, CommitIndex: 2));
        using var target = SnapshotStorageTests.Machine(Path.Combine(root, "application"));
        await target.ApplyBatchAsync(entries.Take(2).ToArray(), default);
    }

    private static Process Start(string root, string mode)
    {
        var configuration = new DirectoryInfo(AppContext.BaseDirectory).Parent!.Name;
        var solution = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "../../../../../"));
        var assembly = Path.Combine(solution, "tools", "Raft.CrashHarness", "bin", configuration, "net8.0", "Raft.CrashHarness.dll");
        var start = new ProcessStartInfo("dotnet") { UseShellExecute = false, RedirectStandardOutput = true, RedirectStandardError = true, CreateNoWindow = true };
        ChildProcessSettings.IsolateFaultHarness(start);
        start.ArgumentList.Add(assembly);
        start.ArgumentList.Add(root);
        start.ArgumentList.Add(mode);
        return Process.Start(start) ?? throw new InvalidOperationException("Cannot start snapshot crash fixture.");
    }
}
