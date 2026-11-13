using System.Diagnostics;
using DBreeze;
using Raft.Storage;

namespace Raft.Tests;

public sealed class CrashRecoveryTests
{
    [Theory]
    [InlineData(ApplicationPersistenceBoundary.BeforeTransaction, false)]
    [InlineData(ApplicationPersistenceBoundary.AfterCommands, false)]
    [InlineData(ApplicationPersistenceBoundary.BeforeCommit, false)]
    [InlineData(ApplicationPersistenceBoundary.AfterCommit, true)]
    [InlineData(ApplicationPersistenceBoundary.AfterCheckpointPublication, true)]
    public async Task Application_process_death_preserves_atomic_effects_and_index_term_checkpoint(ApplicationPersistenceBoundary boundary, bool committed)
    {
        using var directory = new TestDirectory();
        using (var engine = new DBreezeEngine(directory.Path))
        {
            var machine = new DBreezeStateMachine(engine, "business/checkpoint", ["business/values"], Increment);
            await machine.ApplyAsync(new LogEntry(1, 7, "increment"u8), default);
        }
        using var process = StartHarness(directory.Path, "apply-v2-" + boundary);
        try { Assert.Equal("READY", await process.StandardOutput.ReadLineAsync().WaitAsync(TimeSpan.FromSeconds(15))); }
        finally
        {
            if (!process.HasExited) process.Kill(entireProcessTree: true);
            await process.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(10));
        }
        using var recoveredEngine = new DBreezeEngine(directory.Path);
        var recovered = new DBreezeStateMachine(recoveredEngine, "business/checkpoint", ["business/values"], Increment);
        Assert.Equal(new AppliedCheckpoint(committed ? 2 : 1, committed ? 8 : 7), recovered.Checkpoint);
        using var tx = recoveredEngine.GetTransaction();
        Assert.Equal(committed ? 2 : 1, tx.Select<string, long>("business/values", "counter").Value);
        return;

        static void Increment(DBreeze.Transactions.Transaction tx, LogEntry entry)
        {
            var row = tx.Select<string, long>("business/values", "counter");
            tx.Insert<string, long>("business/values", "counter", (row.Exists ? row.Value : 0) + 1);
        }
    }

    public static IEnumerable<object[]> LogBoundaries()
    {
        foreach (var layout in Enum.GetValues<DBreezeLogLayout>())
        foreach (var boundary in Enum.GetValues<PersistenceBoundary>())
            yield return [layout, boundary, boundary is PersistenceBoundary.AfterCommit or PersistenceBoundary.AfterCachePublication];
    }

    [Theory]
    [MemberData(nameof(LogBoundaries))]
    public async Task Checked_log_process_death_at_each_write_boundary_recovers_one_complete_atomic_batch(DBreezeLogLayout layout, PersistenceBoundary boundary, bool committed)
    {
        using var directory = new TestDirectory();
        var options = new RaftOptions { NodeId = "n1", Members = ["n1", "n2", "n3"] };
        using (var storage = new DBreezeLogStorage(directory.Path, options, layout))
        {
            storage.Write(new LogWrite.Batch([
                new LogWrite.HardStateUpdate(new(7, "n2")),
                new LogWrite.Append([new LogEntry(1, 7, "safe"u8)]),
                new LogWrite.HardStateUpdate(new(7, "n2", 1))
            ]));
        }
        using var process = StartHarness(directory.Path, $"v{(int)layout}-" + boundary);
        try { Assert.Equal("READY", await process.StandardOutput.ReadLineAsync().WaitAsync(TimeSpan.FromSeconds(15))); }
        finally
        {
            if (!process.HasExited) process.Kill(entireProcessTree: true);
            await process.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(10));
        }
        using var recovered = new DBreezeLogStorage(directory.Path, options, layout);
        Assert.Equal(new HardState(committed ? 8 : 7, "n2", committed ? 2 : 1), recovered.HardState);
        Assert.Equal(committed ? 2 : 1, recovered.LastIndex);
        Assert.Equal("safe"u8.ToArray(), recovered.Read(1, 1, 1024)[0].Command.ToArray());
        if (committed) Assert.Equal("v2-durable"u8.ToArray(), recovered.Read(2, 1, 1024)[0].Command.ToArray());
    }

    [Fact]
    public async Task Durable_acknowledgment_survives_abrupt_process_exit_without_disposal()
    {
        using var directory = new TestDirectory();
        using var process = StartHarness(directory.Path, "committed");
        await process.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(15));
        Assert.Equal(17, process.ExitCode);
        using var storage = Open(directory.Path);
        Assert.Equal(new HardState(7, "n2", 1), storage.HardState);
        Assert.Equal("crash-durable"u8.ToArray(), storage.Read(1, 1, 1024)[0].Command.ToArray());
    }

    [Fact]
    public async Task Killing_a_process_mid_transaction_preserves_the_prior_metadata_and_log()
    {
        using var directory = new TestDirectory();
        using (var storage = Open(directory.Path))
        {
            storage.SaveHardState(new HardState(7, "n2"));
            storage.Append(new[] { new LogEntry(1, 7, "safe"u8) });
            storage.SaveHardState(new HardState(7, "n2", 1));
        }
        using var process = StartHarness(directory.Path, "uncommitted");
        try { Assert.Equal("READY", await process.StandardOutput.ReadLineAsync().WaitAsync(TimeSpan.FromSeconds(15))); }
        finally
        {
            if (!process.HasExited) process.Kill(entireProcessTree: true);
            await process.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(10));
        }
        using var recovered = Open(directory.Path);
        Assert.Equal(new HardState(7, "n2", 1), recovered.HardState);
        Assert.Equal(1, recovered.LastIndex);
        Assert.Equal("safe"u8.ToArray(), recovered.Read(1, 1, 1024)[0].Command.ToArray());
    }

    private static DBreezeRaftStorage Open(string path) => new(path,
        new RaftOptions { NodeId = "n1", Members = new[] { "n1", "n2", "n3" } });

    private static Process StartHarness(string path, string mode)
    {
        var configuration = new DirectoryInfo(AppContext.BaseDirectory).Parent!.Name;
        var root = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "../../../../../"));
        var assembly = Path.Combine(root, "tools", "Raft.CrashHarness", "bin", configuration, "net8.0", "Raft.CrashHarness.dll");
        var start = new ProcessStartInfo("dotnet") { UseShellExecute = false, RedirectStandardOutput = true, RedirectStandardError = true, CreateNoWindow = true };
        ChildProcessSettings.IsolateFaultHarness(start);
        start.ArgumentList.Add(assembly);
        start.ArgumentList.Add(path);
        start.ArgumentList.Add(mode);
        return Process.Start(start) ?? throw new InvalidOperationException("Cannot start crash harness.");
    }
}
