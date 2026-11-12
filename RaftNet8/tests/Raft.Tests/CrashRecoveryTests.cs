using System.Diagnostics;
using Raft.Storage;

namespace Raft.Tests;

public sealed class CrashRecoveryTests
{
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
        start.ArgumentList.Add(assembly);
        start.ArgumentList.Add(path);
        start.ArgumentList.Add(mode);
        return Process.Start(start) ?? throw new InvalidOperationException("Cannot start crash harness.");
    }
}
