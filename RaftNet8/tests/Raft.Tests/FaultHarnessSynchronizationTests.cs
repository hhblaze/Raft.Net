namespace Raft.Tests;

public sealed class FaultHarnessSynchronizationTests
{
    [Fact]
    public async Task A_native_sharing_lock_must_be_released_before_the_recovery_probe_completes()
    {
        if (!OperatingSystem.IsWindows()) return;
        using var directory = new TestDirectory();
        using var held = new FileStream(Path.Combine(directory.Path, "held"), FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None);
        var waiting = ChildProcessSettings.WaitForFileReleaseAsync(directory.Path);
        await Task.Delay(50);
        Assert.False(waiting.IsCompleted);
        held.Dispose();
        await waiting.WaitAsync(TimeSpan.FromSeconds(2));
    }
}
