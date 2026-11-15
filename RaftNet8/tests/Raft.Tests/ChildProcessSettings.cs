using System.Diagnostics;

namespace Raft.Tests;

internal static class ChildProcessSettings
{
    /// <summary>Synchronize Windows handle rundown before constructing an engine; never retry engine/recovery errors.</summary>
    public static async Task WaitForFileReleaseAsync(string directory)
    {
        if (!OperatingSystem.IsWindows()) return;
        var started = Stopwatch.GetTimestamp();
        while (true)
        {
            try
            {
                foreach (var path in Directory.EnumerateFiles(directory, "*", SearchOption.AllDirectories))
                {
                    using var probe = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.None);
                }
                return;
            }
            catch (IOException error) when ((error.HResult & 0xffff) is 32 or 33 && Stopwatch.GetElapsedTime(started) < TimeSpan.FromSeconds(5))
            { await Task.Delay(25); }
        }
    }

    public static void IsolateFaultHarness(ProcessStartInfo start)
    {
        // Fault children deliberately bypass disposal and are killed at persistence boundaries.
        // Inheriting the parent's native coverage session can terminate them before Main or wait
        // for a collector handshake. Collect the reducer in the test host, not in these raw children.
        start.Environment["CORECLR_ENABLE_PROFILING"] = "0";
        start.Environment["COR_ENABLE_PROFILING"] = "0";
    }
}
