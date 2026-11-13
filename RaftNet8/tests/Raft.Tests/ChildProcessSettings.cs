using System.Diagnostics;

namespace Raft.Tests;

internal static class ChildProcessSettings
{
    public static void IsolateFaultHarness(ProcessStartInfo start)
    {
        // Fault children deliberately bypass disposal and are killed at persistence boundaries.
        // Inheriting the parent's native coverage session can terminate them before Main or wait
        // for a collector handshake. Collect the reducer in the test host, not in these raw children.
        start.Environment["CORECLR_ENABLE_PROFILING"] = "0";
        start.Environment["COR_ENABLE_PROFILING"] = "0";
    }
}
