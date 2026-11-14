using System.Diagnostics;
using System.Diagnostics.Tracing;
using System.Runtime;

namespace Raft.Benchmarks;

/// <summary>Opt-in EventPipe fences, separate from uninstrumented capacity measurements.</summary>
[EventSource(Name = "Raft.Comparison.Measurements")]
internal sealed class MeasurementTrace : EventSource
{
    internal static readonly MeasurementTrace Log = new();
    [NonEvent]
    public void Start(string name, int round, long timestamp) => MeasurementStarted(timestamp);
    [Event(1, Level = EventLevel.Informational)]
    public void MeasurementStarted(long timestamp) => WriteEvent(1, timestamp);
    [Event(2, Level = EventLevel.Informational, Opcode = EventOpcode.Info)]
    public void Stop(long timestamp) => WriteEvent(2, timestamp);
}

internal sealed record RuntimeSample(long CpuTicks, long CompiledMethods, long CompilationTicks,
    long CompletedWorkItems, int WorkerThreads, long PendingWorkItems, int Gen0, int Gen1, int Gen2)
{
    internal static RuntimeSample Capture()
    {
        using var process = Process.GetCurrentProcess();
        return new(process.TotalProcessorTime.Ticks, JitInfo.GetCompiledMethodCount(), JitInfo.GetCompilationTime().Ticks,
            ThreadPool.CompletedWorkItemCount, ThreadPool.ThreadCount, ThreadPool.PendingWorkItemCount,
            GC.CollectionCount(0), GC.CollectionCount(1), GC.CollectionCount(2));
    }
}
