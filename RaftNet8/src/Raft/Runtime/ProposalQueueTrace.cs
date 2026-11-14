using System.Diagnostics;
using System.Diagnostics.Tracing;

namespace Raft.Runtime;

// Calls are completely omitted from ordinary builds. Enable only in a separate
// diagnostic runtime (-p:DefineConstants=RAFT_QUEUE_DIAGNOSTICS), never capacity runs.
[EventSource(Name = "Raft.Net8.QueueDiagnostics")]
internal sealed class ProposalQueueTrace : EventSource
{
    private static readonly ProposalQueueTrace Log = new();
    [Conditional("RAFT_QUEUE_DIAGNOSTICS")]
    internal static void Record(string node, string kind, int entries = 1)
    {
        if (Log.IsEnabled()) Log.Queue(node, kind, entries);
    }
    [Event(1, Level = EventLevel.Informational)]
    // Explicit payload types avoid WriteEvent overloads widening Int32 to Int64,
    // which produces an invalid EventPipe payload for the declared event schema.
    public void Queue(string node, string kind, int entries) => WriteEvent(1, new object?[] { node, kind, entries });
}
