using System.Diagnostics.Tracing;

namespace Raft.Cluster.Demo;

[EventSource(Name = "Raft.Cluster.Measurements")]
internal sealed class MeasurementEvents : EventSource
{
    public static readonly MeasurementEvents Log = new();
    [Event(1, Level = EventLevel.Informational)]
    public void Start(int voters, int payloadBytes, long timestamp) => WriteEvent(1, voters, payloadBytes, timestamp);
    [Event(2, Level = EventLevel.Informational)]
    public void Stop(int voters, int payloadBytes, long timestamp) => WriteEvent(2, voters, payloadBytes, timestamp);
}
