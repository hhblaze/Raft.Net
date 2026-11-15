using System.Diagnostics.Metrics;

namespace Raft.Cluster.Server;

// One listener per process, with a bounded entry per explicitly monitored node. No payloads or I/O in callbacks.
internal sealed class CoreTelemetry : IDisposable
{
    private sealed class Aggregate
    {
        public long Count;
        public double Total;
    }
    private static readonly object RegistryGate = new();
    private static readonly Dictionary<(string Group, string Node), CoreTelemetry> Registry = new();
    private static MeterListener? _listener;
    private readonly (string Group, string Node) _key;
    private readonly Dictionary<string, Aggregate> _values = new(StringComparer.Ordinal);
    private readonly object _gate = new();
    public CoreTelemetry(string group, string node)
    {
        _key = (group, node);
        lock (RegistryGate)
        {
            if (!Registry.TryAdd(_key, this)) throw new InvalidOperationException("Duplicate monitored node identity.");
            if (_listener is not null) return;
            _listener = new();
            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name == RaftInstrumentation.MeterName && instrument is Counter<long> or Histogram<double>)
                    listener.EnableMeasurementEvents(instrument);
            };
            _listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) => Record(instrument.Name, value, tags, false));
            _listener.SetMeasurementEventCallback<double>((instrument, value, tags, _) => Record(instrument.Name, value, tags, true));
            _listener.Start();
        }
    }
    private static void Record(string name, double value, ReadOnlySpan<KeyValuePair<string, object?>> tags, bool duration)
    {
        string? group = null, node = null;
        foreach (var tag in tags)
        {
            if (tag.Key == "raft.group") group = tag.Value as string;
            else if (tag.Key == "raft.node") node = tag.Value as string;
        }
        if (group is null || node is null) return;
        CoreTelemetry? target;
        lock (RegistryGate) Registry.TryGetValue((group, node), out target);
        if (target is null) return;
        lock (target._gate)
        {
            if (!target._values.TryGetValue(name, out var aggregate)) target._values[name] = aggregate = new();
            aggregate.Count++;
            aggregate.Total += duration ? value * 1000 : value;
        }
    }
    public IReadOnlyDictionary<string, CoreMetric> Read()
    {
        lock (_gate) return _values.ToDictionary(pair => pair.Key, pair => new CoreMetric(pair.Value.Count, pair.Value.Total), StringComparer.Ordinal);
    }
    public void Dispose()
    {
        MeterListener? retired = null;
        lock (RegistryGate)
        {
            Registry.Remove(_key);
            if (Registry.Count == 0) { retired = _listener; _listener = null; }
        }
        retired?.Dispose();
    }
}
