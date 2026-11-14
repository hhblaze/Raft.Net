using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Tracing;
using Raft.Storage;
using Raft.Transport;

namespace Raft.Benchmarks;

/// <summary>Opt-in diagnostic counters. Never enabled for uninstrumented capacity runs.</summary>
internal sealed class ComparativeDiagnostics : IDisposable
{
    private readonly QueueListener _queue;
    internal ComparativeDiagnostics() => _queue = new(this);
    public void Dispose() => _queue.Dispose();
    private sealed class QueueListener(ComparativeDiagnostics owner) : EventListener
    {
        protected override void OnEventSourceCreated(EventSource source)
        {
            if (source.Name == "Raft.Net8.QueueDiagnostics") EnableEvents(source, EventLevel.Informational);
        }
        protected override void OnEventWritten(EventWrittenEventArgs args)
        {
            if (args.Payload is { Count: 3 } payload)
                owner.Record(payload[0] + "/Queue/" + payload[1], (int)payload[2]!);
        }
    }
    private sealed class Count
    {
        internal long Calls, Units, Ticks, Max;
        internal void Add(long units, long ticks)
        {
            Interlocked.Increment(ref Calls);
            Interlocked.Add(ref Units, units);
            Interlocked.Add(ref Ticks, ticks);
            var previous = Volatile.Read(ref Max);
            while (units > previous)
            {
                var observed = Interlocked.CompareExchange(ref Max, units, previous);
                if (observed == previous) break;
                previous = observed;
            }
        }
    }
    private readonly ConcurrentDictionary<string, Count> _counts = new();
    private volatile bool _enabled;
    internal void Begin() { _counts.Clear(); _enabled = true; }
    internal void End() => _enabled = false;
    private void Record(string key, long units, long ticks = 0)
    {
        if (_enabled) _counts.GetOrAdd(key, static _ => new()).Add(units, ticks);
    }
    internal void Applied(string node, int count) => Record(node + "/ApplicationBatch", count);
    internal ILogStore Wrap(string node, ILogStore store) => new ObservedStore(this, node, store);
    internal ITransport WrapTransport(string node, TcpRaftTransport transport) => new ObservedTransport(this, node, transport);
    internal object Snapshot() => _counts.OrderBy(kv => kv.Key).ToDictionary(kv => kv.Key, kv => new
    {
        kv.Value.Calls, kv.Value.Units, kv.Value.Max,
        TotalMilliseconds = kv.Value.Ticks * 1000d / Stopwatch.Frequency
    });

    private sealed class ObservedTransport(ComparativeDiagnostics owner, string node, TcpRaftTransport transport) : IPeerDirectoryTransport
    {
        public IReadOnlyList<PeerDescriptor> BootstrapPeers => transport.BootstrapPeers;
        public ValueTask UpdateDirectoryAsync(MembershipState state, CancellationToken token) => transport.UpdateDirectoryAsync(state, token);
        public async ValueTask<RaftRpc> SendAsync(string peerId, RaftRpc request, CancellationToken token)
        {
            owner.Record(node + "/RPC/" + request.GetType().Name, request is AppendRequest append ? append.Entries.Count : 1);
            return await transport.SendAsync(peerId, request, token);
        }
    }

    private sealed class ObservedStore(ComparativeDiagnostics owner, string node, ILogStore store) : ILogStore, ILogStoreProgress
    {
        public long UncommittedBytes => ((ILogStoreProgress)store).UncommittedBytes;
        public ValueTask<RecoveredLogState> RecoverAsync(CancellationToken token) => store.RecoverAsync(token);
        public async ValueTask<LogReadResult> ReadAsync(LogQuery query, CancellationToken token)
        {
            var started = Stopwatch.GetTimestamp();
            var result = await store.ReadAsync(query, token);
            owner.Record(node + "/Query/" + query.GetType().Name, Math.Max(1, result.Entries.IsDefault ? 0 : result.Entries.Length),
                Stopwatch.GetTimestamp() - started);
            return result;
        }
        public async ValueTask WriteAsync(LogWrite write, CancellationToken token)
        {
            var started = Stopwatch.GetTimestamp();
            await store.WriteAsync(write, token);
            owner.Record(node + "/Persist", 1, Stopwatch.GetTimestamp() - started);
            Observe(write);
        }
        private void Observe(LogWrite write)
        {
            if (write is LogWrite.Batch batch) foreach (var child in batch.Writes) Observe(child);
            else owner.Record(node + "/Write/" + write.GetType().Name, write switch
            {
                LogWrite.Append a => a.Entries.Length,
                LogWrite.ReplaceSuffix r => r.Entries.Length,
                _ => 1
            });
        }
        public ValueTask DisposeAsync() => store.DisposeAsync();
    }
}
