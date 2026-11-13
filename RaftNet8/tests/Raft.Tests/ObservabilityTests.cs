using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;
using Raft.Storage;

namespace Raft.Tests;

public sealed class ObservabilityTests
{
    private sealed record LogEvent(EventId Id, string Message, Exception? Error, KeyValuePair<string, object?>[] Fields);
    private sealed class Logs(bool throws = false) : ILoggerFactory, ILogger
    {
        public readonly ConcurrentQueue<LogEvent> Events = new();
        public ILogger CreateLogger(string categoryName) => this;
        public void AddProvider(ILoggerProvider provider) => throw new NotSupportedException();
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel level, EventId id, TState state, Exception? error, Func<TState, Exception?, string> formatter)
        {
            if (throws) throw new InvalidOperationException("Broken logging provider.");
            Events.Enqueue(new(id, formatter(state, error), error,
                state is IEnumerable<KeyValuePair<string, object?>> fields ? fields.ToArray() : []));
        }
        public void Dispose() { }
    }
    private sealed class NoPeers : IRaftTransport
    {
        public ValueTask<RaftRpc> SendAsync(string peerId, RaftRpc request, CancellationToken cancellationToken) =>
            ValueTask.FromException<RaftRpc>(new IOException("No peers."));
    }
    private sealed class FailingApplication(Exception error) : IRaftStateMachine
    {
        private long _applied;
        public long LastApplied => Interlocked.Read(ref _applied);
        public ValueTask ApplyAsync(LogEntry entry, CancellationToken token)
        {
            if (entry.Kind == LogEntryKind.Command) throw error;
            Interlocked.Exchange(ref _applied, entry.Index);
            return ValueTask.CompletedTask;
        }
    }

    [Fact]
    public async Task Structured_logs_metrics_and_proposal_spans_describe_committed_work_without_command_contents()
    {
        const string group = "observed-metrics";
        using var logs = new Logs();
        var measurements = new ConcurrentDictionary<string, double>(StringComparer.Ordinal);
        var stopped = new ConcurrentQueue<Activity>();
        using var meter = new MeterListener();
        meter.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Meter.Name == RaftInstrumentation.MeterName) listener.EnableMeasurementEvents(instrument);
        };
        meter.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
        { if (HasGroup(tags, group)) measurements[instrument.Name] = value; });
        meter.SetMeasurementEventCallback<double>((instrument, value, tags, _) =>
        { if (HasGroup(tags, group)) measurements[instrument.Name] = value; });
        meter.Start();
        using var activities = new ActivityListener
        {
            ShouldListenTo = source => source.Name == RaftInstrumentation.ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity => { if (activity.GetTagItem("raft.group") as string == group) stopped.Enqueue(activity); }
        };
        ActivitySource.AddActivityListener(activities);
        await using (var cluster = new RuntimeCluster(modern: true, configure: options => options with { GroupId = group }, loggerFactory: logs))
        {
            await cluster.StartAsync();
            var leader = await cluster.LeaderAsync();
            var results = await Task.WhenAll(Enumerable.Range(0, 20).Select(i => leader.ProposeAsync(BitConverter.GetBytes(i))));
            await leader.ReadIndexAsync();
            meter.RecordObservableInstruments();
            foreach (var name in new[] { "raft.elections", "raft.term", "raft.role", "raft.commit.index", "raft.applied.index",
                "raft.replication.lag", "raft.proposal.duration", "raft.persistence.duration", "raft.storage.commit.duration", "raft.apply.duration" })
                Assert.Contains(name, measurements.Keys);
            Assert.True(measurements["raft.proposal.duration"] >= 0);
            Assert.Contains(logs.Events, record => record.Id.Id == 1000 && record.Fields.Any(f => f.Key == "NodeId"));
            Assert.Equal(20, stopped.Count);
            Assert.All(stopped, activity =>
            {
                Assert.Equal("raft.propose", activity.OperationName);
                Assert.Equal("success", activity.GetTagItem("raft.outcome"));
                Assert.Contains(results, result => result.Index == (long)activity.GetTagItem("raft.index")!);
                Assert.DoesNotContain(activity.Tags, tag => tag.Key.Contains("payload", StringComparison.Ordinal) || tag.Key.Contains("contents", StringComparison.Ordinal));
            });
        }
        measurements.Clear();
        meter.RecordObservableInstruments();
        Assert.Empty(measurements); // disposed nodes are removed from the weak observable registry
    }

    [Fact]
    public async Task Broken_diagnostic_callbacks_cannot_mask_the_original_application_failure_or_prevent_fail_stop()
    {
        const string group = "observed-errors";
        var before = RaftInstrumentation.CallbackFailures;
        using var meter = new MeterListener();
        meter.InstrumentPublished = (instrument, listener) =>
        { if (instrument.Meter.Name == RaftInstrumentation.MeterName) listener.EnableMeasurementEvents(instrument); };
        meter.SetMeasurementEventCallback<double>((_, _, tags, _) =>
        { if (HasGroup(tags, group)) throw new InvalidOperationException("Broken metric callback."); });
        meter.Start();
        using var activities = new ActivityListener
        {
            ShouldListenTo = source => source.Name == RaftInstrumentation.ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => throw new InvalidOperationException("Broken sampler.")
        };
        ActivitySource.AddActivityListener(activities);
        using var logs = new Logs(throws: true);
        var options = new RaftOptions { NodeId = "n1", Members = ["n1"], GroupId = group };
        var original = new ArgumentException("Deterministic application failure after durable commit.");
        await using var node = new RaftNode(options, DBreezeLogStorage.CreateInMemory(options), new NoPeers(), new FailingApplication(original), logs);
        await node.StartAsync();
        await RuntimeCluster.EventuallyAsync(() => node.Status.Role == RaftRole.Leader && node.Status.LastApplied > 0);
        using var parent = new Activity("caller").Start();
        var proposalError = await Record.ExceptionAsync(() => node.ProposeAsync("command"u8.ToArray()).WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.Same(original, proposalError);
        Assert.Same(original, await Record.ExceptionAsync(() => node.Completion.WaitAsync(TimeSpan.FromSeconds(5))));
        Assert.Same(original, node.Status.Fault);
        Assert.Same(parent, Activity.Current);
        Assert.True(RaftInstrumentation.CallbackFailures > before);
    }

    private static bool HasGroup(ReadOnlySpan<KeyValuePair<string, object?>> tags, string group)
    {
        foreach (var tag in tags) if (tag.Key == "raft.group" && tag.Value as string == group) return true;
        return false;
    }
}
