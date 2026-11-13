using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Raft;

/// <summary>Subscribe by source/meter name. Instrumentation never receives command contents.</summary>
public static class RaftInstrumentation
{
    public const string MeterName = "Raft.Net8";
    public const string ActivitySourceName = "Raft.Net8";
    private static readonly Meter Meter = new(MeterName, "2.0.0");
    internal static readonly ActivitySource Activities = new(ActivitySourceName, "2.0.0");
    private static readonly ConcurrentDictionary<long, WeakReference<RaftNode>> Nodes = new();
    private static long _registration, _callbackFailures;
    public static long CallbackFailures => Interlocked.Read(ref _callbackFailures);
    private static readonly Counter<long> Elections = Meter.CreateCounter<long>("raft.elections", description: "Real election attempts after durable self-vote.");
    private static readonly Counter<long> PreVotes = Meter.CreateCounter<long>("raft.pre_votes", description: "Pre-vote rounds.");
    private static readonly Counter<long> Failures = Meter.CreateCounter<long>("raft.failures", description: "Fail-stop node failures.");
    private static readonly Counter<long> RpcFailures = Meter.CreateCounter<long>("raft.rpc.failures", description: "Transport attempts that did not return a valid response.");
    private static readonly Counter<long> SnapshotSent = Meter.CreateCounter<long>("raft.snapshot.send.bytes", "By", "Snapshot payload bytes attempted, including retries.");
    private static readonly Counter<long> SnapshotReceived = Meter.CreateCounter<long>("raft.snapshot.receive.bytes", "By", "Authorized snapshot payload bytes received, including duplicates.");
    internal static readonly Histogram<double> ProposalDuration = Meter.CreateHistogram<double>("raft.proposal.duration", "s", "Caller latency including admission, quorum and application; outcome distinguishes cancellation/errors.");
    internal static readonly Histogram<double> PersistenceDuration = Meter.CreateHistogram<double>("raft.persistence.duration", "s", "ILogStore durable-write completion latency, including queueing.");
    internal static readonly Histogram<double> CommitDuration = Meter.CreateHistogram<double>("raft.storage.commit.duration", "s", "DBreeze Commit latency; includes fsync for disk storage, not a raw syscall timer.");
    internal static readonly Histogram<double> ApplyDuration = Meter.CreateHistogram<double>("raft.apply.duration", "s", "Application batch completion latency, including its durable checkpoint.");

    static RaftInstrumentation()
    {
        Meter.CreateObservableGauge("raft.term", () => Observe(node => node.RawStatus.Term));
        Meter.CreateObservableGauge("raft.role", () => Observe(node => (long)node.RawStatus.Role));
        Meter.CreateObservableGauge("raft.commit.index", () => Observe(node => node.RawStatus.CommitIndex));
        Meter.CreateObservableGauge("raft.applied.index", () => Observe(node => node.AppliedForMetrics));
        Meter.CreateObservableGauge("raft.proposals.pending", () => Observe(node => (long)node.Metrics.PendingProposals));
        Meter.CreateObservableGauge("raft.reads.pending", () => Observe(node => (long)node.PendingReadsForMetrics));
        Meter.CreateObservableGauge("raft.uncommitted.bytes", () => Observe(node => node.RawStatus.UncommittedBytes), "By");
        Meter.CreateObservableGauge("raft.snapshot.index", () => Observe(node => node.RawStatus.Snapshot?.LastIncludedIndex ?? 0));
        Meter.CreateObservableGauge("raft.snapshot.installing.index", () => Observe(node => node.RawStatus.InstallingSnapshotIndex ?? 0));
        Meter.CreateObservableGauge("raft.snapshot.receive.offset", () => Observe(node => node.ReceivedSnapshotOffsetForMetrics), "By");
        Meter.CreateObservableGauge("raft.replication.lag", () => ObservePeers(offset: false));
        Meter.CreateObservableGauge("raft.snapshot.send.offset", () => ObservePeers(offset: true), "By");
        Meter.CreateObservableGauge("raft.diagnostics.callback.failures", () => CallbackFailures);
    }

    internal static long Register(RaftNode node)
    {
        var id = Interlocked.Increment(ref _registration);
        Nodes[id] = new(node);
        return id;
    }
    internal static void Unregister(long id) => Nodes.TryRemove(id, out _);
    private static IEnumerable<Measurement<long>> Observe(Func<RaftNode, long> value)
    {
        foreach (var pair in Nodes)
            if (pair.Value.TryGetTarget(out var node)) yield return new(value(node), node.MetricTags);
            else Nodes.TryRemove(pair.Key, out _);
    }
    private static IEnumerable<Measurement<long>> ObservePeers(bool offset)
    {
        foreach (var pair in Nodes)
        {
            if (!pair.Value.TryGetTarget(out var node)) { Nodes.TryRemove(pair.Key, out _); continue; }
            var status = node.RawStatus;
            if (status.Role != RaftRole.Leader) continue;
            foreach (var peer in status.Peers)
                yield return new(offset ? peer.SnapshotOffset : Math.Max(0, status.LastLogIndex - peer.MatchIndex),
                    new KeyValuePair<string, object?>[] { new("raft.group", node.GroupForMetrics), new("raft.node", status.NodeId), new("raft.peer", peer.NodeId) });
        }
    }

    internal static ILogger Logger(ILoggerFactory? factory)
    {
        try { return factory?.CreateLogger("Raft.RaftNode") ?? NullLogger.Instance; }
        catch { Interlocked.Increment(ref _callbackFailures); return NullLogger.Instance; }
    }
    internal static bool Enabled(ILogger logger, LogLevel level)
    {
        try { return logger.IsEnabled(level); }
        catch { Interlocked.Increment(ref _callbackFailures); return false; }
    }
    internal static void RpcLog(ILogger logger, string group, string node, string peer, Exception error)
    {
        try { RaftLog.RpcFailure(logger, group, node, peer, error); }
        catch { Interlocked.Increment(ref _callbackFailures); }
    }
    internal static long Start(Histogram<double> instrument) => instrument.Enabled ? Stopwatch.GetTimestamp() : 0;
    internal static void Duration(Histogram<double> instrument, long started, string group, string node, string outcome = "success")
    {
        if (started == 0) return;
        try
        {
            TagList tags = new() { { "raft.group", group }, { "raft.node", node }, { "raft.outcome", outcome } };
            instrument.Record(Stopwatch.GetElapsedTime(started).TotalSeconds, tags);
        }
        catch { Interlocked.Increment(ref _callbackFailures); }
    }
    internal static void Election(string group, string node, bool preVote) => Add(preVote ? PreVotes : Elections, 1, group, node);
    internal static void Failed(string group, string node) => Add(Failures, 1, group, node);
    internal static void RpcFailed(string group, string node, string peer) => Add(RpcFailures, 1, group, node, peer);
    internal static void SnapshotBytes(string group, string node, string peer, int bytes, bool send) => Add(send ? SnapshotSent : SnapshotReceived, bytes, group, node, peer);
    private static void Add(Counter<long> instrument, long count, string group, string node, string? peer = null)
    {
        if (!instrument.Enabled) return;
        try
        {
            TagList tags = new() { { "raft.group", group }, { "raft.node", node } };
            if (peer is not null) tags.Add("raft.peer", peer);
            instrument.Add(count, tags);
        }
        catch { Interlocked.Increment(ref _callbackFailures); }
    }
    internal static Activity? Proposal(string group, string node, int bytes)
    {
        var previous = Activity.Current;
        try
        {
            var activity = Activities.StartActivity("raft.propose", ActivityKind.Internal);
            activity?.SetTag("raft.group", group); activity?.SetTag("raft.node", node); activity?.SetTag("raft.command.bytes", bytes);
            return activity;
        }
        catch { Activity.Current = previous; Interlocked.Increment(ref _callbackFailures); return null; }
    }
    internal static void Stop(Activity? activity)
    {
        try { activity?.Dispose(); }
        catch { Interlocked.Increment(ref _callbackFailures); }
        finally { if (activity is not null && ReferenceEquals(Activity.Current, activity)) Activity.Current = activity.Parent; }
    }
    internal static void Log(Action action)
    {
        try { action(); }
        catch { Interlocked.Increment(ref _callbackFailures); }
    }
}

internal static partial class RaftLog
{
    [LoggerMessage(1000, LogLevel.Information, "Raft {GroupId}/{NodeId} is {Role} in term {Term}; leader {LeaderId}")]
    internal static partial void Role(ILogger logger, string groupId, string nodeId, RaftRole role, long term, string? leaderId);
    [LoggerMessage(1001, LogLevel.Information, "Raft {GroupId}/{NodeId} configuration at {Index}: voters {Voters}; learners {Learners}")]
    internal static partial void Configuration(ILogger logger, string groupId, string nodeId, long index, string voters, string learners);
    [LoggerMessage(1002, LogLevel.Debug, "Raft {GroupId}/{NodeId} RPC to {PeerId} failed")]
    internal static partial void RpcFailure(ILogger logger, string groupId, string nodeId, string peerId, Exception exception);
    [LoggerMessage(1003, LogLevel.Information, "Raft {GroupId}/{NodeId} published snapshot {SnapshotId} at {Index}/{Term}")]
    internal static partial void Snapshot(ILogger logger, string groupId, string nodeId, string snapshotId, long index, long term);
    [LoggerMessage(1100, LogLevel.Error, "Raft {GroupId}/{NodeId} stopped after a persistence/application/worker failure")]
    internal static partial void Fault(ILogger logger, string groupId, string nodeId, Exception exception);
}
