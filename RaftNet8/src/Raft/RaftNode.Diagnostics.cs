using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace Raft;

public sealed partial class RaftNode
{
    private readonly ILogger _logger;
    private readonly long _diagnosticRegistration;
    private readonly KeyValuePair<string, object?>[] _metricTags;
    private long _observedPreVoteRound, _receivedSnapshotOffset;
    internal RaftStatus RawStatus => Volatile.Read(ref _status);
    internal long AppliedForMetrics => Math.Min(Interlocked.Read(ref _lastApplied), RawStatus.CommitIndex);
    internal int PendingReadsForMetrics => _reads.Count;
    internal string GroupForMetrics => _options.GroupId;
    internal long ReceivedSnapshotOffsetForMetrics => Interlocked.Read(ref _receivedSnapshotOffset);
    internal KeyValuePair<string, object?>[] MetricTags => _metricTags;

    private void ObserveTransition()
    {
        var previous = Volatile.Read(ref _status);
        if (previous.Role != _core.Role || previous.Term != _core.Term || previous.LeaderId != _core.LeaderId)
            if (RaftInstrumentation.Enabled(_logger, LogLevel.Information))
                RaftInstrumentation.Log(() => RaftLog.Role(_logger, _options.GroupId, _options.NodeId, _core.Role, _core.Term, _core.LeaderId));
        if (_core.Term > previous.Term && _core.State.HardState.VotedFor == _options.NodeId && _core.Role is RaftRole.Candidate or RaftRole.Leader)
            RaftInstrumentation.Election(_options.GroupId, _options.NodeId, preVote: false);
        if (_core.State.PreVoteRound != _observedPreVoteRound && _core.Role == RaftRole.PreCandidate)
        {
            _observedPreVoteRound = _core.State.PreVoteRound;
            RaftInstrumentation.Election(_options.GroupId, _options.NodeId, preVote: true);
        }
        var config = _core.State.Configuration;
        if ((previous.Configuration?.Index != config.Index || previous.Configuration?.Term != config.Term) &&
            RaftInstrumentation.Enabled(_logger, LogLevel.Information))
            RaftInstrumentation.Log(() => RaftLog.Configuration(_logger, _options.GroupId, _options.NodeId, config.Index,
                string.Join(',', config.Voters), string.Join(',', config.Learners)));
        if (_core.State.Snapshot is { } snapshot && snapshot.StorageId != previous.Snapshot?.StorageId &&
            RaftInstrumentation.Enabled(_logger, LogLevel.Information))
            RaftInstrumentation.Log(() => RaftLog.Snapshot(_logger, _options.GroupId, _options.NodeId, snapshot.StorageId,
                snapshot.LastIncludedIndex, snapshot.LastIncludedTerm));
    }

    private async Task<ProposalResult> SubmitAsync(ReadOnlyMemory<byte> command, bool isNoOp, CancellationToken cancellationToken)
    {
        EnsureRunning();
        if (command.Length > _options.MaxCommandBytes) throw new ArgumentException("Command exceeds MaxCommandBytes.", nameof(command));
        var started = RaftInstrumentation.Start(RaftInstrumentation.ProposalDuration);
        var activity = RaftInstrumentation.Proposal(_options.GroupId, _options.NodeId, command.Length);
        var outcome = "success";
        try
        {
            var result = await EnqueueProposalAsync(command, isNoOp, cancellationToken).ConfigureAwait(false);
            activity?.SetTag("raft.index", result.Index);
            activity?.SetTag("raft.term", result.Term);
            return result;
        }
        catch (OperationCanceledException) { outcome = "canceled"; throw; }
        catch (NotLeaderException) { outcome = "not_leader"; throw; }
        catch (Exception error) { outcome = "error"; activity?.SetStatus(ActivityStatusCode.Error, error.GetType().Name); throw; }
        finally
        {
            activity?.SetTag("raft.outcome", outcome);
            RaftInstrumentation.Stop(activity);
            RaftInstrumentation.Duration(RaftInstrumentation.ProposalDuration, started, _options.GroupId, _options.NodeId, outcome);
        }
    }

    private ValueTask ApplyBatchMeasuredAsync(IBatchStateMachine machine, IReadOnlyList<LogEntry> entries, CancellationToken token) =>
        !RaftInstrumentation.ApplyDuration.Enabled ? machine.ApplyBatchAsync(entries, token) : MeasureBatchAsync(machine, entries, token);
    private async ValueTask MeasureBatchAsync(IBatchStateMachine machine, IReadOnlyList<LogEntry> entries, CancellationToken token)
    {
        var started = RaftInstrumentation.Start(RaftInstrumentation.ApplyDuration);
        var outcome = "success";
        try { await machine.ApplyBatchAsync(entries, token).ConfigureAwait(false); }
        catch { outcome = "error"; throw; }
        finally { RaftInstrumentation.Duration(RaftInstrumentation.ApplyDuration, started, _options.GroupId, _options.NodeId, outcome); }
    }
    private ValueTask ApplyEntryMeasuredAsync(LogEntry entry, CancellationToken token) =>
        !RaftInstrumentation.ApplyDuration.Enabled ? _stateMachine.ApplyAsync(entry, token) : MeasureEntryAsync(entry, token);
    private async ValueTask MeasureEntryAsync(LogEntry entry, CancellationToken token)
    {
        var started = RaftInstrumentation.Start(RaftInstrumentation.ApplyDuration);
        var outcome = "success";
        try { await _stateMachine.ApplyAsync(entry, token).ConfigureAwait(false); }
        catch { outcome = "error"; throw; }
        finally { RaftInstrumentation.Duration(RaftInstrumentation.ApplyDuration, started, _options.GroupId, _options.NodeId, outcome); }
    }
}
