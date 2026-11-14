using System.Collections.Immutable;
using Raft.Storage;

namespace Raft.Core;

internal static partial class RaftCore
{
    private sealed partial class Reducer
    {
        private void StartMembership(long client, MembershipOperationStatus request)
        {
            if (State.Membership is null || State.Options.WireVersion != 3)
            { MembershipError(client, "Argument", "Hot membership requires wire3/raft8-v4."); return; }
            if (State.Role != RaftRole.Leader)
            { MembershipError(client, "NotLeader", "Membership requests require the current leader."); return; }
            Read(new LogQuery.MembershipOperation(request.OperationId, IncludePending: true), new CoreAction.MembershipLookup(client, request));
        }

        private void MembershipLookup(CoreAction.MembershipLookup lookup, MembershipOperationStatus? existing)
        {
            var request = lookup.Request;
            if (existing is not null)
            {
                if (!MembershipCodec.SameRequest(existing, request))
                    MembershipError(lookup.ClientId, "MembershipConflict", "Operation ID belongs to another request.", request.OperationId);
                else Effects.Add(new CoreEffect.Control(new ControlResult.Membership(lookup.ClientId, existing)));
                return;
            }
            var view = State.Membership!;
            var busy = view.Committed.ActiveOperation?.OperationId ?? view.PendingOperationId;
            if (busy is { } active)
            { MembershipError(lookup.ClientId, "MembershipBusy", "Another membership operation is active.", active); return; }
            if (!State.HasCommittedCurrentTerm || State.Configuration.Index > Commit || State.Transfer is not null)
            { MembershipError(lookup.ClientId, "Busy", "Wait for current-term/configuration commitment and transfer completion."); return; }
            var present = view.Committed.Peers.FirstOrDefault(peer => peer.NodeId == request.NodeId);
            var config = State.Configuration;
            if (request.Kind is MembershipOperationKind.AddNode or MembershipOperationKind.AddLearner)
            {
                if ((present is not null && (present.IsRetired || present != request.Peer)) || config.Contains(request.NodeId))
                { MembershipError(lookup.ClientId, "Argument", "Use a new peer identity; existing/retired IDs cannot be added again."); return; }
                if (present is null && view.Committed.Peers.Length >= 1024)
                { MembershipError(lookup.ClientId, "Argument", "The peer directory has reached its identity bound."); return; }
                if (new ClusterConfiguration(config.Voters.Add(request.NodeId), config.Learners).Encode().Length > State.Options.ConfigurationByteLimit)
                { MembershipError(lookup.ClientId, "Argument", "The added peer would exceed configuration byte limits."); return; }
            }
            else if (!config.Contains(request.NodeId))
            { MembershipError(lookup.ClientId, "Argument", "The target is not a current member."); return; }
            if (request.Kind == MembershipOperationKind.Promote && !config.Learners.Contains(request.NodeId, StringComparer.Ordinal))
            { MembershipError(lookup.ClientId, "Argument", "Only a learner can be promoted."); return; }
            if (request.Kind == MembershipOperationKind.RemoveNode && config.IsVoter(request.NodeId) && config.Voters.Length - 1 < State.Options.MinimumVoters)
            { MembershipError(lookup.ClientId, "Argument", "Removal would violate MinimumVoters."); return; }
            var accepted = request with { Index = checked(State.LastIndex + 1), Term = Term, Stage = MembershipOperationStage.Accepted };
            if (AppendMembership(new(MembershipRecordKind.Start, accepted)))
                Push(new CoreAction.MembershipAccepted(lookup.ClientId, accepted));
            else MembershipError(lookup.ClientId, "Busy", "The uncommitted byte budget is full.");
        }

        private void CancelMembership(long client, Guid operationId)
        {
            if (State.Membership is null)
            { MembershipError(client, "Argument", "Cancellation requires wire3/raft8-v4."); return; }
            if (State.Role != RaftRole.Leader)
            { MembershipError(client, "NotLeader", "Cancellation requires the current leader."); return; }
            if (!State.HasCommittedCurrentTerm)
            { MembershipError(client, "Busy", "Wait for a committed current-term entry."); return; }
            Read(new LogQuery.MembershipOperation(operationId, IncludePending: true), new CoreAction.CancelMembershipLookup(client, operationId));
        }

        private void CancelMembershipLookup(CoreAction.CancelMembershipLookup cancel, MembershipOperationStatus? status)
        {
            if (status is null)
            { MembershipError(cancel.ClientId, "Argument", "Unknown membership operation."); return; }
            if (status.IsTerminal)
            {
                if (status.Stage == MembershipOperationStage.Cancelled)
                    Effects.Add(new CoreEffect.Control(new ControlResult.Membership(cancel.ClientId, status)));
                else MembershipError(cancel.ClientId, "MembershipTooLate", "The operation is already terminal.", status.OperationId, status);
                return;
            }
            if (State.Membership!.PendingIndex > Commit && State.Membership.PendingIndex != long.MaxValue)
            { MembershipError(cancel.ClientId, "Busy", "Wait for the pending membership record to commit."); return; }
            if (status.Kind is not MembershipOperationKind.AddNode and not MembershipOperationKind.AddLearner ||
                State.Configuration.IsVoter(status.NodeId))
            { MembershipError(cancel.ClientId, "MembershipTooLate", "The final configuration has started, or the operation is not an Add.", status.OperationId); return; }
            if (status.Stage == MembershipOperationStage.Cancelling)
            { Effects.Add(new CoreEffect.Control(new ControlResult.Membership(cancel.ClientId, status))); return; }
            var cancelling = status with { Stage = MembershipOperationStage.Cancelling, Index = State.LastIndex + 1, Term = Term };
            if (AppendMembership(new(MembershipRecordKind.Cancel, cancelling)))
                Push(new CoreAction.MembershipAccepted(cancel.ClientId, cancelling));
            else MembershipError(cancel.ClientId, "Busy", "The uncommitted byte budget is full.");
        }

        private void CheckMembership()
        {
            if (State.Membership?.Committed.ActiveOperation is not { } operation || State.Role != RaftRole.Leader ||
                !State.HasCommittedCurrentTerm || State.Configuration.Index > Commit ||
                State.Membership.PendingIndex != long.MaxValue) return;
            var config = State.Configuration;
            if (operation.Stage == MembershipOperationStage.Cancelling)
            {
                if (config.IsVoter(operation.NodeId))
                    throw new InvalidOperationException("Cancellation cannot roll back a voter promotion.");
                if (config.Learners.Contains(operation.NodeId, StringComparer.Ordinal))
                    MembershipConfiguration(operation, config.Voters, config.Learners.Remove(operation.NodeId));
                else FinishMembership(operation, MembershipOperationStage.Cancelled);
                return;
            }
            switch (operation.Kind)
            {
                case MembershipOperationKind.AddNode:
                case MembershipOperationKind.AddLearner:
                    if (!config.Contains(operation.NodeId))
                    { MembershipConfiguration(operation, config.Voters, config.Learners.Add(operation.NodeId)); return; }
                    if (config.IsVoter(operation.NodeId) || operation.Kind == MembershipOperationKind.AddLearner)
                    { FinishMembership(operation, MembershipOperationStage.Completed); return; }
                    if (operation.Stage == MembershipOperationStage.Accepted)
                    { ProgressMembership(operation, MembershipOperationStage.CatchingUp); return; }
                    var peer = FindPeer(operation.NodeId);
                    if (peer < 0 || State.LastIndex - State.Peers[peer].MatchIndex > State.Options.MaxLearnerPromotionLag) return;
                    if (operation.Stage != MembershipOperationStage.Promoting)
                    { ProgressMembership(operation, MembershipOperationStage.Promoting); return; }
                    MembershipConfiguration(operation, config.Voters.Add(operation.NodeId), config.Learners.Remove(operation.NodeId));
                    return;
                case MembershipOperationKind.Promote:
                    if (config.IsVoter(operation.NodeId))
                    { FinishMembership(operation, MembershipOperationStage.Completed); return; }
                    var promote = FindPeer(operation.NodeId);
                    if (promote < 0 || State.LastIndex - State.Peers[promote].MatchIndex > State.Options.MaxLearnerPromotionLag) return;
                    if (operation.Stage != MembershipOperationStage.Promoting)
                    { ProgressMembership(operation, MembershipOperationStage.Promoting); return; }
                    MembershipConfiguration(operation, config.Voters.Add(operation.NodeId), config.Learners.Remove(operation.NodeId));
                    return;
                case MembershipOperationKind.RemoveNode:
                    if (!config.Contains(operation.NodeId))
                    { FinishMembership(operation, MembershipOperationStage.Completed); return; }
                    if (operation.NodeId == State.Options.NodeId)
                    {
                        if (State.MembershipTransferFailure is { } failed)
                        { FinishMembership(operation, MembershipOperationStage.Failed, failed.Message); return; }
                        if (State.Transfer is not null) return;
                        if (operation.Stage == MembershipOperationStage.Transferring && operation.Term == Term && operation.SuccessorId is { } target)
                        { BeginTransfer(0, target); return; }
                        var successor = State.Peers.Where(value => config.IsVoter(value.Id) && value.RecentActive)
                            .OrderByDescending(value => value.MatchIndex).ThenBy(value => value.Id, StringComparer.Ordinal).FirstOrDefault();
                        if (successor is null) return; // let heartbeat/quorum processing establish an eligible target
                        ProgressMembership(operation, MembershipOperationStage.Transferring, successor.Id);
                        return;
                    }
                    if (operation.Stage != MembershipOperationStage.Removing)
                    { ProgressMembership(operation, MembershipOperationStage.Removing); return; }
                    MembershipConfiguration(operation, config.Voters.Remove(operation.NodeId), config.Learners.Remove(operation.NodeId));
                    return;
            }
        }

        private void MembershipConfiguration(MembershipOperationStatus operation, ImmutableArray<string> voters, ImmutableArray<string> learners)
        {
            var size = new ClusterConfiguration(voters, learners).Encode().Length + LogEntry.EncodedOverhead;
            if (State.UncommittedBytes <= State.Options.MaxUncommittedBytes - size)
                ChangeConfiguration(new CoreAction.ChangeConfiguration(0, voters, learners) { MembershipOperationId = operation.OperationId });
        }

        private void ProgressMembership(MembershipOperationStatus operation, MembershipOperationStage stage, string? successor = null) =>
            AppendMembership(new(MembershipRecordKind.Progress, operation with { Stage = stage, SuccessorId = successor, Index = State.LastIndex + 1, Term = Term }));

        private void FinishMembership(MembershipOperationStatus operation, MembershipOperationStage stage, string? error = null) =>
            AppendMembership(new(MembershipRecordKind.Finish, operation with { Stage = stage, Error = error, Index = State.LastIndex + 1, Term = Term }));

        private bool AppendMembership(MembershipRecord record)
        {
            var entry = new LogEntry(checked(State.LastIndex + 1), Term, MembershipCodec.Encode(record), LogEntryKind.Membership);
            if (State.UncommittedBytes > State.Options.MaxUncommittedBytes - entry.Command.Length - LogEntry.EncodedOverhead)
                return false; // normal data backpressure must not fail-stop an autonomous coordinator
            Push(new CoreAction.SendAll());
            Push(new CoreAction.AdvanceCommit());
            Persist(AppendAndSingleVoterCommit([entry]));
            return true;
        }

        private void MembershipError(long client, string type, string message, Guid? operation = null,
            MembershipOperationStatus? status = null) =>
            Effects.Add(new CoreEffect.Control(new ControlResult.Membership(client,
                Status: status ?? (type == "MembershipTooLate" ? State.Membership?.Committed.ActiveOperation : null),
                Error: new(type, message), ActiveOperationId: operation)));

        private static bool ChangesMembership(LogWrite write) => write switch
        {
            LogWrite.ReplaceSuffix or LogWrite.CompleteSnapshotInstall or LogWrite.PublishSnapshot => true,
            LogWrite.Append append => append.Entries.Any(entry => entry.Kind == LogEntryKind.Membership),
            LogWrite.Batch batch => batch.Writes.Any(ChangesMembership),
            _ => false
        };

        private void PublishMembership(MembershipView view)
        {
            view.Committed.Validate();
            State = State with { Membership = view, MembershipTransferFailure = view.Committed.ActiveOperation is null ? null : State.MembershipTransferFailure,
                Options = State.Options with { KnownPeers = State.Options.KnownPeers.Concat(view.Committed.Peers.Select(peer => peer.NodeId))
                    .Distinct(StringComparer.Ordinal).ToImmutableArray() } };
        }
    }
}
