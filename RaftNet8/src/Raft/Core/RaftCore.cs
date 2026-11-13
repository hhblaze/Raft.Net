using System.Collections.Immutable;
using Raft.Storage;

namespace Raft.Core;

/// <summary>
/// Pure Figure 2 reducer. Identical immutable state and events produce identical state/effects.
/// Reads, durable writes, and timeout sampling suspend on correlated completion events.
/// </summary>
internal static class RaftCore
{
    public static RaftState Create(RaftOptions options, RecoveredLogState recovered, int electionDeadline)
    {
        var frozen = options.Freeze(allowMissingSelf: recovered.Configuration is not null);
        if (recovered.PendingSnapshotInstall is not null) throw new InvalidOperationException("Finish the durable snapshot intent before starting consensus.");
        if (electionDeadline < frozen.ElectionMinTicks || electionDeadline >= frozen.ElectionMaxTicks)
            throw new ArgumentOutOfRangeException(nameof(electionDeadline));
        var config = (recovered.Configuration ?? new ClusterConfiguration(
            ImmutableArray.CreateRange(frozen.Members), ImmutableArray.CreateRange(frozen.Learners))).Validate();
        frozen = frozen with { Members = config.Voters, Learners = config.Learners,
            KnownPeers = frozen.KnownPeers.Concat(recovered.KnownMembers).Distinct(StringComparer.Ordinal).ToImmutableArray() };
        return new(frozen, recovered.HardState, recovered.LastIndex, recovered.LastTerm,
            config.AllMembers.Concat(recovered.KnownMembers).Distinct(StringComparer.Ordinal)
                .Where(id => id != frozen.NodeId).Select(id => new PeerProgress(id)).ToImmutableArray(), electionDeadline)
        {
            Configuration = config, SupportsConfiguration = recovered.SupportsConfiguration,
            FirstIndex = recovered.FirstIndex, UncommittedBytes = recovered.UncommittedBytes,
            Snapshot = recovered.Snapshot,
            Role = config.IsVoter(frozen.NodeId) ? RaftRole.Follower :
                config.Contains(frozen.NodeId) ? RaftRole.Learner : RaftRole.Removed
        };
    }

    public static CoreTransition Step(RaftState state, CoreEvent input)
    {
        if (state.Fault is not null) return new(state, ImmutableArray<CoreEffect>.Empty);
        var reducer = new Reducer(state);
        reducer.Accept(input);
        reducer.Run();
        return new(reducer.State, reducer.Effects.ToImmutable());
    }

    // Validation runs before the runtime enters any storage/effect execution path.
    internal static void ValidateRequest(RaftOptions options, string from, RaftRpc request, bool validateMembership = true,
        bool localAppendGroup = false)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (localAppendGroup && (options.WireVersion != 2 || request is not AppendRequest))
            throw new ArgumentException("A local append group must contain AppendEntries.", nameof(request));
        RaftOptions.ValidateId(from, nameof(from));
        if (from == options.NodeId || (validateMembership && !options.Members.Contains(from, StringComparer.Ordinal) &&
            !options.Learners.Contains(from, StringComparer.Ordinal) && !options.KnownPeers.Contains(from, StringComparer.Ordinal)))
            throw new ArgumentException("RPC source is not another configured member.", nameof(from));
        if (request.Term < 1) throw new ArgumentException("RPC term must be positive.", nameof(request));
        if (request is VoteRequest or PreVoteRequest)
        {
            var (lastIndex, lastTerm) = request is VoteRequest vote ? (vote.LastLogIndex, vote.LastLogTerm) :
                (((PreVoteRequest)request).LastLogIndex, ((PreVoteRequest)request).LastLogTerm);
            if (lastIndex < 0 || lastTerm < 0 || lastTerm > request.Term || ((lastIndex == 0) != (lastTerm == 0)) ||
                (request is PreVoteRequest { Round: < 1 }))
                throw new ArgumentException("Invalid candidate log metadata.", nameof(request));
        }
        else if (request is TimeoutNowRequest timeout)
        {
            if (timeout.RequestId < 1) throw new ArgumentException("Invalid transfer request.");
        }
        else if (request is SnapshotChunkRequest chunk)
        {
            var metadata = chunk.Metadata;
            if (chunk.RequestId < 1 || metadata is null || chunk.StorageId != metadata.StorageId ||
                metadata.LastIncludedIndex < 1 || metadata.LastIncludedTerm < 1 || metadata.LastIncludedTerm > chunk.Term ||
                metadata.Digest.Length != 32 || chunk.TotalLength < 1 || chunk.TotalLength > options.MaxSnapshotBytes ||
                chunk.Offset < 0 || chunk.Data.Length > options.SnapshotChunkBytes || chunk.Offset > chunk.TotalLength - chunk.Data.Length ||
                chunk.Final != (chunk.Offset + chunk.Data.Length == chunk.TotalLength) ||
                (chunk.Data.Length == 0 && !chunk.Final) || Crc32C.Compute(chunk.Data.Span) != chunk.Checksum)
                throw new ArgumentException("Invalid snapshot chunk metadata/bounds/checksum.", nameof(request));
            RaftOptions.ValidateId(chunk.StorageId, nameof(request));
            _ = new ClusterConfiguration(metadata.Voters, metadata.Learners, metadata.LastIncludedIndex, metadata.LastIncludedTerm).Validate();
        }
        else if (request is AppendRequest append)
        {
            ArgumentNullException.ThrowIfNull(append.Entries);
            if (localAppendGroup && (append.ReadContext != 0 || append.Entries.Count == 0))
                throw new ArgumentException("A local append group must contain plain log data.", nameof(request));
            if (append.PreviousIndex < 0 || append.PreviousIndex >= long.MaxValue - options.MaxAppendEntries ||
                append.PreviousTerm < 0 || append.PreviousTerm > append.Term || append.LeaderCommit < 0 || append.RequestId < 1 || append.ReadContext < 0 ||
                ((append.PreviousIndex == 0) != (append.PreviousTerm == 0)) || append.Entries.Count > options.MaxAppendEntries)
                throw new ArgumentException("Invalid AppendEntries metadata.", nameof(request));
            var bytes = 0;
            var previousTerm = append.PreviousTerm;
            for (var i = 0; i < append.Entries.Count; i++)
            {
                var entry = append.Entries[i];
                ArgumentNullException.ThrowIfNull(entry);
                bytes = checked(bytes + entry.Command.Length + LogEntry.EncodedOverhead);
                if (entry.Index != append.PreviousIndex + i + 1 || entry.Term < previousTerm || entry.Term > append.Term ||
                    entry.Command.Length > options.MaxCommandBytes ||
                    bytes > (localAppendGroup ? options.ProposalBatchByteLimit : options.ReplicationByteLimit) ||
                    (localAppendGroup && entry.Kind == LogEntryKind.Configuration))
                    throw new ArgumentException("Invalid or oversized log batch.", nameof(request));
                previousTerm = entry.Term;
            }
        }
        else throw new ArgumentException("Expected a request.", nameof(request));
    }

    private sealed class Reducer(RaftState initial)
    {
        public RaftState State { get; private set; } = initial;
        public ImmutableArray<CoreEffect>.Builder Effects { get; } = ImmutableArray.CreateBuilder<CoreEffect>();
        private long Term => State.HardState.Term;
        private long Commit => State.HardState.CommitIndex;
        private int Quorum => State.Configuration.Quorum;
        private bool IsVoter => State.Configuration.IsVoter(State.Options.NodeId);

        public void Accept(CoreEvent input)
        {
            switch (input)
            {
                case CoreEvent.SnapshotChunkRead chunk:
                    if (State.Pending?.Effect is CoreEffect.ReadSnapshotChunk pendingChunk && pendingChunk.Id == chunk.Id)
                    {
                        var continuation = (CoreAction.BuildSnapshotChunk)State.Pending.Continuation!;
                        State = State with { Pending = null };
                        Push(continuation with { Data = chunk.Data });
                    }
                    return;
                case CoreEvent.StorageCompleted complete:
                    if (State.Pending?.Effect is CoreEffect.Persist persist && persist.Id == complete.Id)
                    {
                        var continuation = State.Pending.Continuation;
                        State = State with { Pending = null };
                        CompleteWrite(persist.Write);
                        if (complete.Result is { } durable) State = State with { UncommittedBytes = durable.Number };
                        if (continuation is not null) Push(continuation);
                        // Latest-in-log configuration is authoritative, including rollback after suffix replacement.
                        if (State.SupportsConfiguration && ChangesConfiguration(persist.Write)) Push(new CoreAction.RefreshConfiguration());
                    }
                    else if (State.Pending?.Effect is CoreEffect.ReadLog read && read.Id == complete.Id)
                    {
                        var continuation = State.Pending.Continuation!;
                        State = State with { Pending = null };
                        Push(new CoreAction.ReadCompleted(continuation,
                            complete.Result ?? throw new InvalidOperationException("Read completion needs owned data.")));
                    }
                    return; // late/duplicate completions cannot release another operation
                case CoreEvent.TimeoutSampled sample:
                    if (State.Pending?.Effect is CoreEffect.ResetTimer timer && timer.Id == sample.Id)
                    {
                        if (sample.Ticks < State.Options.ElectionMinTicks || sample.Ticks >= State.Options.ElectionMaxTicks)
                            throw new ArgumentOutOfRangeException(nameof(input), "Election sample is outside the configured range.");
                        State = State with { Pending = null, ElectionElapsed = 0, ElectionDeadline = sample.Ticks };
                    }
                    return;
                case CoreEvent.StorageFailed failure:
                    var pendingId = State.Pending?.Effect switch
                    {
                        CoreEffect.ReadLog read => read.Id,
                        CoreEffect.Persist pendingWrite => pendingWrite.Id,
                        CoreEffect.ReadSnapshotChunk snapshotRead => snapshotRead.Id,
                        _ => 0
                    };
                    if (pendingId == failure.Id && pendingId != 0)
                    {
                        State = State with { Fault = failure.Error, Pending = null, Agenda = ImmutableStack<CoreAction>.Empty };
                        Effects.Add(new CoreEffect.ClearOutbound());
                        Effects.Add(new CoreEffect.Halt(failure.Error));
                    }
                    return;
            }
            if (!State.IsIdle) throw new InvalidOperationException("Complete the pending effect before admitting another event.");
            switch (input)
            {
                case CoreEvent.Tick: Push(new CoreAction.Tick()); break;
                case CoreEvent.Campaign: Push(new CoreAction.Campaign()); break;
                case CoreEvent.Request request:
                    ValidateRequest(State.Options, request.From, request.Message,
                        validateMembership: !(request.Message is AppendRequest && request.From == State.LeaderId),
                        localAppendGroup: request.LocalAppendGroup);
                    State = State with { InboundReadContext = request.Message is AppendRequest readAppend ? readAppend.ReadContext : 0 };
                    var message = request.Message is AppendRequest mutable && mutable.Entries is not ImmutableArray<LogEntry>
                        ? mutable with { Entries = ImmutableArray.CreateRange(mutable.Entries) } : request.Message;
                    Push(message switch
                    {
                        PreVoteRequest preVote => new CoreAction.PreVote(request.From, preVote),
                        TimeoutNowRequest timeout => new CoreAction.TimeoutNow(request.From, timeout),
                        VoteRequest vote => new CoreAction.Vote(request.From, vote),
                        AppendRequest append => new CoreAction.Append(request.From, append),
                        SnapshotChunkRequest snapshot => new CoreAction.SnapshotRequest(request.From, snapshot),
                        _ => throw new ArgumentException("Expected a request.")
                    });
                    // A pre-vote carries a prospective term; receiving it never adopts that term.
                    if (request.Message is not PreVoteRequest && request.Message.Term > Term)
                        Push(new CoreAction.BecomeFollower(request.Message.Term, null));
                    break;
                case CoreEvent.Response response: Push(new CoreAction.Response(response.From, response.Message)); break;
                case CoreEvent.Unreachable unreachable: Push(new CoreAction.Unreachable(unreachable.From, unreachable.Message)); break;
                case CoreEvent.Propose propose: Push(new CoreAction.Propose(propose.Commands)); break;
                case CoreEvent.ReadRequest:
                    Push(new CoreAction.Propose(ImmutableArray.Create(new ProposedCommand(Array.Empty<byte>(), true))));
                    break;
                case CoreEvent.ReadIndex read: Push(new CoreAction.StartRead(read.Clients)); break;
                case CoreEvent.TransferLeadership transfer: Push(new CoreAction.BeginTransfer(transfer.ClientId, transfer.Target)); break;
                case CoreEvent.ConfigChange config:
                    Push(new CoreAction.ChangeConfiguration(config.ClientId, config.Voters, config.Learners));
                    break;
                case CoreEvent.SnapshotReady ready:
                    Push(new CoreAction.BeginSnapshot(ready.From, ready.Term, ready.Metadata));
                    break;
                case CoreEvent.SnapshotCompleted complete:
                    if (State.InstallingSnapshot is { } installing && complete.Id == installing.Id && complete.Metadata.StorageId == installing.Intent.Metadata.StorageId)
                        Persist(new LogWrite.CompleteSnapshotInstall(installing.Intent));
                    break;
                case CoreEvent.SnapshotCreated created:
                    if (State.InstallingSnapshot is not null || created.Metadata.LastIncludedIndex <= (State.Snapshot?.LastIncludedIndex ?? 0))
                        Effects.Add(new CoreEffect.Control(new ControlResult.Snapshot(created.ClientId, created.Metadata, new("Busy", "Snapshot was superseded."))));
                    else
                        Persist(new LogWrite.PublishSnapshot(created.Metadata) { RetainEntries = State.Options.SnapshotRetainEntries },
                            new CoreAction.SnapshotPublished(created.ClientId, created.Metadata));
                    break;
                default: throw new ArgumentException("Unsupported core event.", nameof(input));
            }
        }

        public void Run()
        {
            while (State.Pending is null && !State.Agenda.IsEmpty && State.Fault is null)
            {
                var action = State.Agenda.Peek();
                State = State with { Agenda = State.Agenda.Pop() };
                Execute(action);
            }
        }

        private void Execute(CoreAction action)
        {
            switch (action)
            {
                case CoreAction.Tick: Tick(); break;
                case CoreAction.PreCampaign:
                    if (!IsVoter || State.InstallingSnapshot is not null) break;
                    if (Quorum == 1) { Push(new CoreAction.Campaign()); break; }
                    State = State with { Role = RaftRole.PreCandidate, LeaderId = null,
                        Votes = State.Votes.Clear().Add(State.Options.NodeId), RequestId = checked(State.RequestId + 1),
                        Peers = ClearFlights(State.Peers) };
                    State = State with { PreVoteRound = State.RequestId };
                    Effects.Add(new CoreEffect.ClearOutbound());
                    Push(new CoreAction.SendPreVotes());
                    Push(new CoreAction.ResetElection());
                    break;
                case CoreAction.SendPreVotes:
                    var pre = new PreVoteRequest(checked(Term + 1), State.PreVoteRound, State.LastIndex, State.LastTerm);
                    foreach (var peer in State.Peers)
                        if (State.Configuration.IsVoter(peer.Id)) Effects.Add(new CoreEffect.Send(new(peer.Id, pre)));
                    break;
                case CoreAction.PreVote preVote:
                    var proposal = preVote.Request;
                    var fresh = proposal.LastLogTerm > State.LastTerm ||
                        (proposal.LastLogTerm == State.LastTerm && proposal.LastLogIndex >= State.LastIndex);
                    var allowed = IsVoter && proposal.Term > Term &&
                        State.Role != RaftRole.Leader && State.LeaderContactElapsed >= State.Options.ElectionMinTicks && fresh;
                    Reply(new PreVoteResponse(Term, proposal.Round, proposal.Term, allowed));
                    break;
                case CoreAction.StartRead read: StartRead(read.Clients); break;
                case CoreAction.NextRead: NextRead(); break;
                case CoreAction.BeginTransfer transfer: BeginTransfer(transfer.ClientId, transfer.Target); break;
                case CoreAction.CheckTransfer: CheckTransfer(); break;
                case CoreAction.ActivateSnapshot:
                    if (State.InstallingSnapshot is { } install)
                        Effects.Add(new CoreEffect.ActivateSnapshot(install.Id, install.Intent.Metadata));
                    break;
                case CoreAction.SnapshotRequest snapshot: SnapshotRequest(snapshot.From, snapshot.Request); break;
                case CoreAction.PermitSnapshotChunk snapshot:
                    State = State with { LeaderId = snapshot.From, LeaderContactElapsed = 0 };
                    Push(new CoreAction.Reply(new SnapshotChunkResponse(Term, snapshot.Request.RequestId, snapshot.Request.StorageId,
                        snapshot.Request.Metadata!.LastIncludedIndex <= Commit ? snapshot.Request.TotalLength : 0, true,
                        snapshot.Request.Metadata.LastIncludedIndex <= Commit)));
                    Push(new CoreAction.ResetElection());
                    break;
                case CoreAction.BeginSnapshot snapshot: BeginSnapshot(snapshot); break;
                case CoreAction.SnapshotPublished published:
                    Effects.Add(new CoreEffect.Control(new ControlResult.Snapshot(published.ClientId, published.Metadata)));
                    break;
                case CoreAction.BuildSnapshotChunk chunk: BuildSnapshotChunk(chunk.Peer, chunk.Data!); break;
                case CoreAction.ChangeConfiguration config: ChangeConfiguration(config); break;
                case CoreAction.RefreshConfiguration:
                    Read(new LogQuery.ConfigurationAt(State.LastIndex), new CoreAction.RefreshConfiguration());
                    break;
                case CoreAction.ConfigurationAccepted config:
                    Effects.Add(new CoreEffect.Control(new ControlResult.Configuration(config.ClientId, config.Index, Term)));
                    break;
                case CoreAction.FinishConfiguration:
                    if (!IsVoter && State.Role == RaftRole.Leader && State.Configuration.Index <= Commit)
                    {
                        // Announce the final commit before retiring; receivers still recognize their prior leader.
                        State = State with { Peers = ClearFlights(State.Peers) };
                        Push(new CoreAction.SetFollower(null, PreserveOutbound: true));
                        Push(new CoreAction.SendAll());
                    }
                    break;
                case CoreAction.TimeoutNow timeout:
                    if (timeout.Request.Term != Term || timeout.From != State.LeaderId || !IsVoter || State.InstallingSnapshot is not null)
                        Reply(new TimeoutNowResponse(Term, timeout.Request.RequestId, false));
                    else
                    {
                        Push(new CoreAction.TimeoutReply(timeout.Request.RequestId));
                        Push(new CoreAction.Campaign()); // explicit transfer bypasses pre-vote
                    }
                    break;
                case CoreAction.TimeoutReply timeout: Reply(new TimeoutNowResponse(Term, timeout.RequestId, true)); break;
                case CoreAction.Campaign:
                    if (!IsVoter || State.InstallingSnapshot is not null) break;
                    Persist(new LogWrite.HardStateUpdate(new(checked(Term + 1), State.Options.NodeId, Commit)),
                        new CoreAction.FinishCampaign());
                    break;
                case CoreAction.FinishCampaign:
                    State = State with { Role = RaftRole.Candidate, LeaderId = null,
                        Votes = State.Votes.Clear().Add(State.Options.NodeId), Peers = ClearFlights(State.Peers),
                        HasCommittedCurrentTerm = false };
                    Effects.Add(new CoreEffect.ClearOutbound());
                    Push(Quorum == 1 ? new CoreAction.BecomeLeader() : new CoreAction.SendVotes());
                    Push(new CoreAction.ResetElection());
                    break;
                case CoreAction.BecomeLeader: BecomeLeader(); break;
                case CoreAction.BecomeFollower follower:
                    if (follower.Term > Term)
                        Persist(new LogWrite.HardStateUpdate(new(follower.Term, null, Commit)),
                            new CoreAction.SetFollower(follower.Leader));
                    else Push(new CoreAction.SetFollower(follower.Leader));
                    break;
                case CoreAction.SetFollower follower:
                    RejectReads(new("NotLeader", "Leadership was lost."));
                    State = State with { Role = IsVoter ? RaftRole.Follower :
                        State.Configuration.Contains(State.Options.NodeId) ? RaftRole.Learner : RaftRole.Removed, LeaderId = follower.Leader,
                        Votes = State.Votes.Clear(), Peers = ClearFlights(State.Peers), HasCommittedCurrentTerm = false };
                    if (!follower.PreserveOutbound) Effects.Add(new CoreEffect.ClearOutbound());
                    Push(new CoreAction.ResetElection());
                    break;
                case CoreAction.ResetElection:
                    Wait(new CoreEffect.ResetTimer(NextId()));
                    break;
                case CoreAction.SendVotes:
                    var voteRequest = new VoteRequest(Term, State.LastIndex, State.LastTerm);
                    foreach (var peer in State.Peers)
                        if (State.Configuration.IsVoter(peer.Id)) Effects.Add(new CoreEffect.Send(new(peer.Id, voteRequest)));
                    break;
                case CoreAction.Vote request: Vote(request.From, request.Request); break;
                case CoreAction.FinishVote vote:
                    State = State with { LeaderId = null };
                    Push(new CoreAction.Reply(vote.Message));
                    Push(new CoreAction.ResetElection());
                    break;
                case CoreAction.Append append:
                    if (append.Request.Term < Term) Reply(new AppendResponse(Term, append.Request.RequestId, false, 0, checked(State.LastIndex + 1)));
                    else
                    {
                        Push(new CoreAction.BeginAppend(append.From, append.Request));
                        if (State.Role != RaftRole.Follower) Push(new CoreAction.SetFollower(append.From));
                    }
                    break;
                case CoreAction.BeginAppend append:
                    State = State with { LeaderId = append.From, LeaderContactElapsed = 0 };
                    if (State.Transfer is { } priorTransfer && Term > priorTransfer.Term)
                        FinishTransfer(append.From == priorTransfer.Target ? null : new("NotLeader", "Another server became leader."));
                    Push(new CoreAction.CheckPrevious(append.Request));
                    Push(new CoreAction.ResetElection());
                    break;
                case CoreAction.CheckPrevious previous: CheckPrevious(previous.Request); break;
                case CoreAction.CommitAppend append:
                    var matched = checked(append.Request.PreviousIndex + append.Request.Entries.Count);
                    Push(new CoreAction.Reply(new AppendResponse(Term, append.Request.RequestId, true, matched)));
                    var commit = Math.Min(append.Request.LeaderCommit, matched);
                    if (commit > Commit) Persist(new LogWrite.HardStateUpdate(State.HardState with { CommitIndex = commit }));
                    break;
                case CoreAction.Reply reply: Reply(reply.Message); break;
                case CoreAction.Response response: Response(response.From, response.Message); break;
                case CoreAction.AfterSuccess success:
                    CheckReadQuorum();
                    Push(new CoreAction.CheckTransfer());
                    if (Commit != success.OldCommit)
                    {
                        var coalesceCommit = State.Options.CoalesceCommitNotifications && State.Options.WireVersion == 2 &&
                            State.SupportsConfiguration && State.Configuration.Index <= success.OldCommit &&
                            State.Transfer is null && State.ReadRound is null && State.PendingReadCount == 0;
                        Push(new CoreAction.SendAll(OnlyIfNecessary: coalesceCommit));
                    }
                    else if (State.Peers[success.Peer].NextIndex <= State.LastIndex || NeedsReadContext(State.Peers[success.Peer]))
                        Push(new CoreAction.SendPeer(success.Peer));
                    break;
                case CoreAction.AdvanceCommit: AdvanceCommit(); break;
                case CoreAction.SendAll all:
                    for (var i = State.Peers.Length - 1; i >= 0; i--)
                        if (!all.OnlyIfNecessary || State.Peers[i].NextIndex <= State.LastIndex || NeedsReadContext(State.Peers[i]) ||
                            State.Peers[i].Mode == ReplicationMode.Snapshot || !State.Configuration.Contains(State.Peers[i].Id))
                            Push(new CoreAction.SendPeer(i));
                    break;
                case CoreAction.SendPeer send:
                    if (State.Role != RaftRole.Leader) break;
                    var progress = State.Peers[send.Peer];
                    if (progress.RetirementAcknowledged && !State.Configuration.Contains(progress.Id)) break;
                    if (progress.Mode == ReplicationMode.Snapshot ||
                        (progress.NextIndex - 1 < State.FirstIndex && progress.NextIndex - 1 != 0 && progress.NextIndex - 1 != State.Snapshot?.LastIncludedIndex) ||
                        (progress.NextIndex == 1 && State.FirstIndex > 1))
                    {
                        SendSnapshot(send.Peer);
                        break;
                    }
                    var window = progress.Mode == ReplicationMode.Probe || !State.Options.EnablePipelining ? 1 : State.Options.MaxInFlightMessages;
                    if (progress.Flights.Length >= window ||
                        State.Options.MaxInFlightBytes - progress.InFlightBytes < State.Options.MaxCommandBytes + LogEntry.EncodedOverhead + 64L ||
                        (progress.NextIndex > State.LastIndex && !progress.Flights.IsEmpty && !NeedsReadContext(progress))) break;
                    Read(new LogQuery.Replication(progress.NextIndex - 1, State.Options.MaxAppendEntries,
                        (int)Math.Min(State.Options.ReplicationByteLimit, State.Options.MaxInFlightBytes - progress.InFlightBytes - 64)),
                        new CoreAction.BuildAppend(send.Peer));
                    break;
                case CoreAction.Propose propose: Propose(propose.Commands); break;
                case CoreAction.Proposed proposed: Effects.Add(new CoreEffect.Proposed(proposed.FirstIndex)); break;
                case CoreAction.Unreachable unreachable:
                    var peerIndex = FindPeer(unreachable.From);
                    if (peerIndex >= 0 && unreachable.Request is AppendRequest failed &&
                        State.Peers[peerIndex].Flights.Any(flight => flight.RequestId == failed.RequestId) && failed.Term == Term)
                    {
                        var failedPeer = State.Peers[peerIndex];
                        SetPeer(peerIndex, failedPeer with { Flights = ImmutableArray<AppendRequest>.Empty, InFlightBytes = 0,
                            Mode = failedPeer.Mode == ReplicationMode.Snapshot ? ReplicationMode.Snapshot : ReplicationMode.Probe,
                            NextIndex = failedPeer.Mode == ReplicationMode.Snapshot ? failedPeer.NextIndex : failedPeer.MatchIndex + 1 });
                    }
                    if (State.Transfer is { } retryTransfer && unreachable.From == retryTransfer.Target &&
                        unreachable.Request is TimeoutNowRequest timeoutNow && timeoutNow.RequestId == retryTransfer.RequestId)
                        State = State with { Transfer = retryTransfer with { Sent = false } };
                    if (peerIndex >= 0 && unreachable.Request is SnapshotChunkRequest failedChunk && failedChunk.Term == Term &&
                        State.Peers[peerIndex].SnapshotInFlight?.RequestId == failedChunk.RequestId)
                        SetPeer(peerIndex, State.Peers[peerIndex] with { SnapshotInFlight = null });
                    break;
                case CoreAction.ReadCompleted read: ReadCompleted(read.Continuation, read.Result); break;
                default: throw new InvalidOperationException("Unknown continuation.");
            }
        }

        private void Tick()
        {
            for (var i = 0; i < State.Peers.Length; i++)
                if (State.Peers[i].Mode == ReplicationMode.Snapshot)
                    SetPeer(i, State.Peers[i] with { SnapshotHeartbeatElapsed = Math.Min(State.Options.HeartbeatTicks,
                        State.Peers[i].SnapshotHeartbeatElapsed + 1) });
            if (State.Transfer is { } transfer)
            {
                transfer = transfer with { Elapsed = transfer.Elapsed + 1 };
                State = State with { Transfer = transfer };
                if (transfer.Elapsed >= State.Options.ElectionMaxTicks) FinishTransfer(new("Timeout", "Leadership transfer timed out."));
                else if (State.Role == RaftRole.Leader) Push(new CoreAction.CheckTransfer());
            }
            if (State.ReadRound is { } read)
            {
                State = State with { ReadRound = read with { Elapsed = read.Elapsed + 1 } };
                if (State.ReadRound.Elapsed >= State.Options.ElectionMaxTicks) RejectReads(new("Timeout", "Read quorum timed out."));
            }
            if (State.LeaderContactElapsed < int.MaxValue) State = State with { LeaderContactElapsed = State.LeaderContactElapsed + 1 };
            if (State.Role != RaftRole.Leader)
            {
                if (!IsVoter || State.InstallingSnapshot is not null) return;
                State = State with { ElectionElapsed = State.ElectionElapsed + 1 };
                if (State.ElectionElapsed >= State.ElectionDeadline)
                    Push(State.Options.EnablePreVote ? new CoreAction.PreCampaign() : new CoreAction.Campaign());
                return;
            }
            State = State with { QuorumElapsed = State.QuorumElapsed + 1 };
            if (State.QuorumElapsed >= State.Options.ElectionMaxTicks)
            {
                var active = IsVoter ? 1 : 0;
                var peers = State.Peers.ToBuilder();
                for (var i = 0; i < peers.Count; i++)
                {
                    if (peers[i].RecentActive && State.Configuration.IsVoter(peers[i].Id)) active++;
                    peers[i] = peers[i] with { RecentActive = false };
                }
                State = State with { QuorumElapsed = 0, Peers = peers.ToImmutable() };
                if (active < Quorum) { Push(new CoreAction.SetFollower(null)); return; }
            }
            State = State with { HeartbeatElapsed = State.HeartbeatElapsed + 1 };
            if (State.HeartbeatElapsed >= State.Options.HeartbeatTicks)
            {
                State = State with { HeartbeatElapsed = 0 };
                Push(new CoreAction.SendAll());
            }
        }

        private void BecomeLeader()
        {
            var peers = State.Peers.ToBuilder();
            for (var i = 0; i < peers.Count; i++)
                peers[i] = peers[i] with { NextIndex = checked(State.LastIndex + 1), MatchIndex = 0,
                    Flights = ImmutableArray<AppendRequest>.Empty, InFlightBytes = 0, Mode = ReplicationMode.Probe,
                    RecentActive = false, RetirementAcknowledged = false, SendingSnapshot = null, SnapshotInFlight = null, SnapshotOffset = 0 };
            State = State with { Role = RaftRole.Leader, LeaderId = State.Options.NodeId,
                HeartbeatElapsed = 0, QuorumElapsed = 0, Peers = peers.ToImmutable(), HasCommittedCurrentTerm = false };
            Push(new CoreAction.SendAll());
            Push(new CoreAction.AdvanceCommit());
            // Current-term no-op establishes safe commitment of inherited entries (§5.4.2).
            var index = checked(State.LastIndex + 1);
            Persist(AppendAndSingleVoterCommit(ImmutableArray.Create(new LogEntry(index, Term, Array.Empty<byte>(), true))));
        }

        private void Vote(string from, VoteRequest request)
        {
            if (!IsVoter) { Reply(new VoteResponse(Term, false)); return; }
            if (request.Term < Term) { Reply(new VoteResponse(Term, false)); return; }
            var fresh = request.LastLogTerm > State.LastTerm ||
                (request.LastLogTerm == State.LastTerm && request.LastLogIndex >= State.LastIndex);
            var grant = fresh && (State.HardState.VotedFor is null || State.HardState.VotedFor == from);
            if (grant) Persist(new LogWrite.HardStateUpdate(State.HardState with { VotedFor = from }),
                new CoreAction.FinishVote(new(Term, true)));
            else Reply(new VoteResponse(Term, false));
        }

        private void CheckPrevious(AppendRequest request)
        {
            if (State.InstallingSnapshot is not null ||
                (request.PreviousIndex < State.FirstIndex && request.PreviousIndex != 0 && request.PreviousIndex != State.Snapshot?.LastIncludedIndex) ||
                (request.PreviousIndex == 0 && State.FirstIndex > 1))
            {
                Reply(new AppendResponse(Term, request.RequestId, false, 0,
                    State.InstallingSnapshot is null ? checked((State.Snapshot?.LastIncludedIndex ?? 0) + 1) : State.LastIndex + 1));
                return;
            }
            if (request.PreviousIndex > State.LastIndex)
            {
                Reply(new AppendResponse(Term, request.RequestId, false, 0, checked(State.LastIndex + 1)));
                return;
            }
            if (request.PreviousIndex == 0) PreviousTerm(request, 0);
            else if (request.PreviousIndex == State.LastIndex) PreviousTerm(request, State.LastTerm);
            else Read(new LogQuery.Term(request.PreviousIndex), new CoreAction.PreviousTerm(request));
        }

        private void PreviousTerm(AppendRequest request, long term)
        {
            if (term != request.PreviousTerm)
                Read(new LogQuery.FirstIndexOfTerm(request.PreviousIndex), new CoreAction.Conflict(request, term));
            else
            {
                var count = (int)Math.Min(request.Entries.Count, State.LastIndex - request.PreviousIndex);
                if (count == 0) MatchEntries(request, ImmutableArray<long>.Empty);
                else Read(new LogQuery.Terms(request.PreviousIndex + 1, count), new CoreAction.MatchEntries(request));
            }
        }

        private void MatchEntries(AppendRequest request, ImmutableArray<long> terms)
        {
            var expected = (int)Math.Min(request.Entries.Count, State.LastIndex - request.PreviousIndex);
            if (terms.IsDefault || terms.Length != expected) throw new InvalidOperationException("Incomplete log-term read.");
            for (var i = 0; i < request.Entries.Count; i++)
            {
                var entry = request.Entries[i];
                if (i < terms.Length && terms[i] == entry.Term) continue;
                if (entry.Index <= Commit)
                {
                    Reply(new AppendResponse(Term, request.RequestId, false, 0, Commit + 1));
                    return;
                }
                var suffix = ImmutableArray.CreateBuilder<LogEntry>(request.Entries.Count - i);
                for (; i < request.Entries.Count; i++) suffix.Add(request.Entries[i]);
                // A fresh tail extension cannot remove a configuration or invalidate
                // prior entries. Reserve suffix replacement/rollback for a real conflict.
                var entries = suffix.MoveToImmutable();
                LogWrite mutation = entry.Index == State.LastIndex + 1
                    ? new LogWrite.Append(entries)
                    : new LogWrite.ReplaceSuffix(entry.Index, entries);
                var matched = checked(request.PreviousIndex + request.Entries.Count);
                var commit = Math.Min(request.LeaderCommit, matched);
                if (commit > Commit)
                    Persist(new LogWrite.Batch([mutation, new LogWrite.HardStateUpdate(State.HardState with { CommitIndex = commit })]),
                        new CoreAction.Reply(new AppendResponse(Term, request.RequestId, true, matched)));
                else Persist(mutation, new CoreAction.CommitAppend(request));
                return;
            }
            Push(new CoreAction.CommitAppend(request)); // preserve a matching extra local suffix
        }

        private void Response(string from, RaftRpc response)
        {
            var index = FindPeer(from);
            if (index < 0 || response is not VoteResponse and not AppendResponse and not PreVoteResponse and not TimeoutNowResponse and not SnapshotChunkResponse || response.Term < 0) return;
            if (response is PreVoteResponse preVote)
            {
                if (!State.Configuration.IsVoter(from)) return;
                if (preVote.Term > Term) { Push(new CoreAction.BecomeFollower(preVote.Term, null)); return; }
                if (State.Role == RaftRole.PreCandidate && preVote.Round == State.PreVoteRound &&
                    preVote.ProspectiveTerm == Term + 1 && preVote.Granted && !State.Votes.Contains(from))
                {
                    State = State with { Votes = State.Votes.Add(from) };
                    if (State.Votes.Count >= Quorum) Push(new CoreAction.Campaign());
                }
                return;
            }
            if (response.Term > Term) { Push(new CoreAction.BecomeFollower(response.Term, null)); return; }
            if (response.Term != Term) return;
            if (response is SnapshotChunkResponse snapshot)
            {
                SnapshotResponse(index, snapshot);
                return;
            }
            if (response is TimeoutNowResponse timeout)
            {
                // The target increments its term before its vote requests/reply. A delayed negative
                // reply to the previous-term TimeoutNow cannot cancel observation of that election.
                if (State.Role == RaftRole.Leader && State.Transfer is { } current && Term == current.Term &&
                    current.Target == from && current.RequestId == timeout.RequestId && !timeout.Accepted)
                    FinishTransfer(new("Rejected", "Transfer target rejected TimeoutNow."));
                return;
            }
            if (response is VoteResponse vote)
            {
                if (State.Role == RaftRole.Candidate && State.Configuration.IsVoter(from) && vote.Granted && !State.Votes.Contains(from))
                {
                    State = State with { Votes = State.Votes.Add(from) };
                    if (State.Votes.Count >= Quorum) Push(new CoreAction.BecomeLeader());
                }
                return;
            }
            var peer = State.Peers[index];
            if (State.Role != RaftRole.Leader || response is not AppendResponse reply) return;
            var request = peer.Flights.FirstOrDefault(flight => flight.RequestId == reply.RequestId);
            if (request is null) return;
            var through = checked(request.PreviousIndex + request.Entries.Count);
            if (reply.Success && reply.MatchIndex != through) return;
            if (!reply.Success && (reply.ConflictIndex < 1 || reply.ConflictIndex > State.LastIndex + 1 || reply.ConflictTerm < 0)) return;
            peer = peer with { RecentActive = true };
            if (peer.Mode == ReplicationMode.Snapshot)
            {
                peer = peer with { Flights = ImmutableArray<AppendRequest>.Empty, InFlightBytes = 0 };
                if (!reply.Success) { SetPeer(index, peer); return; } // keepalive while streaming
                peer = peer with { Mode = ReplicationMode.Replicate, SendingSnapshot = null, SnapshotInFlight = null };
            }
            if (reply.Success)
            {
                if (State.ReadRound is { } round && request.ReadContext == round.Context && reply.ReadContext == round.Context &&
                    State.Configuration.IsVoter(from))
                    State = State with { ReadRound = round with { Acks = round.Acks.Add(from) } };
                peer = peer with { MatchIndex = Math.Max(peer.MatchIndex, reply.MatchIndex),
                    RetirementAcknowledged = !State.Configuration.Contains(from) && request.LeaderCommit >= State.Configuration.Index &&
                        reply.MatchIndex >= State.Configuration.Index };
                var remaining = peer.Flights.Where(flight => flight.PreviousIndex + flight.Entries.Count > peer.MatchIndex).ToImmutableArray();
                SetPeer(index, peer with { NextIndex = Math.Max(peer.NextIndex, checked(peer.MatchIndex + 1)),
                    Mode = ReplicationMode.Replicate, Flights = remaining, InFlightBytes = remaining.Sum(ReplicationBytes) });
                Push(new CoreAction.AfterSuccess(index, Commit));
                Push(new CoreAction.AdvanceCommit());
            }
            else
            {
                SetPeer(index, peer with { Flights = ImmutableArray<AppendRequest>.Empty, InFlightBytes = 0, Mode = ReplicationMode.Probe });
                if (reply.ConflictTerm == 0) ConflictNext(index, reply, request.PreviousIndex, 0);
                else Read(new LogQuery.LastIndexOfTerm(reply.ConflictTerm), new CoreAction.ConflictNext(index, reply, request.PreviousIndex));
            }
        }

        private void ConflictNext(int index, AppendResponse reply, long rejectedPreviousIndex, long ownTermLast)
        {
            var peer = State.Peers[index];
            var next = reply.ConflictIndex > rejectedPreviousIndex + 1 ? reply.ConflictIndex :
                Math.Min(rejectedPreviousIndex, ownTermLast > 0 ? ownTermLast + 1 : reply.ConflictIndex);
            SetPeer(index, peer with { NextIndex = Math.Max(peer.MatchIndex + 1, Math.Max(1, next)) });
            Push(new CoreAction.SendPeer(index));
        }

        private void SnapshotRequest(string from, SnapshotChunkRequest request)
        {
            if (request.Term < Term || State.Options.SnapshotDirectory is null || !State.SupportsConfiguration)
            {
                Reply(new SnapshotChunkResponse(Term, request.RequestId, request.StorageId, 0, false, false));
                return;
            }
            Push(new CoreAction.PermitSnapshotChunk(from, request));
            if (State.Role is RaftRole.Leader or RaftRole.Candidate or RaftRole.PreCandidate)
                Push(new CoreAction.SetFollower(from));
        }

        private void BeginSnapshot(CoreAction.BeginSnapshot ready)
        {
            if (State.Options.SnapshotDirectory is null || !State.SupportsConfiguration || State.InstallingSnapshot is not null ||
                ready.Term != Term || ready.From != State.LeaderId || ready.Metadata.LastIncludedIndex <= Commit) return;
            if (ready.Metadata.LastIncludedIndex <= State.LastIndex)
                Read(new LogQuery.Term(ready.Metadata.LastIncludedIndex), new CoreAction.SnapshotBoundary(ready.From, ready.Term, ready.Metadata));
            else StartSnapshot(ready.Metadata, retain: false);
        }

        private void StartSnapshot(SnapshotMetadata metadata, bool retain)
        {
            var intent = new SnapshotInstallIntent(metadata, retain);
            State = State with { InstallingSnapshot = new(NextId(), intent) };
            Persist(new LogWrite.BeginSnapshotInstall(intent), new CoreAction.ActivateSnapshot());
        }

        private void SendSnapshot(int index)
        {
            var peer = State.Peers[index];
            if (peer.Mode != ReplicationMode.Snapshot)
            {
                if (State.Snapshot is null) throw new InvalidOperationException("Compacted peer requires a published snapshot.");
                peer = peer with { Mode = ReplicationMode.Snapshot, SendingSnapshot = State.Snapshot, SnapshotOffset = 0,
                    SnapshotInFlight = null, SnapshotHeartbeatElapsed = State.Options.HeartbeatTicks,
                    Flights = ImmutableArray<AppendRequest>.Empty, InFlightBytes = 0 };
            }
            // A throttled/large transfer must not suppress leader contact or CheckQuorum replies.
            if (peer.SnapshotHeartbeatElapsed >= State.Options.HeartbeatTicks && peer.Flights.IsEmpty)
            {
                State = State with { RequestId = checked(State.RequestId + 1) };
                var heartbeat = new AppendRequest(Term, State.RequestId, State.LastIndex, State.LastTerm, Commit, ImmutableArray<LogEntry>.Empty)
                    { ReadContext = State.ReadRound?.Context ?? 0 };
                peer = peer with { Flights = [heartbeat], InFlightBytes = 64, SnapshotHeartbeatElapsed = 0 };
                Effects.Add(new CoreEffect.Send(new(peer.Id, heartbeat)));
            }
            SetPeer(index, peer);
            if (peer.SnapshotInFlight is null)
                Wait(new CoreEffect.ReadSnapshotChunk(NextId(), peer.SendingSnapshot!, peer.SnapshotOffset, State.Options.SnapshotChunkBytes),
                    new CoreAction.BuildSnapshotChunk(index));
        }

        private void BuildSnapshotChunk(int index, SnapshotChunkData data)
        {
            var peer = State.Peers[index];
            if (data.TotalLength < 1 || data.TotalLength > State.Options.MaxSnapshotBytes || peer.SnapshotOffset > data.TotalLength - data.Data.Length)
                throw new InvalidOperationException("Snapshot read returned an invalid chunk.");
            State = State with { RequestId = checked(State.RequestId + 1) };
            var request = new SnapshotChunkRequest(Term, State.RequestId, peer.SendingSnapshot!.StorageId, peer.SnapshotOffset,
                data.TotalLength, Crc32C.Compute(data.Data.Span), peer.SnapshotOffset + data.Data.Length == data.TotalLength,
                peer.SendingSnapshot, data.Data);
            SetPeer(index, peer with { SnapshotInFlight = request });
            Effects.Add(new CoreEffect.Send(new(peer.Id, request)));
        }

        private void SnapshotResponse(int index, SnapshotChunkResponse response)
        {
            var peer = State.Peers[index];
            if (State.Role != RaftRole.Leader || peer.Mode != ReplicationMode.Snapshot || peer.SnapshotInFlight is not { } request ||
                response.RequestId != request.RequestId || response.StorageId != request.StorageId || response.NextOffset < 0 ||
                response.NextOffset > request.TotalLength || (response.Complete && (!response.Success || response.NextOffset != request.TotalLength))) return;
            peer = peer with { RecentActive = true, SnapshotInFlight = null };
            if (response.Complete)
            {
                var match = Math.Max(peer.MatchIndex, peer.SendingSnapshot!.LastIncludedIndex);
                SetPeer(index, peer with { MatchIndex = match, NextIndex = match + 1, Mode = ReplicationMode.Probe,
                    SendingSnapshot = null, SnapshotOffset = 0, Flights = ImmutableArray<AppendRequest>.Empty, InFlightBytes = 0 });
                Push(new CoreAction.AfterSuccess(index, Commit));
                Push(new CoreAction.AdvanceCommit());
            }
            else
            {
                SetPeer(index, peer with { SnapshotOffset = response.NextOffset });
                if (response.Success && response.NextOffset < request.TotalLength && response.NextOffset != request.Offset)
                    Push(new CoreAction.SendPeer(index)); // final verification/rejection polls wait for a tick
            }
        }

        private void StartRead(ImmutableArray<long> clients)
        {
            if (clients.IsDefaultOrEmpty) return;
            CoreFailure? error = State.Role != RaftRole.Leader ? new("NotLeader", "No local read leader.") :
                State.Transfer is not null ? new("Busy", "Leadership transfer is in progress.") :
                State.PendingReadCount + clients.Length > State.Options.MaxPendingReads ? new("Busy", "Read admission limit reached.") : null;
            if (error is not null)
            {
                foreach (var client in clients) Effects.Add(new CoreEffect.Control(new ControlResult.Read(client, 0, Term, error)));
                return;
            }
            State = State with { ReadQueue = State.ReadQueue.Enqueue(clients), PendingReadCount = State.PendingReadCount + clients.Length };
            Push(new CoreAction.NextRead());
        }
        private void NextRead()
        {
            if (State.Role != RaftRole.Leader || State.ReadRound is not null || State.ReadQueue.IsEmpty || !State.HasCommittedCurrentTerm) return;
            var clients = State.ReadQueue.Peek();
            State = State with { ReadQueue = State.ReadQueue.Dequeue(), RequestId = checked(State.RequestId + 1) };
            var acks = ImmutableHashSet.Create<string>(StringComparer.Ordinal);
            if (IsVoter) acks = acks.Add(State.Options.NodeId);
            State = State with { ReadRound = new(State.RequestId, Commit, Term, clients, acks) };
            CheckReadQuorum();
            if (State.ReadRound is not null) Push(new CoreAction.SendAll());
        }
        private void CheckReadQuorum()
        {
            if (State.ReadRound is not { } read || read.Acks.Count < Quorum) return;
            foreach (var client in read.Clients)
                Effects.Add(new CoreEffect.Control(new ControlResult.Read(client, read.Index, read.Term)));
            State = State with { ReadRound = null, PendingReadCount = State.PendingReadCount - read.Clients.Length };
            Push(new CoreAction.NextRead());
        }
        private void RejectReads(CoreFailure error)
        {
            if (State.ReadRound is { } round)
                foreach (var client in round.Clients) Effects.Add(new CoreEffect.Control(new ControlResult.Read(client, 0, Term, error)));
            foreach (var batch in State.ReadQueue)
                foreach (var client in batch) Effects.Add(new CoreEffect.Control(new ControlResult.Read(client, 0, Term, error)));
            State = State with { ReadRound = null, ReadQueue = ImmutableQueue<ImmutableArray<long>>.Empty, PendingReadCount = 0 };
        }
        private void BeginTransfer(long client, string target)
        {
            CoreFailure? error = State.Role != RaftRole.Leader ? new("NotLeader", "Leadership transfer requires a leader.") :
                State.Transfer is not null ? new("Busy", "Another transfer is already in progress.") :
                !State.Configuration.IsVoter(target) ? new("Argument", "The transfer target must be a current voter.") : null;
            if (error is not null) { Effects.Add(new CoreEffect.Control(new ControlResult.Transfer(client, error))); return; }
            if (target == State.Options.NodeId) { Effects.Add(new CoreEffect.Control(new ControlResult.Transfer(client))); return; }
            State = State with { Transfer = new(client, target, Term) };
            RejectReads(new("Busy", "Leadership transfer is in progress."));
            Push(new CoreAction.CheckTransfer());
        }
        private void CheckTransfer()
        {
            if (State.Role != RaftRole.Leader || State.Transfer is not { Sent: false } transfer) return;
            if (Term != transfer.Term) { FinishTransfer(new("NotLeader", "A new election superseded the transfer.")); return; }
            var index = FindPeer(transfer.Target);
            if (index < 0) { FinishTransfer(new("Argument", "Transfer target left the configuration.")); return; }
            if (State.Peers[index].MatchIndex >= State.LastIndex && State.HasCommittedCurrentTerm)
            {
                State = State with { RequestId = checked(State.RequestId + 1) };
                State = State with { Transfer = transfer with { Sent = true, RequestId = State.RequestId } };
                Effects.Add(new CoreEffect.Send(new(transfer.Target, new TimeoutNowRequest(Term, State.RequestId))));
            }
            else Push(new CoreAction.SendPeer(index));
        }
        private void FinishTransfer(CoreFailure? error)
        {
            if (State.Transfer is not { } transfer) return;
            Effects.Add(new CoreEffect.Control(new ControlResult.Transfer(transfer.ClientId, error)));
            State = State with { Transfer = null };
        }

        // Dissertation ch.4, with the 2015 single-server correction: a current-term entry
        // must already be committed before creating a configuration in this term.
        private void ChangeConfiguration(CoreAction.ChangeConfiguration change)
        {
            CoreFailure? error = State.Role != RaftRole.Leader ? new("NotLeader", "Configuration changes require a leader.") :
                !State.SupportsConfiguration || State.Options.WireVersion != 2 ? new("Argument", "Configuration changes require v2 storage and protocol.") :
                State.Transfer is not null ? new("Busy", "Leadership transfer is in progress.") :
                !State.HasCommittedCurrentTerm ? new("Busy", "Commit a current-term entry before changing configuration.") :
                State.Configuration.Index > Commit ? new("Busy", "A configuration change is already uncommitted.") : null;
            ClusterConfiguration? config = null;
            if (error is null)
            {
                try
                {
                    config = new ClusterConfiguration(change.Voters, change.Learners, checked(State.LastIndex + 1), Term).Validate();
                    if (config.Voters.Length < State.Options.MinimumVoters)
                        error = new("Argument", "The change would leave fewer than MinimumVoters.");
                    if (config.AllMembers.Any(id => !State.Options.KnownPeers.Contains(id, StringComparer.Ordinal)))
                        error = new("Argument", "Provision peer IDs on all nodes before adding a learner.");
                    var changed = State.Configuration.AllMembers.Concat(config.AllMembers).Distinct(StringComparer.Ordinal)
                        .Count(id => MemberRole(State.Configuration, id) != MemberRole(config, id));
                    if (changed != 1) error = new("Argument", "Change exactly one server at a time.");
                    foreach (var added in config.Voters.Except(State.Configuration.Voters, StringComparer.Ordinal))
                    {
                        var peer = FindPeer(added);
                        if (!State.Configuration.Learners.Contains(added, StringComparer.Ordinal))
                            error = new("Argument", "Add the server as a learner before promotion.");
                        else if (peer < 0 || State.LastIndex - State.Peers[peer].MatchIndex > State.Options.MaxLearnerPromotionLag)
                            error = new("Busy", "The learner has not caught up sufficiently for promotion.");
                    }
                    if (config.Encode().Length > State.Options.MaxCommandBytes)
                        error = new("Argument", "Configuration exceeds MaxCommandBytes.");
                }
                catch (ArgumentException invalid) { error = new("Argument", invalid.Message); }
            }
            if (error is not null)
            {
                Effects.Add(new CoreEffect.Control(new ControlResult.Configuration(change.ClientId, 0, Term, error)));
                return;
            }
            Push(new CoreAction.ConfigurationAccepted(change.ClientId, config!.Index));
            Push(new CoreAction.SendAll());
            Push(new CoreAction.AdvanceCommit());
            // Never use the old configuration's single-voter shortcut for this entry.
            Persist(new LogWrite.Append([new LogEntry(config.Index, Term, config.Encode(), LogEntryKind.Configuration)]));
        }

        private static int MemberRole(ClusterConfiguration config, string id) =>
            config.IsVoter(id) ? 2 : config.Contains(id) ? 1 : 0;

        private void ActivateConfiguration(ClusterConfiguration config)
        {
            config.Validate();
            if (config.Index == State.Configuration.Index && config.Term == State.Configuration.Term) return;
            if (!ClusterConfiguration.SameMembers(config, State.Configuration))
                RejectReads(new("Busy", "Configuration changed; retry the read with the new quorum."));
            var peers = ImmutableArray.CreateBuilder<PeerProgress>();
            foreach (var id in config.AllMembers)
            {
                if (id == State.Options.NodeId) continue;
                var old = FindPeer(id);
                peers.Add(old >= 0 ? State.Peers[old] with { RecentActive = false, RetirementAcknowledged = false } :
                    new PeerProgress(id, checked(State.LastIndex + 1)));
            }
            foreach (var old in State.Peers)
                if (!config.Contains(old.Id)) peers.Add(old with { RecentActive = false,
                    RetirementAcknowledged = !State.Configuration.Contains(old.Id) && old.RetirementAcknowledged });
            State = State with { Configuration = config, Peers = peers.ToImmutable(), QuorumElapsed = 0,
                Options = State.Options with { Members = config.Voters, Learners = config.Learners,
                    KnownPeers = State.Options.KnownPeers.Concat(config.AllMembers).Distinct(StringComparer.Ordinal).ToImmutableArray() } };
            if (State.Role != RaftRole.Leader)
                State = State with { Role = IsVoter ? RaftRole.Follower : config.Contains(State.Options.NodeId) ? RaftRole.Learner : RaftRole.Removed };
            if (State.Transfer is { } transfer && !config.IsVoter(transfer.Target))
                FinishTransfer(new("Argument", "Transfer target left the voting configuration."));
            Push(new CoreAction.FinishConfiguration());
        }

        private static bool ChangesConfiguration(LogWrite write) => write switch
        {
            LogWrite.ReplaceSuffix => true,
            LogWrite.CompleteSnapshotInstall or LogWrite.PublishSnapshot => true,
            LogWrite.Append append => append.Entries.Any(entry => entry.Kind == LogEntryKind.Configuration),
            LogWrite.Batch batch => batch.Writes.Any(ChangesConfiguration),
            _ => false
        };

        private void AdvanceCommit()
        {
            Span<long> indexes = stackalloc long[State.Configuration.Voters.Length];
            var count = 0;
            if (IsVoter) indexes[count++] = State.LastIndex;
            foreach (var peer in State.Peers)
                if (State.Configuration.IsVoter(peer.Id)) indexes[count++] = peer.MatchIndex;
            if (count != indexes.Length) throw new InvalidOperationException("Voter progress does not match the active configuration.");
            indexes.Sort();
            var candidate = indexes[indexes.Length - Quorum];
            if (candidate <= Commit) return;
            if (candidate == State.LastIndex) CommitTerm(candidate, State.LastTerm);
            else Read(new LogQuery.Term(candidate), new CoreAction.CommitTerm(candidate));
        }

        private void CommitTerm(long candidate, long term)
        {
            if (term == Term) Persist(new LogWrite.HardStateUpdate(State.HardState with { CommitIndex = candidate }));
        }

        private void Propose(ImmutableArray<ProposedCommand> commands)
        {
            if (State.Role != RaftRole.Leader) throw new NotLeaderException(State.LeaderId);
            if (State.Transfer is { } transfer) throw new LeadershipTransferInProgressException(transfer.Target);
            if (commands.Length < 1 || commands.Length > State.Options.MaxAppendEntries) throw new ArgumentOutOfRangeException(nameof(commands));
            var entries = ImmutableArray.CreateBuilder<LogEntry>(commands.Length);
            var bytes = 0;
            var first = checked(State.LastIndex + 1);
            for (var i = 0; i < commands.Length; i++)
            {
                var command = commands[i];
                bytes = checked(bytes + command.Bytes.Length + LogEntry.EncodedOverhead);
                if (command.Bytes.Length > State.Options.MaxCommandBytes || bytes > State.Options.ProposalBatchByteLimit)
                    throw new ArgumentException("Proposal batch exceeds configured limits.", nameof(commands));
                entries.Add(new LogEntry(checked(first + i), Term, command.Bytes, command.IsNoOp));
            }
            if (State.UncommittedBytes > State.Options.MaxUncommittedBytes - bytes)
                throw new UncommittedLogLimitException(State.Options.MaxUncommittedBytes);
            Push(new CoreAction.Proposed(first));
            Push(new CoreAction.SendAll());
            Push(new CoreAction.AdvanceCommit());
            Persist(AppendAndSingleVoterCommit(entries.MoveToImmutable()));
        }

        private LogWrite AppendAndSingleVoterCommit(ImmutableArray<LogEntry> entries)
        {
            var append = new LogWrite.Append(entries);
            return Quorum == 1 && IsVoter
                ? new LogWrite.Batch([append, new LogWrite.HardStateUpdate(State.HardState with { CommitIndex = entries[^1].Index })])
                : append;
        }

        private void ReadCompleted(CoreAction continuation, LogReadResult result)
        {
            switch (continuation)
            {
                case CoreAction.PreviousTerm previous: PreviousTerm(previous.Request, result.Number); break;
                case CoreAction.Conflict conflict:
                    Reply(new AppendResponse(Term, conflict.Request.RequestId, false, 0, result.Number, conflict.Term));
                    break;
                case CoreAction.MatchEntries entries: MatchEntries(entries.Request, result.Terms); break;
                case CoreAction.CommitTerm commit: CommitTerm(commit.Index, result.Number); break;
                case CoreAction.ConflictNext conflict: ConflictNext(conflict.Peer, conflict.Message, conflict.RejectedPreviousIndex, result.Number); break;
                case CoreAction.RefreshConfiguration:
                    ActivateConfiguration(result.Configuration ?? throw new InvalidOperationException("Missing active configuration."));
                    break;
                case CoreAction.SnapshotBoundary snapshot:
                    StartSnapshot(snapshot.Metadata, result.Number == snapshot.Metadata.LastIncludedTerm);
                    break;
                case CoreAction.BuildAppend append:
                    var peer = State.Peers[append.Peer];
                    State = State with { RequestId = checked(State.RequestId + 1) };
                    var request = new AppendRequest(Term, State.RequestId, peer.NextIndex - 1, result.PreviousTerm, Commit, result.Entries);
                    request = request with { ReadContext = State.ReadRound?.Context ?? 0 };
                    SetPeer(append.Peer, peer with { Flights = peer.Flights.Add(request), InFlightBytes = peer.InFlightBytes + ReplicationBytes(request),
                        NextIndex = peer.Mode == ReplicationMode.Replicate ? request.PreviousIndex + request.Entries.Count + 1 : peer.NextIndex });
                    Effects.Add(new CoreEffect.Send(new(peer.Id, request)));
                    if (request.Entries.Count > 0 && peer.Mode == ReplicationMode.Replicate) Push(new CoreAction.SendPeer(append.Peer));
                    break;
                default: throw new InvalidOperationException("Unknown read continuation.");
            }
        }

        private void CompleteWrite(LogWrite write)
        {
            switch (write)
            {
                case LogWrite.HardStateUpdate hard:
                    var oldCommit = Commit;
                    State = State with { HardState = hard.State };
                    if (Commit == State.LastIndex) State = State with { UncommittedBytes = 0 };
                    if (Commit > oldCommit)
                    {
                        Effects.Add(new CoreEffect.Apply(Commit));
                        if (State.Role == RaftRole.Leader)
                        {
                            State = State with { HasCommittedCurrentTerm = true };
                            Push(new CoreAction.NextRead());
                            Push(new CoreAction.CheckTransfer());
                            Push(new CoreAction.FinishConfiguration());
                        }
                    }
                    break;
                case LogWrite.Append append:
                    State = State with { LastIndex = append.Entries[^1].Index, LastTerm = append.Entries[^1].Term,
                        UncommittedBytes = State.UncommittedBytes + append.Entries.Sum(entry => (long)entry.Command.Length + LogEntry.EncodedOverhead) };
                    break;
                case LogWrite.ReplaceSuffix suffix:
                    State = State with { LastIndex = suffix.Entries[^1].Index, LastTerm = suffix.Entries[^1].Term };
                    break;
                case LogWrite.PublishSnapshot publish:
                    State = State with { Snapshot = publish.Metadata,
                        FirstIndex = Math.Max(State.FirstIndex, publish.Metadata.LastIncludedIndex - publish.RetainEntries + 1) };
                    break;
                case LogWrite.CompleteSnapshotInstall complete:
                    var metadata = complete.Intent.Metadata;
                    State = State with { Snapshot = metadata, FirstIndex = metadata.LastIncludedIndex + 1,
                        LastIndex = complete.Intent.RetainSuffix ? State.LastIndex : metadata.LastIncludedIndex,
                        LastTerm = complete.Intent.RetainSuffix ? State.LastTerm : metadata.LastIncludedTerm,
                        HardState = State.HardState with { CommitIndex = metadata.LastIncludedIndex }, InstallingSnapshot = null };
                    Effects.Add(new CoreEffect.Apply(metadata.LastIncludedIndex));
                    break;
                case LogWrite.Batch batch:
                    foreach (var item in batch.Writes) CompleteWrite(item);
                    break;
            }
        }

        private int FindPeer(string id)
        {
            for (var i = 0; i < State.Peers.Length; i++) if (State.Peers[i].Id == id) return i;
            return -1;
        }
        private static ImmutableArray<PeerProgress> ClearFlights(ImmutableArray<PeerProgress> peers)
        {
            var builder = peers.ToBuilder();
            for (var i = 0; i < builder.Count; i++) builder[i] = builder[i] with {
                Flights = ImmutableArray<AppendRequest>.Empty, InFlightBytes = 0, Mode = ReplicationMode.Probe,
                SendingSnapshot = null, SnapshotInFlight = null, SnapshotOffset = 0 };
            return builder.ToImmutable();
        }
        private bool NeedsReadContext(PeerProgress peer) => State.ReadRound is { } round && !round.Acks.Contains(peer.Id) &&
            !peer.Flights.Any(flight => flight.ReadContext == round.Context);
        private static long ReplicationBytes(AppendRequest request) => 64L +
            request.Entries.Sum(entry => (long)entry.Command.Length + LogEntry.EncodedOverhead);
        private void SetPeer(int index, PeerProgress peer) => State = State with { Peers = State.Peers.SetItem(index, peer) };
        private void Push(CoreAction action) => State = State with { Agenda = State.Agenda.Push(action) };
        private void Reply(RaftRpc reply) => Effects.Add(new CoreEffect.Reply(
            reply is AppendResponse append ? append with { ReadContext = State.InboundReadContext } : reply));
        private long NextId()
        {
            State = State with { EffectId = checked(State.EffectId + 1) };
            return State.EffectId;
        }
        private void Wait(CoreEffect effect, CoreAction? continuation = null)
        {
            State = State with { Pending = new(effect, continuation) };
            Effects.Add(effect);
        }
        private void Read(LogQuery query, CoreAction continuation) => Wait(new CoreEffect.ReadLog(NextId(), query), continuation);
        private void Persist(LogWrite write, CoreAction? continuation = null) => Wait(new CoreEffect.Persist(NextId(), write), continuation);
    }
}
