using System.Collections.Immutable;
using Raft.Storage;

namespace Raft.Core;

internal readonly record struct OutboundRpc(string PeerId, RaftRpc Request);
internal readonly record struct ProposedCommand(byte[] Bytes, bool IsNoOp = false);
internal sealed record CoreFailure(string Type, string Message);
internal sealed record ReadRound(long Context, long Index, long Term, ImmutableArray<long> Clients,
    ImmutableHashSet<string> Acks, int Elapsed = 0);
internal sealed record LeadershipTransfer(long ClientId, string Target, long Term, int Elapsed = 0,
    bool Sent = false, long RequestId = 0);
internal sealed record InstallingSnapshot(long Id, SnapshotInstallIntent Intent);
internal abstract record ControlResult
{
    public sealed record Read(long ClientId, long Index, long Term, CoreFailure? Error = null) : ControlResult;
    public sealed record Transfer(long ClientId, CoreFailure? Error = null) : ControlResult;
    public sealed record Configuration(long ClientId, long Index, long Term, CoreFailure? Error = null) : ControlResult;
    public sealed record Snapshot(long ClientId, SnapshotMetadata Metadata, CoreFailure? Error = null) : ControlResult;
}

internal sealed record PeerProgress(string Id, long NextIndex = 1, long MatchIndex = 0,
    bool RecentActive = false, bool RetirementAcknowledged = false)
{
    public ReplicationMode Mode { get; init; } = ReplicationMode.Probe;
    public ImmutableArray<AppendRequest> Flights { get; init; } = ImmutableArray<AppendRequest>.Empty;
    public long InFlightBytes { get; init; }
    public AppendRequest? InFlight => Flights.IsEmpty ? null : Flights[0];
    public SnapshotMetadata? SendingSnapshot { get; init; }
    public long SnapshotOffset { get; init; }
    public SnapshotChunkRequest? SnapshotInFlight { get; init; }
    public int SnapshotHeartbeatElapsed { get; init; }
}

/// <summary>Immutable state. No live store, clock, random provider, task, or executable continuation.</summary>
internal sealed record RaftState(RaftOptions Options, HardState HardState, long LastIndex, long LastTerm,
    ImmutableArray<PeerProgress> Peers, int ElectionDeadline)
{
    public RaftRole Role { get; init; }
    public ClusterConfiguration Configuration { get; init; } = null!;
    public bool SupportsConfiguration { get; init; }
    public long FirstIndex { get; init; } = 1;
    public long UncommittedBytes { get; init; }
    public SnapshotMetadata? Snapshot { get; init; }
    public InstallingSnapshot? InstallingSnapshot { get; init; }
    public string? LeaderId { get; init; }
    public int ElectionElapsed { get; init; }
    public int LeaderContactElapsed { get; init; } = int.MaxValue;
    public long PreVoteRound { get; init; }
    public bool HasCommittedCurrentTerm { get; init; }
    public ReadRound? ReadRound { get; init; }
    public ImmutableQueue<ImmutableArray<long>> ReadQueue { get; init; } = ImmutableQueue<ImmutableArray<long>>.Empty;
    public LeadershipTransfer? Transfer { get; init; }
    public long InboundReadContext { get; init; }
    public int PendingReadCount { get; init; }
    public int HeartbeatElapsed { get; init; }
    public int QuorumElapsed { get; init; }
    public long RequestId { get; init; }
    public long EffectId { get; init; }
    public ImmutableHashSet<string> Votes { get; init; } = ImmutableHashSet.Create<string>(StringComparer.Ordinal);
    public ImmutableStack<CoreAction> Agenda { get; init; } = ImmutableStack<CoreAction>.Empty;
    public PendingEffect? Pending { get; init; }
    public CoreFailure? Fault { get; init; }
    public bool IsIdle => Pending is null && Agenda.IsEmpty;
}

internal abstract record CoreEvent
{
    public sealed record Tick : CoreEvent;
    public sealed record Campaign : CoreEvent;
    // LocalAppendGroup is never a wire field. The runtime may aggregate separately
    // admitted plain frames within the existing bounded local persistence budget.
    public sealed record Request(string From, RaftRpc Message, bool LocalAppendGroup = false) : CoreEvent;
    public sealed record Response(string From, RaftRpc Message) : CoreEvent;
    public sealed record Unreachable(string From, RaftRpc Message) : CoreEvent;
    public sealed record Propose(ImmutableArray<ProposedCommand> Commands) : CoreEvent;
    public sealed record ReadRequest : CoreEvent;
    public sealed record ReadIndex(ImmutableArray<long> Clients) : CoreEvent;
    public sealed record StorageCompleted(long Id, LogReadResult? Result = null) : CoreEvent;
    public sealed record StorageFailed(long Id, CoreFailure Error) : CoreEvent;
    public sealed record TimeoutSampled(long Id, int Ticks) : CoreEvent;
    public sealed record SnapshotCompleted(long Id, SnapshotMetadata Metadata) : CoreEvent;
    public sealed record SnapshotCreated(long ClientId, SnapshotMetadata Metadata) : CoreEvent;
    public sealed record SnapshotReady(string From, long Term, SnapshotMetadata Metadata) : CoreEvent;
    public sealed record SnapshotChunkRead(long Id, SnapshotChunkData Data) : CoreEvent;
    public sealed record ConfigChange(ImmutableArray<string> Voters, ImmutableArray<string> Learners) : CoreEvent
    { public long ClientId { get; init; } }
    public sealed record TransferLeadership(string Target) : CoreEvent { public long ClientId { get; init; } }
}

internal abstract record CoreEffect
{
    public sealed record ReadLog(long Id, LogQuery Query) : CoreEffect;
    public sealed record Persist(long Id, LogWrite Write) : CoreEffect;
    public sealed record ResetTimer(long Id) : CoreEffect;
    public sealed record Send(OutboundRpc Rpc) : CoreEffect;
    public sealed record ClearOutbound : CoreEffect;
    public sealed record Reply(RaftRpc Message) : CoreEffect;
    public sealed record Proposed(long FirstIndex) : CoreEffect;
    public sealed record Apply(long ThroughIndex) : CoreEffect;
    public sealed record Halt(CoreFailure Error) : CoreEffect;
    public sealed record Control(ControlResult Result) : CoreEffect;
    public sealed record ReadSnapshotChunk(long Id, SnapshotMetadata Metadata, long Offset, int Count) : CoreEffect;
    public sealed record ActivateSnapshot(long Id, SnapshotMetadata Metadata) : CoreEffect;
}

internal sealed record PendingEffect(CoreEffect Effect, CoreAction? Continuation = null);
internal sealed record CoreTransition(RaftState State, ImmutableArray<CoreEffect> Effects);

internal abstract record CoreAction
{
    public sealed record Tick : CoreAction;
    public sealed record Campaign : CoreAction;
    public sealed record PreCampaign : CoreAction;
    public sealed record SendPreVotes : CoreAction;
    public sealed record PreVote(string From, PreVoteRequest Request) : CoreAction;
    public sealed record TimeoutNow(string From, TimeoutNowRequest Request) : CoreAction;
    public sealed record StartRead(ImmutableArray<long> Clients) : CoreAction;
    public sealed record NextRead : CoreAction;
    public sealed record BeginTransfer(long ClientId, string Target) : CoreAction;
    public sealed record CheckTransfer : CoreAction;
    public sealed record TimeoutReply(long RequestId) : CoreAction;
    public sealed record ChangeConfiguration(long ClientId, ImmutableArray<string> Voters, ImmutableArray<string> Learners) : CoreAction;
    public sealed record ConfigurationAccepted(long ClientId, long Index) : CoreAction;
    public sealed record RefreshConfiguration : CoreAction;
    public sealed record FinishConfiguration : CoreAction;
    public sealed record SnapshotRequest(string From, SnapshotChunkRequest Request) : CoreAction;
    public sealed record PermitSnapshotChunk(string From, SnapshotChunkRequest Request) : CoreAction;
    public sealed record BeginSnapshot(string From, long Term, SnapshotMetadata Metadata) : CoreAction;
    public sealed record SnapshotBoundary(string From, long Term, SnapshotMetadata Metadata) : CoreAction;
    public sealed record ActivateSnapshot : CoreAction;
    public sealed record SnapshotPublished(long ClientId, SnapshotMetadata Metadata) : CoreAction;
    public sealed record BuildSnapshotChunk(int Peer, SnapshotChunkData? Data = null) : CoreAction;
    public sealed record FinishCampaign : CoreAction;
    public sealed record BecomeLeader : CoreAction;
    public sealed record BecomeFollower(long Term, string? Leader) : CoreAction;
    public sealed record SetFollower(string? Leader, bool PreserveOutbound = false) : CoreAction;
    public sealed record ResetElection : CoreAction;
    public sealed record SendVotes : CoreAction;
    public sealed record Vote(string From, VoteRequest Request) : CoreAction;
    public sealed record FinishVote(VoteResponse Message) : CoreAction;
    public sealed record Append(string From, AppendRequest Request) : CoreAction;
    public sealed record BeginAppend(string From, AppendRequest Request) : CoreAction;
    public sealed record CheckPrevious(AppendRequest Request) : CoreAction;
    public sealed record PreviousTerm(AppendRequest Request) : CoreAction;
    public sealed record Conflict(AppendRequest Request, long Term) : CoreAction;
    public sealed record MatchEntries(AppendRequest Request) : CoreAction;
    public sealed record CommitAppend(AppendRequest Request) : CoreAction;
    public sealed record Reply(RaftRpc Message) : CoreAction;
    public sealed record Response(string From, RaftRpc Message) : CoreAction;
    public sealed record AfterSuccess(int Peer, long OldCommit) : CoreAction;
    public sealed record ConflictNext(int Peer, AppendResponse Message, long RejectedPreviousIndex) : CoreAction;
    public sealed record AdvanceCommit : CoreAction;
    public sealed record CommitTerm(long Index) : CoreAction;
    public sealed record SendAll(bool OnlyIfNecessary = false) : CoreAction;
    public sealed record SendPeer(int Peer) : CoreAction;
    public sealed record BuildAppend(int Peer) : CoreAction;
    public sealed record Propose(ImmutableArray<ProposedCommand> Commands) : CoreAction;
    public sealed record Proposed(long FirstIndex) : CoreAction;
    public sealed record Unreachable(string From, RaftRpc Request) : CoreAction;
    public sealed record ReadCompleted(CoreAction Continuation, LogReadResult Result) : CoreAction;
}
