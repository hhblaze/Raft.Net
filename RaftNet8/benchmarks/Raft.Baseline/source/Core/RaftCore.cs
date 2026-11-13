using Raft.Storage;

namespace Raft.Core;

internal readonly record struct OutboundRpc(string PeerId, RaftRpc Request);
internal readonly record struct ProposedCommand(byte[] Bytes, bool IsNoOp = false);

/// <summary>Single-owner deterministic Figure 2 state machine. No network calls, timers, or application callbacks.</summary>
internal sealed class RaftCore
{
    private sealed class Progress
    {
        public long NextIndex = 1;
        public long MatchIndex;
        public AppendRequest? InFlight;
        public bool RecentActive;
    }

    private readonly RaftOptions _options;
    private readonly IRaftStorage _storage;
    private readonly Random _random;
    private readonly Dictionary<string, Progress> _peers;
    private readonly HashSet<string> _votes = new(StringComparer.Ordinal);
    private readonly Queue<OutboundRpc> _outgoing = new();
    private readonly long[] _quorumIndexes;
    private int _electionElapsed, _electionDeadline, _heartbeatElapsed, _quorumElapsed;
    private long _requestId;

    public RaftCore(RaftOptions options, IRaftStorage storage, Random? random = null)
    {
        _options = options.Freeze();
        _storage = storage;
        _random = random ?? Random.Shared;
        _peers = _options.Members.Where(id => id != _options.NodeId)
            .ToDictionary(id => id, _ => new Progress(), StringComparer.Ordinal);
        _quorumIndexes = new long[_options.Members.Count];
        ResetElection();
    }

    public RaftRole Role { get; private set; }
    public string? LeaderId { get; private set; }
    public long Term => _storage.HardState.Term;
    public long CommitIndex => _storage.HardState.CommitIndex;
    public long LastIndex => _storage.LastIndex;
    private int Quorum => _options.Members.Count / 2 + 1;

    public bool TryDequeue(out OutboundRpc rpc) => _outgoing.TryDequeue(out rpc);

    public void Tick()
    {
        if (Role != RaftRole.Leader)
        {
            if (++_electionElapsed >= _electionDeadline) Campaign();
            return;
        }
        if (++_quorumElapsed >= _options.ElectionMaxTicks)
        {
            _quorumElapsed = 0;
            var active = 1;
            foreach (var peer in _peers.Values)
            {
                if (peer.RecentActive) active++;
                peer.RecentActive = false;
            }
            if (active < Quorum)
            {
                BecomeFollower(Term, null);
                return;
            }
        }
        if (++_heartbeatElapsed >= _options.HeartbeatTicks)
        {
            _heartbeatElapsed = 0;
            SendAll();
        }
    }

    internal void Campaign()
    {
        _storage.SaveHardState(new HardState(checked(Term + 1), _options.NodeId, CommitIndex));
        Role = RaftRole.Candidate;
        LeaderId = null;
        _votes.Clear();
        _votes.Add(_options.NodeId);
        _outgoing.Clear();
        ResetElection();
        foreach (var progress in _peers.Values) progress.InFlight = null;
        if (Quorum == 1) BecomeLeader();
        else
        {
            var request = new VoteRequest(Term, LastIndex, _storage.LastTerm);
            foreach (var id in _peers.Keys) _outgoing.Enqueue(new(id, request));
        }
    }

    private void BecomeLeader()
    {
        Role = RaftRole.Leader;
        LeaderId = _options.NodeId;
        _heartbeatElapsed = _quorumElapsed = 0;
        foreach (var progress in _peers.Values)
        {
            progress.NextIndex = checked(LastIndex + 1);
            progress.MatchIndex = 0;
            progress.InFlight = null;
            progress.RecentActive = false;
        }
        // A current-term entry is necessary to safely commit inherited entries (paper §5.4.2).
        _storage.Append(new[] { new LogEntry(checked(LastIndex + 1), Term, Array.Empty<byte>(), true) });
        AdvanceCommit();
        SendAll();
    }

    private void BecomeFollower(long term, string? leader)
    {
        if (term > Term) _storage.SaveHardState(new HardState(term, null, CommitIndex));
        Role = RaftRole.Follower;
        LeaderId = leader;
        _votes.Clear();
        _outgoing.Clear();
        foreach (var peer in _peers.Values) peer.InFlight = null;
        ResetElection();
    }

    public long Propose(IReadOnlyList<ProposedCommand> commands)
    {
        if (Role != RaftRole.Leader) throw new NotLeaderException(LeaderId);
        if (commands.Count < 1 || commands.Count > _options.MaxAppendEntries)
            throw new ArgumentOutOfRangeException(nameof(commands));
        var entries = new LogEntry[commands.Count];
        var bytes = 0;
        var first = checked(LastIndex + 1);
        for (var i = 0; i < commands.Count; i++)
        {
            var command = commands[i];
            bytes = checked(bytes + command.Bytes.Length + LogEntry.EncodedOverhead);
            if (command.Bytes.Length > _options.MaxCommandBytes || bytes > _options.MaxAppendBytes)
                throw new ArgumentException("Proposal batch exceeds configured limits.", nameof(commands));
            entries[i] = new LogEntry(checked(first + i), Term, command.Bytes, command.IsNoOp);
        }
        _storage.Append(entries);
        AdvanceCommit();
        SendAll();
        return first;
    }

    public RaftRpc HandleRequest(string from, RaftRpc request)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (!_peers.ContainsKey(from)) throw new ArgumentException("RPC source is not another configured member.", nameof(from));
        ValidateRequest(request);
        if (request.Term > Term) BecomeFollower(request.Term, null);
        return request switch
        {
            VoteRequest vote => HandleVote(from, vote),
            AppendRequest append => HandleAppend(from, append),
            _ => throw new ArgumentException("Expected a Raft request.", nameof(request))
        };
    }

    private VoteResponse HandleVote(string from, VoteRequest request)
    {
        if (request.Term < Term) return new(Term, false);
        var state = _storage.HardState;
        var fresh = request.LastLogTerm > _storage.LastTerm ||
                    (request.LastLogTerm == _storage.LastTerm && request.LastLogIndex >= LastIndex);
        var grant = fresh && (state.VotedFor is null || state.VotedFor == from);
        if (grant)
        {
            _storage.SaveHardState(state with { VotedFor = from });
            LeaderId = null;
            ResetElection();
        }
        return new(Term, grant);
    }

    private AppendResponse HandleAppend(string from, AppendRequest request)
    {
        if (request.Term < Term) return new(Term, request.RequestId, false, 0, checked(LastIndex + 1));
        if (Role != RaftRole.Follower) BecomeFollower(Term, from);
        LeaderId = from;
        ResetElection();
        if (request.PreviousIndex > LastIndex)
            return new(Term, request.RequestId, false, 0, checked(LastIndex + 1));
        var previousTerm = _storage.GetTerm(request.PreviousIndex);
        if (previousTerm != request.PreviousTerm)
            return new(Term, request.RequestId, false, 0, _storage.FirstIndexOfTerm(request.PreviousIndex), previousTerm);

        for (var i = 0; i < request.Entries.Count; i++)
        {
            var entry = request.Entries[i];
            if (entry.Index <= LastIndex && _storage.GetTerm(entry.Index) == entry.Term) continue;
            if (entry.Index <= CommitIndex)
                return new(Term, request.RequestId, false, 0, CommitIndex + 1);
            // Delete only on conflict, never because a heartbeat/batch ends before our existing suffix.
            _storage.ReplaceSuffix(entry.Index, i == 0 ? request.Entries : request.Entries.Skip(i).ToArray());
            break;
        }
        var matched = checked(request.PreviousIndex + request.Entries.Count);
        var commit = Math.Min(request.LeaderCommit, matched);
        if (commit > CommitIndex) _storage.SaveHardState(_storage.HardState with { CommitIndex = commit });
        return new(Term, request.RequestId, true, matched);
    }

    public void HandleResponse(string from, RaftRpc response)
    {
        if (!_peers.TryGetValue(from, out var progress)) return;
        if (response is not VoteResponse and not AppendResponse || response.Term < 0) return;
        if (response.Term > Term)
        {
            BecomeFollower(response.Term, null);
            return;
        }
        if (response.Term != Term) return;
        if (response is VoteResponse vote)
        {
            if (Role == RaftRole.Candidate && vote.Granted && _votes.Add(from) && _votes.Count >= Quorum) BecomeLeader();
            return;
        }
        if (Role != RaftRole.Leader || response is not AppendResponse reply ||
            progress.InFlight is not { } request || request.RequestId != reply.RequestId) return;
        var sentThrough = checked(request.PreviousIndex + request.Entries.Count);
        if (reply.Success && reply.MatchIndex != sentThrough) return;
        if (!reply.Success && (reply.ConflictIndex < 1 || reply.ConflictIndex > LastIndex + 1 || reply.ConflictTerm < 0)) return;
        progress.InFlight = null;
        progress.RecentActive = true;
        if (reply.Success)
        {
            progress.MatchIndex = Math.Max(progress.MatchIndex, reply.MatchIndex);
            progress.NextIndex = checked(progress.MatchIndex + 1);
            var oldCommit = CommitIndex;
            AdvanceCommit();
            if (CommitIndex != oldCommit) SendAll();
            else if (progress.NextIndex <= LastIndex) SendAppend(from, progress);
        }
        else
        {
            var ownTermLast = reply.ConflictTerm == 0 ? 0 : _storage.LastIndexOfTerm(reply.ConflictTerm);
            var next = ownTermLast > 0 ? ownTermLast + 1 : reply.ConflictIndex;
            progress.NextIndex = Math.Max(progress.MatchIndex + 1, Math.Max(1, Math.Min(progress.NextIndex - 1, next)));
            SendAppend(from, progress);
        }
    }

    public void ReportUnreachable(string from, RaftRpc request)
    {
        if (_peers.TryGetValue(from, out var progress) && request is AppendRequest append &&
            progress.InFlight?.RequestId == append.RequestId && append.Term == Term)
            progress.InFlight = null;
        // A tick schedules retries. Immediate failures must not form a busy retry loop.
    }

    private void AdvanceCommit()
    {
        _quorumIndexes[0] = LastIndex;
        var i = 1;
        foreach (var progress in _peers.Values) _quorumIndexes[i++] = progress.MatchIndex;
        Array.Sort(_quorumIndexes);
        var candidate = _quorumIndexes[_quorumIndexes.Length - Quorum];
        if (candidate > CommitIndex && _storage.GetTerm(candidate) == Term)
            _storage.SaveHardState(_storage.HardState with { CommitIndex = candidate });
    }

    private void SendAll()
    {
        foreach (var (id, progress) in _peers) SendAppend(id, progress);
    }

    private void SendAppend(string id, Progress progress)
    {
        if (progress.InFlight is not null) return;
        var previous = progress.NextIndex - 1;
        var entries = _storage.Read(progress.NextIndex, _options.MaxAppendEntries, _options.MaxAppendBytes);
        var request = new AppendRequest(Term, checked(++_requestId), previous, _storage.GetTerm(previous), CommitIndex, entries);
        progress.InFlight = request;
        _outgoing.Enqueue(new(id, request));
    }

    private void ResetElection()
    {
        _electionElapsed = 0;
        _electionDeadline = _random.Next(_options.ElectionMinTicks, _options.ElectionMaxTicks);
    }

    private void ValidateRequest(RaftRpc request)
    {
        if (request.Term < 1) throw new ArgumentException("RPC term must be positive.", nameof(request));
        if (request is VoteRequest vote)
        {
            if (vote.LastLogIndex < 0 || vote.LastLogTerm < 0 || vote.LastLogTerm > vote.Term ||
                ((vote.LastLogIndex == 0) != (vote.LastLogTerm == 0)))
                throw new ArgumentException("Invalid candidate log metadata.", nameof(request));
        }
        else if (request is AppendRequest append)
        {
            ArgumentNullException.ThrowIfNull(append.Entries);
            if (append.PreviousIndex < 0 || append.PreviousIndex >= long.MaxValue - _options.MaxAppendEntries ||
                append.PreviousTerm < 0 || append.PreviousTerm > append.Term || append.LeaderCommit < 0 || append.RequestId < 1 ||
                ((append.PreviousIndex == 0) != (append.PreviousTerm == 0)) || append.Entries.Count > _options.MaxAppendEntries)
                throw new ArgumentException("Invalid AppendEntries metadata.", nameof(request));
            var bytes = 0;
            var previousTerm = append.PreviousTerm;
            for (var i = 0; i < append.Entries.Count; i++)
            {
                var entry = append.Entries[i];
                ArgumentNullException.ThrowIfNull(entry);
                bytes = checked(bytes + entry.Command.Length + LogEntry.EncodedOverhead);
                if (entry.Index != append.PreviousIndex + i + 1 || entry.Term < previousTerm || entry.Term > append.Term ||
                    entry.Command.Length > _options.MaxCommandBytes || bytes > _options.MaxAppendBytes)
                    throw new ArgumentException("Invalid or oversized log batch.", nameof(request));
                previousTerm = entry.Term;
            }
        }
        else throw new ArgumentException("Expected a request.", nameof(request));
    }
}
