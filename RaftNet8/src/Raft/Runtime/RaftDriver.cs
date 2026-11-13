using System.Collections.Immutable;
using Raft.Core;
using Raft.Storage;

namespace Raft.Runtime;

/// <summary>Single-owner effect executor. The node's channel loop owns all calls.</summary>
internal sealed class RaftDriver
{
    private readonly ILogStore _store;
    private readonly IRandom _random;
    private readonly Func<SnapshotMetadata, long, int, CancellationToken, ValueTask<SnapshotChunkData>>? _readSnapshot;
    private readonly Action<long, SnapshotMetadata>? _activateSnapshot;
    private readonly Queue<OutboundRpc> _outgoing = new();
    private readonly Queue<ControlResult> _control = new();
    private Exception? _storageFailure;
    internal RaftState State { get; private set; }
    public RaftRole Role => State.Role;
    public string? LeaderId => State.LeaderId;
    public long Term => State.HardState.Term;
    public long CommitIndex => State.HardState.CommitIndex;
    public long LastIndex => State.LastIndex;
    public long ApplyThrough { get; private set; }

    public RaftDriver(RaftOptions options, ILogStore store, RecoveredLogState recovered, IRandom random,
        Func<SnapshotMetadata, long, int, CancellationToken, ValueTask<SnapshotChunkData>>? readSnapshot = null,
        Action<long, SnapshotMetadata>? activateSnapshot = null)
    {
        _store = store;
        _random = random;
        _readSnapshot = readSnapshot;
        _activateSnapshot = activateSnapshot;
        var frozen = RecoveredOptions(options.Freeze(allowMissingSelf: recovered.Configuration is not null), recovered);
        State = RaftCore.Create(frozen, recovered, random.Next(frozen.ElectionMinTicks, frozen.ElectionMaxTicks));
        ApplyThrough = recovered.HardState.CommitIndex;
    }

    internal static void ValidateRecovered(RaftOptions frozen, RecoveredLogState recovered)
    {
        if (recovered.Identity is { } identity &&
            (identity.NodeId != frozen.NodeId || identity.GroupId != frozen.GroupId ||
             (!recovered.SupportsConfiguration &&
              !identity.Bootstrap.Voters.ToHashSet(StringComparer.Ordinal).SetEquals(frozen.Members))))
            throw new InvalidDataException("Runtime identity/configuration differs from the recovered store.");
    }

    internal static RaftOptions RecoveredOptions(RaftOptions options, RecoveredLogState recovered)
    {
        ValidateRecovered(options, recovered);
        return recovered.Configuration is { } config
            ? (options with { Members = config.Validate().Voters, Learners = config.Learners }).Freeze(allowMissingSelf: true)
            : options;
    }

    // Existing deterministic tests/benchmarks execute effects inline, without creating worker lifetimes.
    public RaftDriver(RaftOptions options, IRaftStorage storage, Random? random = null)
        : this(options, new InlineLogStore(storage), RaftStorageAdapter.Recover(storage),
            random is null ? new SystemRaftRandom() : new SystemRaftRandom(random)) { }

    public bool TryDequeue(out OutboundRpc rpc) => _outgoing.TryDequeue(out rpc);
    public bool TryDequeueControl(out ControlResult? result) => _control.TryDequeue(out result);
    public async ValueTask ReadIndexAsync(IReadOnlyList<long> clients, CancellationToken token = default) =>
        _ = await ExecuteAsync(new CoreEvent.ReadIndex(ImmutableArray.CreateRange(clients)), token).ConfigureAwait(false);
    public async ValueTask TransferAsync(long client, string target, CancellationToken token = default) =>
        _ = await ExecuteAsync(new CoreEvent.TransferLeadership(target) { ClientId = client }, token).ConfigureAwait(false);
    public async ValueTask ChangeConfigurationAsync(long client, IReadOnlyList<string> voters, IReadOnlyList<string> learners,
        CancellationToken token = default) =>
        _ = await ExecuteAsync(new CoreEvent.ConfigChange(ImmutableArray.CreateRange(voters), ImmutableArray.CreateRange(learners))
            { ClientId = client }, token).ConfigureAwait(false);
    public async ValueTask SnapshotReadyAsync(string from, long term, SnapshotMetadata metadata, CancellationToken token) =>
        _ = await ExecuteAsync(new CoreEvent.SnapshotReady(from, term, metadata), token).ConfigureAwait(false);
    public async ValueTask SnapshotCompletedAsync(long id, SnapshotMetadata metadata, CancellationToken token) =>
        _ = await ExecuteAsync(new CoreEvent.SnapshotCompleted(id, metadata), token).ConfigureAwait(false);
    public async ValueTask SnapshotCreatedAsync(long clientId, SnapshotMetadata metadata, CancellationToken token) =>
        _ = await ExecuteAsync(new CoreEvent.SnapshotCreated(clientId, metadata), token).ConfigureAwait(false);
    internal bool Admits(string from, RaftRpc request) => State.Options.KnownPeers.Contains(from, StringComparer.Ordinal);
    public void Tick() => TickAsync().GetAwaiter().GetResult();
    public void Campaign() => CampaignAsync().GetAwaiter().GetResult();
    public long Propose(IReadOnlyList<ProposedCommand> commands) => ProposeAsync(commands).GetAwaiter().GetResult();
    public RaftRpc HandleRequest(string from, RaftRpc request) => HandleRequestAsync(from, request).GetAwaiter().GetResult();
    public void HandleResponse(string from, RaftRpc response) => HandleResponseAsync(from, response).GetAwaiter().GetResult();
    public void ReportUnreachable(string from, RaftRpc request) => ReportUnreachableAsync(from, request).GetAwaiter().GetResult();

    public async ValueTask TickAsync(CancellationToken token = default) =>
        _ = await ExecuteAsync(new CoreEvent.Tick(), token).ConfigureAwait(false);

    public async ValueTask CampaignAsync(CancellationToken token = default) =>
        _ = await ExecuteAsync(new CoreEvent.Campaign(), token).ConfigureAwait(false);

    public async ValueTask<long> ProposeAsync(IReadOnlyList<ProposedCommand> commands, CancellationToken token = default) =>
        (await ExecuteAsync(new CoreEvent.Propose(ImmutableArray.CreateRange(commands)), token).ConfigureAwait(false)).FirstIndex;

    public async ValueTask<RaftRpc> HandleRequestAsync(string from, RaftRpc request, CancellationToken token = default)
    {
        if (!Admits(from, request)) throw new ArgumentException("RPC source is outside the active configuration.", nameof(from));
        var owned = OwnRequest(State.Options, from, request, validateMembership: false);
        return (await ExecuteAsync(new CoreEvent.Request(from, owned), token).ConfigureAwait(false)).Reply
            ?? throw new InvalidOperationException("Request completed without a reply.");
    }

    public async ValueTask HandleResponseAsync(string from, RaftRpc response, CancellationToken token = default) =>
        _ = await ExecuteAsync(new CoreEvent.Response(from, response), token).ConfigureAwait(false);

    internal async ValueTask<AppendResponse> HandleAppendGroupAsync(string from, AppendRequest request, CancellationToken token)
    {
        if (!Admits(from, request)) throw new ArgumentException("RPC source is outside the active configuration.", nameof(from));
        var owned = OwnRequest(State.Options, from, request, validateMembership: false, localAppendGroup: true);
        return (await ExecuteAsync(new CoreEvent.Request(from, owned, LocalAppendGroup: true), token).ConfigureAwait(false)).Reply
            as AppendResponse ?? throw new InvalidOperationException("Append group completed without an append reply.");
    }

    public async ValueTask ReportUnreachableAsync(string from, RaftRpc request, CancellationToken token = default) =>
        _ = await ExecuteAsync(new CoreEvent.Unreachable(from, request), token).ConfigureAwait(false);

    internal static RaftRpc OwnRequest(RaftOptions options, string from, RaftRpc request, bool validateMembership = true,
        bool localAppendGroup = false)
    {
        var owned = request switch
        {
            AppendRequest append when append.Entries is not ImmutableArray<LogEntry> => append with { Entries = ImmutableArray.CreateRange(append.Entries) },
            SnapshotChunkRequest snapshot => snapshot with { Data = snapshot.Data.ToArray() },
            _ => request
        };
        RaftCore.ValidateRequest(options, from, owned, validateMembership, localAppendGroup);
        return owned;
    }

    private readonly record struct Result(RaftRpc? Reply = null, long FirstIndex = 0);

    private async ValueTask<Result> ExecuteAsync(CoreEvent input, CancellationToken token)
    {
        if (_storageFailure is not null) throw new InvalidOperationException("Consensus storage has failed.", _storageFailure);
        Result result = default;
        while (true)
        {
            var transition = RaftCore.Step(State, input);
            State = transition.State;
            CoreEvent? completion = null;
            foreach (var effect in transition.Effects)
            {
                switch (effect)
                {
                    case CoreEffect.Persist persist:
                        var persistenceStarted = RaftInstrumentation.Start(RaftInstrumentation.PersistenceDuration);
                        var persistenceOutcome = "success";
                        try
                        {
                            await _store.WriteAsync(persist.Write, token).ConfigureAwait(false);
                            completion = new CoreEvent.StorageCompleted(persist.Id,
                                _store is ILogStoreProgress progress ? new(Number: progress.UncommittedBytes) : null);
                        }
                        catch (Exception error) { persistenceOutcome = "error"; FailStorage(persist.Id, error); throw; }
                        finally { RaftInstrumentation.Duration(RaftInstrumentation.PersistenceDuration, persistenceStarted,
                            State.Options.GroupId, State.Options.NodeId, persistenceOutcome); }
                        break;
                    case CoreEffect.ReadLog read:
                        try
                        {
                            var data = await _store.ReadAsync(read.Query, token).ConfigureAwait(false);
                            completion = new CoreEvent.StorageCompleted(read.Id, data);
                        }
                        catch (Exception error) { FailStorage(read.Id, error); throw; }
                        break;
                    case CoreEffect.ResetTimer timer:
                        completion = new CoreEvent.TimeoutSampled(timer.Id,
                            _random.Next(State.Options.ElectionMinTicks, State.Options.ElectionMaxTicks));
                        break;
                    case CoreEffect.ReadSnapshotChunk chunk:
                        try
                        {
                            var readChunk = _readSnapshot ?? throw new NotSupportedException("Configure a snapshot repository.");
                            var data = await readChunk(chunk.Metadata, chunk.Offset, chunk.Count, token).ConfigureAwait(false);
                            completion = new CoreEvent.SnapshotChunkRead(chunk.Id, data);
                        }
                        catch (Exception error) { FailStorage(chunk.Id, error); throw; }
                        break;
                    case CoreEffect.ActivateSnapshot activate:
                        (_activateSnapshot ?? throw new NotSupportedException("Configure a snapshot state machine."))(activate.Id, activate.Metadata);
                        break;
                    case CoreEffect.Send send: _outgoing.Enqueue(send.Rpc); break;
                    case CoreEffect.ClearOutbound: _outgoing.Clear(); break;
                    case CoreEffect.Reply reply: result = result with { Reply = reply.Message }; break;
                    case CoreEffect.Proposed proposed: result = result with { FirstIndex = proposed.FirstIndex }; break;
                    case CoreEffect.Apply apply: ApplyThrough = apply.ThroughIndex; break;
                    case CoreEffect.Control control: _control.Enqueue(control.Result); break;
                    case CoreEffect.Halt halt: throw new InvalidOperationException(halt.Error.Message, _storageFailure);
                }
            }
            if (completion is null)
            {
                if (!State.IsIdle) throw new InvalidOperationException("A suspended core did not emit its external effect.");
                return result;
            }
            input = completion;
        }
    }

    private void FailStorage(long id, Exception error)
    {
        _storageFailure = error;
        State = RaftCore.Step(State, new CoreEvent.StorageFailed(id,
            new(error.GetType().FullName ?? error.GetType().Name, error.Message))).State;
        _outgoing.Clear();
    }
}
