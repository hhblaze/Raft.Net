using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Threading.Channels;
using Raft.Core;
using Raft.Runtime;
using Raft.Storage;
using Microsoft.Extensions.Logging;

namespace Raft;

public readonly record struct NodeMetrics(int PendingProposals, int InFlightRpcs, long TransportFailures);

/// <summary>Bounded asynchronous host for one Raft group. Owns the supplied storage.</summary>
public sealed partial class RaftNode : IAsyncDisposable
{
    private abstract record Work;
    private sealed record TickWork : Work;
    private sealed record ProposalReadyWork : Work;
    private sealed record RequestWork(string From, RaftRpc Request, TaskCompletionSource<RaftRpc> Completion) : Work;
    private sealed record ResponseWork(string From, RaftRpc Request, RaftRpc? Response) : Work;
    private sealed record ReadWork(long Id, CancellationToken CallerToken) : Work;
    private sealed record TransferWork(long Id, string Target) : Work;
    private enum MembershipOperation { AddLearner, Promote, Remove }
    private sealed record ConfigurationWork(long Id, string NodeId, MembershipOperation Operation) : Work;
    private sealed class PendingConfiguration
    {
        public readonly TaskCompletionSource<ProposalResult> Completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public long Index = -1, Term;
    }
    private sealed class PendingRead
    {
        public readonly TaskCompletionSource<ReadIndexResult> Completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public long Index = -1, Term;
    }
    private sealed record ProposalWork(byte[] Bytes, bool IsNoOp, CancellationToken CallerToken) : Work
    {
        public readonly TaskCompletionSource<ProposalResult> Completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int Released;
        public long Term;
        public long QueuedAt;
    }

    private static readonly TickWork TickMessage = new();
    private static readonly ProposalReadyWork ProposalMessage = new();
    private readonly object _lifecycleGate = new();
    private readonly RaftOptions _options;
    private readonly ConcurrentDictionary<long, PendingConfiguration> _configurations = new();
    private readonly SemaphoreSlim _configurationAdmission = new(1, 1);
    private readonly ILogStore _logStore;
    private readonly ITransport _transport;
    private readonly IStateMachine _stateMachine;
    private readonly RaftDriver _core;
    private readonly IClock _clock;
    private readonly Channel<Work> _work;
    private readonly Channel<ProposalWork> _proposals;
    private readonly object _proposalGate = new();
    private int _queuedProposalCount, _firstProposalBytes;
    private long _queuedProposalBytes, _firstProposalTimestamp;
    private readonly Channel<byte> _proposalChanges = Channel.CreateBounded<byte>(new BoundedChannelOptions(1)
    { SingleReader = true, FullMode = BoundedChannelFullMode.DropWrite, AllowSynchronousContinuations = false });
    private readonly Channel<byte> _applySignal = Channel.CreateBounded<byte>(new BoundedChannelOptions(1)
    {
        SingleReader = true, SingleWriter = true, FullMode = BoundedChannelFullMode.DropOldest
    });
    private readonly SemaphoreSlim _admission;
    private readonly SemaphoreSlim _readAdmission;
    private readonly SemaphoreSlim _transferAdmission = new(1, 1);
    private readonly CancellationTokenSource _lifetime = new();
    private readonly ConcurrentDictionary<long, ProposalWork> _pending = new();
    private readonly ConcurrentDictionary<long, Task> _rpcs = new();
    private readonly ConcurrentDictionary<long, PendingRead> _reads = new();
    private readonly ConcurrentDictionary<long, TaskCompletionSource<bool>> _transfers = new();
    private readonly TaskCompletionSource _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private RaftStatus _status;
    private Exception? _fault;
    private Task? _supervisor;
    private Task? _disposeTask;
    private long _commitTarget, _lastApplied, _rpcId, _transportFailures, _controlId;
    private int _tickQueued, _proposalQueued, _admitted;
    private bool _disposed;

    public RaftNode(RaftOptions options, IRaftStorage storage, IRaftTransport transport, IRaftStateMachine stateMachine)
        : this(options, storage, transport, stateMachine, loggerFactory: null) { }

    public RaftNode(RaftOptions options, IRaftStorage storage, IRaftTransport transport, IRaftStateMachine stateMachine, ILoggerFactory? loggerFactory)
        : this(options, transport, stateMachine, new SystemRaftClock(), new SystemRaftRandom(),
            legacyStorage: storage ?? throw new ArgumentNullException(nameof(storage)), loggerFactory: loggerFactory) { }

    /// <summary>
    /// Recovers an asynchronous log store before constructing the node. The node owns the store after success.
    /// Clock and random providers belong to the driver, never the consensus reducer.
    /// </summary>
    public static ValueTask<RaftNode> CreateAsync(RaftOptions options, ILogStore logStore,
        ITransport transport, IStateMachine stateMachine, IClock? clock = null, IRandom? random = null,
        CancellationToken cancellationToken = default) =>
        CreateAsync(options, logStore, transport, stateMachine, clock, random, cancellationToken, loggerFactory: null);

    /// <summary>Logging overload preserving the original CreateAsync signature and null-argument overload resolution.</summary>
    public static async ValueTask<RaftNode> CreateAsync(RaftOptions options, ILogStore logStore,
        ITransport transport, IStateMachine stateMachine, IClock? clock, IRandom? random,
        CancellationToken cancellationToken, ILoggerFactory? loggerFactory)
    {
        ArgumentNullException.ThrowIfNull(logStore);
        var frozen = options.Freeze();
        ArgumentNullException.ThrowIfNull(transport);
        ArgumentNullException.ThrowIfNull(stateMachine);
        var recovered = await logStore.RecoverAsync(cancellationToken).ConfigureAwait(false);
        if (frozen.WireVersion == 3)
        {
            if (recovered.Membership is null || transport is not IPeerDirectoryTransport directory)
                throw new ArgumentException("Wire3 requires raft8-v4 and an IPeerDirectoryTransport.");
            await directory.UpdateDirectoryAsync(recovered.Membership.Committed, cancellationToken).ConfigureAwait(false);
        }
        recovered = await RecoverSnapshotAsync(frozen, logStore, stateMachine, recovered, cancellationToken).ConfigureAwait(false);
        if (stateMachine is ICheckpointStateMachine checkpointed && checkpointed.Checkpoint.Index > 0)
        {
            var checkpoint = checkpointed.Checkpoint;
            var term = await logStore.ReadAsync(new LogQuery.Term(checkpoint.Index), cancellationToken).ConfigureAwait(false);
            if (term.Number != checkpoint.Term || stateMachine.LastApplied != checkpoint.Index)
                throw new InvalidDataException("Application checkpoint index/term differs from the durable log.");
        }
        return new(frozen, transport, stateMachine, clock ?? new SystemRaftClock(), random ?? new SystemRaftRandom(),
            logStore: logStore, recovered: recovered, loggerFactory: loggerFactory);
    }

    private RaftNode(RaftOptions options, ITransport transport, IStateMachine stateMachine, IClock clock,
        IRandom random, IRaftStorage? legacyStorage = null, ILogStore? logStore = null, RecoveredLogState? recovered = null,
        ILoggerFactory? loggerFactory = null)
    {
        _options = options.Freeze();
        _transport = transport ?? throw new ArgumentNullException(nameof(transport));
        _stateMachine = stateMachine ?? throw new ArgumentNullException(nameof(stateMachine));
        _clock = clock;
        var durable = recovered ?? RaftStorageAdapter.Recover(legacyStorage!);
        if (durable.PendingSnapshotInstall is not null || (legacyStorage is not null && durable.Snapshot is not null))
            throw new InvalidOperationException("Use CreateAsync with a snapshot-capable state machine to recover snapshot data.");
        RaftDriver.ValidateRecovered(_options, durable);
        _options = RaftDriver.RecoveredOptions(_options, durable);
        if (_options.WireVersion == 3)
        {
            if (durable.Membership is null || transport is not IPeerDirectoryTransport)
                throw new ArgumentException("Wire3 requires raft8-v4 and a dynamic directory transport.");
            _membershipRequestAdmission = new(_options.MaxPendingReads, _options.MaxPendingReads);
        }
        _logger = RaftInstrumentation.Logger(loggerFactory);
        _metricTags = [new("raft.group", _options.GroupId), new("raft.node", _options.NodeId)];
        _lastApplied = stateMachine.LastApplied;
        if (_lastApplied < 0 || _lastApplied > durable.HardState.CommitIndex)
            throw new InvalidDataException("Application checkpoint is outside the durable committed log.");
        if (stateMachine is ICheckpointStateMachine checkpointed)
        {
            var checkpoint = checkpointed.Checkpoint;
            if (checkpoint.Index != _lastApplied || ((checkpoint.Index == 0) != (checkpoint.Term == 0)) ||
                (legacyStorage is not null && checkpoint.Index > 0 && legacyStorage.GetTerm(checkpoint.Index) != checkpoint.Term))
                throw new InvalidDataException("Application checkpoint index/term differs from durable state.");
        }
        if (_options.SnapshotDirectory is not null)
        {
            if (_stateMachine is not ISnapshotStateMachine || !durable.SupportsConfiguration)
                throw new ArgumentException("Snapshots require a snapshot-capable application and v2 log storage.");
            _snapshots = new SnapshotRepository(_options);
        }
        else if (durable.Snapshot is not null) throw new ArgumentException("Configure the durable SnapshotDirectory before opening a compacted log.");
        _logStore = logStore ?? new RaftStorageAdapter(legacyStorage!);
        _core = new RaftDriver(_options, _logStore, durable, random,
            _snapshots is { } snapshots ? snapshots.ReadChunkAsync : null,
            _snapshots is not null ? StartSnapshotInstallation : null);
        _status = new(_options.NodeId, _core.Role, _core.Term, null, _core.CommitIndex, _core.LastIndex, _lastApplied)
            { Configuration = _core.State.Configuration, Snapshot = _core.State.Snapshot, Membership = _core.State.Membership?.Committed };
        _admission = new SemaphoreSlim(_options.MaxPendingProposals, _options.MaxPendingProposals);
        _readAdmission = new SemaphoreSlim(_options.MaxPendingReads, _options.MaxPendingReads);
        _work = Channel.CreateBounded<Work>(new BoundedChannelOptions(checked(_options.MaxPendingProposals + _options.Members.Count * 4 + 16))
        {
            SingleReader = true, FullMode = BoundedChannelFullMode.Wait, AllowSynchronousContinuations = false
        });
        _proposals = Channel.CreateBounded<ProposalWork>(new BoundedChannelOptions(_options.MaxPendingProposals)
        {
            SingleReader = true, FullMode = BoundedChannelFullMode.Wait, AllowSynchronousContinuations = false
        });
        _diagnosticRegistration = RaftInstrumentation.Register(this);
    }

    public RaftStatus Status
    {
        get
        {
            var status = Volatile.Read(ref _status);
            return status with { LastApplied = Math.Min(Interlocked.Read(ref _lastApplied), status.CommitIndex), Fault = Volatile.Read(ref _fault) };
        }
    }
    public NodeMetrics Metrics => new(Volatile.Read(ref _admitted), _rpcs.Count, Interlocked.Read(ref _transportFailures));
    /// <summary>Completes on shutdown, or faults on a storage/application failure. Observe this task in services.</summary>
    public Task Completion => _completion.Task;

    public Task StartAsync()
    {
        lock (_lifecycleGate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_supervisor is not null) throw new InvalidOperationException("A node instance can only start once.");
            _supervisor = SuperviseAsync();
            return Task.CompletedTask;
        }
    }

    /// <summary>Returns after quorum commit and local application. Retry deduplication belongs in application commands.</summary>
    public Task<ProposalResult> ProposeAsync(ReadOnlyMemory<byte> command, CancellationToken cancellationToken = default) =>
        SubmitAsync(command, false, cancellationToken);

    /// <summary>Commits and applies a current-term no-op, providing a quorum-backed fence for subsequent local reads.</summary>
    public Task<ProposalResult> ReadBarrierAsync(CancellationToken cancellationToken = default) =>
        SubmitAsync(ReadOnlyMemory<byte>.Empty, true, cancellationToken);

    /// <summary>Fresh quorum read fence without a log entry. Returns only after local application reaches it.</summary>
    public async Task<ReadIndexResult> ReadIndexAsync(CancellationToken cancellationToken = default)
    {
        EnsureRunning();
        if (_options.WireVersion < 2) throw new NotSupportedException("ReadIndex requires wire v2; use ReadBarrierAsync in v1 mode.");
        using var enqueueToken = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _lifetime.Token);
        await _readAdmission.WaitAsync(enqueueToken.Token).ConfigureAwait(false);
        var id = Interlocked.Increment(ref _controlId);
        var read = new PendingRead();
        _reads.TryAdd(id, read);
        try { await _work.Writer.WriteAsync(new ReadWork(id, cancellationToken), enqueueToken.Token).ConfigureAwait(false); }
        catch (Exception error) { FinishRead(id, error); throw; }
        return await read.Completion.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Stops proposals, catches up a voter, and waits to observe that target as the new leader.</summary>
    public async Task TransferLeadershipAsync(string nodeId, CancellationToken cancellationToken = default)
    {
        EnsureRunning();
        if (_options.WireVersion < 2) throw new NotSupportedException("Leadership transfer requires wire v2.");
        RaftOptions.ValidateId(nodeId, nameof(nodeId));
        using var enqueueToken = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _lifetime.Token);
        await _transferAdmission.WaitAsync(enqueueToken.Token).ConfigureAwait(false);
        var id = Interlocked.Increment(ref _controlId);
        var completion = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        _transfers.TryAdd(id, completion);
        try { await _work.Writer.WriteAsync(new TransferWork(id, nodeId), enqueueToken.Token).ConfigureAwait(false); }
        catch (Exception error) { FinishTransfer(id, error); throw; }
        await completion.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    public Task<ProposalResult> AddLearnerAsync(string nodeId, CancellationToken cancellationToken = default) =>
        _options.WireVersion == 3 ? LegacyMembershipAsync(nodeId, MembershipOperationKind.AddLearner, cancellationToken) :
            ChangeMembershipAsync(nodeId, MembershipOperation.AddLearner, cancellationToken);
    public Task<ProposalResult> PromoteToVoterAsync(string nodeId, CancellationToken cancellationToken = default) =>
        _options.WireVersion == 3 ? LegacyMembershipAsync(nodeId, MembershipOperationKind.Promote, cancellationToken) :
            ChangeMembershipAsync(nodeId, MembershipOperation.Promote, cancellationToken);
    public Task<ProposalResult> RemoveNodeAsync(string nodeId, CancellationToken cancellationToken = default) =>
        _options.WireVersion == 3 ? LegacyMembershipAsync(nodeId, MembershipOperationKind.RemoveNode, cancellationToken) :
            ChangeMembershipAsync(nodeId, MembershipOperation.Remove, cancellationToken);

    private async Task<ProposalResult> ChangeMembershipAsync(string nodeId, MembershipOperation operation, CancellationToken token)
    {
        EnsureRunning();
        RaftOptions.ValidateId(nodeId, nameof(nodeId));
        using var enqueueToken = CancellationTokenSource.CreateLinkedTokenSource(token, _lifetime.Token);
        if (!await _configurationAdmission.WaitAsync(0, enqueueToken.Token).ConfigureAwait(false))
            throw new InvalidOperationException("A configuration request is already pending.");
        var id = Interlocked.Increment(ref _controlId);
        var pending = new PendingConfiguration();
        _configurations.TryAdd(id, pending);
        try { await _work.Writer.WriteAsync(new ConfigurationWork(id, nodeId, operation), enqueueToken.Token).ConfigureAwait(false); }
        catch (Exception error) { FinishConfiguration(id, error); throw; }
        return await pending.Completion.Task.WaitAsync(token).ConfigureAwait(false);
    }

    private async Task<ProposalResult> EnqueueProposalAsync(ReadOnlyMemory<byte> command, bool isNoOp, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _lifetime.Token.ThrowIfCancellationRequested();
        if (!_admission.Wait(0))
            await WaitForProposalAdmissionAsync(cancellationToken).ConfigureAwait(false);
        Interlocked.Increment(ref _admitted);
        ProposalWork proposal;
        try { proposal = new ProposalWork(command.ToArray(), isNoOp, cancellationToken); }
        catch { Interlocked.Decrement(ref _admitted); _admission.Release(); throw; }
        try
        {
            lock (_proposalGate)
            {
                cancellationToken.ThrowIfCancellationRequested();
                _lifetime.Token.ThrowIfCancellationRequested();
                proposal.QueuedAt = System.Diagnostics.Stopwatch.GetTimestamp();
                // Admission counts queued plus in-flight commands, so an admitted
                // command always fits this equally bounded channel unless closed.
                if (!_proposals.Writer.TryWrite(proposal)) throw _fault ?? new ObjectDisposedException(nameof(RaftNode));
                _queuedProposalCount++;
                _queuedProposalBytes += proposal.Bytes.Length + LogEntry.EncodedOverhead;
                if (_queuedProposalCount == 1)
                { _firstProposalTimestamp = proposal.QueuedAt; _firstProposalBytes = proposal.Bytes.Length + LogEntry.EncodedOverhead; }
            }
        }
        catch { Release(proposal); throw; }
        // Queue ownership has transferred. Caller cancellation can only cancel waiting;
        // notification must survive it so no admitted proposal loses its wakeup.
        _proposalChanges.Writer.TryWrite(0);
        // Cancellation stops waiting; it cannot roll back a command that has reached the durable log.
        return await proposal.Completion.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask WaitForProposalAdmissionAsync(CancellationToken token)
    {
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(token, _lifetime.Token);
        await _admission.WaitAsync(stop.Token).ConfigureAwait(false);
    }

    /// <summary>Connect a transport's inbound handler here. Storage is durable before the reply completes.</summary>
    public async ValueTask<RaftRpc> HandleAsync(string from, RaftRpc request, CancellationToken cancellationToken = default)
    {
        EnsureRunning();
        request = RaftDriver.OwnRequest(_options, from, request, validateMembership: false);
        var completion = new TaskCompletionSource<RaftRpc>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var enqueueToken = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _lifetime.Token);
        await _work.Writer.WriteAsync(new RequestWork(from, request, completion), enqueueToken.Token).ConfigureAwait(false);
        // Once queued, the owner completes this request with the actual storage/shutdown error.
        // Canceling this wait with the lifetime token would race that error and hide it as cancellation.
        var response = await completion.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        if (request is SnapshotChunkRequest chunk && response is SnapshotChunkResponse { Success: true, Complete: false } allowed)
            return await ReceiveSnapshotChunkAsync(from, chunk, allowed.Term, cancellationToken).ConfigureAwait(false);
        return response;
    }

    private async Task SuperviseAsync()
    {
        var tasks = new[] { Task.Run(RunAsync), Task.Run(TicksAsync), Task.Run(CollectProposalsAsync), StartApplicationWorker() };
        try
        {
            var ended = await Task.WhenAny(tasks).ConfigureAwait(false);
            if (!_lifetime.IsCancellationRequested)
                Fail(ended.Exception?.GetBaseException() ?? new InvalidOperationException("A node worker exited unexpectedly."));
            await Task.WhenAll(tasks).ConfigureAwait(false);
        }
        catch (Exception error) { Fail(error); }
        await StopSnapshotWorkersAsync().ConfigureAwait(false);
        try { await Task.WhenAll(_rpcs.Values).ConfigureAwait(false); }
        catch (Exception error) { Fail(error); }
        var failure = Volatile.Read(ref _fault) ?? new ObjectDisposedException(nameof(RaftNode));
        while (_work.Reader.TryRead(out var remaining)) Reject(remaining, failure);
        while (_proposals.Reader.TryRead(out var queued)) Finish(queued, null, failure);
        foreach (var pair in _pending)
            if (_pending.TryRemove(pair.Key, out var proposal)) Finish(proposal, null, failure);
        foreach (var id in _reads.Keys) FinishRead(id, failure);
        foreach (var id in _transfers.Keys) FinishTransfer(id, failure);
        foreach (var id in _configurations.Keys) FinishConfiguration(id, failure);
        foreach (var id in _membershipRequests.Keys) FailMembershipRequest(id, failure);
        if (_fault is not null) _completion.TrySetException(_fault);
        else _completion.TrySetResult();
    }

    private async Task RunAsync()
    {
        Work? active = null;
        try
        {
            Publish();
            while (await _work.Reader.WaitToReadAsync(_lifetime.Token).ConfigureAwait(false))
            {
                while (TryReadWork(out active))
                {
                    _lifetime.Token.ThrowIfCancellationRequested();
                    switch (active)
                    {
                        case TickWork:
                            Interlocked.Exchange(ref _tickQueued, 0);
                            await _core.TickAsync(_lifetime.Token).ConfigureAwait(false);
                            MaybeCollectSnapshots();
                            break;
                        case RequestWork incoming:
                            // Shape was checked before admission; membership is checked against the owner's
                            // current config. Once execution begins, every storage error remains fatal.
                            if (!_core.Admits(incoming.From, incoming.Request))
                                incoming.Completion.TrySetException(new ArgumentException("RPC source is outside the active configuration."));
                            else await ProcessRequestsAsync(incoming).ConfigureAwait(false);
                            break;
                        case ResponseWork reply:
                            await ProcessResponsesAsync(reply).ConfigureAwait(false);
                            break;
                        case ProposalReadyWork:
                            if (TryTakeProposal(out var proposal))
                            {
                                active = proposal; // failure before batch ownership must still release this admission
                                await ProcessProposalsAsync(proposal).ConfigureAwait(false);
                            }
                            else
                            {
                                ProposalQueueTrace.Record(_options.NodeId, "EmptyDispatch", 0);
                                Interlocked.Exchange(ref _proposalQueued, 0);
                                _proposalChanges.Writer.TryWrite(0);
                            }
                            break;
                        case ReadWork read:
                            await ProcessReadsAsync(read).ConfigureAwait(false);
                            break;
                        case TransferWork transfer:
                            await _core.TransferAsync(transfer.Id, transfer.Target, _lifetime.Token).ConfigureAwait(false);
                            break;
                        case ConfigurationWork config:
                            await ProcessConfigurationAsync(config).ConfigureAwait(false);
                            break;
                        case MembershipWork membership:
                            await _core.StartMembershipAsync(membership.Id, membership.Request, _lifetime.Token).ConfigureAwait(false);
                            break;
                        case CancelMembershipWork cancel:
                            await _core.CancelMembershipAsync(cancel.Id, cancel.OperationId, _lifetime.Token).ConfigureAwait(false);
                            break;
                        case SnapshotCreatedWork snapshot:
                            await _core.SnapshotCreatedAsync(snapshot.ClientId, snapshot.Metadata, _lifetime.Token).ConfigureAwait(false);
                            break;
                        case SnapshotReadyWork snapshot:
                            await _core.SnapshotReadyAsync(snapshot.From, snapshot.Term, snapshot.Metadata, _lifetime.Token).ConfigureAwait(false);
                            break;
                        case SnapshotInstalledWork snapshot:
                            await _core.SnapshotCompletedAsync(snapshot.Id, snapshot.Metadata, _lifetime.Token).ConfigureAwait(false);
                            Interlocked.Exchange(ref _snapshotActivationPending, 0);
                            break;
                    }
                    active = null;
                    Publish();
                    if (_core.State.Membership?.Committed is { } directory && directory.Revision != _publishedDirectoryRevision)
                        await PublishDirectoryAsync(directory).ConfigureAwait(false);
                    DrainControls();
                    while (_core.TryDequeue(out var rpc)) Dispatch(rpc);
                }
            }
        }
        catch (OperationCanceledException) when (_lifetime.IsCancellationRequested) { }
        catch (Exception error) { Fail(error); }
        finally
        {
            if (active is not null) Reject(active, _fault ?? new ObjectDisposedException(nameof(RaftNode)));
        }
    }

    private bool TryReadWork(out Work? work)
        => _work.Reader.TryRead(out work);

    private bool TryTakeProposal([System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out ProposalWork? proposal)
    {
        lock (_proposalGate)
        {
            if (!_proposals.Reader.TryRead(out proposal)) return false;
            _queuedProposalCount--;
            _queuedProposalBytes -= proposal.Bytes.Length + LogEntry.EncodedOverhead;
            UpdateFirstProposal();
        }
        return true;
    }

    // Caller holds _proposalGate. Only the consensus owner reads the command channel.
    private void UpdateFirstProposal()
    {
        if (_proposals.Reader.TryPeek(out var first))
        { _firstProposalTimestamp = first.QueuedAt; _firstProposalBytes = first.Bytes.Length + LogEntry.EncodedOverhead; }
        else { _firstProposalTimestamp = 0; _firstProposalBytes = 0; }
    }

    private async Task CollectProposalsAsync()
    {
        // A reusable wake timer owns no consensus state. Its callback only signals
        // the bounded metadata channel; the worker rechecks the actual deadline.
        using var deadline = new Timer(static state =>
            ((ChannelWriter<byte>)state!).TryWrite(0), _proposalChanges.Writer,
            Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
        try
        {
            while (await _proposalChanges.Reader.WaitToReadAsync(_lifetime.Token).ConfigureAwait(false))
            {
                ProposalQueueTrace.Record(_options.NodeId, "CollectorWake");
                while (_proposalChanges.Reader.TryRead(out _)) { }
                var remaining = Timeout.InfiniteTimeSpan;
                var ready = false;
                lock (_proposalGate)
                {
                    var count = _queuedProposalCount;
                    if (count > 0 && Volatile.Read(ref _proposalQueued) == 0)
                    {
                        var firstBytes = _firstProposalBytes;
                        var status = Volatile.Read(ref _status);
                        var delay = (long)firstBytes * _options.MaxAppendEntries > _options.MaxAppendBytes || _queuedProposalBytes + firstBytes > _options.MaxAppendBytes
                            ? _options.BulkProposalBatchDelayLimit : _options.ProposalBatchDelay;
                        if (Volatile.Read(ref _admitted) <= count && delay > _options.ProposalBatchDelay)
                            delay = _options.ProposalBatchDelay;
                        if (status.Role != RaftRole.Leader || count >= _options.MaxAppendEntries || _queuedProposalBytes >= _options.ProposalBatchByteLimit)
                            delay = TimeSpan.Zero;
                        else if (status.Configuration?.Voters.Length == 1 && delay > TimeSpan.FromMilliseconds(1))
                            delay = TimeSpan.FromMilliseconds(1);
                        remaining = delay - System.Diagnostics.Stopwatch.GetElapsedTime(_firstProposalTimestamp);
                        if (remaining <= TimeSpan.Zero)
                        {
                            Volatile.Write(ref _proposalQueued, 1);
                            ready = true;
                            remaining = Timeout.InfiniteTimeSpan;
                        }
                    }
                }
                deadline.Change(remaining, Timeout.InfiniteTimeSpan);
                if (ready)
                {
                    ProposalQueueTrace.Record(_options.NodeId, "ReadySignal");
                    await _work.Writer.WriteAsync(ProposalMessage, _lifetime.Token).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException) when (_lifetime.IsCancellationRequested) { }
        catch (ChannelClosedException) when (_lifetime.IsCancellationRequested) { }
        catch (Exception error) { Fail(error); }
    }

    private async ValueTask ProcessRequestsAsync(RequestWork first)
    {
        if (!CanGroup(first) || !_work.Reader.TryPeek(out var peek) || peek is not RequestWork next ||
            !Follows(first, next) || ((AppendRequest)first.Request).Entries.Count + ((AppendRequest)next.Request).Entries.Count > _options.MaxAppendEntries ||
            AppendBytes((AppendRequest)first.Request) + AppendBytes((AppendRequest)next.Request) > _options.ProposalBatchByteLimit)
        {
            first.Completion.TrySetResult(await _core.HandleRequestAsync(first.From, first.Request, _lifetime.Token).ConfigureAwait(false));
            return;
        }
        var group = new List<RequestWork>(Math.Min(_options.MaxAppendEntries, 16)) { first };
        try
        {
            var count = ((AppendRequest)first.Request).Entries.Count;
            var bytes = AppendBytes((AppendRequest)first.Request);
            while (_work.Reader.TryPeek(out peek) && peek is RequestWork candidate && Follows(group[^1], candidate))
            {
                var append = (AppendRequest)candidate.Request;
                var addedBytes = AppendBytes(append);
                if (append.Entries.Count > _options.MaxAppendEntries - count || addedBytes > _options.ProposalBatchByteLimit - bytes) break;
                group.Add(candidate); // record ownership before removing it from the queue
                _work.Reader.TryRead(out _);
                count += append.Entries.Count;
                bytes += addedBytes;
            }
            var original = (AppendRequest)first.Request;
            var last = (AppendRequest)group[^1].Request;
            var entries = ImmutableArray.CreateBuilder<LogEntry>(count);
            foreach (var item in group)
                foreach (var entry in ((AppendRequest)item.Request).Entries) entries.Add(entry);
            var combined = new AppendRequest(original.Term, last.RequestId, original.PreviousIndex, original.PreviousTerm,
                last.LeaderCommit, entries.MoveToImmutable());
            var reply = await _core.HandleAppendGroupAsync(first.From, combined, _lifetime.Token).ConfigureAwait(false);
            foreach (var item in group)
            {
                var request = (AppendRequest)item.Request;
                item.Completion.TrySetResult(reply with { RequestId = request.RequestId,
                    MatchIndex = reply.Success ? request.PreviousIndex + request.Entries.Count : reply.MatchIndex });
            }
        }
        catch (Exception error)
        {
            foreach (var item in group) item.Completion.TrySetException(error);
            throw;
        }

        bool CanGroup(RequestWork item) => _options.WireVersion >= 2 && _core.State.SupportsConfiguration &&
            (_core.Role is RaftRole.Follower or RaftRole.Learner) && _core.State.ReadRound is null &&
            _core.State.InstallingSnapshot is null && Volatile.Read(ref _snapshotActivationPending) == 0 &&
            _core.State.Configuration.Index <= _core.CommitIndex && _core.State.Configuration.IsVoter(item.From) &&
            item.Request is AppendRequest append && Plain(append) && append.Term == _core.Term &&
            append.PreviousIndex == _core.LastIndex && append.PreviousTerm == _core.State.LastTerm;

        static bool Follows(RequestWork prior, RequestWork item) => item.From == prior.From &&
            prior.Request is AppendRequest previous && item.Request is AppendRequest append && Plain(append) &&
            append.Term == previous.Term && append.LeaderCommit >= previous.LeaderCommit &&
            append.PreviousIndex == previous.PreviousIndex + previous.Entries.Count &&
            append.PreviousTerm == previous.Entries[^1].Term;
    }

    private static bool Plain(AppendRequest request)
    {
        if (request.ReadContext != 0 || request.Entries.Count == 0) return false;
        foreach (var entry in request.Entries) if (entry.Kind is LogEntryKind.Configuration or LogEntryKind.Membership) return false;
        return true;
    }

    private static int AppendBytes(AppendRequest request)
    {
        var bytes = 0;
        foreach (var entry in request.Entries) bytes = checked(bytes + entry.Command.Length + LogEntry.EncodedOverhead);
        return bytes;
    }

    private async ValueTask ProcessResponsesAsync(ResponseWork first)
    {
        if (!CanOrder(first) || !_work.Reader.TryPeek(out var next) || next is not ResponseWork following || !CanOrder(following))
        { await Deliver(first).ConfigureAwait(false); return; }
        var limit = Math.Min(256, Math.Max(2, _options.MaxInFlightMessages * _core.State.Peers.Length));
        var batch = new List<ResponseWork>(Math.Min(limit, 16)) { first };
        while (batch.Count < limit && _work.Reader.TryPeek(out next) && next is ResponseWork response && CanOrder(response))
        { _work.Reader.TryRead(out _); batch.Add(response); }
        // Cumulative successes are valid in any network order. Deliver the longest
        // already-queued prefixes first so intermediate ACKs do not each force a commit.
        // Every reply still passes the core's correlation/current-term/quorum checks.
        batch.Sort(static (left, right) => ((AppendResponse)right.Response!).MatchIndex.CompareTo(((AppendResponse)left.Response!).MatchIndex));
        foreach (var response in batch) await Deliver(response).ConfigureAwait(false);
        return;

        bool CanOrder(ResponseWork response) => _core.Role == RaftRole.Leader && _core.State.ReadRound is null && _core.State.PendingReadCount == 0 &&
            _core.State.Transfer is null && _core.State.Configuration.Index <= _core.CommitIndex &&
            _core.State.Configuration.Contains(response.From) &&
            response.Request is AppendRequest { ReadContext: 0 } request && request.Term == _core.Term &&
            response.Response is AppendResponse { Success: true, ReadContext: 0 } reply && reply.Term == _core.Term;

        async ValueTask Deliver(ResponseWork response)
        {
            if (response.Response is null) await _core.ReportUnreachableAsync(response.From, response.Request, _lifetime.Token).ConfigureAwait(false);
            else await _core.HandleResponseAsync(response.From, response.Response, _lifetime.Token).ConfigureAwait(false);
        }
    }

    private async ValueTask ProcessProposalsAsync(ProposalWork first)
    {
        var proposals = new List<ProposalWork>(Math.Min(128, _options.MaxAppendEntries));
        var bytes = 0;
        try
        {
            Add(first);
            lock (_proposalGate)
            {
                while (proposals.Count < _options.MaxAppendEntries && _proposals.Reader.TryPeek(out var candidate))
                {
                    if (bytes + candidate.Bytes.Length + LogEntry.EncodedOverhead > _options.ProposalBatchByteLimit) break;
                    Add(candidate);
                    _proposals.Reader.TryRead(out _);
                    _queuedProposalCount--;
                    _queuedProposalBytes -= candidate.Bytes.Length + LogEntry.EncodedOverhead;
                }
                UpdateFirstProposal();
                Volatile.Write(ref _proposalQueued, 0);
            }
            _proposalChanges.Writer.TryWrite(0);
            ProposalQueueTrace.Record(_options.NodeId, "Group", proposals.Count);
            if (proposals.Count == 0) return;
            if (_core.Role != RaftRole.Leader || _core.State.Transfer is not null)
            {
                Exception error = _core.State.Transfer is { } transfer
                    ? new LeadershipTransferInProgressException(transfer.Target) : new NotLeaderException(_core.LeaderId);
                foreach (var proposal in proposals) Finish(proposal, null, error);
                return;
            }
            if (_core.State.UncommittedBytes > _options.MaxUncommittedBytes - bytes)
            {
                foreach (var proposal in proposals) Finish(proposal, null, new UncommittedLogLimitException(_options.MaxUncommittedBytes));
                return;
            }
            var commands = proposals.Select(p => new ProposedCommand(p.Bytes, p.IsNoOp)).ToArray();
            var start = await _core.ProposeAsync(commands, _lifetime.Token).ConfigureAwait(false);
            for (var i = 0; i < proposals.Count; i++)
            {
                proposals[i].Term = _core.Term;
                if (!_pending.TryAdd(start + i, proposals[i])) throw new InvalidOperationException("Duplicate pending log index.");
            }
        }
        catch (Exception error)
        {
            foreach (var proposal in proposals) Finish(proposal, null, error);
            throw;
        }
        return;

        void Add(ProposalWork proposal)
        {
            if (proposal.CallerToken.IsCancellationRequested)
            {
                proposal.Completion.TrySetCanceled(proposal.CallerToken);
                Release(proposal);
            }
            else
            {
                proposals.Add(proposal);
                bytes += proposal.Bytes.Length + LogEntry.EncodedOverhead;
            }
        }
    }

    private void Publish()
    {
        var prior = Volatile.Read(ref _status);
        ObserveTransition();
        Volatile.Write(ref _status, new(_options.NodeId, _core.Role, _core.Term, _core.LeaderId,
            _core.CommitIndex, _core.LastIndex, Interlocked.Read(ref _lastApplied))
        {
            LeadershipTransferTarget = _core.State.Transfer?.Target, Configuration = _core.State.Configuration,
            Peers = _core.State.Peers.Select(peer => new RaftPeerStatus(peer.Id, peer.MatchIndex, peer.NextIndex,
                _core.State.Configuration.IsVoter(peer.Id)) { Mode = peer.Mode,
                    InFlightMessages = peer.Flights.Length + (peer.SnapshotInFlight is null ? 0 : 1),
                    InFlightBytes = peer.InFlightBytes + (peer.SnapshotInFlight?.Data.Length ?? 0), SnapshotOffset = peer.SnapshotOffset }).ToImmutableArray(),
            Snapshot = _core.State.Snapshot, InstallingSnapshotIndex = _core.State.InstallingSnapshot?.Intent.Metadata.LastIncludedIndex,
            UncommittedBytes = _core.State.UncommittedBytes, Membership = _core.State.Membership?.Committed
        });
        if (_core.Role != RaftRole.Leader)
        {
            foreach (var pair in _pending)
                if (pair.Key > _core.CommitIndex && _pending.TryRemove(pair.Key, out var proposal))
                    Finish(proposal, null, new NotLeaderException(_core.LeaderId));
            foreach (var pair in _configurations)
                if (Interlocked.Read(ref pair.Value.Index) > _core.CommitIndex)
                    FinishConfiguration(pair.Key, new NotLeaderException(_core.LeaderId));
        }
        if (Volatile.Read(ref _queuedProposalCount) > 0 && (prior.Role != _core.Role ||
            prior.Configuration?.Voters.Length != _core.State.Configuration.Voters.Length || prior.CommitIndex != _core.CommitIndex))
            _proposalChanges.Writer.TryWrite(0);
        Interlocked.Exchange(ref _commitTarget, _core.ApplyThrough);
        if (_core.ApplyThrough > Interlocked.Read(ref _lastApplied)) _applySignal.Writer.TryWrite(0);
    }

    private async Task TicksAsync()
    {
        try
        {
            await using var timer = _clock.CreateTicker(_options.TickInterval);
            while (await timer.WaitForNextTickAsync(_lifetime.Token).ConfigureAwait(false))
            {
                if (Interlocked.CompareExchange(ref _tickQueued, 1, 0) == 0 && !_work.Writer.TryWrite(TickMessage))
                    Interlocked.Exchange(ref _tickQueued, 0);
            }
        }
        catch (OperationCanceledException) when (_lifetime.IsCancellationRequested) { }
        catch (Exception error) { Fail(error); }
    }

    private Task StartApplicationWorker()
    {
        var stopped = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        try
        {
            // Application callbacks may synchronously flush DBreeze. Isolate that
            // blocking work from Raft/TCP and DBreeze's own ThreadPool continuations.
            var thread = new Thread(() =>
            {
                try { Apply(); stopped.TrySetResult(); }
                catch (Exception error) { stopped.TrySetException(error); }
            }) { IsBackground = true, Name = "Raft apply " + _options.NodeId };
            thread.Start();
        }
        catch (Exception error) { Fail(error); stopped.TrySetException(error); }
        return stopped.Task;
    }

    private static T WaitOnApplicationThread<T>(ValueTask<T> operation) => operation.IsCompleted
        ? operation.GetAwaiter().GetResult() : operation.AsTask().GetAwaiter().GetResult();

    private static void WaitOnApplicationThread(ValueTask operation)
    {
        if (operation.IsCompleted) operation.GetAwaiter().GetResult();
        else operation.AsTask().GetAwaiter().GetResult();
    }

    private void Apply()
    {
        try
        {
            while (WaitOnApplicationThread(_applySignal.Reader.WaitToReadAsync(_lifetime.Token)))
            {
                while (_applySignal.Reader.TryRead(out _)) { }
                while (Interlocked.Read(ref _lastApplied) < Interlocked.Read(ref _commitTarget))
                {
                    _applyGate.Wait(_lifetime.Token);
                    try
                    {
                    if (Volatile.Read(ref _snapshotActivationPending) != 0) break;
                    var applied = Interlocked.Read(ref _lastApplied);
                    var count = (int)Math.Min(_options.MaxAppendEntries, Interlocked.Read(ref _commitTarget) - applied);
                    var read = WaitOnApplicationThread(_logStore.ReadAsync(new LogQuery.Entries(applied + 1, count, _options.MaxAppendBytes),
                        _lifetime.Token));
                    var entries = read.Entries;
                    if (_stateMachine is IBatchStateMachine batched)
                    {
                        WaitOnApplicationThread(ApplyBatchMeasuredAsync(batched, entries, _lifetime.Token));
                        if (_stateMachine.LastApplied != entries[^1].Index)
                            throw new InvalidOperationException("Batch state machine must advance LastApplied through the batch.");
                        ValidateAppliedCheckpoint(entries[^1]);
                        foreach (var entry in entries) Applied(entry);
                        CompleteReadyReads();
                        continue;
                    }
                    foreach (var entry in entries)
                    {
                        _lifetime.Token.ThrowIfCancellationRequested();
                        WaitOnApplicationThread(ApplyEntryMeasuredAsync(entry, _lifetime.Token));
                        if (_stateMachine.LastApplied != entry.Index)
                            throw new InvalidOperationException("State machine must advance LastApplied to the entry it applied.");
                        ValidateAppliedCheckpoint(entry);
                        Applied(entry);
                        CompleteReadyReads();
                    }
                    }
                    finally { _applyGate.Release(); }
                }
            }
        }
        catch (OperationCanceledException) when (_lifetime.IsCancellationRequested) { }
        catch (Exception error) { Fail(error); }
    }

    private void Applied(LogEntry entry)
    {
        Interlocked.Exchange(ref _lastApplied, entry.Index);
        if (entry.Index >= Interlocked.Read(ref _commitTarget) && Volatile.Read(ref _queuedProposalCount) > 0)
            _proposalChanges.Writer.TryWrite(0);
        if (_pending.TryRemove(entry.Index, out var proposal)) Finish(proposal, new(entry.Index, entry.Term), null);
        MaybeStartAutomaticSnapshot(entry.Index);
    }

    private void ValidateAppliedCheckpoint(LogEntry entry)
    {
        if (_stateMachine is ICheckpointStateMachine machine && machine.Checkpoint != new AppliedCheckpoint(entry.Index, entry.Term))
            throw new InvalidOperationException("State machine must publish the applied index and term together.");
    }

    private void Dispatch(OutboundRpc rpc)
    {
        var id = Interlocked.Increment(ref _rpcId);
        var task = SendAsync(rpc);
        _rpcs.TryAdd(id, task);
        _ = task.ContinueWith(completed =>
        {
            if (completed.IsFaulted && !_lifetime.IsCancellationRequested) Fail(completed.Exception!.GetBaseException());
            _rpcs.TryRemove(id, out var ignored);
        },
            CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
    }

    private async Task SendAsync(OutboundRpc rpc)
    {
        RaftRpc? response = null;
        if (rpc.Request is SnapshotChunkRequest chunk)
        {
            try { await ThrottleSnapshotAsync(chunk.Data.Length, _lifetime.Token).ConfigureAwait(false); }
            catch (OperationCanceledException) when (_lifetime.IsCancellationRequested) { return; }
            RaftInstrumentation.SnapshotBytes(_options.GroupId, _options.NodeId, rpc.PeerId, chunk.Data.Length, send: true);
        }
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(_lifetime.Token);
        deadline.CancelAfter(_options.RpcTimeout);
        try
        {
            response = await _transport.SendAsync(rpc.PeerId, rpc.Request, deadline.Token).AsTask()
                .WaitAsync(deadline.Token).ConfigureAwait(false);
            if (!RaftRpcProtocol.IsExpectedResponse(rpc.Request, response))
                throw new InvalidDataException("Unexpected Raft RPC response type.");
        }
        catch (Exception error) when (!_lifetime.IsCancellationRequested)
        {
            response = null;
            Interlocked.Increment(ref _transportFailures);
            RaftInstrumentation.RpcFailed(_options.GroupId, _options.NodeId, rpc.PeerId);
            RaftInstrumentation.RpcLog(_logger, _options.GroupId, _options.NodeId, rpc.PeerId, error);
        }
        catch (Exception) when (_lifetime.IsCancellationRequested) { return; }
        try { await ReportRpcCompletionAsync(rpc.PeerId, rpc.Request, response).ConfigureAwait(false); }
        catch (OperationCanceledException) when (_lifetime.IsCancellationRequested) { }
        catch (ChannelClosedException) { }
    }

    // The transport completion enters the same bounded owner queue as other events.
    // Kept internal so fault tests can deterministically duplicate/reorder real completions.
    internal ValueTask ReportRpcCompletionAsync(string peerId, RaftRpc request, RaftRpc? response) =>
        _work.Writer.WriteAsync(new ResponseWork(peerId, request, response), _lifetime.Token);

    private void Fail(Exception error)
    {
        if (Interlocked.CompareExchange(ref _fault, error, null) is not null) return;
        _work.Writer.TryComplete(error);
        _proposals.Writer.TryComplete(error);
        _proposalChanges.Writer.TryComplete(error);
        _applySignal.Writer.TryComplete(error);
        _lifetime.Cancel();
        RaftInstrumentation.Failed(_options.GroupId, _options.NodeId);
        RaftInstrumentation.Log(() => RaftLog.Fault(_logger, _options.GroupId, _options.NodeId, error));
    }

    private void Reject(Work work, Exception error)
    {
        if (work is ProposalWork proposal) Finish(proposal, null, error);
        else if (work is RequestWork request) request.Completion.TrySetException(error);
        else if (work is ReadWork read) FinishRead(read.Id, error);
        else if (work is TransferWork transfer) FinishTransfer(transfer.Id, error);
        else if (work is ConfigurationWork config) FinishConfiguration(config.Id, error);
        else if (work is MembershipWork membership) FailMembershipRequest(membership.Id, error);
        else if (work is CancelMembershipWork cancel) FailMembershipRequest(cancel.Id, error);
    }

    private async ValueTask ProcessConfigurationAsync(ConfigurationWork work)
    {
        var config = _core.State.Configuration;
        var voters = config.Voters;
        var learners = config.Learners;
        switch (work.Operation)
        {
            case MembershipOperation.AddLearner: learners = learners.Add(work.NodeId); break;
            case MembershipOperation.Promote: learners = learners.Remove(work.NodeId); voters = voters.Add(work.NodeId); break;
            case MembershipOperation.Remove: learners = learners.Remove(work.NodeId); voters = voters.Remove(work.NodeId); break;
        }
        await _core.ChangeConfigurationAsync(work.Id, voters, learners, _lifetime.Token).ConfigureAwait(false);
    }

    private async ValueTask ProcessReadsAsync(ReadWork first)
    {
        var ids = new List<long>();
        Add(first);
        while (ids.Count < _options.MaxAppendEntries && _work.Reader.TryPeek(out var next) && next is ReadWork read)
        {
            _work.Reader.TryRead(out _);
            Add(read);
        }
        if (ids.Count != 0) await _core.ReadIndexAsync(ids, _lifetime.Token).ConfigureAwait(false);
        return;
        void Add(ReadWork read)
        {
            if (read.CallerToken.IsCancellationRequested) FinishRead(read.Id, new OperationCanceledException(read.CallerToken));
            else ids.Add(read.Id);
        }
    }
    private void DrainControls()
    {
        while (_core.TryDequeueControl(out var result))
        {
            switch (result)
            {
                case ControlResult.Read read:
                    if (read.Error is not null) FinishRead(read.ClientId, ControlError(read.Error));
                    else if (_reads.TryGetValue(read.ClientId, out var pending))
                    {
                        Interlocked.Exchange(ref pending.Term, read.Term);
                        Interlocked.Exchange(ref pending.Index, read.Index);
                        TryFinishRead(read.ClientId, pending);
                    }
                    break;
                case ControlResult.Membership membership:
                    CompleteMembership(membership);
                    break;
                case ControlResult.Transfer transfer:
                    FinishTransfer(transfer.ClientId, transfer.Error is null ? null : ControlError(transfer.Error));
                    break;
                case ControlResult.Configuration config:
                    if (config.Error is not null) FinishConfiguration(config.ClientId, ControlError(config.Error));
                    else if (_configurations.TryGetValue(config.ClientId, out var pendingConfig))
                    {
                        Interlocked.Exchange(ref pendingConfig.Term, config.Term);
                        Interlocked.Exchange(ref pendingConfig.Index, config.Index);
                        if (_core.Role != RaftRole.Leader && config.Index > _core.CommitIndex)
                            FinishConfiguration(config.ClientId, new NotLeaderException(_core.LeaderId));
                        else TryFinishConfiguration(config.ClientId, pendingConfig);
                    }
                    break;
                case ControlResult.Snapshot snapshot:
                    if (_snapshotCompletions.TryRemove(snapshot.ClientId, out var completion)) completion.TrySetResult(snapshot);
                    break;
            }
        }
    }
    private Exception ControlError(CoreFailure error) => error.Type switch
    {
        "NotLeader" => new NotLeaderException(_core.LeaderId),
        "Timeout" => new TimeoutException(error.Message),
        "Argument" => new ArgumentException(error.Message),
        "Busy" when _options.WireVersion == 3 => new MembershipControlBusyException(error.Message),
        _ => new InvalidOperationException(error.Message)
    };
    private void CompleteReadyReads()
    {
        foreach (var pair in _reads) TryFinishRead(pair.Key, pair.Value);
        foreach (var pair in _configurations) TryFinishConfiguration(pair.Key, pair.Value);
    }
    private void TryFinishConfiguration(long id, PendingConfiguration pending)
    {
        var index = Interlocked.Read(ref pending.Index);
        if (index < 0 || Interlocked.Read(ref _lastApplied) < index || !_configurations.TryRemove(id, out _)) return;
        pending.Completion.TrySetResult(new(index, Interlocked.Read(ref pending.Term)));
        _configurationAdmission.Release();
    }
    private void FinishConfiguration(long id, Exception error)
    {
        if (!_configurations.TryRemove(id, out var pending)) return;
        pending.Completion.TrySetException(error);
        _configurationAdmission.Release();
    }
    private void TryFinishRead(long id, PendingRead read)
    {
        var index = Interlocked.Read(ref read.Index);
        if (index < 0 || Interlocked.Read(ref _lastApplied) < index || !_reads.TryRemove(id, out _)) return;
        read.Completion.TrySetResult(new(index, Interlocked.Read(ref read.Term)));
        _readAdmission.Release();
    }
    private void FinishRead(long id, Exception error)
    {
        if (!_reads.TryRemove(id, out var read)) return;
        if (error is OperationCanceledException canceled) read.Completion.TrySetCanceled(canceled.CancellationToken);
        else read.Completion.TrySetException(error);
        _readAdmission.Release();
    }
    private void FinishTransfer(long id, Exception? error)
    {
        if (!_transfers.TryRemove(id, out var completion)) return;
        if (error is null) completion.TrySetResult(true); else completion.TrySetException(error);
        _transferAdmission.Release();
    }

    private void Finish(ProposalWork proposal, ProposalResult? result, Exception? error)
    {
        if (error is not null) proposal.Completion.TrySetException(error);
        else proposal.Completion.TrySetResult(result!.Value);
        Release(proposal);
    }

    private void Release(ProposalWork proposal)
    {
        if (Interlocked.Exchange(ref proposal.Released, 1) != 0) return;
        Interlocked.Decrement(ref _admitted);
        _admission.Release();
    }

    private void EnsureRunning()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_fault is { } error) throw new InvalidOperationException("Raft node has faulted.", error);
        if (_supervisor is null) throw new InvalidOperationException("Start the Raft node first.");
        if (_lifetime.IsCancellationRequested) throw new ObjectDisposedException(nameof(RaftNode));
    }

    public ValueTask DisposeAsync()
    {
        lock (_lifecycleGate)
        {
            _disposed = true;
            return new(_disposeTask ??= DisposeCoreAsync());
        }
    }

    private async Task DisposeCoreAsync()
    {
        _lifetime.Cancel();
        _work.Writer.TryComplete();
        _proposals.Writer.TryComplete();
        _proposalChanges.Writer.TryComplete();
        _applySignal.Writer.TryComplete();
        if (_supervisor is not null) await _supervisor.ConfigureAwait(false);
        else _completion.TrySetResult();
        try
        {
            await _logStore.DisposeAsync().ConfigureAwait(false);
            if (_snapshots is not null) await _snapshots.DisposeAsync().ConfigureAwait(false);
        }
        finally { RaftInstrumentation.Unregister(_diagnosticRegistration); }
        // Admission waiters can still be unwinding linked cancellation; do not dispose their semaphore/CTS underneath them.
    }
}
