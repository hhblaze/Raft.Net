using System.Collections.Concurrent;
using System.Threading.Channels;
using Raft.Core;
using Raft.Storage;

namespace Raft;

public readonly record struct NodeMetrics(int PendingProposals, int InFlightRpcs, long TransportFailures);

/// <summary>Bounded asynchronous host for one fixed-membership Raft group. Owns the supplied storage.</summary>
public sealed class RaftNode : IAsyncDisposable
{
    private abstract record Work;
    private sealed record TickWork : Work;
    private sealed record RequestWork(string From, RaftRpc Request, TaskCompletionSource<RaftRpc> Completion) : Work;
    private sealed record ResponseWork(string From, RaftRpc Request, RaftRpc? Response) : Work;
    private sealed record ProposalWork(byte[] Bytes, bool IsNoOp, CancellationToken CallerToken) : Work
    {
        public readonly TaskCompletionSource<ProposalResult> Completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int Released;
    }

    private static readonly TickWork TickMessage = new();
    private readonly object _lifecycleGate = new();
    private readonly RaftOptions _options;
    private readonly IRaftStorage _storage;
    private readonly IRaftTransport _transport;
    private readonly IRaftStateMachine _stateMachine;
    private readonly RaftCore _core;
    private readonly Channel<Work> _work;
    private readonly Channel<byte> _applySignal = Channel.CreateBounded<byte>(new BoundedChannelOptions(1)
    {
        SingleReader = true, SingleWriter = true, FullMode = BoundedChannelFullMode.DropOldest
    });
    private readonly SemaphoreSlim _admission;
    private readonly CancellationTokenSource _lifetime = new();
    private readonly ConcurrentDictionary<long, ProposalWork> _pending = new();
    private readonly ConcurrentDictionary<long, Task> _rpcs = new();
    private readonly TaskCompletionSource _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private RaftStatus _status;
    private Exception? _fault;
    private Task? _supervisor;
    private Task? _disposeTask;
    private long _commitTarget, _lastApplied, _rpcId, _transportFailures;
    private int _tickQueued, _admitted;
    private bool _disposed;

    public RaftNode(RaftOptions options, IRaftStorage storage, IRaftTransport transport, IRaftStateMachine stateMachine)
    {
        _options = options.Freeze();
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        _transport = transport ?? throw new ArgumentNullException(nameof(transport));
        _stateMachine = stateMachine ?? throw new ArgumentNullException(nameof(stateMachine));
        _core = new RaftCore(_options, storage);
        _lastApplied = stateMachine.LastApplied;
        if (_lastApplied < 0 || _lastApplied > _core.CommitIndex)
            throw new InvalidDataException("Application checkpoint is outside the durable committed log.");
        _status = new(_options.NodeId, _core.Role, _core.Term, null, _core.CommitIndex, _core.LastIndex, _lastApplied);
        _admission = new SemaphoreSlim(_options.MaxPendingProposals, _options.MaxPendingProposals);
        _work = Channel.CreateBounded<Work>(new BoundedChannelOptions(checked(_options.MaxPendingProposals + _options.Members.Count * 4 + 16))
        {
            SingleReader = true, FullMode = BoundedChannelFullMode.Wait, AllowSynchronousContinuations = false
        });
    }

    public RaftStatus Status => Volatile.Read(ref _status) with
    {
        LastApplied = Interlocked.Read(ref _lastApplied), Fault = Volatile.Read(ref _fault)
    };
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

    private async Task<ProposalResult> SubmitAsync(ReadOnlyMemory<byte> command, bool isNoOp, CancellationToken cancellationToken)
    {
        EnsureRunning();
        if (command.Length > _options.MaxCommandBytes) throw new ArgumentException("Command exceeds MaxCommandBytes.", nameof(command));
        using var enqueueToken = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _lifetime.Token);
        await _admission.WaitAsync(enqueueToken.Token).ConfigureAwait(false);
        Interlocked.Increment(ref _admitted);
        ProposalWork proposal;
        try { proposal = new ProposalWork(command.ToArray(), isNoOp, cancellationToken); }
        catch { Interlocked.Decrement(ref _admitted); _admission.Release(); throw; }
        try { await _work.Writer.WriteAsync(proposal, enqueueToken.Token).ConfigureAwait(false); }
        catch { Release(proposal); throw; }
        // Cancellation stops waiting; it cannot roll back a command that has reached the durable log.
        return await proposal.Completion.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Connect a transport's inbound handler here. Storage is durable before the reply completes.</summary>
    public async ValueTask<RaftRpc> HandleAsync(string from, RaftRpc request, CancellationToken cancellationToken = default)
    {
        EnsureRunning();
        var completion = new TaskCompletionSource<RaftRpc>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var enqueueToken = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _lifetime.Token);
        await _work.Writer.WriteAsync(new RequestWork(from, request, completion), enqueueToken.Token).ConfigureAwait(false);
        // Once queued, the owner completes this request with the actual storage/shutdown error.
        // Canceling this wait with the lifetime token would race that error and hide it as cancellation.
        return await completion.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task SuperviseAsync()
    {
        var tasks = new[] { Task.Run(RunAsync), Task.Run(TicksAsync), Task.Run(ApplyAsync) };
        await Task.WhenAll(tasks).ConfigureAwait(false);
        await Task.WhenAll(_rpcs.Values).ConfigureAwait(false);
        var failure = Volatile.Read(ref _fault) ?? new ObjectDisposedException(nameof(RaftNode));
        while (_work.Reader.TryRead(out var remaining)) Reject(remaining, failure);
        foreach (var pair in _pending)
            if (_pending.TryRemove(pair.Key, out var proposal)) Finish(proposal, null, failure);
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
                while (_work.Reader.TryRead(out active))
                {
                    _lifetime.Token.ThrowIfCancellationRequested();
                    switch (active)
                    {
                        case TickWork:
                            Interlocked.Exchange(ref _tickQueued, 0);
                            _core.Tick();
                            break;
                        case RequestWork incoming:
                            try { incoming.Completion.TrySetResult(_core.HandleRequest(incoming.From, incoming.Request)); }
                            catch (ArgumentException invalid) { incoming.Completion.TrySetException(invalid); }
                            break;
                        case ResponseWork reply:
                            if (reply.Response is null) _core.ReportUnreachable(reply.From, reply.Request);
                            else _core.HandleResponse(reply.From, reply.Response);
                            break;
                        case ProposalWork proposal:
                            ProcessProposals(proposal);
                            break;
                    }
                    active = null;
                    Publish();
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

    private void ProcessProposals(ProposalWork first)
    {
        var proposals = new List<ProposalWork>(Math.Min(128, _options.MaxAppendEntries));
        var bytes = 0;
        Add(first);
        while (proposals.Count < _options.MaxAppendEntries && _work.Reader.TryPeek(out var next) && next is ProposalWork proposal &&
               bytes + proposal.Bytes.Length + LogEntry.EncodedOverhead <= _options.MaxAppendBytes)
        {
            _work.Reader.TryRead(out _);
            Add(proposal);
        }
        if (proposals.Count == 0) return;
        if (_core.Role != RaftRole.Leader)
        {
            foreach (var proposal in proposals) Finish(proposal, null, new NotLeaderException(_core.LeaderId));
            return;
        }
        try
        {
            var commands = proposals.Select(p => new ProposedCommand(p.Bytes, p.IsNoOp)).ToArray();
            var start = _core.Propose(commands);
            for (var i = 0; i < proposals.Count; i++)
                if (!_pending.TryAdd(start + i, proposals[i])) throw new InvalidOperationException("Duplicate pending log index.");
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
        Volatile.Write(ref _status, new(_options.NodeId, _core.Role, _core.Term, _core.LeaderId,
            _core.CommitIndex, _core.LastIndex, Interlocked.Read(ref _lastApplied)));
        if (_core.Role != RaftRole.Leader)
        {
            foreach (var pair in _pending)
                if (pair.Key > _core.CommitIndex && _pending.TryRemove(pair.Key, out var proposal))
                    Finish(proposal, null, new NotLeaderException(_core.LeaderId));
        }
        Interlocked.Exchange(ref _commitTarget, _core.CommitIndex);
        if (_core.CommitIndex > Interlocked.Read(ref _lastApplied)) _applySignal.Writer.TryWrite(0);
    }

    private async Task TicksAsync()
    {
        try
        {
            using var timer = new PeriodicTimer(_options.TickInterval);
            while (await timer.WaitForNextTickAsync(_lifetime.Token).ConfigureAwait(false))
            {
                if (Interlocked.CompareExchange(ref _tickQueued, 1, 0) == 0 && !_work.Writer.TryWrite(TickMessage))
                    Interlocked.Exchange(ref _tickQueued, 0);
            }
        }
        catch (OperationCanceledException) when (_lifetime.IsCancellationRequested) { }
        catch (Exception error) { Fail(error); }
    }

    private async Task ApplyAsync()
    {
        try
        {
            while (await _applySignal.Reader.WaitToReadAsync(_lifetime.Token).ConfigureAwait(false))
            {
                while (_applySignal.Reader.TryRead(out _)) { }
                while (Interlocked.Read(ref _lastApplied) < Interlocked.Read(ref _commitTarget))
                {
                    var applied = Interlocked.Read(ref _lastApplied);
                    var count = (int)Math.Min(_options.MaxAppendEntries, Interlocked.Read(ref _commitTarget) - applied);
                    var entries = _storage.Read(applied + 1, count, _options.MaxAppendBytes);
                    if (_stateMachine is IRaftBatchStateMachine batched)
                    {
                        await batched.ApplyBatchAsync(entries, _lifetime.Token).ConfigureAwait(false);
                        if (_stateMachine.LastApplied != entries[^1].Index)
                            throw new InvalidOperationException("Batch state machine must advance LastApplied through the batch.");
                        foreach (var entry in entries) Applied(entry);
                        continue;
                    }
                    foreach (var entry in entries)
                    {
                        _lifetime.Token.ThrowIfCancellationRequested();
                        await _stateMachine.ApplyAsync(entry, _lifetime.Token).ConfigureAwait(false);
                        if (_stateMachine.LastApplied != entry.Index)
                            throw new InvalidOperationException("State machine must advance LastApplied to the entry it applied.");
                        Applied(entry);
                    }
                }
            }
        }
        catch (OperationCanceledException) when (_lifetime.IsCancellationRequested) { }
        catch (Exception error) { Fail(error); }
    }

    private void Applied(LogEntry entry)
    {
        Interlocked.Exchange(ref _lastApplied, entry.Index);
        if (_pending.TryRemove(entry.Index, out var proposal)) Finish(proposal, new(entry.Index, entry.Term), null);
    }

    private void Dispatch(OutboundRpc rpc)
    {
        var id = Interlocked.Increment(ref _rpcId);
        var task = SendAsync(rpc);
        _rpcs.TryAdd(id, task);
        _ = task.ContinueWith(completed => { _rpcs.TryRemove(id, out var ignored); },
            CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
    }

    private async Task SendAsync(OutboundRpc rpc)
    {
        RaftRpc? response = null;
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(_lifetime.Token);
        deadline.CancelAfter(_options.RpcTimeout);
        try
        {
            response = await _transport.SendAsync(rpc.PeerId, rpc.Request, deadline.Token).AsTask()
                .WaitAsync(deadline.Token).ConfigureAwait(false);
            if ((rpc.Request is AppendRequest && response is not AppendResponse) ||
                (rpc.Request is VoteRequest && response is not VoteResponse))
                throw new InvalidDataException("Unexpected Raft RPC response type.");
        }
        catch (Exception) when (!_lifetime.IsCancellationRequested)
        {
            response = null;
            Interlocked.Increment(ref _transportFailures);
        }
        catch (Exception) when (_lifetime.IsCancellationRequested) { return; }
        try { await _work.Writer.WriteAsync(new ResponseWork(rpc.PeerId, rpc.Request, response), _lifetime.Token).ConfigureAwait(false); }
        catch (OperationCanceledException) when (_lifetime.IsCancellationRequested) { }
        catch (ChannelClosedException) { }
    }

    private void Fail(Exception error)
    {
        if (Interlocked.CompareExchange(ref _fault, error, null) is not null) return;
        _work.Writer.TryComplete(error);
        _applySignal.Writer.TryComplete(error);
        _lifetime.Cancel();
    }

    private void Reject(Work work, Exception error)
    {
        if (work is ProposalWork proposal) Finish(proposal, null, error);
        else if (work is RequestWork request) request.Completion.TrySetException(error);
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
        _applySignal.Writer.TryComplete();
        if (_supervisor is not null) await _supervisor.ConfigureAwait(false);
        else _completion.TrySetResult();
        _storage.Dispose();
        // Admission waiters can still be unwinding linked cancellation; do not dispose their semaphore/CTS underneath them.
    }
}
