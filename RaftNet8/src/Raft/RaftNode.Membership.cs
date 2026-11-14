using System.Collections.Concurrent;
using Raft.Storage;
using Raft.Core;

namespace Raft;

public sealed partial class RaftNode
{
    private sealed record MembershipWork(long Id, MembershipOperationStatus Request) : Work;
    private sealed record CancelMembershipWork(long Id, Guid OperationId) : Work;
    private sealed class MembershipControlBusyException(string message) : InvalidOperationException(message);
    private readonly ConcurrentDictionary<long, TaskCompletionSource<MembershipOperationStatus>> _membershipRequests = new();
    private SemaphoreSlim? _membershipRequestAdmission;
    private long _publishedDirectoryRevision = -1;

    public IReadOnlyList<PeerDescriptor> PeerDirectory => Status.Membership?.Peers ?? [];

    /// <summary>Registers a new ID, adds a learner, catches it up and commits promotion. Caller cancellation only stops waiting.</summary>
    public Task<MembershipOperationStatus> AddNodeAsync(PeerDescriptor peer, Guid operationId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(peer);
        peer.Validate();
        return MembershipOperationAsync(new(operationId, MembershipOperationKind.AddNode, peer.NodeId, peer,
            MembershipOperationStage.Accepted, 0, 0), cancellationToken);
    }

    /// <summary>Transfers first when removing the current leader, then commits removal on its successor.</summary>
    public Task<MembershipOperationStatus> RemoveNodeAsync(string nodeId, Guid operationId, CancellationToken cancellationToken = default) =>
        MembershipOperationAsync(new(operationId, MembershipOperationKind.RemoveNode, nodeId, null,
            MembershipOperationStage.Accepted, 0, 0), cancellationToken);

    /// <summary>A fresh leader/quorum read of durable operation state, or null for an unknown ID.</summary>
    public async Task<MembershipOperationStatus?> GetMembershipOperationAsync(Guid operationId, CancellationToken cancellationToken = default)
    {
        RequireMembership(operationId);
        await MembershipReadFenceAsync(cancellationToken).ConfigureAwait(false);
        LogReadResult result;
        try { result = await _logStore.ReadAsync(new LogQuery.MembershipOperation(operationId), cancellationToken).ConfigureAwait(false); }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested || _lifetime.IsCancellationRequested) { throw; }
        catch (Exception error) { Fail(error); throw; }
        if (result.MembershipOperation is { } operation && operation.Index > Interlocked.Read(ref _lastApplied))
            await MembershipReadFenceAsync(cancellationToken).ConfigureAwait(false);
        return result.MembershipOperation;
    }

    /// <summary>Journaled cancellation of an Add before its final voter configuration. Waits for learner cleanup.</summary>
    public async Task<MembershipOperationStatus> CancelMembershipOperationAsync(Guid operationId, CancellationToken cancellationToken = default)
    {
        RequireMembership(operationId);
        await SubmitMembershipWithRetryAsync(null, operationId, cancellationToken).ConfigureAwait(false);
        return await AwaitMembershipAsync(operationId, cancellationToken).ConfigureAwait(false);
    }

    private async Task<MembershipOperationStatus> MembershipOperationAsync(MembershipOperationStatus request, CancellationToken token)
    {
        RequireMembership(request.OperationId);
        // Shape/business validation precedes queue admission. Execution failures are not reclassified.
        MembershipCodec.ValidateStatus(request);
        await SubmitMembershipWithRetryAsync(request, request.OperationId, token).ConfigureAwait(false);
        return await AwaitMembershipAsync(request.OperationId, token).ConfigureAwait(false);
    }

    private void RequireMembership(Guid operationId)
    {
        EnsureRunning();
        if (operationId == Guid.Empty) throw new ArgumentException("Use a non-empty stable operation ID.", nameof(operationId));
        if (_options.WireVersion != 3 || _membershipRequestAdmission is null)
            throw new NotSupportedException("Hot membership requires wire3, raft8-v4 and a dynamic directory transport.");
    }

    private async Task MembershipReadFenceAsync(CancellationToken token)
    {
        while (true)
        {
            try { await ReadIndexAsync(token).ConfigureAwait(false); return; }
            catch (MembershipControlBusyException) { await Task.Delay(_options.TickInterval, token).ConfigureAwait(false); }
        }
    }

    private async Task SubmitMembershipAsync(MembershipOperationStatus? request, Guid operationId, CancellationToken token)
    {
        var admission = _membershipRequestAdmission!;
        using var enqueue = CancellationTokenSource.CreateLinkedTokenSource(token, _lifetime.Token);
        await admission.WaitAsync(enqueue.Token).ConfigureAwait(false);
        var id = Interlocked.Increment(ref _controlId);
        var completion = new TaskCompletionSource<MembershipOperationStatus>(TaskCreationOptions.RunContinuationsAsynchronously);
        _membershipRequests[id] = completion;
        try
        {
            await _work.Writer.WriteAsync(request is null ? new CancelMembershipWork(id, operationId) : new MembershipWork(id, request), enqueue.Token).ConfigureAwait(false);
            await completion.Task.WaitAsync(token).ConfigureAwait(false);
        }
        finally
        {
            _membershipRequests.TryRemove(id, out _);
            if (completion.Task.IsFaulted) _ = completion.Task.Exception;
            admission.Release();
        }
    }

    private async Task SubmitMembershipWithRetryAsync(MembershipOperationStatus? request, Guid operationId, CancellationToken token)
    {
        while (true)
        {
            try { await SubmitMembershipAsync(request, operationId, token).ConfigureAwait(false); return; }
            catch (MembershipControlBusyException) { await Task.Delay(_options.TickInterval, token).ConfigureAwait(false); }
        }
    }

    private async Task<MembershipOperationStatus> AwaitMembershipAsync(Guid operationId, CancellationToken token)
    {
        while (true)
        {
            token.ThrowIfCancellationRequested();
            var status = Status;
            if (status.Role != RaftRole.Leader) throw new NotLeaderException(status.LeaderId);
            if (status.LeadershipTransferTarget is null)
            {
                var operation = await GetMembershipOperationAsync(operationId, token).ConfigureAwait(false);
                if (operation?.IsTerminal == true) return operation;
            }
            await Task.Delay(_options.TickInterval, token).ConfigureAwait(false);
        }
    }

    private async Task<ProposalResult> LegacyMembershipAsync(string nodeId, MembershipOperationKind kind, CancellationToken token)
    {
        RaftOptions.ValidateId(nodeId, nameof(nodeId));
        PeerDescriptor? peer = null;
        if (kind == MembershipOperationKind.AddLearner)
            peer = PeerDirectory.FirstOrDefault(value => value.NodeId == nodeId && !value.IsRetired)
                ?? throw new ArgumentException("Use AddNodeAsync with a descriptor for a previously unknown peer.", nameof(nodeId));
        var operation = await MembershipOperationAsync(new(Guid.NewGuid(), kind, nodeId, peer,
            MembershipOperationStage.Accepted, 0, 0), token).ConfigureAwait(false);
        if (operation.Stage != MembershipOperationStage.Completed)
            throw new InvalidOperationException(operation.Error ?? $"Membership operation ended as {operation.Stage}.");
        return new(operation.Index, operation.Term);
    }

    private void CompleteMembership(ControlResult.Membership result)
    {
        if (!_membershipRequests.TryRemove(result.ClientId, out var completion)) return;
        if (result.Error is null)
            completion.TrySetResult(result.Status ?? throw new InvalidOperationException("Membership completion has no status."));
        else
            completion.TrySetException(result.Error.Type switch
            {
                "MembershipBusy" => new MembershipChangeInProgressException(result.ActiveOperationId!.Value),
                "MembershipConflict" => new MembershipOperationConflictException(result.ActiveOperationId!.Value),
                "MembershipTooLate" => new MembershipCancellationTooLateException(result.ActiveOperationId!.Value) { Status = result.Status },
                _ => ControlError(result.Error)
            });
    }

    private void FailMembershipRequest(long id, Exception error)
    {
        if (_membershipRequests.TryRemove(id, out var completion)) completion.TrySetException(error);
    }

    private async ValueTask PublishDirectoryAsync(MembershipState directory)
    {
        await ((IPeerDirectoryTransport)_transport).UpdateDirectoryAsync(directory, _lifetime.Token).ConfigureAwait(false);
        _publishedDirectoryRevision = directory.Revision;
    }
}
