using System.Collections.Immutable;

namespace Raft;

/// <summary>Public routing data, never voting authority or a private credential.</summary>
public sealed record PeerDescriptor(string NodeId, string Host, int Port)
{
    public bool IsRetired { get; init; }

    internal PeerDescriptor Validate(bool allowRetired = false)
    {
        RaftOptions.ValidateId(NodeId, nameof(NodeId));
        if (string.IsNullOrWhiteSpace(Host) || Host.Length > 253 || Port is < 1 or > 65535 ||
            (!allowRetired && IsRetired)) throw new ArgumentException("Invalid peer address, port or retired identity.");
        foreach (var c in Host)
            if (!char.IsAsciiLetterOrDigit(c) && c is not '.' and not '-' and not ':')
                throw new ArgumentException("Use an ASCII DNS name or an unbracketed IP address.", nameof(Host));
        return this;
    }
}

public enum MembershipOperationKind { AddNode, AddLearner, Promote, RemoveNode }
public enum MembershipOperationStage
{
    Accepted, CatchingUp, Promoting, Transferring, Removing, Cancelling, Completed, Cancelled, Failed
}

/// <summary>Durable status. A cancelled caller wait does not change this operation.</summary>
public sealed record MembershipOperationStatus(Guid OperationId, MembershipOperationKind Kind, string NodeId,
    PeerDescriptor? Peer, MembershipOperationStage Stage, long Index, long Term)
{
    public string? SuccessorId { get; init; }
    public string? Error { get; init; }
    public bool IsTerminal => Stage is MembershipOperationStage.Completed or MembershipOperationStage.Cancelled or MembershipOperationStage.Failed;
}

public sealed class MembershipChangeInProgressException(Guid operationId)
    : InvalidOperationException($"Membership operation '{operationId}' is already active.")
{
    public Guid OperationId { get; } = operationId;
}

public sealed class MembershipOperationConflictException(Guid operationId)
    : ArgumentException($"Operation ID '{operationId}' belongs to a different membership request.")
{
    public Guid OperationId { get; } = operationId;
}

public sealed class MembershipCancellationTooLateException(Guid operationId)
    : InvalidOperationException($"Membership operation '{operationId}' cannot be cancelled in its current state; inspect Status.")
{
    public Guid OperationId { get; } = operationId;
    public MembershipOperationStatus? Status { get; init; }
}

/// <summary>Owned, bounded directory and one active operation. Completed history stays indexed in the store.</summary>
public sealed record MembershipState(long Index, long Revision, ImmutableArray<PeerDescriptor> Peers,
    MembershipOperationStatus? ActiveOperation = null)
{
    internal MembershipState Validate()
    {
        if (Index < 0 || Revision < 0 || Revision > Index || Peers.IsDefault || Peers.Length > 1024 ||
            Peers.Select(peer => peer.NodeId).Distinct(StringComparer.Ordinal).Count() != Peers.Length)
            throw new InvalidDataException("Invalid membership state.");
        foreach (var peer in Peers) peer.Validate(allowRetired: true);
        if (ActiveOperation is { } active)
        {
            MembershipCodec.ValidateStatus(active);
            if (active.IsTerminal || active.Index != Index || active.Term < 1)
                throw new InvalidDataException("Active operation disagrees with the membership frontier.");
        }
        return this;
    }
}

/// <summary>Optional transport capability required by wire3; updates only routing and admission.</summary>
public interface IPeerDirectoryTransport : ITransport
{
    IReadOnlyList<PeerDescriptor> BootstrapPeers { get; }
    ValueTask UpdateDirectoryAsync(MembershipState state, CancellationToken cancellationToken);
}

internal sealed record MembershipView(MembershipState Committed, long PendingIndex = long.MaxValue,
    Guid? PendingOperationId = null);
internal enum MembershipRecordKind { Start, Progress, Cancel, Finish }
internal sealed record MembershipRecord(MembershipRecordKind RecordKind, MembershipOperationStatus Operation);

internal static class MembershipCodec
{
    internal const int MaxRecordBytes = 4096;
    internal const int MaxDirectoryBytes = 900_000;

    internal static void ValidateLogPayload(ReadOnlySpan<byte> bytes)
    {
        try { _ = Decode(bytes); }
        catch (Exception error) when (error is System.Text.Json.JsonException or ArgumentException or InvalidDataException)
        { throw new ArgumentException("Invalid membership log payload.", nameof(bytes), error); }
    }

    internal static void ValidateSnapshotRequest(Storage.SnapshotMetadata metadata, bool v4)
    {
        if (v4 != (metadata.Membership is not null)) throw new ArgumentException("Incompatible snapshot service format.");
        if (metadata.Membership is not { } membership) return;
        try { membership.Validate(); }
        catch (Exception error) when (error is ArgumentException or InvalidDataException)
        { throw new ArgumentException("Invalid snapshot membership.", nameof(metadata), error); }
        if (membership.Index > metadata.LastIncludedIndex || membership.ActiveOperation?.Term > metadata.LastIncludedTerm ||
            metadata.Voters.Concat(metadata.Learners).Any(id => !membership.Peers.Any(peer => peer.NodeId == id && !peer.IsRetired)))
            throw new ArgumentException("Snapshot configuration differs from its directory.");
    }

    internal static byte[] EncodeDirectory(MembershipState directory)
    {
        directory.Validate();
        var bytes = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(directory with { ActiveOperation = null });
        if (bytes.Length > MaxDirectoryBytes) throw new InvalidDataException("Directory exceeds its frame bound.");
        return bytes;
    }

    internal static MembershipState DecodeDirectory(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length is < 1 or > MaxDirectoryBytes) throw new InvalidDataException("Invalid directory size.");
        var value = System.Text.Json.JsonSerializer.Deserialize<MembershipState>(bytes)
            ?? throw new InvalidDataException("Missing directory.");
        value.Validate();
        if (value.ActiveOperation is not null) throw new InvalidDataException("Routing repair cannot publish operation state.");
        return value;
    }

    internal static void ValidateStatus(MembershipOperationStatus status)
    {
        if (status.OperationId == Guid.Empty || status.Kind is < MembershipOperationKind.AddNode or > MembershipOperationKind.RemoveNode ||
            status.Stage is < MembershipOperationStage.Accepted or > MembershipOperationStage.Failed ||
            status.Index < 0 || status.Term < 0 || status.Error?.Length > 1024)
            throw new InvalidDataException("Invalid membership operation.");
        RaftOptions.ValidateId(status.NodeId, nameof(status.NodeId));
        if (status.SuccessorId is { } successor) RaftOptions.ValidateId(successor, nameof(status.SuccessorId));
        if (status.Peer is { } peer)
        {
            peer.Validate();
            if (peer.NodeId != status.NodeId) throw new InvalidDataException("Membership target differs from its descriptor.");
        }
        if (status.Kind is MembershipOperationKind.AddNode or MembershipOperationKind.AddLearner && status.Peer is null)
            throw new InvalidDataException("Adding a peer requires its descriptor.");
        if (status.Kind is MembershipOperationKind.RemoveNode or MembershipOperationKind.Promote && status.Peer is not null)
            throw new InvalidDataException("Only an Add carries a descriptor.");
    }

    internal static byte[] Encode(MembershipRecord record)
    {
        ValidateStatus(record.Operation);
        if (record.RecordKind is < MembershipRecordKind.Start or > MembershipRecordKind.Finish)
            throw new InvalidDataException("Unknown membership record.");
        var stage = record.Operation.Stage;
        if ((record.RecordKind == MembershipRecordKind.Start && stage != MembershipOperationStage.Accepted) ||
            (record.RecordKind == MembershipRecordKind.Cancel && (stage != MembershipOperationStage.Cancelling ||
                record.Operation.Kind is not MembershipOperationKind.AddNode and not MembershipOperationKind.AddLearner)) ||
            (record.RecordKind == MembershipRecordKind.Progress && stage is not MembershipOperationStage.CatchingUp and
                not MembershipOperationStage.Promoting and not MembershipOperationStage.Transferring and not MembershipOperationStage.Removing) ||
            (record.RecordKind == MembershipRecordKind.Finish && !record.Operation.IsTerminal) ||
            (stage is MembershipOperationStage.Transferring or MembershipOperationStage.Removing && record.Operation.Kind != MembershipOperationKind.RemoveNode) ||
            (stage is MembershipOperationStage.CatchingUp or MembershipOperationStage.Promoting && record.Operation.Kind == MembershipOperationKind.RemoveNode))
            throw new InvalidDataException("Invalid membership record stage.");
        var bytes = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(record);
        if (bytes.Length > MaxRecordBytes) throw new ArgumentException("Membership record exceeds its bound.");
        return bytes;
    }

    internal static MembershipRecord Decode(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length is < 1 or > MaxRecordBytes) throw new InvalidDataException("Invalid membership record size.");
        var record = System.Text.Json.JsonSerializer.Deserialize<MembershipRecord>(bytes)
            ?? throw new InvalidDataException("Missing membership record.");
        _ = Encode(record);
        return record;
    }

    internal static bool SameRequest(MembershipOperationStatus left, MembershipOperationStatus right) =>
        left.OperationId == right.OperationId && left.Kind == right.Kind && left.NodeId == right.NodeId && left.Peer == right.Peer;

    internal static MembershipState Apply(MembershipState state, MembershipRecord record, long index, long term)
    {
        var operation = record.Operation with { Index = index, Term = term };
        if (index <= state.Index || term < 1) throw new InvalidDataException("Nonascending membership frontier.");
        _ = Encode(record);
        ValidateStatus(operation);
        var peers = state.Peers;
        switch (record.RecordKind)
        {
            case MembershipRecordKind.Start:
                if (state.ActiveOperation is not null) throw new InvalidDataException("Overlapping committed membership operations.");
                if (operation.Peer is { } peer && operation.Kind is MembershipOperationKind.AddNode or MembershipOperationKind.AddLearner)
                {
                    var existing = peers.FindIndex(value => value.NodeId == peer.NodeId);
                    if (existing >= 0 && peers[existing] != peer)
                        throw new InvalidDataException("A membership record cannot replace an existing/retired identity.");
                    peers = existing < 0 ? peers.Add(peer) : peers;
                    if (peers.Length > 1024) throw new InvalidDataException("Peer directory exceeds its bound.");
                }
                return new(index, peers == state.Peers ? state.Revision : index, peers, operation with { Stage = MembershipOperationStage.Accepted });
            case MembershipRecordKind.Progress:
            case MembershipRecordKind.Cancel:
                if (state.ActiveOperation is not { } active || !SameRequest(active, operation))
                    throw new InvalidDataException("Membership progress has no matching active intent.");
                return state with { Index = index, ActiveOperation = operation };
            case MembershipRecordKind.Finish:
                if (state.ActiveOperation is not { } finishing || !SameRequest(finishing, operation) || !operation.IsTerminal)
                    throw new InvalidDataException("Membership completion has no matching active intent.");
                if ((operation.Kind == MembershipOperationKind.RemoveNode && operation.Stage == MembershipOperationStage.Completed) ||
                    (operation.Kind is MembershipOperationKind.AddNode or MembershipOperationKind.AddLearner && operation.Stage == MembershipOperationStage.Cancelled))
                {
                    var retired = peers.FindIndex(value => value.NodeId == operation.NodeId);
                    if (retired >= 0) peers = peers.SetItem(retired, peers[retired] with { IsRetired = true });
                    return new(index, index, peers);
                }
                return state with { Index = index, ActiveOperation = null };
            default: throw new InvalidDataException("Unknown membership record.");
        }
    }

    internal static int FindIndex<T>(this ImmutableArray<T> values, Func<T, bool> predicate)
    {
        for (var i = 0; i < values.Length; i++) if (predicate(values[i])) return i;
        return -1;
    }
}
