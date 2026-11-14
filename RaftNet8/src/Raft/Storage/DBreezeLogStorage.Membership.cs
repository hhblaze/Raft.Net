using System.Buffers.Binary;
using System.Collections.Immutable;
using System.Text;
using System.Text.Json;

namespace Raft.Storage;

public sealed partial class DBreezeLogStorage
{
    private MembershipState? _membership, _membershipBootstrap;
    private long _nextMembershipIndex = long.MaxValue;
    private Guid? _pendingMembershipOperation;
    private string _historyGeneration = "initial";

    private static byte[] MembershipBytes(MembershipState state) => StorageCodecV2.Wrap(JsonSerializer.SerializeToUtf8Bytes(state.Validate()));
    private static MembershipState MembershipValue(byte[] bytes) =>
        (JsonSerializer.Deserialize<MembershipState>(StorageCodecV2.Open(bytes)) ?? throw new InvalidDataException("Missing membership state.")).Validate();
    private static byte[] ResultBytes(MembershipOperationStatus operation)
    {
        MembershipCodec.ValidateStatus(operation);
        if (!operation.IsTerminal || operation.Index < 1 || operation.Term < 1)
            throw new InvalidDataException("A history row needs a terminal durable boundary.");
        return StorageCodecV2.Wrap(JsonSerializer.SerializeToUtf8Bytes(operation));
    }
    private static MembershipOperationStatus ResultValue(byte[] bytes)
    {
        var value = JsonSerializer.Deserialize<MembershipOperationStatus>(StorageCodecV2.Open(bytes))
            ?? throw new InvalidDataException("Missing membership result.");
        MembershipCodec.ValidateStatus(value);
        if (!value.IsTerminal || value.Index < 1 || value.Term < 1) throw new InvalidDataException("A history row must be terminal and durable.");
        return value;
    }
    private static string HistoryKey(string generation, Guid operation) => generation + "/" + operation.ToString("N");

    private void InitializeMembership(DBreezeLogTransaction tx, bool fresh)
    {
        if (Layout != DBreezeLogLayout.UnifiedTableV4) return;
        if (fresh)
        {
            if (_options.Members.Concat(_options.Learners).Any(id =>
                !_options.InitialPeers.Any(peer => peer.NodeId == id && !peer.IsRetired)))
                throw new ArgumentException("Initial wire3 voters and learners require live directory descriptors.");
            _membershipBootstrap = new(0, 0, _options.InitialPeers.OrderBy(peer => peer.NodeId, StringComparer.Ordinal).ToImmutableArray());
            _membership = _membershipBootstrap;
            tx.Insert<string, byte[]>(MetaTable, "membershipBootstrap", MembershipBytes(_membershipBootstrap));
            tx.Insert<string, byte[]>(MetaTable, "membership", MembershipBytes(_membership));
            tx.Insert<string, byte[]>(MetaTable, "historyGeneration", StorageCodecV2.Wrap("initial"u8));
        }
        else
        {
            _membershipBootstrap = MembershipValue(tx.Select<string, byte[]>(MetaTable, "membershipBootstrap").Value);
            _membership = MembershipValue(tx.Select<string, byte[]>(MetaTable, "membership").Value);
            _historyGeneration = Encoding.ASCII.GetString(StorageCodecV2.Open(tx.Select<string, byte[]>(MetaTable, "historyGeneration").Value));
            RaftOptions.ValidateId(_historyGeneration, nameof(_historyGeneration));
            if (_membership.Index > _state.CommitIndex) throw new InvalidDataException("Membership is ahead of durable commit.");
            var rebuilt = MembershipAt(tx, _state.CommitIndex);
            if (!MembershipBytes(rebuilt).AsSpan().SequenceEqual(MembershipBytes(_membership)))
                throw new InvalidDataException("Materialized membership differs from its committed history.");
        }
        (_nextMembershipIndex, _pendingMembershipOperation) = PendingMembership(tx, _state.CommitIndex, _frontier.LastIndex);
    }

    private MembershipState MembershipAt(DBreezeLogTransaction tx, long through)
    {
        if (Layout != DBreezeLogLayout.UnifiedTableV4) throw new NotSupportedException("Membership operations require raft8-v4.");
        var value = _snapshot is { } snapshot && snapshot.LastIncludedIndex <= through
            ? snapshot.Membership ?? throw new InvalidDataException("V4 snapshot has no membership state.")
            : _membershipBootstrap ?? throw new InvalidDataException("Missing membership bootstrap.");
        if (through < (_snapshot?.LastIncludedIndex ?? 0) || through > _state.CommitIndex)
            throw new ArgumentOutOfRangeException(nameof(through), "Membership fence is outside the retained committed history.");
        foreach (var row in tx.SelectForwardStartFrom<long, byte[]>(MembershipIndexTable, value.Index + 1, true))
        {
            if (row.Key > through) break;
            var entry = StorageCodecV2.Entry(row.Key, row.Value, _options.MaxCommandBytes, allowMembership: true);
            value = MembershipCodec.Apply(value, MembershipCodec.Decode(entry.Command.Span), row.Key, entry.Term);
        }
        return value;
    }

    private static (long Index, Guid? Operation) PendingMembership(DBreezeLogTransaction tx, long commit, long tail)
    {
        foreach (var row in tx.SelectForwardStartFrom<long, byte[]>(MembershipIndexTable, commit + 1, true))
        {
            if (row.Key > tail) break;
            var entry = StorageCodecV2.Entry(row.Key, row.Value, MembershipCodec.MaxRecordBytes, allowMembership: true);
            return (row.Key, MembershipCodec.Decode(entry.Command.Span).Operation.OperationId);
        }
        return (long.MaxValue, null);
    }

    private void UpdateMembership(DBreezeLogTransaction tx, HardState state, StorageCodecV2.Frontier tail,
        SnapshotMetadata? snapshot, IReadOnlyList<LogWrite> commands, List<LogEntry> inserted, long? replacedFrom,
        ref MembershipState? membership, ref long nextIndex, ref Guid? pendingOperation, ref string generation)
    {
        if (Layout != DBreezeLogLayout.UnifiedTableV4) return;
        LogWrite.CompleteSnapshotInstall? install = null;
        for (var i = 0; i < commands.Count; i++)
            if (commands[i] is LogWrite.CompleteSnapshotInstall completed) install = completed;
        if (install is not null)
        {
            membership = install.Intent.Metadata.Membership?.Validate() ?? throw new InvalidDataException("V4 image lacks membership.");
            tx.Insert<string, byte[]>(MetaTable, "historyGarbage/" + generation, StorageCodecV2.Wrap([]));
            generation = install.Intent.Metadata.StorageId;
            var ready = tx.Select<string, byte[]>(MetaTable, "historyReady/" + generation);
            if (!ready.Exists || BinaryPrimitives.ReadInt64LittleEndian(StorageCodecV2.Open(ready.Value)) != install.Intent.Metadata.LastIncludedIndex)
                throw new InvalidDataException("Snapshot membership history was not prepared at its fence.");
            tx.Insert<string, byte[]>(MetaTable, "historyGeneration", StorageCodecV2.Wrap(Encoding.ASCII.GetBytes(generation)));
            tx.RemoveKey<string>(MetaTable, "historyGarbage/" + generation);
        }
        var changedIndex = replacedFrom is not null || inserted.Any(entry => entry.Kind == LogEntryKind.Membership) || install is not null;
        if (changedIndex || nextIndex <= state.CommitIndex)
        {
            var current = membership ?? throw new InvalidDataException("Missing membership state.");
            foreach (var row in tx.SelectForwardStartFrom<long, byte[]>(MembershipIndexTable, current.Index + 1, true))
            {
                if (row.Key > state.CommitIndex) break;
                var entry = StorageCodecV2.Entry(row.Key, row.Value, _options.MaxCommandBytes, allowMembership: true);
                var record = MembershipCodec.Decode(entry.Command.Span);
                current = MembershipCodec.Apply(current, record, row.Key, entry.Term);
                if (record.RecordKind == MembershipRecordKind.Finish)
                {
                    var result = record.Operation with { Index = row.Key, Term = entry.Term };
                    tx.Insert<string, byte[]>(MembershipHistoryTable, HistoryKey(generation, result.OperationId), ResultBytes(result));
                }
            }
            membership = current;
            (nextIndex, pendingOperation) = PendingMembership(tx, state.CommitIndex, tail.LastIndex);
        }
        if (!ReferenceEquals(membership, _membership))
            tx.Insert<string, byte[]>(MetaTable, "membership", MembershipBytes(membership!));
    }

    private static void StageHistory(DBreezeLogTransaction tx, LogWrite.StageMembershipHistory stage)
    {
        RaftOptions.ValidateId(stage.Generation, nameof(stage.Generation));
        if (stage.Entries.IsDefault || stage.Entries.Length > 128 || stage.ThroughIndex < 1)
            throw new ArgumentException("Invalid snapshot history staging batch.");
        // Immutable generation IDs cannot be reused to replace the currently published result history.
        var current = tx.Select<string, byte[]>(MetaTable, "historyGeneration");
        if (current.Exists && Encoding.ASCII.GetString(StorageCodecV2.Open(current.Value)) == stage.Generation)
            throw new InvalidOperationException("Cannot stage into the active history generation.");
        tx.Insert<string, byte[]>(MetaTable, "historyGarbage/" + stage.Generation, StorageCodecV2.Wrap([]));
        foreach (var result in stage.Entries)
        {
            if (!result.IsTerminal || result.Index > stage.ThroughIndex) throw new InvalidDataException("History result lies beyond its snapshot fence.");
            var key = HistoryKey(stage.Generation, result.OperationId);
            var bytes = ResultBytes(result);
            var existing = tx.Select<string, byte[]>(MembershipHistoryTable, key);
            if (existing.Exists && !existing.Value.AsSpan().SequenceEqual(bytes))
                throw new InvalidDataException("Snapshot history ID was staged with different content.");
            tx.Insert<string, byte[]>(MembershipHistoryTable, key, bytes);
        }
        if (stage.Complete)
        {
            Span<byte> fence = stackalloc byte[8];
            BinaryPrimitives.WriteInt64LittleEndian(fence, stage.ThroughIndex);
            tx.Insert<string, byte[]>(MetaTable, "historyReady/" + stage.Generation, StorageCodecV2.Wrap(fence));
        }
    }

    private static bool PruneHistory(DBreezeLogTransaction tx, ImmutableArray<string> protectedGenerations, string activeGeneration)
    {
        if (protectedGenerations.IsDefault || protectedGenerations.Length > 1028)
            throw new ArgumentException("Invalid protected history generations.");
        const string markerPrefix = "historyGarbage/";
        var visitedMarkers = 0;
        foreach (var marker in tx.SelectForwardStartFrom<string, byte[]>(MetaTable, markerPrefix, true))
        {
            if (!marker.Key.StartsWith(markerPrefix, StringComparison.Ordinal) || ++visitedMarkers > 128) break;
            var generation = marker.Key[markerPrefix.Length..];
            RaftOptions.ValidateId(generation, nameof(generation));
            if (generation == activeGeneration || protectedGenerations.Contains(generation, StringComparer.Ordinal)) continue;
            var prefix = generation + "/";
            var removed = 0;
            foreach (var row in tx.SelectForwardStartFrom<string, byte[]>(MembershipHistoryTable, prefix, true, AsReadVisibilityScope: true))
            {
                if (!row.Key.StartsWith(prefix, StringComparison.Ordinal)) break;
                tx.RemoveKey<string>(MembershipHistoryTable, row.Key);
                if (++removed == 128) return true;
            }
            tx.RemoveKey<string>(MetaTable, marker.Key);
            tx.RemoveKey<string>(MetaTable, "historyReady/" + generation);
            return true;
        }
        return false;
    }

    private LogReadResult QueryMembership(LogQuery query)
    {
        if (_membership is null) throw new NotSupportedException("Membership operations require raft8-v4.");
        if (query is LogQuery.Membership)
            return new() { Membership = new(_membership, _nextMembershipIndex, _pendingMembershipOperation) };
        using var tx = OpenTransaction();
        _reads++;
        if (query is LogQuery.MembershipAt at)
            return new() { Membership = new(MembershipAt(tx, at.Index)) };
        if (query is LogQuery.MembershipOperation operation)
        {
            if (_membership.ActiveOperation is { } active && active.OperationId == operation.OperationId)
                return new() { MembershipOperation = active };
            if (operation.IncludePending && _pendingMembershipOperation == operation.OperationId)
            {
                var pending = tx.Select<long, byte[]>(MembershipIndexTable, _nextMembershipIndex);
                var entry = StorageCodecV2.Entry(_nextMembershipIndex, pending.Value, _options.MaxCommandBytes, allowMembership: true);
                return new() { MembershipOperation = MembershipCodec.Decode(entry.Command.Span).Operation with { Index = entry.Index, Term = entry.Term } };
            }
            var row = tx.Select<string, byte[]>(MembershipHistoryTable, HistoryKey(_historyGeneration, operation.OperationId));
            return new() { MembershipOperation = row.Exists ? ResultValue(row.Value) : null };
        }
        if (query is LogQuery.MembershipHistory page)
        {
            if (page.Count is < 1 or > 128 || page.ThroughIndex < 0) throw new ArgumentException("Invalid membership history page.");
            var prefix = _historyGeneration + "/";
            var after = page.AfterKey ?? prefix;
            if (!after.StartsWith(prefix, StringComparison.Ordinal)) throw new ArgumentException("History cursor belongs to another generation.");
            var results = ImmutableArray.CreateBuilder<MembershipOperationStatus>();
            string? next = null;
            var visited = 0;
            foreach (var row in tx.SelectForwardStartFrom<string, byte[]>(MembershipHistoryTable, after, page.AfterKey is null))
            {
                if (!row.Key.StartsWith(prefix, StringComparison.Ordinal)) break;
                var value = ResultValue(row.Value);
                if (value.Index <= page.ThroughIndex) results.Add(value);
                if (++visited == page.Count) { next = row.Key; break; }
            }
            return new() { MembershipHistory = results.ToImmutable(), ContinuationKey = next };
        }
        throw new ArgumentException("Unknown membership query.");
    }
}
