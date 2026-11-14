using Raft.Storage;

namespace Raft.Tests;

public sealed class MembershipStorageTests
{
    internal static RaftOptions Options(string id = "n1") => new()
    {
        NodeId = id, Members = ["n1"], WireVersion = 3,
        InitialPeers = [new("n1", "127.0.0.1", 7301)]
    };

    private static LogEntry Record(long index, MembershipRecordKind kind, MembershipOperationStatus operation) =>
        new(index, 1, MembershipCodec.Encode(new(kind, operation)), LogEntryKind.Membership);

    [Fact]
    public void V4_keeps_uncommitted_intent_separate_from_committed_directory_and_indexes_terminal_result()
    {
        using var store = DBreezeLogStorage.CreateInMemory(Options(), DBreezeLogLayout.UnifiedTableV4);
        store.SaveHardState(new(1));
        var request = new MembershipOperationStatus(Guid.NewGuid(), MembershipOperationKind.AddNode, "n2",
            new("n2", "localhost", 7302), MembershipOperationStage.Accepted, 0, 0);
        store.Append([Record(1, MembershipRecordKind.Start, request)]);
        var pending = store.Query(new LogQuery.Membership()).Membership!;
        Assert.Null(pending.Committed.ActiveOperation);
        Assert.Single(pending.Committed.Peers);
        Assert.Equal(1, pending.PendingIndex);
        Assert.Equal(request.OperationId, pending.PendingOperationId);
        store.SaveHardState(new(1, null, 1));
        var committed = store.Query(new LogQuery.Membership()).Membership!;
        Assert.Equal(request.OperationId, committed.Committed.ActiveOperation!.OperationId);
        Assert.Equal(2, committed.Committed.Peers.Length);
        store.Write(new LogWrite.Batch([
            new LogWrite.Append([Record(2, MembershipRecordKind.Cancel, request with { Stage = MembershipOperationStage.Cancelling })]),
            new LogWrite.HardStateUpdate(new(1, null, 2))]));
        store.Write(new LogWrite.Batch([
            new LogWrite.Append([Record(3, MembershipRecordKind.Finish, request with { Stage = MembershipOperationStage.Cancelled })]),
            new LogWrite.HardStateUpdate(new(1, null, 3))]));
        Assert.Null(store.Query(new LogQuery.Membership()).Membership!.Committed.ActiveOperation);
        Assert.True(store.Query(new LogQuery.Membership()).Membership!.Committed.Peers.Single(peer => peer.NodeId == "n2").IsRetired);
        var result = store.Query(new LogQuery.MembershipOperation(request.OperationId)).MembershipOperation;
        Assert.Equal(MembershipOperationStage.Cancelled, result!.Stage);
        Assert.Equal(3, result.Index);
        var writes = store.Metrics.WriteTransactions;
        store.SaveHardState(store.HardState);
        Assert.Equal(writes, store.Metrics.WriteTransactions);
    }

    [Fact]
    public void Replacing_uncommitted_intent_releases_reservation_without_publishing_peer()
    {
        using var store = DBreezeLogStorage.CreateInMemory(Options(), DBreezeLogLayout.UnifiedTableV4);
        store.SaveHardState(new(1));
        var request = new MembershipOperationStatus(Guid.NewGuid(), MembershipOperationKind.AddNode, "n2",
            new("n2", "127.0.0.1", 7302), MembershipOperationStage.Accepted, 0, 0);
        store.Append([Record(1, MembershipRecordKind.Start, request)]);
        store.SaveHardState(new(2));
        store.ReplaceSuffix(1, [new LogEntry(1, 2, [], true)]);
        var view = store.Recover().Membership!;
        Assert.Equal(long.MaxValue, view.PendingIndex);
        Assert.Null(view.Committed.ActiveOperation);
        Assert.Single(view.Committed.Peers);
        Assert.Null(store.Query(new LogQuery.MembershipOperation(request.OperationId)).MembershipOperation);
    }

    [Fact]
    public void New_layout_rejects_old_readers_and_recovers_operation_after_disk_reopen()
    {
        var path = Path.Combine(Path.GetTempPath(), "raft-v4-" + Guid.NewGuid().ToString("N"));
        try
        {
            var request = new MembershipOperationStatus(Guid.NewGuid(), MembershipOperationKind.RemoveNode, "n1", null,
                MembershipOperationStage.Accepted, 0, 0);
            using (var store = new DBreezeLogStorage(path, Options(), DBreezeLogLayout.UnifiedTableV4))
            {
                store.SaveHardState(new(1));
                store.Write(new LogWrite.Batch([new LogWrite.Append([Record(1, MembershipRecordKind.Start, request)]),
                    new LogWrite.HardStateUpdate(new(1, null, 1))]));
            }
            using (var reopened = new DBreezeLogStorage(path, Options(), DBreezeLogLayout.UnifiedTableV4))
                Assert.Equal(request.OperationId, reopened.Recover().Membership!.Committed.ActiveOperation!.OperationId);
            var older = Options() with { WireVersion = 2, InitialPeers = [] };
            Assert.Throws<InvalidDataException>(() => new DBreezeLogStorage(path, older, DBreezeLogLayout.UnifiedTableV3));
            Assert.Throws<InvalidDataException>(() => new DBreezeLogStorage(path, older));
        }
        finally { if (Directory.Exists(path)) Directory.Delete(path, true); }
    }
}
