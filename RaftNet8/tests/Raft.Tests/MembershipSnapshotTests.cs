using System.Collections.Immutable;
using System.Security.Cryptography;
using Raft.Storage;

namespace Raft.Tests;

public sealed class MembershipSnapshotTests
{

    private sealed class JoiningTransport : IPeerDirectoryTransport
    {
        public IReadOnlyList<PeerDescriptor> BootstrapPeers => [];
        public ValueTask UpdateDirectoryAsync(MembershipState state, CancellationToken token) => ValueTask.CompletedTask;
        public ValueTask<RaftRpc> SendAsync(string peerId, RaftRpc request, CancellationToken token) =>
            ValueTask.FromException<RaftRpc>(new IOException("No live peers in this prefix fixture."));
    }

    [Fact]
    public async Task A_joining_replica_can_snapshot_the_prefix_before_its_first_logged_membership_entry()
    {
        using var directory = new TestDirectory();
        var options = new RaftOptions { NodeId = "n4", Members = ["n1", "n2", "n3"], JoiningNode = true,
            WireVersion = 3, InitialPeers = [new("n1", "127.0.0.1", 7301), new("n2", "127.0.0.1", 7302), new("n3", "127.0.0.1", 7303)],
            SnapshotDirectory = Path.Combine(directory.Path, "images") };
        var storage = DBreezeLogStorage.CreateInMemory(options, DBreezeLogLayout.UnifiedTableV4);
        storage.SaveHardState(new(1));
        var entry = new LogEntry(1, 1, [], true);
        storage.Append([entry]);
        storage.SaveHardState(new(1, CommitIndex: 1));
        using var app = SnapshotStorageTests.Machine(Path.Combine(directory.Path, "app"));
        await app.ApplyAsync(entry, default);
        await using var log = new RaftStorageAdapter(storage);
        await using var node = await RaftNode.CreateAsync(options, log, new JoiningTransport(), app);
        await node.StartAsync();
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var snapshot = await node.CreateSnapshotAsync(deadline.Token);
        Assert.Equal(new[] { "n1", "n2", "n3" }, snapshot.Voters);
        Assert.Empty(snapshot.Learners);
        Assert.DoesNotContain(snapshot.Membership!.Peers, peer => peer.NodeId == "n4");
        Assert.Equal(1, snapshot.LastIncludedIndex);
        Assert.Null(node.Status.Fault);
        Assert.Equal(RaftRole.Removed, node.Status.Role);
    }
    private sealed class DigestCheckingApplication(ISnapshotStateMachine inner) : ISnapshotStateMachine
    {
        public AppliedCheckpoint Checkpoint => inner.Checkpoint;
        public long LastApplied => inner.LastApplied;
        public ValueTask ApplyAsync(LogEntry entry, CancellationToken token) => inner.ApplyAsync(entry, token);
        private static void Verify(SnapshotImage image)
        {
            using var file = File.OpenRead(image.Path);
            Assert.Equal(image.Length, file.Length);
            Assert.Equal(image.Metadata.Digest.ToArray(), SHA256.HashData(file));
        }
        public ValueTask CreateSnapshotAsync(SnapshotBuildContext context, string outputPath, CancellationToken token)
        {
            if (context.Previous is { } previous) Verify(previous);
            return inner.CreateSnapshotAsync(context, outputPath, token);
        }
        public ValueTask PrepareSnapshotAsync(SnapshotImage image, long maxBytes, CancellationToken token)
        { Verify(image); return inner.PrepareSnapshotAsync(image, maxBytes, token); }
        public ValueTask InstallSnapshotAsync(SnapshotImage image, CancellationToken token)
        { Verify(image); return inner.InstallSnapshotAsync(image, token); }
    }
    [Fact]
    public async Task History_is_streamed_across_pages_at_the_application_fence_and_survives_install_and_reopen()
    {
        using var directory = new TestDirectory();
        var options = MembershipStorageTests.Options() with { SnapshotDirectory = Path.Combine(directory.Path, "images") };
        using var source = DBreezeLogStorage.CreateInMemory(options, DBreezeLogLayout.UnifiedTableV4);
        source.SaveHardState(new(1));
        var operations = new List<Guid>();
        var entries = new List<LogEntry>();
        for (var i = 0; i < 150; i++)
        {
            var id = Guid.NewGuid();
            operations.Add(id);
            var operation = new MembershipOperationStatus(id, MembershipOperationKind.AddNode, "peer" + i,
                new("peer" + i, "localhost", 8000 + i), MembershipOperationStage.Accepted, 0, 0);
            foreach (var (kind, stage) in new[] { (MembershipRecordKind.Start, MembershipOperationStage.Accepted),
                (MembershipRecordKind.Cancel, MembershipOperationStage.Cancelling), (MembershipRecordKind.Finish, MembershipOperationStage.Cancelled) })
                entries.Add(new(entries.Count + 1, 1, MembershipCodec.Encode(new(kind, operation with { Stage = stage })), LogEntryKind.Membership));
        }
        source.Append(entries);
        source.SaveHardState(new(1, CommitIndex: entries.Count));
        // A fence before every result still scans only the requested number of rows per page.
        string? after = null;
        var pages = 0;
        do
        {
            var page = source.Query(new LogQuery.MembershipHistory(after, 7, 0));
            Assert.Empty(page.MembershipHistory);
            after = page.ContinuationKey;
            pages++;
        } while (after is not null);
        Assert.Equal(22, pages);
        using var app = SnapshotStorageTests.Machine(Path.Combine(directory.Path, "source-app"));
        await app.ApplyBatchAsync(entries, default);
        await using var sourceLog = new RaftStorageAdapter(source);
        await using var repository = new SnapshotRepository(options);
        const long fence = 300; // application replay + administrative projection include the same first100 operations
        var context = new SnapshotBuildContext(new(fence, 1), source.Recover().Configuration!, null, options,
            (from, count, bytes, token) => sourceLog.ReadAsync(new LogQuery.Entries(from, count, bytes), token))
            { Membership = source.Query(new LogQuery.MembershipAt(fence)).Membership!.Committed };
        var snapshotMachine = new MembershipSnapshotMachine(new DigestCheckingApplication(app), sourceLog, options);
        var image = await repository.CreateAsync(snapshotMachine, context, default);
        var nextContext = new SnapshotBuildContext(new(entries.Count, 1), source.Recover().Configuration!, image, options,
            (from, count, bytes, token) => sourceLog.ReadAsync(new LogQuery.Entries(from, count, bytes), token))
            { Membership = source.Recover().Membership!.Committed };
        var following = await repository.CreateAsync(snapshotMachine, nextContext, default);
        Assert.Equal(entries.Count, following.Metadata.LastIncludedIndex);
        var targetPath = Path.Combine(directory.Path, "target-log");
        using var targetApp = SnapshotStorageTests.Machine(Path.Combine(directory.Path, "target-app"));
        await using (var target = new RaftStorageAdapter(new DBreezeLogStorage(targetPath, options, DBreezeLogLayout.UnifiedTableV4)))
        {
            await target.WriteAsync(new LogWrite.HardStateUpdate(new(1)), default);
            var installer = new MembershipSnapshotMachine(new DigestCheckingApplication(targetApp), target, options);
            await installer.PrepareSnapshotAsync(image, options.MaxSnapshotUnpackedBytes, default);
            var intent = new SnapshotInstallIntent(image.Metadata, false);
            await target.WriteAsync(new LogWrite.BeginSnapshotInstall(intent), default);
            await installer.InstallSnapshotAsync(image, default);
            await target.WriteAsync(new LogWrite.CompleteSnapshotInstall(intent), default);
            Assert.Equal(new(fence, 1), targetApp.Checkpoint);
            for (var i = 0; i < 150; i++)
            {
                var result = (await target.ReadAsync(new LogQuery.MembershipOperation(operations[i]), default)).MembershipOperation;
                if (i < 100) Assert.Equal(MembershipOperationStage.Cancelled, result!.Stage);
                else Assert.Null(result);
            }
            var active = (await target.RecoverAsync(default)).Membership!.Committed;
            Assert.Equal(101, active.Peers.Length);
            await target.WriteAsync(new LogWrite.PruneMembershipHistory(ImmutableArray<string>.Empty), default);
            Assert.NotNull((await target.ReadAsync(new LogQuery.MembershipOperation(operations[0]), default)).MembershipOperation);
            // An abandoned candidate is safe to collect, while the active generation remains readable.
            var resultToStage = (await target.ReadAsync(new LogQuery.MembershipOperation(operations[0]), default)).MembershipOperation!;
            await target.WriteAsync(new LogWrite.StageMembershipHistory("abandoned", [resultToStage], ThroughIndex: fence), default);
            await target.WriteAsync(new LogWrite.PruneMembershipHistory(ImmutableArray<string>.Empty), default);
        }
        using var reopened = new DBreezeLogStorage(targetPath, options, DBreezeLogLayout.UnifiedTableV4);
        Assert.Equal(fence, reopened.HardState.CommitIndex);
        Assert.Equal(101, reopened.Recover().Membership!.Committed.Peers.Length);
        Assert.Equal(MembershipOperationStage.Cancelled, reopened.Query(new LogQuery.MembershipOperation(operations[99])).MembershipOperation!.Stage);
        var writes = reopened.Metrics.WriteTransactions;
        reopened.Write(new LogWrite.PruneMembershipHistory(ImmutableArray<string>.Empty));
        Assert.Equal(writes, reopened.Metrics.WriteTransactions); // no unchanged maintenance flush
    }
}
