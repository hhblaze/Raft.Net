using System.Collections.Immutable;
using DBreeze;
using Raft.Storage;

namespace Raft.Tests;

public sealed class UnifiedMigrationTests
{
    private static RaftOptions Options => new() { NodeId = "n1", Members = ["n1", "n2", "n3"], KnownPeers = ["n4"] };

    [Fact]
    public void Offline_v3_copy_preserves_compaction_membership_vote_application_and_snapshot_files()
    {
        using var directory = new TestDirectory();
        var source = Path.Combine(directory.Path, "source");
        var target = Path.Combine(directory.Path, "target");
        var membership = new ClusterConfiguration(["n1", "n2", "n3"], ["n4"], 257, 7);
        using (var engine = new DBreezeEngine(source))
        {
            using var log = new DBreezeLogStorage(engine, Options);
            log.SaveHardState(new(7, "n2"));
            log.Append(Enumerable.Range(1, 256).Select(i => new LogEntry(i, 7, BitConverter.GetBytes(i))).ToArray());
            log.Append([new LogEntry(257, 7, membership.Encode(), LogEntryKind.Configuration)]);
            log.SaveHardState(new(7, "n2", 256));
            log.Write(new LogWrite.PublishSnapshot(new(240, 7, Options.Members.ToImmutableArray(), [], "image240", new byte[32].ToImmutableArray()))
                { RetainEntries = 8 });
            using var tx = engine.GetTransaction();
            tx.SynchronizeTables("business/data", "business/checkpoint");
            tx.Insert<string, string>("business/data", "key", "keep");
            tx.Insert<string, byte[]>("business/checkpoint", "applied", DBreezeStateMachine.EncodeCheckpoint(new(256, 7)));
            tx.Commit();
        }
        Directory.CreateDirectory(Path.Combine(source, "snapshots"));
        File.WriteAllBytes(Path.Combine(source, "snapshots", "image240.fixture"), "image bytes"u8.ToArray());
        var result = RaftStorageMigration.CopyV2ToV3(source, target, Options);
        Assert.Equal(257, result.LastIndex);
        using (var migrated = new DBreezeLogStorage(target, Options, DBreezeLogLayout.UnifiedTableV3))
        {
            var recovered = migrated.Recover();
            Assert.Equal(new HardState(7, "n2", 256), recovered.HardState);
            Assert.Equal(233, recovered.FirstIndex);
            Assert.Equal(240, recovered.Snapshot!.LastIncludedIndex);
            Assert.Equal(membership.Encode(), recovered.Configuration!.Encode());
            Assert.Contains("n4", recovered.KnownMembers);
            Assert.Equal(membership.Encode(), migrated.Read(257, 1, 4096)[0].Command.ToArray());
            // A real rollback still restores the snapshot/bootstrap configuration.
            migrated.ReplaceSuffix(257, [new LogEntry(257, 7, "replacement"u8)]);
            Assert.Empty(migrated.Recover().Configuration!.Learners);
        }
        using (var engine = new DBreezeEngine(target))
        using (var tx = engine.GetTransaction())
        {
            Assert.Equal("keep", tx.Select<string, string>("business/data", "key").Value);
            Assert.Equal(new(256, 7), DBreezeStateMachine.DecodeCheckpoint(tx.Select<string, byte[]>("business/checkpoint", "applied").Value));
            Assert.False(tx.SelectForward<long, byte[]>(DBreezeLogStorage.LogTable).Any());
        }
        Assert.Equal("image bytes"u8.ToArray(), File.ReadAllBytes(Path.Combine(target, "snapshots", "image240.fixture")));
        Assert.True(File.Exists(Path.Combine(target, "raft-v3-migration.json")));
        Assert.False(File.Exists(Path.Combine(target, ".raft-migration-incomplete")));
        Assert.Throws<InvalidDataException>(() => new DBreezeLogStorage(target, Options));
        Assert.Throws<InvalidDataException>(() => new DBreezeRaftStorage(target, Options));
        using var untouched = new DBreezeLogStorage(source, Options);
        Assert.Equal(membership.Encode(), untouched.Recover().Configuration!.Encode());
    }

    [Fact]
    public void A_previously_migrated_empty_v2_database_can_be_copied_to_v3()
    {
        using var directory = new TestDirectory();
        var source = Path.Combine(directory.Path, "v1");
        var intermediate = Path.Combine(directory.Path, "v2");
        var target = Path.Combine(directory.Path, "v3");
        using (var log = new DBreezeRaftStorage(source, Options)) log.SaveHardState(new(3, "n2"));
        RaftStorageMigration.CopyV1ToV2(source, intermediate, Options);
        RaftStorageMigration.CopyV2ToV3(intermediate, target, Options);
        using var result = new DBreezeLogStorage(target, Options, DBreezeLogLayout.UnifiedTableV3);
        Assert.Equal(new HardState(3, "n2"), result.HardState);
        Assert.Equal(0, result.LastIndex);
        Assert.True(File.Exists(Path.Combine(target, "raft-migration.json")));
    }

    [Fact]
    public void Canceled_conversion_keeps_source_readable_and_destination_unusable()
    {
        using var directory = new TestDirectory();
        var source = Path.Combine(directory.Path, "source");
        var target = Path.Combine(directory.Path, "partial");
        using (var log = new DBreezeLogStorage(source, Options))
        {
            log.SaveHardState(new(1));
            log.Append(Enumerable.Range(1, 300).Select(i => new LogEntry(i, 1, "safe"u8)).ToArray());
        }
        using var cancellation = new CancellationTokenSource();
        Assert.Throws<OperationCanceledException>(() => RaftStorageMigration.CopyV2ToV3(source, target, Options,
            _ => cancellation.Cancel(), cancellation.Token));
        Assert.Throws<InvalidDataException>(() => new DBreezeLogStorage(target, Options));
        Assert.Throws<InvalidDataException>(() => new DBreezeLogStorage(target, Options, DBreezeLogLayout.UnifiedTableV3));
        using var original = new DBreezeLogStorage(source, Options);
        Assert.Equal(300, original.LastIndex);
    }

    [Fact]
    public void V3_migration_does_not_initialize_an_unrecognized_source_or_overwrite_a_destination()
    {
        using var directory = new TestDirectory();
        var source = Path.Combine(directory.Path, "unknown");
        var target = Path.Combine(directory.Path, "target");
        using (var engine = new DBreezeEngine(source))
        using (var tx = engine.GetTransaction()) { tx.Insert("application", "value", "keep"); tx.Commit(); }
        Assert.Throws<InvalidDataException>(() => RaftStorageMigration.CopyV2ToV3(source, target, Options));
        Assert.False(Directory.Exists(target));
        Assert.Throws<ArgumentException>(() => RaftStorageMigration.CopyV2ToV3(source, source, Options));
        using var verify = new DBreezeEngine(source);
        using var read = verify.GetTransaction();
        Assert.False(read.Select<string, byte[]>(DBreezeLogStorage.MetaTable, "identity").Exists);
        Assert.False(read.Select<string, byte[]>(DBreezeLogStorage.MetaTable, "state").Exists);
        Assert.Equal("keep", read.Select<string, string>("application", "value").Value);
    }
}
