using DBreeze;
using Raft.Storage;

namespace Raft.Tests;

public sealed class MigrationTests
{
    private static RaftOptions Options => new() { NodeId = "n1", Members = ["n1", "n2", "n3"] };

    [Fact]
    public void Migration_rejects_an_unrecognized_database_without_initializing_v1_identity_in_the_source()
    {
        using var directory = new TestDirectory();
        var source = Path.Combine(directory.Path, "historical");
        var target = Path.Combine(directory.Path, "target");
        using (var engine = new DBreezeEngine(source))
        using (var tx = engine.GetTransaction())
        {
            tx.Insert<string, string>("historical/application", "key", "preserve");
            tx.Commit();
        }
        Assert.Throws<InvalidDataException>(() => RaftStorageMigration.CopyV1ToV2(source, target, Options));
        Assert.False(Directory.Exists(target));
        using var verify = new DBreezeEngine(source);
        using var read = verify.GetTransaction();
        Assert.False(read.Select<string, byte[]>("raft8/meta", "identity").Exists);
        Assert.False(read.Select<string, byte[]>("raft8/meta", "state").Exists);
        Assert.Equal("preserve", read.Select<string, string>("historical/application", "key").Value);
    }

    [Fact]
    public void An_initialized_empty_v1_database_is_a_valid_migration_source()
    {
        using var directory = new TestDirectory();
        var source = Path.Combine(directory.Path, "source");
        var target = Path.Combine(directory.Path, "target");
        using (var original = new DBreezeRaftStorage(source, Options)) Assert.Equal(0, original.LastIndex);
        var copied = RaftStorageMigration.CopyV1ToV2(source, target, Options);
        Assert.Equal(0, copied.LastIndex);
        using var result = new DBreezeLogStorage(target, Options);
        Assert.Equal(new HardState(), result.HardState);
        Assert.Equal(0, result.LastIndex);
    }

    [Fact]
    public void Offline_copy_preserves_application_tables_log_hard_state_and_source()
    {
        using var directory = new TestDirectory();
        var source = Path.Combine(directory.Path, "source");
        var target = Path.Combine(directory.Path, "target");
        using (var engine = new DBreezeEngine(source))
        {
            using var original = new DBreezeRaftStorage(engine, Options);
            original.SaveHardState(new(7, "n2"));
            original.Append(Enumerable.Range(1, 257).Select(i => new LogEntry(i, 7, BitConverter.GetBytes(i))).ToArray());
            original.SaveHardState(new(7, "n2", 256));
            using var tx = engine.GetTransaction();
            tx.SynchronizeTables("business/data", "business/checkpoint");
            tx.Insert<string, string>("business/data", "key", "retained");
            tx.Insert<string, long>("business/checkpoint", "lastApplied", 256);
            tx.Commit();
        }
        var result = RaftStorageMigration.CopyV1ToV2(source, target, Options);
        Assert.Equal(257, result.LastIndex);
        using (var modern = new DBreezeLogStorage(target, Options))
        {
            Assert.Equal(new HardState(7, "n2", 256), modern.HardState);
            Assert.Equal(257, BitConverter.ToInt32(modern.Read(257, 1, 1024)[0].Command.Span));
        }
        using (var engine = new DBreezeEngine(target))
        {
            using var tx = engine.GetTransaction();
            Assert.Equal("retained", tx.Select<string, string>("business/data", "key").Value);
            Assert.Equal(256, tx.Select<string, long>("business/checkpoint", "lastApplied").Value);
        }
        using (var original = new DBreezeRaftStorage(source, Options))
            Assert.Equal(new HardState(7, "n2", 256), original.HardState);
        Assert.Throws<InvalidDataException>(() => new DBreezeRaftStorage(target, Options));
        Assert.True(File.Exists(Path.Combine(target, "raft-migration.json")));
        Assert.False(File.Exists(Path.Combine(target, ".raft-migration-incomplete")));
    }

    [Fact]
    public void Canceled_partial_copy_cannot_be_opened_as_a_new_cluster()
    {
        using var directory = new TestDirectory();
        var source = Path.Combine(directory.Path, "source");
        var target = Path.Combine(directory.Path, "target");
        using (var original = new DBreezeRaftStorage(source, Options))
        {
            original.SaveHardState(new(1));
            original.Append(Enumerable.Range(1, 300).Select(i => new LogEntry(i, 1, "x"u8)).ToArray());
        }
        using var cancel = new CancellationTokenSource();
        Assert.Throws<OperationCanceledException>(() => RaftStorageMigration.CopyV1ToV2(source, target, Options,
            _ => cancel.Cancel(), cancel.Token));
        Assert.Throws<InvalidDataException>(() => new DBreezeLogStorage(target, Options));
        using var sourceStillValid = new DBreezeRaftStorage(source, Options);
        Assert.Equal(300, sourceStillValid.LastIndex);
    }

    [Fact]
    public void Migration_never_overwrites_or_targets_a_child_of_the_source()
    {
        using var directory = new TestDirectory();
        using (var original = new DBreezeRaftStorage(directory.Path, Options)) original.SaveHardState(new(1));
        Assert.Throws<ArgumentException>(() => RaftStorageMigration.CopyV1ToV2(directory.Path, directory.Path, Options));
        Assert.Throws<ArgumentException>(() => RaftStorageMigration.CopyV1ToV2(directory.Path, Path.Combine(directory.Path, "child"), Options));
    }
}
