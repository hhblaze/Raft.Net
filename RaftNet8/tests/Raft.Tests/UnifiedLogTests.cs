extern alias baseline;

using System.Collections.Immutable;
using DBreeze;
using DBreeze.Utils;
using Raft.Storage;

namespace Raft.Tests;

public sealed class UnifiedLogTests
{
    [Theory]
    [InlineData("raft8/log")]
    [InlineData(DBreezeLogStorage.LogTable)]
    [InlineData(DBreezeLogStorage.ConfigurationTable)]
    public void Missing_legacy_identity_never_allows_v3_to_silently_initialize_over_reserved_data(string table)
    {
        using var engine = new DBreezeEngine(new DBreezeConfiguration { Storage = DBreezeConfiguration.eStorage.MEMORY });
        using (var tx = engine.GetTransaction()) { tx.Insert<long, byte[]>(table, 1, new byte[] { 1 }); tx.Commit(); }
        var options = new RaftOptions { NodeId = "n1", Members = ["n1"] };
        Assert.Throws<InvalidDataException>(() => new DBreezeLogStorage(engine, options, DBreezeLogLayout.UnifiedTableV3));
        using var read = engine.GetTransaction();
        Assert.Equal(new byte[] { 1 }, read.Select<long, byte[]>(table, 1).Value);
        Assert.False(engine.Scheme.IfUserTableExists(DBreezeLogStorage.UnifiedTable));
    }

    [Fact]
    public void Unified_format_persists_checked_state_and_refuses_incompatible_readers()
    {
        using var directory = new TestDirectory();
        var options = new RaftOptions { NodeId = "n1", Members = ["n1"] };
        using (var store = new DBreezeLogStorage(directory.Path, options, DBreezeLogLayout.UnifiedTableV3))
        {
            store.Write(new LogWrite.Batch([new LogWrite.HardStateUpdate(new(2)),
                new LogWrite.Append([new LogEntry(1, 2, "one"u8), new LogEntry(2, 2, "two"u8)]),
                new LogWrite.HardStateUpdate(new(2, CommitIndex: 2))]));
            Assert.Equal(DBreezeLogLayout.UnifiedTableV3, store.Layout);
            Assert.Equal(0, store.Recover().UncommittedBytes);
        }
        using (var reopened = new DBreezeLogStorage(directory.Path, options, DBreezeLogLayout.UnifiedTableV3))
        {
            Assert.Equal(new HardState(2, CommitIndex: 2), reopened.HardState);
            Assert.Equal(new[] { 1L, 2L }, reopened.Read(1, 10, 4096).Select(e => e.Index));
        }
        Assert.Throws<InvalidDataException>(() => new DBreezeLogStorage(directory.Path, options));
        Assert.Throws<InvalidDataException>(() => new DBreezeRaftStorage(directory.Path, options));
        Assert.Throws<InvalidDataException>(() => new baseline::Raft.Storage.DBreezeRaftStorage(directory.Path,
            new baseline::Raft.RaftOptions { NodeId = "n1", Members = ["n1"] }));
        using var engine = new DBreezeEngine(directory.Path);
        using var tx = engine.GetTransaction();
        Assert.False(tx.Select<string, byte[]>(DBreezeLogStorage.MetaTable, "identity").Exists);
        Assert.False(tx.Select<string, byte[]>("raft8/meta", "identity").Exists);
        var manifest = tx.Select<byte[], byte[]>(DBreezeLogStorage.UnifiedTable, 1.ToIndex("manifest")).Value;
        Assert.Equal(new HardState(2, CommitIndex: 2), StorageCodecV2.Manifest(manifest).State);
        Assert.False(tx.Select<byte[], byte[]>(DBreezeLogStorage.UnifiedTable, 1.ToIndex("frontier")).Exists);
        // Older unreleased flat-v3 readers must reject before they can truncate a log.
        var oldState = tx.Select<byte[], byte[]>(DBreezeLogStorage.UnifiedTable, 1.ToIndex("state")).Value;
        Assert.Throws<InvalidDataException>(() => StorageCodecV2.Hard(oldState));
    }

    [Fact]
    public void Packed_reader_refuses_prior_prototype_before_any_unpromised_tail_cleanup()
    {
        using var engine = new DBreezeEngine(new DBreezeConfiguration { Storage = DBreezeConfiguration.eStorage.MEMORY });
        var options = new RaftOptions { NodeId = "n1", Members = ["n1"] };
        using (var fresh = new DBreezeLogStorage(engine, options, DBreezeLogLayout.UnifiedTableV3)) { }
        using (var tx = engine.GetTransaction())
        {
            tx.SynchronizeTables(DBreezeLogStorage.UnifiedTable);
            tx.RemoveKey<byte[]>(DBreezeLogStorage.UnifiedTable, 1.ToIndex("manifest"));
            tx.Insert<byte[], byte[]>(DBreezeLogStorage.UnifiedTable, 1.ToIndex("state"), StorageCodecV2.Hard(new HardState()));
            tx.Insert<byte[], byte[]>(DBreezeLogStorage.UnifiedTable, 1.ToIndex("frontier"), StorageCodecV2.Tail(new StorageCodecV2.Frontier(1, 0, 0, 0, 0)));
            tx.Insert<byte[], byte[]>(DBreezeLogStorage.UnifiedTable, 2.ToIndex(1L), new byte[] { 42 });
            tx.Commit();
        }
        Assert.Throws<InvalidDataException>(() => new DBreezeLogStorage(engine, options, DBreezeLogLayout.UnifiedTableV3));
        using var read = engine.GetTransaction();
        Assert.Equal(new byte[] { 42 }, read.Select<byte[], byte[]>(DBreezeLogStorage.UnifiedTable, 2.ToIndex(1L)).Value);
        Assert.False(read.Select<byte[], byte[]>(DBreezeLogStorage.UnifiedTable, 1.ToIndex("manifest")).Exists);
    }

    [Theory]
    [InlineData("crc")]
    [InlineData("commit")]
    [InlineData("bytes")]
    public void Invalid_manifest_fails_recovery_without_deleting_the_unpromised_tail(string damage)
    {
        using var engine = new DBreezeEngine(new DBreezeConfiguration { Storage = DBreezeConfiguration.eStorage.MEMORY });
        var options = new RaftOptions { NodeId = "n1", Members = ["n1"] };
        using (var store = new DBreezeLogStorage(engine, options, DBreezeLogLayout.UnifiedTableV3))
        {
            store.SaveHardState(new(7, "n1"));
            store.Append([new LogEntry(1, 7, "durable"u8)]);
            store.SaveHardState(new(7, "n1", 1));
        }
        using (var tx = engine.GetTransaction())
        {
            tx.SynchronizeTables(DBreezeLogStorage.UnifiedTable);
            var key = 1.ToIndex("manifest");
            var record = tx.Select<byte[], byte[]>(DBreezeLogStorage.UnifiedTable, key).Value;
            var (state, tail) = StorageCodecV2.Manifest(record);
            if (damage == "crc") record[^1] ^= 1;
            else record = StorageCodecV2.Manifest(damage == "commit" ? state with { CommitIndex = 2 } : state,
                damage == "bytes" ? tail with { CommittedBytes = tail.CommittedBytes + 1 } : tail);
            tx.Insert<byte[], byte[]>(DBreezeLogStorage.UnifiedTable, key, record);
            tx.Insert<byte[], byte[]>(DBreezeLogStorage.UnifiedTable, 2.ToIndex(2L), new byte[] { 42 });
            tx.Commit();
        }
        Assert.Throws<InvalidDataException>(() => new DBreezeLogStorage(engine, options, DBreezeLogLayout.UnifiedTableV3));
        using var read = engine.GetTransaction();
        Assert.Equal(new byte[] { 42 }, read.Select<byte[], byte[]>(DBreezeLogStorage.UnifiedTable, 2.ToIndex(2L)).Value);
    }

    [Theory]
    [InlineData(PersistenceBoundary.BeforeTransaction, false)]
    [InlineData(PersistenceBoundary.AfterLogMutation, false)]
    [InlineData(PersistenceBoundary.BeforeCommit, false)]
    [InlineData(PersistenceBoundary.AfterCommit, true)]
    [InlineData(PersistenceBoundary.AfterCachePublication, true)]
    public void Unified_atomic_frontier_and_log_recover_at_every_write_boundary(PersistenceBoundary boundary, bool persisted)
    {
        using var engine = new DBreezeEngine(new DBreezeConfiguration { Storage = DBreezeConfiguration.eStorage.MEMORY });
        var options = new RaftOptions { NodeId = "n1", Members = ["n1"] };
        var armed = false;
        using (var store = new DBreezeLogStorage(engine, options, DBreezeLogLayout.UnifiedTableV3,
            observe: point => { if (armed && point == boundary) throw new IOException("cut"); }))
        {
            store.SaveHardState(new(1));
            armed = true;
            Assert.Throws<IOException>(() => store.Append([new LogEntry(1, 1, "durable"u8)]));
            Assert.Throws<InvalidOperationException>(() => store.Recover());
        }
        using var reopened = new DBreezeLogStorage(engine, options, DBreezeLogLayout.UnifiedTableV3);
        Assert.Equal(persisted ? 1 : 0, reopened.LastIndex);
        Assert.Equal(persisted ? 28 : 0, reopened.Recover().UncommittedBytes);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Unified_recovery_rejects_promised_corruption_but_discards_only_an_unpromised_tail(bool promised)
    {
        using var engine = new DBreezeEngine(new DBreezeConfiguration { Storage = DBreezeConfiguration.eStorage.MEMORY });
        var options = new RaftOptions { NodeId = "n1", Members = ["n1"] };
        using (var store = new DBreezeLogStorage(engine, options, DBreezeLogLayout.UnifiedTableV3))
        { store.SaveHardState(new(1)); store.Append([new LogEntry(1, 1, "safe"u8)]); }
        using (var tx = engine.GetTransaction())
        {
            tx.SynchronizeTables(DBreezeLogStorage.UnifiedTable);
                if (promised)
                {
                    var bytes = tx.Select<byte[], byte[]>(DBreezeLogStorage.UnifiedTable, 2.ToIndex(1L)).Value;
                    bytes[^1] ^= 1;
                    tx.Insert<byte[], byte[]>(DBreezeLogStorage.UnifiedTable, 2.ToIndex(1L), bytes);
                }
                else tx.Insert<byte[], byte[]>(DBreezeLogStorage.UnifiedTable, 2.ToIndex(2L), new byte[] { 1, 2, 3 });
            tx.Commit();
        }
        if (promised) Assert.Throws<InvalidDataException>(() => new DBreezeLogStorage(engine, options, DBreezeLogLayout.UnifiedTableV3));
        else
        {
            using var recovered = new DBreezeLogStorage(engine, options, DBreezeLogLayout.UnifiedTableV3);
            Assert.Equal(1, recovered.LastIndex);
            Assert.Equal("safe"u8.ToArray(), recovered.Read(1, 2, 4096)[0].Command.ToArray());
        }
    }
}
