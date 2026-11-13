using DBreeze;
using DBreeze.Utils;
using Raft.Storage;

namespace Raft.Tests;

public sealed class StorageNamespaceTests
{
    [Theory]
    [InlineData(DBreezeLogLayout.SeparateTablesV2)]
    [InlineData(DBreezeLogLayout.UnifiedTableV3)]
    public void Forward_backward_suffix_and_clear_ranges_cannot_cross_logical_type_boundaries(DBreezeLogLayout layout)
    {
        using var engine = new DBreezeEngine(new DBreezeConfiguration { Storage = DBreezeConfiguration.eStorage.MEMORY });
        using (var tx = new DBreezeLogTransaction(engine.GetTransaction(), layout, writable: true))
        {
            tx.SynchronizeTables(DBreezeLogStorage.MetaTable, DBreezeLogStorage.LogTable, DBreezeLogStorage.ConfigurationTable);
            tx.Insert(DBreezeLogStorage.MetaTable, "sentinel", "keep"u8.ToArray());
            for (long i = 1; i <= 5; i++) tx.Insert(DBreezeLogStorage.LogTable, i, BitConverter.GetBytes(i));
            tx.Insert(DBreezeLogStorage.ConfigurationTable, 2L, "config2"u8.ToArray());
            tx.Insert(DBreezeLogStorage.ConfigurationTable, 4L, "config4"u8.ToArray());
            tx.Commit();
        }
        using (var read = new DBreezeLogTransaction(engine.GetTransaction(), layout, writable: false))
        {
            Assert.Equal(new long[] { 2, 3, 4, 5 }, read.SelectForwardStartFrom<long, byte[]>(DBreezeLogStorage.LogTable, 2, true).Select(r => r.Key));
            Assert.Equal(new long[] { 3, 4, 5 }, read.SelectForwardStartFrom<long, byte[]>(DBreezeLogStorage.LogTable, 2, false).Select(r => r.Key));
            Assert.Equal(new long[] { 4, 3, 2, 1 }, read.SelectBackwardStartFrom<long, byte[]>(DBreezeLogStorage.LogTable, 4, true).Select(r => r.Key));
            Assert.Equal(new long[] { 3, 2, 1 }, read.SelectBackwardStartFrom<long, byte[]>(DBreezeLogStorage.LogTable, 4, false).Select(r => r.Key));
        }
        using (var tx = new DBreezeLogTransaction(engine.GetTransaction(), layout, writable: true))
        {
            tx.SynchronizeTables(DBreezeLogStorage.LogTable);
            foreach (var row in tx.SelectForwardStartFrom<long, byte[]>(DBreezeLogStorage.LogTable, 3, true, AsReadVisibilityScope: true))
                tx.RemoveKey(DBreezeLogStorage.LogTable, row.Key);
            tx.Commit();
        }
        using (var tx = new DBreezeLogTransaction(engine.GetTransaction(), layout, writable: true))
        {
            tx.SynchronizeTables(DBreezeLogStorage.LogTable);
            Assert.Equal(new long[] { 1, 2 }, tx.SelectForward<long, byte[]>(DBreezeLogStorage.LogTable).Select(r => r.Key));
            tx.RemoveAllKeys(DBreezeLogStorage.LogTable, false);
            tx.Commit();
        }
        using (var verify = new DBreezeLogTransaction(engine.GetTransaction(), layout, writable: false))
        {
            Assert.Empty(verify.SelectForward<long, byte[]>(DBreezeLogStorage.LogTable));
            Assert.Equal("keep"u8.ToArray(), verify.Select<string, byte[]>(DBreezeLogStorage.MetaTable, "sentinel").Value);
            Assert.Equal(new long[] { 2, 4 }, verify.SelectForward<long, byte[]>(DBreezeLogStorage.ConfigurationTable).Select(r => r.Key));
        }
        if (layout == DBreezeLogLayout.UnifiedTableV3)
        {
            // Verify the public DBreeze key convention independently of the routing wrapper.
            using var raw = engine.GetTransaction();
            Assert.Equal("config2"u8.ToArray(), raw.Select<byte[], byte[]>(DBreezeLogStorage.UnifiedTable, 3.ToIndex(2L)).Value);
        }
    }
}
