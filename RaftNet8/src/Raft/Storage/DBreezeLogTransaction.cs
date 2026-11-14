using DBreeze.DataTypes;
using DBreeze.Transactions;
using DBreeze.Utils;

namespace Raft.Storage;

/// <summary>Explicit on-disk table layout; v3 packs hard state and frontier in one checked manifest.</summary>
public enum DBreezeLogLayout { SeparateTablesV2 = 2, UnifiedTableV3 = 3, UnifiedTableV4 = 4 }

/// <summary>A transaction-scoped row view; partial header reads never materialize full payloads.</summary>
internal readonly struct DBreezeLogRow<TKey, TValue>
{
    private readonly Row<TKey, TValue>? _typed;
    private readonly Row<byte[], TValue>? _unified;
    internal DBreezeLogRow(Row<TKey, TValue> row) { _typed = row; Key = row.Key; }
    internal DBreezeLogRow(Row<byte[], TValue> row, TKey key) { _unified = row; Key = key; }
    public TKey Key { get; }
    public bool Exists => _typed?.Exists ?? _unified!.Exists;
    public TValue Value => _typed is { } row ? row.Value : _unified!.Value;
    public byte[] GetValuePart(uint from, uint count) =>
        _typed is { } row ? row.GetValuePart(from, count) : _unified!.GetValuePart(from, count);
}

/// <summary>
/// V3 uses ordered ToIndex prefixes 1=metadata,2=log,3=configuration in one physical trie.
/// Every traversal/deletion is confined to its namespace. Handles never escape this synchronous transaction.
/// </summary>
internal sealed class DBreezeLogTransaction(Transaction inner, DBreezeLogLayout layout, bool writable, bool initializing = false) : IDisposable
{
    private static byte Prefix(string table) => table switch
    {
        DBreezeLogStorage.MetaTable => 1,
        DBreezeLogStorage.LogTable => 2,
        DBreezeLogStorage.ConfigurationTable => 3,
        DBreezeLogStorage.MembershipIndexTable => 4,
        DBreezeLogStorage.MembershipHistoryTable => 5,
        _ => throw new ArgumentException("Unknown logical Raft table.", nameof(table))
    };
    private bool Unified => layout != DBreezeLogLayout.SeparateTablesV2;
    private string Table => layout == DBreezeLogLayout.UnifiedTableV4 ? DBreezeLogStorage.UnifiedTableV4 : DBreezeLogStorage.UnifiedTable;
    internal static byte[] Key<TKey>(byte prefix, TKey key) => prefix.ToIndex(key!);
    private void RequireWritable()
    {
        if (!writable) throw new InvalidOperationException("A read transaction cannot mutate the log.");
    }
    public void SynchronizeTables(params string[] tables)
    {
        RequireWritable();
        if (!Unified) inner.SynchronizeTables(tables);
        else if (initializing) inner.SynchronizeTables(Table, DBreezeLogStorage.UnifiedTable, DBreezeLogStorage.MetaTable, "raft8/meta");
        else inner.SynchronizeTables(Table);
    }
    public void WriteLegacyGuards()
    {
        if (!Unified) return;
        if (!initializing) throw new InvalidOperationException("Layout guards belong to initialization.");
        inner.Insert<string, byte[]>(DBreezeLogStorage.MetaTable, "state", StorageCodecV2.Wrap((layout == DBreezeLogLayout.UnifiedTableV4 ? "raft8-v4"u8 : "raft8-v3"u8)));
        inner.Insert<string, byte[]>("raft8/meta", "state", (layout == DBreezeLogLayout.UnifiedTableV4 ? "raft8-v4"u8 : "raft8-v3"u8).ToArray());
        WriteV4Guard();
        // Unreleased v3 prototypes expected separate state/frontier rows. Fail closed there too.
        inner.Insert<byte[], byte[]>(Table, Key(1, "state"), StorageCodecV2.Wrap((layout == DBreezeLogLayout.UnifiedTableV4 ? "packed-v4"u8 : "packed-v3"u8)));
    }
    public void WriteV4Guard()
    {
        if (layout == DBreezeLogLayout.UnifiedTableV4)
            inner.Insert<byte[], byte[]>(DBreezeLogStorage.UnifiedTable, Key(1, "state"), StorageCodecV2.Wrap("raft8-v4"u8));
    }
    public DBreezeLogRow<TKey, TValue> Select<TKey, TValue>(string table, TKey key) => Unified && table != "raft8/meta"
        ? new(inner.Select<byte[], TValue>(Table, Key(Prefix(table), key)), key)
        : new(inner.Select<TKey, TValue>(table, key));
    public void Insert<TKey, TValue>(string table, TKey key, TValue value)
    {
        RequireWritable();
        if (Unified) inner.Insert<byte[], TValue>(Table, Key(Prefix(table), key), value);
        else inner.Insert(table, key, value);
    }
    public void RemoveKey<TKey>(string table, TKey key)
    {
        RequireWritable();
        if (Unified) inner.RemoveKey<byte[]>(Table, Key(Prefix(table), key));
        else inner.RemoveKey(table, key);
    }
    public void RemoveAllKeys(string table, bool withFileRecreation)
    {
        RequireWritable();
        if (!Unified) { inner.RemoveAllKeys(table, withFileRecreation); return; }
        if (withFileRecreation) throw new NotSupportedException("A logical namespace cannot recreate its shared file.");
        // Snapshot replacement clears only this ordered namespace, in bounded memory.
        foreach (var row in inner.SelectForwardStartsWith<byte[], byte[]>(Table,
            [Prefix(table)], AsReadVisibilityScope: true))
            inner.RemoveKey<byte[]>(Table, row.Key);
    }
    public IEnumerable<DBreezeLogRow<TKey, TValue>> SelectForward<TKey, TValue>(string table, bool AsReadVisibilityScope = false) =>
        Unified ? UnifiedRows<TKey, TValue>(inner.SelectForwardStartsWith<byte[], TValue>(Table,
            [Prefix(table)], AsReadVisibilityScope)) : Rows(inner.SelectForward<TKey, TValue>(table, AsReadVisibilityScope));
    public IEnumerable<DBreezeLogRow<TKey, TValue>> SelectBackward<TKey, TValue>(string table, bool AsReadVisibilityScope = false) =>
        Unified ? UnifiedRows<TKey, TValue>(inner.SelectBackwardStartsWith<byte[], TValue>(Table,
            [Prefix(table)], AsReadVisibilityScope)) : Rows(inner.SelectBackward<TKey, TValue>(table, AsReadVisibilityScope));
    public IEnumerable<DBreezeLogRow<TKey, TValue>> SelectForwardStartFrom<TKey, TValue>(string table, TKey from, bool include,
        bool AsReadVisibilityScope = false) => Unified
        ? UnifiedRows<TKey, TValue>(inner.SelectForwardFromTo<byte[], TValue>(Table,
            Key(Prefix(table), from), include, [(byte)(Prefix(table) + 1)], false, AsReadVisibilityScope))
        : Rows(inner.SelectForwardStartFrom<TKey, TValue>(table, from, include, AsReadVisibilityScope));
    public IEnumerable<DBreezeLogRow<TKey, TValue>> SelectBackwardStartFrom<TKey, TValue>(string table, TKey from, bool include,
        bool AsReadVisibilityScope = false) => Unified
        ? UnifiedRows<TKey, TValue>(inner.SelectBackwardFromTo<byte[], TValue>(Table,
            Key(Prefix(table), from), include, [Prefix(table)], true, AsReadVisibilityScope))
        : Rows(inner.SelectBackwardStartFrom<TKey, TValue>(table, from, include, AsReadVisibilityScope));

    private static IEnumerable<DBreezeLogRow<TKey, TValue>> Rows<TKey, TValue>(IEnumerable<Row<TKey, TValue>> source)
    {
        foreach (var row in source) yield return new(row);
    }
    private static IEnumerable<DBreezeLogRow<TKey, TValue>> UnifiedRows<TKey, TValue>(IEnumerable<Row<byte[], TValue>> source)
    {
        foreach (var row in source)
        {
            var key = row.Key;
            if (key.Length < 2 || (typeof(TKey) == typeof(long) && key.Length != 9))
                throw new InvalidDataException("Invalid v3 composite key.");
            yield return new(row, DataTypesConvertor.ConvertBack<TKey>(key[1..]));
        }
    }
    public void Commit() { RequireWritable(); inner.Commit(); }
    public void Dispose() => inner.Dispose();
}
