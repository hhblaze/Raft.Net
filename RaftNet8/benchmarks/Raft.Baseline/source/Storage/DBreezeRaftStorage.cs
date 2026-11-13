using System.Buffers.Binary;
using System.Text;
using DBreeze;

namespace Raft.Storage;

/// <summary>Versioned Raft storage. One engine/folder per physical node; no legacy database compatibility.</summary>
public sealed class DBreezeRaftStorage : IRaftStorage
{
    private readonly object _gate = new();
    private readonly DBreezeEngine _engine;
    private readonly bool _ownsEngine;
    private readonly RaftOptions _options;
    private readonly string _meta;
    private readonly string _log;
    private readonly Dictionary<long, LinkedListNode<LogEntry>> _cache = new();
    private readonly LinkedList<LogEntry> _lru = new();
    private HardState _state = new();
    private long _lastIndex, _lastTerm, _cacheBytes, _reads, _writes, _hits;
    private bool _disposed;

    public DBreezeRaftStorage(string path, RaftOptions options)
        : this(new DBreezeEngine(path), options, ownsEngine: true) { }

    /// <summary>The caller retains engine ownership unless ownsEngine is explicitly true.</summary>
    public DBreezeRaftStorage(DBreezeEngine engine, RaftOptions options, bool ownsEngine = false)
    {
        _engine = engine ?? throw new ArgumentNullException(nameof(engine));
        _ownsEngine = ownsEngine;
        try
        {
            _options = options.Freeze();
            _meta = "raft8/meta";
            _log = "raft8/log";
            Initialize();
        }
        catch
        {
            if (_ownsEngine) _engine.Dispose();
            throw;
        }
    }

    public static DBreezeRaftStorage CreateInMemory(RaftOptions options) =>
        new(new DBreezeEngine(new DBreezeConfiguration { Storage = DBreezeConfiguration.eStorage.MEMORY }), options, true);

    public HardState HardState { get { lock (_gate) return _state; } }
    public long LastIndex { get { lock (_gate) return _lastIndex; } }
    public long LastTerm { get { lock (_gate) return _lastTerm; } }
    public StorageMetrics Metrics { get { lock (_gate) return new(_reads, _writes, _hits, _cache.Count, _cacheBytes); } }

    private void Initialize()
    {
        using var transaction = _engine.GetTransaction();
        transaction.SynchronizeTables(_meta, _log);
        _reads++;
        var identity = Encoding.UTF8.GetBytes("raft8-v1\n" + _options.GroupId + "\n" + _options.NodeId + "\n" +
            string.Join('\n', _options.Members.Order(StringComparer.Ordinal)));
        var identityRow = transaction.Select<string, byte[]>(_meta, "identity");
        var stateRow = transaction.Select<string, byte[]>(_meta, "state");
        using (var tail = transaction.SelectBackward<long, byte[]>(_log).GetEnumerator())
        {
            if (tail.MoveNext())
            {
                var entry = DecodeEntry(tail.Current.Key, tail.Current.Value);
                _lastIndex = entry.Index;
                _lastTerm = entry.Term;
                Cache(entry);
            }
        }
        if (!identityRow.Exists)
        {
            if (stateRow.Exists || _lastIndex != 0) throw new InvalidDataException("Raft storage has data without an identity.");
            transaction.Insert<string, byte[]>(_meta, "identity", identity);
            transaction.Insert<string, byte[]>(_meta, "state", EncodeState(_state));
            transaction.Commit();
            _writes++;
        }
        else
        {
            if (!identityRow.Value.AsSpan().SequenceEqual(identity) || !stateRow.Exists)
                throw new InvalidDataException("Raft storage identity, membership, or schema differs. Use the original configuration.");
            _state = DecodeState(stateRow.Value);
            if (_state.Term < _lastTerm || _state.CommitIndex > _lastIndex || _state.CommitIndex < 0 ||
                (_state.VotedFor is not null && !_options.Members.Contains(_state.VotedFor, StringComparer.Ordinal)))
                throw new InvalidDataException("Raft hard state does not agree with the log or membership.");
            if (_state.CommitIndex > 0 && !transaction.Select<long, byte[]>(_log, _state.CommitIndex).Exists)
                throw new InvalidDataException("Committed log entry is missing.");
        }
    }

    public void SaveHardState(HardState state)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            if (state == _state) return;
            if (state.Term < _state.Term || state.Term < _lastTerm || state.CommitIndex < _state.CommitIndex ||
                state.CommitIndex > _lastIndex || (state.Term == 0 && state.VotedFor is not null) ||
                (state.Term == _state.Term && _state.VotedFor is not null && state.VotedFor != _state.VotedFor) ||
                (state.VotedFor is not null && !_options.Members.Contains(state.VotedFor, StringComparer.Ordinal)))
                throw new InvalidOperationException("Term, vote, or commit would violate durable Raft state.");
            using var transaction = _engine.GetTransaction();
            transaction.SynchronizeTables(_meta);
            transaction.Insert<string, byte[]>(_meta, "state", EncodeState(state));
            transaction.Commit();
            _writes++;
            _state = state;
        }
    }

    public void Append(IReadOnlyList<LogEntry> entries)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            if (entries.Count == 0) return;
            ReplaceSuffix(checked(_lastIndex + 1), entries);
        }
    }

    public void ReplaceSuffix(long fromIndex, IReadOnlyList<LogEntry> entries)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            if (fromIndex <= _state.CommitIndex || fromIndex < 1 || fromIndex - 1 > _lastIndex)
                throw new InvalidOperationException("Cannot replace a committed prefix or create a log gap.");
            var previousTerm = GetTerm(fromIndex - 1);
            var expected = fromIndex;
            foreach (var entry in entries)
            {
                if (entry.Index != expected || entry.Term < previousTerm || entry.Term > _state.Term ||
                    entry.Command.Length > _options.MaxCommandBytes)
                    throw new InvalidOperationException("Log entries must be contiguous with nondecreasing, persisted terms.");
                expected = checked(expected + 1);
                previousTerm = entry.Term;
            }
            if (entries.Count == 0 && fromIndex > _lastIndex) return;
            using (var transaction = _engine.GetTransaction())
            {
                transaction.SynchronizeTables(_log);
                if (fromIndex <= _lastIndex)
                {
                    foreach (var row in transaction.SelectForwardStartFrom<long, byte[]>(
                                 _log, fromIndex, true, AsReadVisibilityScope: true))
                        transaction.RemoveKey<long>(_log, row.Key);
                }
                foreach (var entry in entries)
                    transaction.Insert<long, byte[]>(_log, entry.Index, EncodeEntry(entry));
                transaction.Commit();
                _writes++;
            }
            // Never expose uncommitted values through the cache.
            var cursor = _lru.First;
            while (cursor is not null)
            {
                var next = cursor.Next;
                if (cursor.Value.Index >= fromIndex) Evict(cursor);
                cursor = next;
            }
            foreach (var entry in entries) Cache(entry);
            _lastIndex = entries.Count == 0 ? fromIndex - 1 : entries[^1].Index;
            _lastTerm = previousTerm;
        }
    }

    public long GetTerm(long index)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            if (index == 0) return 0;
            if (index < 0 || index > _lastIndex) throw new ArgumentOutOfRangeException(nameof(index));
            if (index == _lastIndex) return _lastTerm;
            if (TryCached(index, out var entry)) return entry!.Term;
            using var transaction = _engine.GetTransaction();
            _reads++;
            var row = transaction.Select<long, byte[]>(_log, index);
            if (!row.Exists) throw new InvalidDataException($"Log gap at index {index}.");
            // Consistency checks need only the fixed header, not a potentially 64 KiB command.
            return DecodeTerm(row.GetValuePart(0, 9));
        }
    }

    public IReadOnlyList<LogEntry> Read(long fromIndex, int maxCount, int maxBytes)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            if (fromIndex < 1 || maxCount < 1 || maxBytes < 1) throw new ArgumentOutOfRangeException(nameof(fromIndex));
            if (fromIndex > _lastIndex) return Array.Empty<LogEntry>();
            var result = new List<LogEntry>(Math.Min(maxCount, 128));
            var bytes = 0;
            var index = fromIndex;
            while (index <= _lastIndex && result.Count < maxCount && TryCached(index, out var entry))
            {
                if (!Add(entry!)) return result;
                index++;
            }
            if (index > _lastIndex || result.Count == maxCount) return result;
            using var transaction = _engine.GetTransaction();
            _reads++;
            foreach (var row in transaction.SelectForwardStartFrom<long, byte[]>(_log, index, true))
            {
                if (row.Key != index) throw new InvalidDataException($"Log gap at index {index}.");
                var entry = DecodeEntry(row.Key, row.Value);
                Cache(entry);
                if (!Add(entry)) break;
                index++;
                if (result.Count == maxCount) break;
            }
            if (result.Count == 0) throw new InvalidDataException($"Log gap at index {fromIndex}.");
            return result;

            bool Add(LogEntry entry)
            {
                var size = entry.Command.Length + LogEntry.EncodedOverhead;
                if (size > maxBytes - bytes)
                {
                    if (result.Count == 0) throw new ArgumentException("Byte limit cannot hold the next log entry.", nameof(maxBytes));
                    return false;
                }
                result.Add(entry);
                bytes += size;
                return true;
            }
        }
    }

    public long FirstIndexOfTerm(long index)
    {
        lock (_gate)
        {
            var term = GetTerm(index);
            if (index == 0) return 0;
            var first = index;
            using var transaction = _engine.GetTransaction();
            _reads++;
            foreach (var row in transaction.SelectBackwardStartFrom<long, byte[]>(_log, index, true))
            {
                if (DecodeTerm(row.GetValuePart(0, 9)) != term) break;
                first = row.Key;
            }
            return first;
        }
    }

    public long LastIndexOfTerm(long term)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            if (term == _lastTerm) return _lastIndex;
            using var transaction = _engine.GetTransaction();
            _reads++;
            foreach (var row in transaction.SelectBackward<long, byte[]>(_log))
            {
                var found = DecodeTerm(row.GetValuePart(0, 9));
                if (found == term) return row.Key;
                if (found < term) break;
            }
            return 0;
        }
    }

    private bool TryCached(long index, out LogEntry? entry)
    {
        if (_cache.TryGetValue(index, out var node))
        {
            _lru.Remove(node);
            _lru.AddLast(node);
            entry = node.Value;
            _hits++;
            return true;
        }
        entry = null;
        return false;
    }

    private void Cache(LogEntry entry)
    {
        var size = entry.Command.Length + LogEntry.EncodedOverhead;
        if (_options.LogCacheEntries == 0 || size > _options.LogCacheBytes) return;
        if (_cache.TryGetValue(entry.Index, out var old)) Evict(old);
        while (_lru.First is not null && (_cache.Count >= _options.LogCacheEntries || _cacheBytes + size > _options.LogCacheBytes))
            Evict(_lru.First);
        _cache.Add(entry.Index, _lru.AddLast(entry));
        _cacheBytes += size;
    }

    private void Evict(LinkedListNode<LogEntry> node)
    {
        _cache.Remove(node.Value.Index);
        _cacheBytes -= node.Value.Command.Length + LogEntry.EncodedOverhead;
        _lru.Remove(node);
    }

    private static byte[] EncodeEntry(LogEntry entry)
    {
        var bytes = new byte[9 + entry.Command.Length];
        BinaryPrimitives.WriteInt64LittleEndian(bytes, entry.Term);
        bytes[8] = entry.IsNoOp ? (byte)1 : (byte)0;
        entry.Command.Span.CopyTo(bytes.AsSpan(9));
        return bytes;
    }

    private LogEntry DecodeEntry(long index, byte[] bytes)
    {
        var term = DecodeTerm(bytes);
        if (bytes[8] > 1 || bytes.Length - 9 > _options.MaxCommandBytes || (bytes[8] == 1 && bytes.Length != 9))
            throw new InvalidDataException("Invalid log entry encoding or configured command limit.");
        return new LogEntry(index, term, bytes.AsSpan(9), bytes[8] == 1);
    }

    private static long DecodeTerm(byte[] bytes)
    {
        if (bytes.Length < 9) throw new InvalidDataException("Truncated log entry.");
        var term = BinaryPrimitives.ReadInt64LittleEndian(bytes);
        if (term < 1) throw new InvalidDataException("Invalid log term.");
        return term;
    }

    private static byte[] EncodeState(HardState state)
    {
        var vote = state.VotedFor is null ? Array.Empty<byte>() : Encoding.UTF8.GetBytes(state.VotedFor);
        var bytes = new byte[20 + vote.Length];
        BinaryPrimitives.WriteInt64LittleEndian(bytes, state.Term);
        BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(8), state.CommitIndex);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(16), vote.Length);
        vote.CopyTo(bytes.AsSpan(20));
        return bytes;
    }

    private static HardState DecodeState(byte[] bytes)
    {
        if (bytes.Length < 20) throw new InvalidDataException("Truncated hard state.");
        var length = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(16));
        var term = BinaryPrimitives.ReadInt64LittleEndian(bytes);
        var commit = BinaryPrimitives.ReadInt64LittleEndian(bytes.AsSpan(8));
        if (length < 0 || length > 128 || length != bytes.Length - 20 || term < 0 || commit < 0 || (term == 0 && length != 0))
            throw new InvalidDataException("Invalid hard state.");
        return new HardState(term, length == 0 ? null : Encoding.UTF8.GetString(bytes.AsSpan(20)), commit);
    }

    private void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(_disposed, this);
    public void Dispose()
    {
        lock (_gate)
        {
            if (_disposed) return;
            _disposed = true;
            _cache.Clear();
            _lru.Clear();
            _cacheBytes = 0;
            if (_ownsEngine) _engine.Dispose();
        }
    }
}
