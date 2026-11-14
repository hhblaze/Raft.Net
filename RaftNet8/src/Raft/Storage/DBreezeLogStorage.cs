using System.Buffers.Binary;
using System.Collections.Immutable;
using DBreeze;
using DBreeze.Transactions;

namespace Raft.Storage;

public enum PersistenceBoundary { BeforeTransaction, AfterLogMutation, BeforeCommit, AfterCommit, AfterCachePublication }

/// <summary>
/// Checked DBreeze store with explicit v2 separate tables or v3 prefixed single-table layout.
/// The durable frontier is committed atomically with log/config changes. Layout changes require offline copy migration.
/// </summary>
public sealed partial class DBreezeLogStorage : IExtendedRaftStorage, IRaftStorageIdentity
{
    public const string MetaTable = "raft8/v2/meta";
    public const string LogTable = "raft8/v2/log";
    public const string ConfigurationTable = "raft8/v2/config";
    public const string UnifiedTable = "raft8/v3";
    public const string UnifiedTableV4 = "raft8/v4";
    internal const string MembershipIndexTable = "raft8/v4/management";
    internal const string MembershipHistoryTable = "raft8/v4/results";
    public DBreezeLogLayout Layout { get; }
    private readonly object _gate = new();
    // Lock order is storage gate -> cache gate. Cache-only reads never take the
    // storage gate or perform I/O, so an unrelated flush cannot delay application.
    private readonly object _cacheGate = new();
    private bool _writeInProgress;
    private readonly DBreezeEngine _engine;
    private readonly bool _ownsEngine;
    private readonly RaftOptions _options;
    private readonly Action<PersistenceBoundary>? _observe;
    private readonly bool _migration;
    private readonly Dictionary<long, LinkedListNode<LogEntry>> _cache = new();
    private readonly LinkedList<LogEntry> _lru = new();
    private long _cacheBytes, _reads, _writes, _hits;
    private HardState _state = new();
    private StorageCodecV2.Frontier _frontier = new(1, 0, 0, 0, 0);
    private SnapshotMetadata? _snapshot;
    private SnapshotInstallIntent? _installIntent;
    private ClusterConfiguration _configuration = null!;
    private ImmutableArray<string> _knownMembers = ImmutableArray<string>.Empty;
    private StorageIdentity _identity = null!;
    private Exception? _failure;
    private bool _disposed;

    public DBreezeLogStorage(string path, RaftOptions options, Action<PersistenceBoundary>? observe = null)
        : this(OpenEngine(path), options, true, observe) { }

    public DBreezeLogStorage(string path, RaftOptions options, DBreezeLogLayout layout, Action<PersistenceBoundary>? observe = null)
        : this(OpenEngine(path), options, true, observe, false, layout) { }

    public DBreezeLogStorage(DBreezeEngine engine, RaftOptions options, DBreezeLogLayout layout, bool ownsEngine = false,
        Action<PersistenceBoundary>? observe = null)
        : this(engine, options, ownsEngine, observe, false, layout) { }

    public DBreezeLogStorage(DBreezeEngine engine, RaftOptions options, bool ownsEngine = false,
        Action<PersistenceBoundary>? observe = null)
        : this(engine, options, ownsEngine, observe, false) { }

    private DBreezeLogStorage(DBreezeEngine engine, RaftOptions options, bool ownsEngine,
        Action<PersistenceBoundary>? observe, bool migration, DBreezeLogLayout layout = DBreezeLogLayout.SeparateTablesV2)
    {
        _engine = engine ?? throw new ArgumentNullException(nameof(engine));
        _ownsEngine = ownsEngine;
        _observe = observe;
        _migration = migration;
        Layout = layout;
        try
        {
            if (layout is not DBreezeLogLayout.SeparateTablesV2 and not DBreezeLogLayout.UnifiedTableV3 and not DBreezeLogLayout.UnifiedTableV4)
                throw new ArgumentOutOfRangeException(nameof(layout));
            _options = options.Freeze();
            if ((layout == DBreezeLogLayout.UnifiedTableV4) != (_options.WireVersion == 3))
                throw new ArgumentException("Wire3 requires explicit raft8-v4 storage, and raft8-v4 requires wire3.");
            CheckLayout();
            Initialize();
        }
        catch { if (_ownsEngine) _engine.Dispose(); throw; }
    }
    internal static DBreezeLogStorage OpenMigrationTarget(DBreezeEngine engine, RaftOptions options,
        DBreezeLogLayout layout = DBreezeLogLayout.SeparateTablesV2) =>
        new(engine, options, false, null, true, layout);
    private static DBreezeEngine OpenEngine(string path)
    {
        if (File.Exists(Path.Combine(path, ".raft-migration-incomplete")))
            throw new InvalidDataException("Migration is incomplete; do not start this target.");
        return new(path);
    }

    public static DBreezeLogStorage CreateInMemory(RaftOptions options, Action<PersistenceBoundary>? observe = null) =>
        new(new DBreezeEngine(new DBreezeConfiguration { Storage = DBreezeConfiguration.eStorage.MEMORY }), options, true, observe);

    public static DBreezeLogStorage CreateInMemory(RaftOptions options, DBreezeLogLayout layout,
        Action<PersistenceBoundary>? observe = null) =>
        new(new DBreezeEngine(new DBreezeConfiguration { Storage = DBreezeConfiguration.eStorage.MEMORY }), options, true, observe, false, layout);

    private DBreezeLogTransaction OpenTransaction(bool writable = false, bool initializing = false) =>
        new(_engine.GetTransaction(), Layout, writable, initializing);

    private void CheckLayout()
    {
        using var tx = _engine.GetTransaction();
        var hasV4 = _engine.Scheme.IfUserTableExists(UnifiedTableV4) && tx.SelectForward<byte[], byte[]>(UnifiedTableV4).Any();
        if (Layout == DBreezeLogLayout.UnifiedTableV4)
        {
            if (hasV4)
            {
                if (!tx.Select<byte[], byte[]>(UnifiedTableV4, DBreezeLogTransaction.Key(1, "identity")).Exists)
                    throw new InvalidDataException("V4 data has no identity.");
                return;
            }
            if (new[] { UnifiedTable, MetaTable, LogTable, ConfigurationTable, "raft8/meta", "raft8/log" }
                .Any(table => _engine.Scheme.IfUserTableExists(table) && tx.SelectForward<byte[], byte[]>(table).Any()))
                throw new InvalidDataException("Use a fresh database for raft8-v4; older data is not converted implicitly.");
            return;
        }
        if (hasV4) throw new InvalidDataException("This is a raft8-v4 database; select its explicit layout.");
        var hasUnified = _engine.Scheme.IfUserTableExists(UnifiedTable) && tx.SelectForward<byte[], byte[]>(UnifiedTable).Any();
        if (Layout == DBreezeLogLayout.SeparateTablesV2)
        {
            if (hasUnified) throw new InvalidDataException("This is a unified v3 database; select its explicit layout.");
            return;
        }
        var v1 = tx.Select<string, byte[]>("raft8/meta", "identity").Exists;
        var v2 = tx.Select<string, byte[]>(MetaTable, "identity").Exists;
        if (v1 || v2) throw new InvalidDataException("Existing v1/v2 data requires explicit offline copy migration to v3.");
        if (!hasUnified && new[] { MetaTable, LogTable, ConfigurationTable, "raft8/meta", "raft8/log" }
            .Any(table => _engine.Scheme.IfUserTableExists(table) && tx.SelectForward<byte[], byte[]>(table).Any()))
            throw new InvalidDataException("Unrecognized or incomplete existing Raft state; refusing v3 initialization.");
        if (hasUnified)
        {
            if (!tx.Select<byte[], byte[]>(UnifiedTable, DBreezeLogTransaction.Key(1, "identity")).Exists)
                throw new InvalidDataException("Unified v3 data has no identity.");
        }
    }

    public HardState HardState { get { lock (_gate) { Check(); return _state; } } }
    public long LastIndex { get { lock (_gate) { Check(); return _frontier.LastIndex; } } }
    public long LastTerm { get { lock (_gate) { Check(); return _frontier.LastTerm; } } }
    public StorageIdentity Identity { get { lock (_gate) { Check(); return _identity; } } }
    public StorageMetrics Metrics { get { lock (_gate) lock (_cacheGate) return new(_reads, _writes, _hits, _cache.Count, _cacheBytes); } }

    private void Initialize()
    {
        using var tx = OpenTransaction(writable: true, initializing: true);
        tx.SynchronizeTables(MetaTable, LogTable, ConfigurationTable);
        _reads++;
        var migration = tx.Select<string, byte[]>(MetaTable, "migration");
        if (!_migration && migration.Exists && System.Text.Encoding.UTF8.GetString(StorageCodecV2.Open(migration.Value)) != "complete")
            throw new InvalidDataException("Migration is incomplete.");
        var identity = tx.Select<string, byte[]>(MetaTable, "identity");
        if (!identity.Exists)
        {
            if (!_migration && tx.Select<string, byte[]>("raft8/meta", "identity").Exists)
                throw new InvalidDataException("raft8-v1 data exists. Use the offline copy migration tool.");
            if (tx.SelectForward<long, byte[]>(LogTable).Any() || tx.Select<string, byte[]>(MetaTable, "state").Exists)
                throw new InvalidDataException("V2 data has no identity.");
            _identity = new(_options.GroupId, _options.NodeId,
                new(ImmutableArray.CreateRange(_options.Members), ImmutableArray.CreateRange(_options.Learners)));
            _configuration = _identity.Bootstrap.Validate();
            _knownMembers = _configuration.AllMembers.ToImmutableArray();
            tx.Insert<string, byte[]>(MetaTable, "identity", StorageCodecV2.Identity(_identity));
            InitializeMembership(tx, fresh: true);
            WriteState(tx, _state, _frontier);
            if (_migration)
                tx.Insert<string, byte[]>(MetaTable, "migration", StorageCodecV2.Wrap("incomplete"u8));
            tx.WriteLegacyGuards();
            Commit(tx);
            _writes++;
            return;
        }
        _identity = StorageCodecV2.Identity(identity.Value);
        var known = tx.Select<string, byte[]>(MetaTable, "knownMembers");
        _knownMembers = known.Exists ? ClusterConfiguration.Decode(StorageCodecV2.Open(known.Value)).Voters :
            _identity.Bootstrap.AllMembers.ToImmutableArray();
        if (_identity.GroupId != _options.GroupId || _identity.NodeId != _options.NodeId)
            throw new InvalidDataException("The persisted group/node identity differs.");
        if (Layout is DBreezeLogLayout.UnifiedTableV3 or DBreezeLogLayout.UnifiedTableV4)
        {
            var manifest = tx.Select<string, byte[]>(MetaTable, "manifest");
            if (!manifest.Exists) throw new InvalidDataException("Missing packed v3 durable manifest; incompatible prototype format.");
            (_state, _frontier) = StorageCodecV2.ReadManifest(manifest.Value, Layout == DBreezeLogLayout.UnifiedTableV4);
        }
        else
        {
            var hard = tx.Select<string, byte[]>(MetaTable, "state");
            var frontier = tx.Select<string, byte[]>(MetaTable, "frontier");
            if (!hard.Exists || !frontier.Exists) throw new InvalidDataException("Missing durable state/frontier.");
            _state = StorageCodecV2.Hard(hard.Value);
            _frontier = StorageCodecV2.Tail(frontier.Value);
        }
        var snapshot = tx.Select<string, byte[]>(MetaTable, "snapshot");
        if (snapshot.Exists) _snapshot = StorageCodecV2.Snapshot(snapshot.Value);
        var intent = tx.Select<string, byte[]>(MetaTable, "installIntent");
        if (intent.Exists)
        {
            _installIntent = StorageCodecV2.Intent(intent.Value);
            if (_installIntent.Metadata.LastIncludedIndex <= _state.CommitIndex || _installIntent.Metadata.LastIncludedTerm > _state.Term)
                throw new InvalidDataException("Snapshot install intent disagrees with durable hard state.");
        }
        if (_frontier.FirstIndex < 1 || _frontier.LastIndex < _frontier.FirstIndex - 1 ||
            _frontier.LastIndex < (_snapshot?.LastIncludedIndex ?? 0) || _state.Term < _frontier.LastTerm ||
            _state.CommitIndex < (_snapshot?.LastIncludedIndex ?? 0) || _state.CommitIndex > _frontier.LastIndex ||
            (_snapshot is null && _frontier.FirstIndex != 1))
            throw new InvalidDataException("Durable frontier, hard state, and snapshot disagree.");
        _configuration = BaseConfiguration(_snapshot);
        var expected = _frontier.FirstIndex;
        long previousTerm = 0, total = 0, committed = 0;
        var cleanup = false;
        foreach (var row in tx.SelectForward<long, byte[]>(LogTable))
        {
            if (row.Key > _frontier.LastIndex)
            {
                // Never promised: frontier and every acknowledged append share one DBreeze commit.
                cleanup = true;
                continue;
            }
            if (row.Key != expected) throw new InvalidDataException($"Durable log gap at {expected}.");
            var entry = StorageCodecV2.Entry(row.Key, row.Value, _options.MaxCommandBytes, Layout == DBreezeLogLayout.UnifiedTableV4);
            if (entry.Term < previousTerm || entry.Term > _state.Term)
                throw new InvalidDataException("Log terms disagree with durable state.");
            expected++;
            previousTerm = entry.Term;
            total += Size(entry);
            if (entry.Index <= _state.CommitIndex) committed += Size(entry);
            if (entry.Kind == LogEntryKind.Configuration && entry.Index > (_snapshot?.LastIncludedIndex ?? 0))
                _configuration = ClusterConfiguration.Decode(entry.Command.Span, entry.Index, entry.Term);
            if (entry.Kind == LogEntryKind.Configuration)
            {
                var indexed = tx.Select<long, byte[]>(ConfigurationTable, row.Key);
                if (!indexed.Exists || !StorageCodecV2.Open(indexed.Value).SequenceEqual(ConfigValue(entry)))
                    throw new InvalidDataException("Missing or inconsistent configuration index.");
            }
            if (Layout == DBreezeLogLayout.UnifiedTableV4 && entry.Kind == LogEntryKind.Membership)
            {
                var control = tx.Select<long, byte[]>(MembershipIndexTable, entry.Index);
                if (!control.Exists || !control.Value.AsSpan().SequenceEqual(row.Value))
                    throw new InvalidDataException("Missing or inconsistent membership index.");
            }
            Cache(entry);
        }
        if (expected != _frontier.LastIndex + 1 ||
            (_frontier.LastIndex >= _frontier.FirstIndex && previousTerm != _frontier.LastTerm) ||
            (_frontier.LastIndex < _frontier.FirstIndex && _frontier.LastTerm != (_snapshot?.LastIncludedTerm ?? 0)) ||
            total != _frontier.TotalBytes || committed != _frontier.CommittedBytes)
            throw new InvalidDataException("Durable log/frontier length or byte totals disagree.");
        VerifyConfigurationIndex(tx);
        InitializeMembership(tx, fresh: false);
        cleanup |= tx.SelectForwardStartFrom<long, byte[]>(ConfigurationTable, _frontier.LastIndex + 1, true).Any();
        if (cleanup)
        {
            foreach (var row in tx.SelectForwardStartFrom<long, byte[]>(LogTable, _frontier.LastIndex + 1, true, AsReadVisibilityScope: true))
                tx.RemoveKey<long>(LogTable, row.Key);
            foreach (var row in tx.SelectForwardStartFrom<long, byte[]>(ConfigurationTable, _frontier.LastIndex + 1, true, AsReadVisibilityScope: true))
                tx.RemoveKey<long>(ConfigurationTable, row.Key);
            Commit(tx);
            _writes++;
        }
    }

    private void WriteState(DBreezeLogTransaction tx, HardState state, StorageCodecV2.Frontier frontier)
    {
        if (Layout is DBreezeLogLayout.UnifiedTableV3 or DBreezeLogLayout.UnifiedTableV4)
            tx.Insert<string, byte[]>(MetaTable, "manifest", StorageCodecV2.Manifest(state, frontier, Layout == DBreezeLogLayout.UnifiedTableV4));
        else
        {
            tx.Insert<string, byte[]>(MetaTable, "state", StorageCodecV2.Hard(state));
            tx.Insert<string, byte[]>(MetaTable, "frontier", StorageCodecV2.Tail(frontier));
        }
    }

    private void VerifyConfigurationIndex(DBreezeLogTransaction tx)
    {
        foreach (var row in tx.SelectForward<long, byte[]>(ConfigurationTable))
        {
            if (row.Key > _frontier.LastIndex) continue; // cleaned as an unpromised tail
            var log = tx.Select<long, byte[]>(LogTable, row.Key);
            if (!log.Exists) throw new InvalidDataException("Configuration index has no corresponding log entry.");
            var entry = StorageCodecV2.Entry(row.Key, log.Value, _options.MaxCommandBytes, Layout == DBreezeLogLayout.UnifiedTableV4);
            if (entry.Kind != LogEntryKind.Configuration ||
                !StorageCodecV2.Open(row.Value).SequenceEqual(ConfigValue(entry)))
                throw new InvalidDataException("Configuration index differs from the log.");
        }
    }

    public RecoveredLogState Recover()
    {
        lock (_gate)
        {
            Check();
            return new(_state, _frontier.LastIndex, _frontier.LastTerm, _snapshot)
            {
                FirstIndex = _frontier.FirstIndex, Identity = _identity, Configuration = _configuration,
                UncommittedBytes = _frontier.TotalBytes - _frontier.CommittedBytes, SupportsConfiguration = true,
                KnownMembers = _knownMembers, PendingSnapshotInstall = _installIntent,
                Membership = _membership is null ? null : new(_membership, _nextMembershipIndex, _pendingMembershipOperation)
            };
        }
    }

    public void SaveHardState(HardState state) => Write(new LogWrite.HardStateUpdate(state));
    public void Append(IReadOnlyList<LogEntry> entries) => Write(new LogWrite.Append(ImmutableArray.CreateRange(entries)));
    public void ReplaceSuffix(long fromIndex, IReadOnlyList<LogEntry> entries) =>
        Write(new LogWrite.ReplaceSuffix(fromIndex, ImmutableArray.CreateRange(entries)));

    public void Write(LogWrite write)
    {
        lock (_gate)
        {
            Check();
            if (write is LogWrite.HardStateUpdate same && same.State == _state) return;
            if (write is LogWrite.Append empty && empty.Entries.IsEmpty) return;
            var commands = Flatten(write).ToArray();
            if (commands.Length == 0) return;
            if (commands.Length > 8192 || commands.Count(c => c is LogWrite.ReplaceSuffix) > 1)
                throw new ArgumentException("Invalid atomic mutation batch.");
            var state = _state;
            var tail = _frontier;
            var snapshot = _snapshot;
            var installIntent = _installIntent;
            var configuration = _configuration;
            var knownMembers = _knownMembers;
            var membership = _membership;
            var nextMembershipIndex = _nextMembershipIndex;
            var pendingMembershipOperation = _pendingMembershipOperation;
            var historyGeneration = _historyGeneration;
            var inserted = new List<LogEntry>();
            long? replacedFrom = null;
            var modified = false;
            lock (_cacheGate) _writeInProgress = true;
            try
            {
                _observe?.Invoke(PersistenceBoundary.BeforeTransaction);
                using var tx = OpenTransaction(writable: true);
                tx.SynchronizeTables(MetaTable, LogTable, ConfigurationTable);
                foreach (var command in commands)
                {
                    switch (command)
                    {
                        case LogWrite.HardStateUpdate hard:
                            ValidateHard(state, hard.State, tail);
                            if (hard.State.CommitIndex > state.CommitIndex)
                                tail = tail with { CommittedBytes = hard.State.CommitIndex == tail.LastIndex
                                    ? tail.TotalBytes
                                    : inserted.Count == 0 && replacedFrom is null && tail.FirstIndex == _frontier.FirstIndex &&
                                      TryCachedBytes(state.CommitIndex + 1, hard.State.CommitIndex, out var committedBytes)
                                    ? tail.CommittedBytes + committedBytes
                                    : tail.CommittedBytes + Bytes(tx, state.CommitIndex + 1, hard.State.CommitIndex) };
                            state = hard.State;
                            break;
                        case LogWrite.Append append:
                            Insert(checked(tail.LastIndex + 1), append.Entries, replace: false);
                            break;
                        case LogWrite.ReplaceSuffix suffix:
                            if (inserted.Count != 0) throw new ArgumentException("A suffix replacement must precede appends in an atomic batch.");
                            Insert(suffix.FromIndex, suffix.Entries, replace: true);
                            break;
                        case LogWrite.PublishSnapshot publish:
                            var meta = publish.Metadata;
                            if (meta.LastIncludedIndex < (_snapshot?.LastIncludedIndex ?? 0) ||
                                meta.LastIncludedIndex > state.CommitIndex || meta.LastIncludedTerm != Term(tx, meta.LastIncludedIndex, tail, snapshot) ||
                                meta.Digest.Length != 32 || publish.RetainEntries < 0)
                                throw new InvalidOperationException("Snapshot is not a valid committed boundary.");
                            _ = new ClusterConfiguration(meta.Voters, meta.Learners, meta.LastIncludedIndex, meta.LastIncludedTerm).Validate();
                            var active = Configuration(tx, meta.LastIncludedIndex, snapshot);
                            if (!SameMembers(active, new(meta.Voters, meta.Learners)))
                                throw new InvalidOperationException("Snapshot configuration differs from its boundary.");
                            if (Layout == DBreezeLogLayout.UnifiedTableV4 &&
                                (meta.Membership is null || !MembershipBytes(meta.Membership).AsSpan().SequenceEqual(MembershipBytes(MembershipAt(tx, meta.LastIncludedIndex)))))
                                throw new InvalidOperationException("Snapshot membership differs from its boundary.");
                            var first = Math.Max(tail.FirstIndex, meta.LastIncludedIndex - publish.RetainEntries + 1);
                            var removed = Bytes(tx, tail.FirstIndex, first - 1);
                            foreach (var row in tx.SelectForwardStartFrom<long, byte[]>(LogTable, tail.FirstIndex, true, AsReadVisibilityScope: true))
                            {
                                if (row.Key >= first) break;
                                tx.RemoveKey<long>(LogTable, row.Key);
                                tx.RemoveKey<long>(ConfigurationTable, row.Key);
                                if (Layout == DBreezeLogLayout.UnifiedTableV4) tx.RemoveKey<long>(MembershipIndexTable, row.Key);
                            }
                            tail = tail with { FirstIndex = first, TotalBytes = tail.TotalBytes - removed, CommittedBytes = tail.CommittedBytes - removed };
                            snapshot = meta;
                            tx.Insert<string, byte[]>(MetaTable, "snapshot", StorageCodecV2.Snapshot(meta));
                            configuration = Configuration(tx, tail.LastIndex, snapshot);
                            modified = true;
                            break;
                        case LogWrite.BeginSnapshotInstall begin:
                            _ = StorageCodecV2.Intent(StorageCodecV2.Intent(begin.Intent));
                            if (begin.Intent.Metadata.LastIncludedIndex <= state.CommitIndex || begin.Intent.Metadata.LastIncludedTerm > state.Term ||
                                (installIntent is not null && !StorageCodecV2.Intent(installIntent).AsSpan().SequenceEqual(StorageCodecV2.Intent(begin.Intent))))
                                throw new InvalidOperationException("Snapshot install intent is stale or conflicts with an existing install.");
                            installIntent = begin.Intent;
                            tx.Insert<string, byte[]>(MetaTable, "installIntent", StorageCodecV2.Intent(installIntent));
                            modified = true;
                            break;
                        case LogWrite.CompleteSnapshotInstall complete:
                            if (installIntent is null || !StorageCodecV2.Intent(installIntent).AsSpan().SequenceEqual(StorageCodecV2.Intent(complete.Intent)))
                                throw new InvalidOperationException("Snapshot completion has no matching durable intent.");
                            var included = complete.Intent.Metadata;
                            if (complete.Intent.RetainSuffix)
                            {
                                if (included.LastIncludedIndex > tail.LastIndex || Term(tx, included.LastIncludedIndex, tail, snapshot) != included.LastIncludedTerm)
                                    throw new InvalidOperationException("Snapshot suffix retention lost its matching boundary.");
                                var remainingBytes = Bytes(tx, included.LastIncludedIndex + 1, tail.LastIndex);
                                foreach (var row in tx.SelectForward<long, byte[]>(LogTable, AsReadVisibilityScope: true))
                                {
                                    if (row.Key > included.LastIncludedIndex) break;
                                    tx.RemoveKey<long>(LogTable, row.Key);
                                    tx.RemoveKey<long>(ConfigurationTable, row.Key);
                                    if (Layout == DBreezeLogLayout.UnifiedTableV4) tx.RemoveKey<long>(MembershipIndexTable, row.Key);
                                }
                                tail = tail with { FirstIndex = included.LastIncludedIndex + 1, TotalBytes = remainingBytes, CommittedBytes = 0 };
                            }
                            else
                            {
                                tx.RemoveAllKeys(LogTable, withFileRecreation: false);
                                tx.RemoveAllKeys(ConfigurationTable, withFileRecreation: false);
                                if (Layout == DBreezeLogLayout.UnifiedTableV4) tx.RemoveAllKeys(MembershipIndexTable, withFileRecreation: false);
                                tail = new(included.LastIncludedIndex + 1, included.LastIncludedIndex, included.LastIncludedTerm, 0, 0);
                                replacedFrom = 1;
                            }
                            state = state with { CommitIndex = included.LastIncludedIndex };
                            snapshot = included;
                            configuration = Configuration(tx, tail.LastIndex, snapshot);
                            knownMembers = knownMembers.Union(configuration.AllMembers, StringComparer.Ordinal).ToImmutableArray();
                            tx.Insert<string, byte[]>(MetaTable, "snapshot", StorageCodecV2.Snapshot(snapshot));
                            tx.RemoveKey<string>(MetaTable, "installIntent");
                            installIntent = null;
                            modified = true;
                            break;
                        case LogWrite.StageMembershipHistory stage:
                            if (Layout != DBreezeLogLayout.UnifiedTableV4) throw new NotSupportedException("History staging requires v4.");
                            StageHistory(tx, stage);
                            modified = true;
                            break;
                        case LogWrite.PruneMembershipHistory prune:
                            if (Layout != DBreezeLogLayout.UnifiedTableV4) throw new NotSupportedException("History collection requires v4.");
                            var pruned = PruneHistory(tx, prune.ProtectedGenerations, historyGeneration);
                            if (!pruned && commands.Length == 1) return; // idle maintenance must not create an unchanged durable write
                            modified |= pruned;
                            break;
                        default: throw new ArgumentException("Unknown v2 mutation.");
                    }
                }
                UpdateMembership(tx, state, tail, snapshot, commands, inserted, replacedFrom,
                    ref membership, ref nextMembershipIndex, ref pendingMembershipOperation, ref historyGeneration);
                _observe?.Invoke(PersistenceBoundary.AfterLogMutation);
                WriteState(tx, state, tail);
                if (knownMembers != _knownMembers)
                    tx.Insert<string, byte[]>(MetaTable, "knownMembers", StorageCodecV2.Wrap(new ClusterConfiguration(knownMembers, []).Encode()));
                modified = true;
                _observe?.Invoke(PersistenceBoundary.BeforeCommit);
                Commit(tx);
                _writes++;
                _observe?.Invoke(PersistenceBoundary.AfterCommit);
                lock (_cacheGate)
                {
                    var invalidateCache = replacedFrom is not null || tail.FirstIndex > _frontier.FirstIndex;
                    _state = state;
                    _frontier = tail;
                    _snapshot = snapshot;
                    _installIntent = installIntent;
                    _configuration = configuration;
                    _knownMembers = knownMembers;
                    _membership = membership;
                    _nextMembershipIndex = nextMembershipIndex;
                    _pendingMembershipOperation = pendingMembershipOperation;
                    _historyGeneration = historyGeneration;
                    // Ordinary appends/commit-only writes cannot invalidate existing immutable cache entries.
                    // Avoid walking the entire bounded LRU for every durable mutation.
                    var cursor = invalidateCache ? _lru.First : null;
                    while (cursor is not null)
                    {
                        var next = cursor.Next;
                        if (cursor.Value.Index < tail.FirstIndex || (replacedFrom is { } from && cursor.Value.Index >= from)) Evict(cursor);
                        cursor = next;
                    }
                    foreach (var entry in inserted) Cache(entry);
                }
                _observe?.Invoke(PersistenceBoundary.AfterCachePublication);
                return;

                void Insert(long from, ImmutableArray<LogEntry> entries, bool replace)
                {
                    if (from < 1 || from > tail.LastIndex + 1 || from <= state.CommitIndex)
                        throw new InvalidOperationException("Cannot replace committed entries or create a gap.");
                    var previous = Term(tx, from - 1, tail, snapshot);
                    var expected = from;
                    foreach (var entry in entries)
                    {
                        if (entry.Index != expected++ || entry.Term < previous || entry.Term > state.Term ||
                            (entry.Kind == LogEntryKind.Command && entry.Command.Length > _options.MaxCommandBytes))
                            throw new InvalidOperationException("Invalid ascending log batch.");
                        if (entry.Kind == LogEntryKind.Configuration)
                        {
                            var parsed = ClusterConfiguration.Decode(entry.Command.Span, entry.Index, entry.Term);
                            knownMembers = knownMembers.Union(parsed.AllMembers, StringComparer.Ordinal).ToImmutableArray();
                            if (knownMembers.Length > 1024) throw new InvalidOperationException("Too many historical member IDs.");
                        }
                        if (entry.Kind == LogEntryKind.Membership)
                        {
                            if (Layout != DBreezeLogLayout.UnifiedTableV4) throw new InvalidDataException("Membership entries require v4.");
                            _ = MembershipCodec.Decode(entry.Command.Span);
                        }
                        previous = entry.Term;
                    }
                    if (!replace && from != tail.LastIndex + 1) throw new InvalidOperationException("Append must extend the tail.");
                    if (replace)
                    {
                        var removed = Bytes(tx, from, tail.LastIndex);
                        foreach (var row in tx.SelectForwardStartFrom<long, byte[]>(LogTable, from, true, AsReadVisibilityScope: true))
                        {
                            tx.RemoveKey<long>(LogTable, row.Key);
                            tx.RemoveKey<long>(ConfigurationTable, row.Key);
                            if (Layout == DBreezeLogLayout.UnifiedTableV4) tx.RemoveKey<long>(MembershipIndexTable, row.Key);
                        }
                        tail = tail with { LastIndex = from - 1, LastTerm = Term(tx, from - 1, tail, snapshot), TotalBytes = tail.TotalBytes - removed };
                        replacedFrom = from;
                        modified = true;
                    }
                    foreach (var entry in entries)
                    {
                        tx.Insert<long, byte[]>(LogTable, entry.Index, StorageCodecV2.Entry(entry));
                        if (entry.Kind == LogEntryKind.Configuration)
                            tx.Insert<long, byte[]>(ConfigurationTable, entry.Index, StorageCodecV2.Wrap(ConfigValue(entry)));
                        if (entry.Kind == LogEntryKind.Membership)
                            tx.Insert<long, byte[]>(MembershipIndexTable, entry.Index, StorageCodecV2.Entry(entry));
                        inserted.Add(entry);
                        tail = tail with { LastIndex = entry.Index, LastTerm = entry.Term, TotalBytes = tail.TotalBytes + Size(entry) };
                        modified = true;
                    }
                    if (replace || entries.Any(e => e.Kind == LogEntryKind.Configuration))
                        configuration = Configuration(tx, tail.LastIndex, snapshot);
                }
            }
            catch (Exception error)
            {
                if (modified || _observe is not null) lock (_cacheGate) _failure = error;
                throw;
            }
            finally { lock (_cacheGate) _writeInProgress = false; }
        }
    }

    private void Commit(DBreezeLogTransaction tx)
    {
        var started = RaftInstrumentation.Start(RaftInstrumentation.CommitDuration);
        var outcome = "success";
        try { tx.Commit(); }
        catch { outcome = "error"; throw; }
        finally { RaftInstrumentation.Duration(RaftInstrumentation.CommitDuration, started, _options.GroupId, _options.NodeId, outcome); }
    }

    private static IEnumerable<LogWrite> Flatten(LogWrite write)
    {
        if (write is LogWrite.Batch batch)
        {
            foreach (var item in batch.Writes)
            {
                if (item is LogWrite.Batch) throw new ArgumentException("Nested mutation batches are not supported.");
                yield return item;
            }
        }
        else yield return write;
    }
    private static void ValidateHard(HardState old, HardState next, StorageCodecV2.Frontier tail)
    {
        if (next.Term < old.Term || next.Term < tail.LastTerm || next.CommitIndex < old.CommitIndex ||
            next.CommitIndex > tail.LastIndex || (next.Term == 0 && next.VotedFor is not null) ||
            (old.Term == next.Term && old.VotedFor is not null && next.VotedFor != old.VotedFor))
            throw new InvalidOperationException("Hard state would violate term/vote/commit monotonicity.");
        if (next.VotedFor is not null) RaftOptions.ValidateId(next.VotedFor, nameof(next.VotedFor));
    }

    public long GetTerm(long index)
    {
        lock (_gate)
        {
            Check();
            if (TryKnownTerm(index, out var known)) return known;
            using var tx = OpenTransaction();
            _reads++;
            return Term(tx, index, _frontier, _snapshot);
        }
    }
    private bool TryKnownTerm(long index, out long term)
    {
        if (index == 0) { term = 0; return true; }
        if (index == _snapshot?.LastIncludedIndex) { term = _snapshot.LastIncludedTerm; return true; }
        if (index == _frontier.LastIndex) { term = _frontier.LastTerm; return true; }
        if (TryCached(index, out var cached)) { term = cached!.Term; return true; }
        term = 0;
        return false;
    }
    private static long Term(DBreezeLogTransaction tx, long index, StorageCodecV2.Frontier tail, SnapshotMetadata? snapshot)
    {
        if (index == 0) return 0;
        if (index == snapshot?.LastIncludedIndex) return snapshot.LastIncludedTerm;
        if (index == tail.LastIndex) return tail.LastTerm;
        if (index < tail.FirstIndex || index > tail.LastIndex) throw new ArgumentOutOfRangeException(nameof(index));
        var row = tx.Select<long, byte[]>(LogTable, index);
        if (!row.Exists) throw new InvalidDataException($"Missing durable index {index}.");
        return StorageCodecV2.EntryHeader(index, row.GetValuePart(0, StorageCodecV2.EntryHeaderBytes)).Term;
    }
    public IReadOnlyList<LogEntry> Read(long fromIndex, int maxCount, int maxBytes)
    {
        lock (_gate)
        {
            Check();
            if (fromIndex < _frontier.FirstIndex || maxCount < 1 || maxBytes < 1) throw new ArgumentOutOfRangeException(nameof(fromIndex));
            if (fromIndex > _frontier.LastIndex) return Array.Empty<LogEntry>();
            var result = new List<LogEntry>(Math.Min(maxCount, 128));
            var bytes = 0;
            var index = fromIndex;
            while (index <= _frontier.LastIndex && result.Count < maxCount && TryCached(index, out var cached))
            {
                if (!Add(cached!)) return result;
                index++;
            }
            if (index > _frontier.LastIndex || result.Count == maxCount) return result;
            using var tx = OpenTransaction();
            _reads++;
            foreach (var row in tx.SelectForwardStartFrom<long, byte[]>(LogTable, index, true))
            {
                if (row.Key > _frontier.LastIndex) break;
                if (row.Key != index) throw new InvalidDataException($"Log gap at {index}.");
                var entry = StorageCodecV2.Entry(row.Key, row.Value, _options.MaxCommandBytes, Layout == DBreezeLogLayout.UnifiedTableV4);
                Cache(entry);
                if (!Add(entry)) return result;
                index++;
                if (result.Count == maxCount) return result;
            }
            if (index <= _frontier.LastIndex) throw new InvalidDataException($"Log gap at {index}.");
            return result;

            bool Add(LogEntry entry)
            {
                var size = Size(entry);
                if (size > maxBytes - bytes)
                {
                    if (result.Count == 0) throw new ArgumentException("The byte limit cannot hold the next entry.", nameof(maxBytes));
                    return false;
                }
                result.Add(entry); bytes += size; return true;
            }
        }
    }
    public long FirstIndexOfTerm(long index)
    {
        lock (_gate)
        {
            Check();
            var term = GetTerm(index);
            if (index == 0) return 0;
            var first = index;
            using var tx = OpenTransaction();
            _reads++;
            foreach (var row in tx.SelectBackwardStartFrom<long, byte[]>(LogTable, index, true))
            {
                if (StorageCodecV2.EntryHeader(row.Key, row.GetValuePart(0, StorageCodecV2.EntryHeaderBytes)).Term != term) break;
                first = row.Key;
            }
            if (_snapshot?.LastIncludedTerm == term) first = Math.Min(first, _snapshot.LastIncludedIndex);
            return first;
        }
    }
    public long LastIndexOfTerm(long term)
    {
        lock (_gate)
        {
            Check();
            if (term == _frontier.LastTerm) return _frontier.LastIndex;
            using var tx = OpenTransaction();
            _reads++;
            foreach (var row in tx.SelectBackward<long, byte[]>(LogTable))
            {
                if (row.Key > _frontier.LastIndex) continue;
                var found = StorageCodecV2.EntryHeader(row.Key, row.GetValuePart(0, StorageCodecV2.EntryHeaderBytes)).Term;
                if (found == term) return row.Key;
                if (found < term) break;
            }
            return _snapshot?.LastIncludedTerm == term ? _snapshot.LastIncludedIndex : 0;
        }
    }
    public LogReadResult Query(LogQuery query)
    {
        lock (_gate)
        {
            Check();
            switch (query)
            {
                case LogQuery.Membership or LogQuery.MembershipAt or LogQuery.MembershipOperation or LogQuery.MembershipHistory:
                    return QueryMembership(query);
                case LogQuery.Snapshot: return new(Snapshot: _snapshot);
                case LogQuery.ConfigurationAt at:
                    using (var tx = OpenTransaction()) { _reads++; return new() { Configuration = Configuration(tx, at.Index, _snapshot) }; }
                case LogQuery.EncodedBytes range:
                    using (var tx = OpenTransaction()) { _reads++; return new(Number: Bytes(tx, range.FromIndex, range.ThroughIndex)); }
                case LogQuery.Term term: return new(Number: GetTerm(term.Index));
                case LogQuery.FirstIndexOfTerm first: return new(Number: FirstIndexOfTerm(first.Index));
                case LogQuery.LastIndexOfTerm last: return new(Number: LastIndexOfTerm(last.Value));
                case LogQuery.Terms terms:
                    DBreezeLogTransaction? termRead = null;
                    try
                    {
                        var values = ImmutableArray.CreateBuilder<long>(terms.Count);
                        for (var i = 0; i < terms.Count; i++)
                        {
                            var index = checked(terms.FromIndex + i);
                            if (TryKnownTerm(index, out var known)) values.Add(known);
                            else
                            {
                                if (termRead is null) { termRead = OpenTransaction(); _reads++; }
                                values.Add(Term(termRead, index, _frontier, _snapshot));
                            }
                        }
                        return new(Terms: values.MoveToImmutable());
                    }
                    finally { termRead?.Dispose(); }
                case LogQuery.Entries entries: return new(Entries: ImmutableArray.CreateRange(Read(entries.FromIndex, entries.MaxCount, entries.MaxBytes)));
                case LogQuery.Replication replication:
                    return new(PreviousTerm: GetTerm(replication.PreviousIndex),
                        Entries: ImmutableArray.CreateRange(Read(replication.PreviousIndex + 1, replication.MaxCount, replication.MaxBytes)));
                default: throw new ArgumentException("Unknown log query.", nameof(query));
            }
        }
    }

    // Only already-published immutable entries/metadata are eligible. Never wait
    // for cache publication, and never open a DB transaction. A durable flush holds
    // only the storage gate; already-published entries remain readable throughout it.
    internal bool TryQueryCached(LogQuery query, out LogReadResult? result)
    {
        result = null;
        if (!Monitor.TryEnter(_cacheGate)) return false;
        try
        {
            Check();
            switch (query)
            {
                case LogQuery.Membership when _membership is not null:
                    result = new() { Membership = new(_membership, _nextMembershipIndex, _pendingMembershipOperation) };
                    return true;
                case LogQuery.Snapshot:
                    result = new(Snapshot: _snapshot);
                    return true;
                case LogQuery.Term term when TryKnownTerm(term.Index, out var known):
                    result = new(Number: known);
                    return true;
                case LogQuery.Terms terms when terms.Count >= 0:
                    var values = ImmutableArray.CreateBuilder<long>(terms.Count);
                    for (var i = 0; i < terms.Count; i++)
                    {
                        if (!TryKnownTerm(checked(terms.FromIndex + i), out var value)) return false;
                        values.Add(value);
                    }
                    result = new(Terms: values.MoveToImmutable());
                    return true;
                case LogQuery.Entries entries when TryEntries(entries.FromIndex, entries.MaxCount, entries.MaxBytes, out var batch):
                    result = new(Entries: batch);
                    return true;
                case LogQuery.Replication replication when TryKnownTerm(replication.PreviousIndex, out var previous) &&
                    TryEntries(checked(replication.PreviousIndex + 1), replication.MaxCount, replication.MaxBytes, out var replicationBatch):
                    result = new(PreviousTerm: previous, Entries: replicationBatch);
                    return true;
                default:
                    return false;
            }
        }
        finally { Monitor.Exit(_cacheGate); }

        bool TryEntries(long from, int maxCount, int maxBytes, out ImmutableArray<LogEntry> entries)
        {
            entries = default;
            if (from < _frontier.FirstIndex || maxCount < 1 || maxBytes < 1) return false;
            if (from > _frontier.LastIndex)
            {
                // A caller requesting a pending append must wait on the worker,
                // rather than receive an empty range from the preceding frontier.
                if (_writeInProgress) return false;
                entries = ImmutableArray<LogEntry>.Empty;
                return true;
            }
            if (!_cache.ContainsKey(from)) return false;
            var batch = ImmutableArray.CreateBuilder<LogEntry>(Math.Min(maxCount, 128));
            var bytes = 0;
            for (var index = from; index <= _frontier.LastIndex && batch.Count < maxCount; index++)
            {
                if (!TryCached(index, out var entry)) return false;
                var size = Size(entry!);
                if (size > maxBytes - bytes)
                {
                    if (batch.Count == 0) return false; // preserve ordinary Query's validation/error path
                    break;
                }
                batch.Add(entry!);
                bytes += size;
            }
            entries = batch.ToImmutable();
            return true;
        }
    }

    private static long Bytes(DBreezeLogTransaction tx, long from, long through)
    {
        if (through < from) return 0;
        long total = 0, expected = from;
        foreach (var row in tx.SelectForwardStartFrom<long, byte[]>(LogTable, from, true))
        {
            if (row.Key > through) break;
            if (row.Key != expected++) throw new InvalidDataException("Missing entry while accounting bytes.");
            total = checked(total + StorageCodecV2.EntryHeader(row.Key, row.GetValuePart(0, StorageCodecV2.EntryHeaderBytes)).EncodedBytes);
        }
        if (expected != through + 1) throw new InvalidDataException("Incomplete byte-accounting range.");
        return total;
    }
    private ClusterConfiguration BaseConfiguration(SnapshotMetadata? snapshot) => snapshot is null ? _identity.Bootstrap :
        new(snapshot.Voters, snapshot.Learners, snapshot.LastIncludedIndex, snapshot.LastIncludedTerm);
    private ClusterConfiguration Configuration(DBreezeLogTransaction tx, long through, SnapshotMetadata? snapshot)
    {
        foreach (var row in tx.SelectBackwardStartFrom<long, byte[]>(ConfigurationTable, through, true))
        {
            if (snapshot is not null && row.Key <= snapshot.LastIncludedIndex) break;
            var value = StorageCodecV2.Open(row.Value);
            if (value.Length < 8) throw new InvalidDataException("Truncated configuration index.");
            return ClusterConfiguration.Decode(value[8..], row.Key, BinaryPrimitives.ReadInt64LittleEndian(value));
        }
        return BaseConfiguration(snapshot);
    }
    private static byte[] ConfigValue(LogEntry entry)
    {
        var value = new byte[8 + entry.Command.Length];
        BinaryPrimitives.WriteInt64LittleEndian(value, entry.Term);
        entry.Command.Span.CopyTo(value.AsSpan(8));
        return value;
    }
    internal static bool SameMembers(ClusterConfiguration left, ClusterConfiguration right) =>
        left.Voters.ToHashSet(StringComparer.Ordinal).SetEquals(right.Voters) &&
        left.Learners.ToHashSet(StringComparer.Ordinal).SetEquals(right.Learners);
    private static int Size(LogEntry entry) => checked(entry.Command.Length + LogEntry.EncodedOverhead);
    private bool TryCachedBytes(long from, long through, out long bytes)
    {
        lock (_cacheGate)
        {
            bytes = 0;
            for (var index = from; index <= through; index++)
            {
                if (!_cache.TryGetValue(index, out var cached)) return false;
                bytes = checked(bytes + Size(cached.Value));
                if (index == long.MaxValue) break;
            }
            return true;
        }
    }
    private bool TryCached(long index, out LogEntry? entry)
    {
        lock (_cacheGate)
        {
            if (_cache.TryGetValue(index, out var node))
            {
                _lru.Remove(node); _lru.AddLast(node); _hits++; entry = node.Value; return true;
            }
            entry = null; return false;
        }
    }
    private void Cache(LogEntry entry)
    {
        lock (_cacheGate)
        {
            var size = Size(entry);
            if (_options.LogCacheEntries == 0 || size > _options.LogCacheBytes) return;
            if (_cache.TryGetValue(entry.Index, out var previous)) Evict(previous);
            while (_lru.First is not null && (_cache.Count >= _options.LogCacheEntries || _cacheBytes + size > _options.LogCacheBytes)) Evict(_lru.First);
            _cache.Add(entry.Index, _lru.AddLast(entry)); _cacheBytes += size;
        }
    }
    private void Evict(LinkedListNode<LogEntry> node) { _cache.Remove(node.Value.Index); _lru.Remove(node); _cacheBytes -= Size(node.Value); }
    private void Check()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_failure is not null) throw new InvalidOperationException("Storage failed; close and recover before continuing.", _failure);
    }
    public void Dispose()
    {
        lock (_gate)
        {
            if (_disposed) return;
            lock (_cacheGate)
            {
                _disposed = true; _cache.Clear(); _lru.Clear(); _cacheBytes = 0;
            }
            if (_ownsEngine) _engine.Dispose();
        }
    }
}
