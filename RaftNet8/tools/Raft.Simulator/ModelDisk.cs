using System.Collections.Immutable;
using Raft.Storage;

namespace Raft.Simulation;

internal sealed record ModelImage(SnapshotMetadata Metadata, byte[] Bytes, ImmutableArray<LogEntry> Prefix,
    ImmutableDictionary<int, int> Values, ImmutableHashSet<long> CommandIds);

/// <summary>Only flushed transactions live here; pending writes belong to the scheduler and vanish on crash.</summary>
internal sealed class ModelDisk
{
    private readonly RaftOptions _options;
    private SortedDictionary<long, byte[]> _records = new();
    private byte[] _hard = StorageCodecV2.Hard(new HardState());
    private byte[] _frontier = StorageCodecV2.Tail(new StorageCodecV2.Frontier(1, 0, 0, 0, 0));
    private HardState _hardState = new();
    private StorageCodecV2.Frontier _tailState = new(1, 0, 0, 0, 0);
    private SnapshotMetadata? _snapshot;
    private SnapshotInstallIntent? _intent;
    private ImmutableHashSet<string> _known;
    private readonly ClusterConfiguration _bootstrap;
    public readonly Dictionary<string, ModelImage> Images = new(StringComparer.Ordinal);
    // Oracle-only retained history: never used by a log query or a core decision.
    public SortedDictionary<long, LogEntry> History { get; private set; } = new();
    public long Flushes { get; private set; }

    public ModelDisk(RaftOptions options)
    {
        _options = options;
        _bootstrap = new(ImmutableArray.CreateRange(options.Members), ImmutableArray.CreateRange(options.Learners));
        _known = _bootstrap.AllMembers.ToImmutableHashSet(StringComparer.Ordinal);
    }
    public HardState Hard => _hardState;
    private StorageCodecV2.Frontier Tail => _tailState;
    public long Last => Tail.LastIndex;
    public long First => Tail.FirstIndex;
    public long UncommittedBytes => Tail.TotalBytes - Tail.CommittedBytes;
    public long RecoverIntentIndex() => _intent?.Metadata.LastIncludedIndex ?? 0;

    public RecoveredLogState Recover()
    {
        var hard = StorageCodecV2.Hard(_hard);
        var tail = StorageCodecV2.Tail(_frontier);
        if (hard != _hardState || tail != _tailState) throw new InvalidDataException("Model cached metadata differs from disk.");
        var recoveredHistory = new SortedDictionary<long, LogEntry>();
        if (_snapshot is not null)
        {
            var image = Images[_snapshot.StorageId];
            foreach (var entry in image.Prefix) recoveredHistory[entry.Index] = entry;
        }
        long total = 0, committed = 0, previous = 0;
        for (var index = tail.FirstIndex; index <= tail.LastIndex; index++)
        {
            if (!_records.TryGetValue(index, out var bytes)) throw new InvalidDataException("Model lost a promised record.");
            var entry = StorageCodecV2.Entry(index, bytes, _options.MaxCommandBytes);
            if (entry.Term < previous || entry.Term > hard.Term) throw new InvalidDataException("Model log term ordering is corrupt.");
            previous = entry.Term;
            total += Size(entry);
            if (entry.Index <= hard.CommitIndex) committed += Size(entry);
            recoveredHistory[index] = entry;
        }
        if (hard.CommitIndex > tail.LastIndex || hard.CommitIndex < (_snapshot?.LastIncludedIndex ?? 0) ||
            total != tail.TotalBytes || committed != tail.CommittedBytes ||
            (tail.LastIndex >= tail.FirstIndex && previous != tail.LastTerm))
            throw new InvalidDataException("Model durable frontier is corrupt.");
        foreach (var unpromised in _records.Keys.Where(index => index > tail.LastIndex).ToArray()) _records.Remove(unpromised);
        History = recoveredHistory;
        return new(hard, tail.LastIndex, tail.LastTerm, _snapshot)
        {
            FirstIndex = tail.FirstIndex, UncommittedBytes = UncommittedBytes, Configuration = ConfigurationAt(tail.LastIndex),
            Identity = new(_options.GroupId, _options.NodeId, _bootstrap), SupportsConfiguration = true,
            KnownMembers = _known.Order(StringComparer.Ordinal).ToImmutableArray(), PendingSnapshotInstall = _intent
        };
    }

    public LogReadResult Query(LogQuery query) => query switch
    {
        LogQuery.Term at => new(Number: Term(at.Index)),
        LogQuery.Terms range => new(Terms: Enumerable.Range(0, range.Count).Select(i => Term(range.FromIndex + i)).ToImmutableArray()),
        LogQuery.Entries range => new(Entries: Read(range.FromIndex, range.MaxCount, range.MaxBytes)),
        LogQuery.Replication range => new(PreviousTerm: Term(range.PreviousIndex), Entries: Read(range.PreviousIndex + 1, range.MaxCount, range.MaxBytes)),
        LogQuery.FirstIndexOfTerm at => new(Number: FirstOfTerm(at.Index)),
        LogQuery.LastIndexOfTerm at => new(Number: LastOfTerm(at.Value)),
        LogQuery.Snapshot => new(Snapshot: _snapshot),
        LogQuery.ConfigurationAt at => new() { Configuration = ConfigurationAt(at.Index) },
        LogQuery.EncodedBytes range => new(Number: ReadBytes(range.FromIndex, range.ThroughIndex)),
        _ => throw new ArgumentException("Unknown model query.")
    };

    private long Term(long index)
    {
        if (index == 0) return 0;
        if (index == _snapshot?.LastIncludedIndex) return _snapshot.LastIncludedTerm;
        if (index < First || index > Last || !_records.TryGetValue(index, out var bytes)) throw new InvalidDataException($"No readable term at {index}.");
        return StorageCodecV2.EntryHeader(index, bytes.AsSpan(0, StorageCodecV2.EntryHeaderBytes)).Term;
    }
    private ImmutableArray<LogEntry> Read(long from, int count, int maxBytes)
    {
        if (from < First || count < 1 || maxBytes < 1) throw new ArgumentException("Invalid model read range.");
        var result = ImmutableArray.CreateBuilder<LogEntry>();
        var bytes = 0;
        for (var index = from; index <= Last && result.Count < count; index++)
        {
            var entry = StorageCodecV2.Entry(index, _records[index], _options.MaxCommandBytes);
            if (Size(entry) > maxBytes - bytes)
            {
                if (result.Count == 0) throw new ArgumentException("Model byte range cannot hold the next entry.");
                break;
            }
            result.Add(entry);
            bytes += Size(entry);
        }
        return result.ToImmutable();
    }
    private long FirstOfTerm(long index)
    {
        if (index == 0) return 0;
        var term = Term(index);
        while (index > First && Term(index - 1) == term) index--;
        if (_snapshot?.LastIncludedTerm == term) index = Math.Min(index, _snapshot.LastIncludedIndex);
        return index;
    }
    private long LastOfTerm(long term)
    {
        var result = _snapshot?.LastIncludedTerm == term ? _snapshot.LastIncludedIndex : 0;
        foreach (var pair in _records.Reverse())
            if (pair.Key <= Last && StorageCodecV2.EntryHeader(pair.Key, pair.Value.AsSpan(0, StorageCodecV2.EntryHeaderBytes)).Term == term)
                return Math.Max(result, pair.Key);
        return result;
    }
    private long ReadBytes(long from, long through)
    {
        long bytes = 0;
        for (var index = from; index <= through; index++) bytes += StorageCodecV2.EntryHeader(index, _records[index].AsSpan(0, StorageCodecV2.EntryHeaderBytes)).EncodedBytes;
        return bytes;
    }
    private ClusterConfiguration ConfigurationAt(long index)
    {
        foreach (var pair in _records.Reverse())
        {
            if (pair.Key > index || pair.Key > Last) continue;
            var entry = StorageCodecV2.Entry(pair.Key, pair.Value, _options.MaxCommandBytes);
            if (entry.Kind == LogEntryKind.Configuration && entry.Index > (_snapshot?.LastIncludedIndex ?? 0))
                return ClusterConfiguration.Decode(entry.Command.Span, entry.Index, entry.Term);
        }
        return _snapshot is null ? _bootstrap : new(_snapshot.Voters, _snapshot.Learners, _snapshot.LastIncludedIndex, _snapshot.LastIncludedTerm);
    }

    public void WriteAtomic(LogWrite write)
    {
        var records = new SortedDictionary<long, byte[]>(_records);
        var history = new SortedDictionary<long, LogEntry>(History);
        var hard = Hard;
        var tail = Tail;
        var snapshot = _snapshot;
        var intent = _intent;
        var known = _known;
        Apply(write);
        long total = 0, committed = 0;
        foreach (var pair in records)
        {
            if (pair.Key < tail.FirstIndex || pair.Key > tail.LastIndex) continue;
            var bytes = StorageCodecV2.EntryHeader(pair.Key, pair.Value.AsSpan(0, StorageCodecV2.EntryHeaderBytes)).EncodedBytes;
            total += bytes;
            if (pair.Key <= hard.CommitIndex) committed += bytes;
        }
        _records = records;
        History = history;
        _hard = StorageCodecV2.Hard(hard);
        _hardState = hard;
        _tailState = tail with { TotalBytes = total, CommittedBytes = committed };
        _frontier = StorageCodecV2.Tail(_tailState);
        _snapshot = snapshot;
        _intent = intent;
        _known = known;
        Flushes++;
        return;

        long ReadTerm(long index) => index == 0 ? 0 : snapshot?.LastIncludedIndex == index ? snapshot.LastIncludedTerm :
            StorageCodecV2.EntryHeader(index, records[index].AsSpan(0, StorageCodecV2.EntryHeaderBytes)).Term;
        void Append(ImmutableArray<LogEntry> entries)
        {
            foreach (var entry in entries)
            {
                if (entry.Index != tail.LastIndex + 1 || entry.Term < tail.LastTerm || entry.Term > hard.Term)
                    throw new InvalidDataException("Model append is not an ascending extension.");
                records[entry.Index] = StorageCodecV2.Entry(entry);
                history[entry.Index] = entry;
                tail = tail with { LastIndex = entry.Index, LastTerm = entry.Term };
                if (entry.Kind == LogEntryKind.Configuration)
                    known = known.Union(ClusterConfiguration.Decode(entry.Command.Span).AllMembers);
            }
        }
        void Apply(LogWrite item)
        {
            switch (item)
            {
                case LogWrite.Batch batch: foreach (var command in batch.Writes) Apply(command); break;
                case LogWrite.HardStateUpdate update:
                    var next = update.State;
                    if (next.Term < hard.Term || next.CommitIndex < hard.CommitIndex || next.CommitIndex > tail.LastIndex ||
                        (next.Term == hard.Term && hard.VotedFor is not null && next.VotedFor != hard.VotedFor))
                        throw new InvalidDataException("Model hard state regressed.");
                    hard = next;
                    break;
                case LogWrite.Append append: Append(append.Entries); break;
                case LogWrite.ReplaceSuffix suffix:
                    if (suffix.FromIndex <= hard.CommitIndex || suffix.FromIndex > tail.LastIndex + 1) throw new InvalidDataException("Model replaced a committed prefix or created a gap.");
                    var term = ReadTerm(suffix.FromIndex - 1);
                    foreach (var index in records.Keys.Where(index => index >= suffix.FromIndex).ToArray()) records.Remove(index);
                    foreach (var index in history.Keys.Where(index => index >= suffix.FromIndex).ToArray()) history.Remove(index);
                    tail = tail with { LastIndex = suffix.FromIndex - 1, LastTerm = term };
                    Append(suffix.Entries);
                    break;
                case LogWrite.PublishSnapshot publish:
                    var image = publish.Metadata;
                    if (image.LastIncludedIndex > hard.CommitIndex || ReadTerm(image.LastIncludedIndex) != image.LastIncludedTerm || !Images.ContainsKey(image.StorageId))
                        throw new InvalidDataException("Model snapshot is not a durable committed prefix.");
                    var first = Math.Max(tail.FirstIndex, image.LastIncludedIndex - publish.RetainEntries + 1);
                    foreach (var index in records.Keys.Where(index => index < first).ToArray()) records.Remove(index);
                    snapshot = image;
                    tail = tail with { FirstIndex = first };
                    break;
                case LogWrite.BeginSnapshotInstall begin:
                    if (begin.Intent.Metadata.LastIncludedIndex <= hard.CommitIndex) throw new InvalidDataException("Model began stale installation.");
                    intent = begin.Intent;
                    break;
                case LogWrite.CompleteSnapshotInstall complete:
                    if (intent?.Metadata.StorageId != complete.Intent.Metadata.StorageId) throw new InvalidDataException("Model completed a missing intent.");
                    var included = complete.Intent.Metadata;
                    var retain = complete.Intent.RetainSuffix;
                    if (retain && ReadTerm(included.LastIncludedIndex) != included.LastIncludedTerm) throw new InvalidDataException("Model retained a conflicting suffix.");
                    var suffixHistory = retain ? history.Values.Where(entry => entry.Index > included.LastIncludedIndex).ToArray() : [];
                    history.Clear();
                    foreach (var entry in Images[included.StorageId].Prefix.Concat(suffixHistory)) history[entry.Index] = entry;
                    foreach (var index in records.Keys.Where(index => !retain || index <= included.LastIncludedIndex).ToArray()) records.Remove(index);
                    tail = retain ? tail with { FirstIndex = included.LastIncludedIndex + 1 } :
                        new(included.LastIncludedIndex + 1, included.LastIncludedIndex, included.LastIncludedTerm, 0, 0);
                    hard = hard with { CommitIndex = included.LastIncludedIndex };
                    snapshot = included;
                    known = known.Union(included.Voters).Union(included.Learners);
                    intent = null;
                    break;
                default: throw new ArgumentException("Unknown model write.");
            }
        }
    }

    public void TearUnpromisedTail()
    {
        var entry = new LogEntry(Last + 1, Math.Max(1, Hard.Term), [42]);
        _records[entry.Index] = StorageCodecV2.Entry(entry)[..^3];
    }
    public void CorruptPromisedRecord(long index)
    {
        var bytes = _records[index].ToArray();
        bytes[^1] ^= 0x80;
        _records[index] = bytes;
    }
    internal static int Size(LogEntry entry) => entry.Command.Length + LogEntry.EncodedOverhead;
    internal static bool Same(LogEntry left, LogEntry right) => left.Index == right.Index && left.Term == right.Term && left.Kind == right.Kind && left.Command.Span.SequenceEqual(right.Command.Span);
}
