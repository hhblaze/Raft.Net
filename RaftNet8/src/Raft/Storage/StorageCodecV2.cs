using System.Buffers.Binary;
using System.Collections.Immutable;
using System.Text;
using System.Text.Json;

namespace Raft.Storage;

internal static class StorageCodecV2
{
    internal const uint EntryMagic = 0x4C325246; // RF2L
    internal const uint MetaMagic = 0x4D325246;  // RF2M
    internal const uint ManifestMagic = 0x4D335246; // RF3M
    internal const int EnvelopeBytes = 12;
    internal const int EntryHeaderBytes = EnvelopeBytes + 25;

    public static byte[] Wrap(ReadOnlySpan<byte> body, uint magic = MetaMagic)
    {
        var result = new byte[checked(body.Length + EnvelopeBytes)];
        BinaryPrimitives.WriteUInt32LittleEndian(result, magic);
        BinaryPrimitives.WriteInt32LittleEndian(result.AsSpan(4), result.Length);
        body.CopyTo(result.AsSpan(EnvelopeBytes));
        BinaryPrimitives.WriteUInt32LittleEndian(result.AsSpan(8), Checksum(result));
        return result;
    }

    public static ReadOnlySpan<byte> Open(ReadOnlySpan<byte> record, uint magic = MetaMagic)
    {
        if (record.Length < EnvelopeBytes || BinaryPrimitives.ReadUInt32LittleEndian(record) != magic ||
            BinaryPrimitives.ReadInt32LittleEndian(record[4..]) != record.Length ||
            BinaryPrimitives.ReadUInt32LittleEndian(record[8..]) != Checksum(record))
            throw new InvalidDataException("Invalid v2 record length, version, or CRC32C.");
        return record[EnvelopeBytes..];
    }

    private static uint Checksum(ReadOnlySpan<byte> record) =>
        ~Crc32C.Update(Crc32C.Update(uint.MaxValue, record[..8]), record[EnvelopeBytes..]);

    public static byte[] Entry(LogEntry entry)
    {
        var record = new byte[checked(EntryHeaderBytes + entry.Command.Length)];
        BinaryPrimitives.WriteUInt32LittleEndian(record, EntryMagic);
        BinaryPrimitives.WriteInt32LittleEndian(record.AsSpan(4), record.Length);
        var body = record.AsSpan(EnvelopeBytes);
        BinaryPrimitives.WriteInt64LittleEndian(body, entry.Index);
        BinaryPrimitives.WriteInt64LittleEndian(body[8..], entry.Term);
        body[16] = (byte)entry.Kind;
        BinaryPrimitives.WriteInt32LittleEndian(body[17..], entry.Command.Length);
        BinaryPrimitives.WriteUInt32LittleEndian(body[21..], Crc32C.Compute(body[..21]));
        entry.Command.Span.CopyTo(body[25..]);
        BinaryPrimitives.WriteUInt32LittleEndian(record.AsSpan(8), Checksum(record));
        return record;
    }

    public static LogEntry Entry(long key, byte[] record, int maxCommandBytes)
    {
        var body = Open(record, EntryMagic);
        if (body.Length < 25 || BinaryPrimitives.ReadUInt32LittleEndian(body[21..]) != Crc32C.Compute(body[..21]))
            throw new InvalidDataException("Truncated or corrupt v2 entry header.");
        var index = BinaryPrimitives.ReadInt64LittleEndian(body);
        var term = BinaryPrimitives.ReadInt64LittleEndian(body[8..]);
        var kind = (LogEntryKind)body[16];
        var length = BinaryPrimitives.ReadInt32LittleEndian(body[17..]);
        if (index != key || index < 1 || term < 1 || kind > LogEntryKind.Configuration ||
            length < 0 || length != body.Length - 25 ||
            (kind == LogEntryKind.Command && length > maxCommandBytes) ||
            (kind == LogEntryKind.Configuration && length > 270_000) ||
            (kind == LogEntryKind.NoOp && length != 0))
            throw new InvalidDataException("Invalid v2 entry key, term, kind, or payload length.");
        if (kind == LogEntryKind.Configuration) _ = ClusterConfiguration.Decode(body[25..], index, term);
        return new(index, term, body[25..], kind);
    }

    public static (long Term, int EncodedBytes) EntryHeader(long key, ReadOnlySpan<byte> header)
    {
        if (header.Length < EntryHeaderBytes || BinaryPrimitives.ReadUInt32LittleEndian(header) != EntryMagic ||
            BinaryPrimitives.ReadInt64LittleEndian(header[12..]) != key ||
            BinaryPrimitives.ReadUInt32LittleEndian(header[33..]) != Crc32C.Compute(header.Slice(12, 21)))
            throw new InvalidDataException("Invalid entry header.");
        var length = BinaryPrimitives.ReadInt32LittleEndian(header[4..]);
        var payload = BinaryPrimitives.ReadInt32LittleEndian(header[29..]);
        var term = BinaryPrimitives.ReadInt64LittleEndian(header[20..]);
        if (payload < 0 || payload != length - EntryHeaderBytes || term < 1)
            throw new InvalidDataException("Invalid entry header length or term.");
        return (term, checked(payload + LogEntry.EncodedOverhead));
    }

    public static byte[] Hard(HardState state)
    {
        var vote = state.VotedFor is null ? Array.Empty<byte>() : Encoding.ASCII.GetBytes(state.VotedFor);
        var body = new byte[20 + vote.Length];
        BinaryPrimitives.WriteInt64LittleEndian(body, state.Term);
        BinaryPrimitives.WriteInt64LittleEndian(body.AsSpan(8), state.CommitIndex);
        BinaryPrimitives.WriteInt32LittleEndian(body.AsSpan(16), vote.Length);
        vote.CopyTo(body.AsSpan(20));
        return Wrap(body);
    }
    public static HardState Hard(byte[] record)
    {
        return ReadHard(Open(record));
    }
    private static HardState ReadHard(ReadOnlySpan<byte> body)
    {
        if (body.Length < 20) throw new InvalidDataException("Truncated hard state.");
        var term = BinaryPrimitives.ReadInt64LittleEndian(body);
        var commit = BinaryPrimitives.ReadInt64LittleEndian(body[8..]);
        var length = BinaryPrimitives.ReadInt32LittleEndian(body[16..]);
        if (term < 0 || commit < 0 || length < 0 || length > 128 || length != body.Length - 20 || (term == 0 && length != 0))
            throw new InvalidDataException("Invalid hard state.");
        var vote = length == 0 ? null : Encoding.ASCII.GetString(body[20..]);
        if (vote is not null)
            try { RaftOptions.ValidateId(vote, nameof(vote)); }
            catch (ArgumentException error) { throw new InvalidDataException("Invalid vote.", error); }
        return new(term, vote, commit);
    }

    public static byte[] Identity(StorageIdentity identity)
    {
        using var buffer = new MemoryStream();
        using var writer = new BinaryWriter(buffer, Encoding.UTF8, leaveOpen: true);
        writer.Write(identity.GroupId);
        writer.Write(identity.NodeId);
        var config = identity.Bootstrap.Encode();
        writer.Write(config.Length);
        writer.Write(config);
        writer.Flush();
        return Wrap(buffer.GetBuffer().AsSpan(0, (int)buffer.Length));
    }
    public static StorageIdentity Identity(byte[] record)
    {
        var body = Open(record);
        if (body.Length > 280_000) throw new InvalidDataException("Identity is too large.");
        using var buffer = new MemoryStream(body.ToArray(), writable: false);
        using var reader = new BinaryReader(buffer, Encoding.UTF8);
        var group = reader.ReadString();
        var node = reader.ReadString();
        var count = reader.ReadInt32();
        if (count < 0 || count > buffer.Length - buffer.Position) throw new InvalidDataException("Invalid identity.");
        var config = ClusterConfiguration.Decode(reader.ReadBytes(count));
        if (buffer.Position != buffer.Length) throw new InvalidDataException("Trailing identity data.");
        return new(group, node, config);
    }

    internal sealed record Frontier(long FirstIndex, long LastIndex, long LastTerm, long TotalBytes, long CommittedBytes);
    public static byte[] Tail(Frontier frontier)
    {
        Span<byte> body = stackalloc byte[40];
        BinaryPrimitives.WriteInt64LittleEndian(body, frontier.FirstIndex);
        BinaryPrimitives.WriteInt64LittleEndian(body[8..], frontier.LastIndex);
        BinaryPrimitives.WriteInt64LittleEndian(body[16..], frontier.LastTerm);
        BinaryPrimitives.WriteInt64LittleEndian(body[24..], frontier.TotalBytes);
        BinaryPrimitives.WriteInt64LittleEndian(body[32..], frontier.CommittedBytes);
        return Wrap(body);
    }
    public static Frontier Tail(byte[] record)
    {
        return ReadTail(Open(record));
    }
    private static Frontier ReadTail(ReadOnlySpan<byte> body)
    {
        if (body.Length != 40) throw new InvalidDataException("Invalid frontier length.");
        return new(BinaryPrimitives.ReadInt64LittleEndian(body), BinaryPrimitives.ReadInt64LittleEndian(body[8..]),
            BinaryPrimitives.ReadInt64LittleEndian(body[16..]), BinaryPrimitives.ReadInt64LittleEndian(body[24..]),
            BinaryPrimitives.ReadInt64LittleEndian(body[32..]));
    }

    // One checked row publishes the complete durable state and frontier together.
    // V2 keeps its original two independently checked rows and exact bytes.
    public static byte[] Manifest(HardState state, Frontier frontier)
    {
        var voteLength = state.VotedFor?.Length ?? 0;
        Span<byte> body = stackalloc byte[checked(60 + voteLength)];
        BinaryPrimitives.WriteInt64LittleEndian(body, state.Term);
        BinaryPrimitives.WriteInt64LittleEndian(body[8..], state.CommitIndex);
        BinaryPrimitives.WriteInt32LittleEndian(body[16..], voteLength);
        if (state.VotedFor is { } vote) Encoding.ASCII.GetBytes(vote, body[20..]);
        var tail = body[(20 + voteLength)..];
        BinaryPrimitives.WriteInt64LittleEndian(tail, frontier.FirstIndex);
        BinaryPrimitives.WriteInt64LittleEndian(tail[8..], frontier.LastIndex);
        BinaryPrimitives.WriteInt64LittleEndian(tail[16..], frontier.LastTerm);
        BinaryPrimitives.WriteInt64LittleEndian(tail[24..], frontier.TotalBytes);
        BinaryPrimitives.WriteInt64LittleEndian(tail[32..], frontier.CommittedBytes);
        return Wrap(body, ManifestMagic);
    }

    public static (HardState State, Frontier Tail) Manifest(byte[] record)
    {
        var body = Open(record, ManifestMagic);
        if (body.Length is < 60 or > 188) throw new InvalidDataException("Invalid v3 manifest length.");
        var hardLength = body.Length - 40;
        return (ReadHard(body[..hardLength]), ReadTail(body[hardLength..]));
    }
    public static byte[] Snapshot(SnapshotMetadata snapshot) => Wrap(JsonSerializer.SerializeToUtf8Bytes(snapshot));
    public static byte[] Intent(SnapshotInstallIntent intent) => Wrap(JsonSerializer.SerializeToUtf8Bytes(intent));
    public static SnapshotInstallIntent Intent(byte[] record)
    {
        var intent = JsonSerializer.Deserialize<SnapshotInstallIntent>(Open(record))
            ?? throw new InvalidDataException("Invalid snapshot install intent.");
        _ = Snapshot(Snapshot(intent.Metadata));
        return intent;
    }
    public static SnapshotMetadata Snapshot(byte[] record)
    {
        var snapshot = JsonSerializer.Deserialize<SnapshotMetadata>(Open(record))
            ?? throw new InvalidDataException("Invalid snapshot metadata.");
        if (snapshot.LastIncludedIndex < 1 || snapshot.LastIncludedTerm < 1 || snapshot.Digest.Length != 32)
            throw new InvalidDataException("Invalid snapshot boundary/digest.");
        _ = new ClusterConfiguration(snapshot.Voters, snapshot.Learners, snapshot.LastIncludedIndex, snapshot.LastIncludedTerm).Validate();
        RaftOptions.ValidateId(snapshot.StorageId, nameof(snapshot.StorageId));
        return snapshot;
    }
}
