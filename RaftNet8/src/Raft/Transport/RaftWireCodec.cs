using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using Raft.Storage;

namespace Raft.Transport;

internal readonly record struct RpcEnvelope(string GroupId, string From, RaftRpc Message)
{
    public int Version { get; init; } = 1;
    public long ExchangeId { get; init; }
}

internal static class RaftWireCodec
{
    internal static PooledFrame Encode(string groupId, string from, RaftRpc rpc, int maxFrameBytes, int version = 1, long exchangeId = 0)
    {
        var frame = new PooledFrame(maxFrameBytes);
        try
        {
            Encode(frame, groupId, from, rpc, maxFrameBytes, version, exchangeId);
            return frame;
        }
        catch { frame.Dispose(); throw; }
    }

    internal static void Encode(IBufferWriter<byte> destination, string groupId, string from, RaftRpc rpc, int maxFrameBytes,
        int version = 1, long exchangeId = 0)
    {
        RaftOptions.ValidateId(groupId, nameof(groupId));
        RaftOptions.ValidateId(from, nameof(from));
        if (version is not 1 and not 2 and not 3 || (version >= 2 && exchangeId < 1)) throw new InvalidDataException("Invalid wire version/exchange ID.");
        var metadata = rpc is SnapshotChunkRequest { Metadata: { } snapshot } ? StorageCodecV2.Snapshot(snapshot) : Array.Empty<byte>();
        var directory = rpc switch
        {
            PeerDirectoryRequest request => MembershipCodec.EncodeDirectory(request.Directory),
            PeerDirectoryResponse response => MembershipCodec.EncodeDirectory(response.Directory),
            _ => Array.Empty<byte>()
        };
        var kind = Kind(rpc);
        if (version == 1 && kind > 4) throw new InvalidDataException("This message requires wire v2.");
        if (version < 3 && kind > 10) throw new InvalidDataException("Directory exchange requires wire3.");
        var length = checked(18 + groupId.Length + from.Length + (version >= 2 ? 8 : 0) + (rpc switch
        {
            VoteRequest => 16, VoteResponse => 1, AppendRequest => 36 + (version >= 2 ? 8 : 0), AppendResponse => 33 + (version >= 2 ? 8 : 0),
            PreVoteRequest => 24, PreVoteResponse => 17, TimeoutNowRequest => 8, TimeoutNowResponse => 9,
            SnapshotChunkRequest chunk => 41 + chunk.StorageId.Length + metadata.Length + chunk.Data.Length,
            SnapshotChunkResponse chunk => 22 + chunk.StorageId.Length,
            PeerDirectoryRequest or PeerDirectoryResponse => 4 + directory.Length,
            _ => throw new InvalidDataException("Unknown RPC.")
        }));
        if (rpc is AppendRequest batch)
            for (var i = 0; i < batch.Entries.Count; i++)
                length = checked(length + LogEntry.EncodedOverhead + batch.Entries[i].Command.Length);
        if (length > maxFrameBytes) throw new InvalidDataException("Frame exceeds configured limit.");
        // The pooled transport frame is contiguous and its exact length is known.
        // Reserve once instead of growing/copying it as every log entry is encoded.
        // Other IBufferWriter implementations may be segmented; keep their streaming contract.
        if (destination is PooledFrame pooled) pooled.GetSpan(checked(length + 4));
        var frame = new WireWriter(destination);
        frame.WriteInt32(length);
        frame.WriteByte((byte)version);
        frame.WriteByte(kind);
        frame.WriteString(groupId);
        frame.WriteString(from);
        frame.WriteInt64(rpc.Term);
        if (version >= 2) frame.WriteInt64(exchangeId);
            switch (rpc)
            {
                case VoteRequest vote:
                    frame.WriteInt64(vote.LastLogIndex);
                    frame.WriteInt64(vote.LastLogTerm);
                    break;
                case VoteResponse vote:
                    frame.WriteByte(vote.Granted ? (byte)1 : (byte)0);
                    break;
                case AppendRequest append:
                    frame.WriteInt64(append.RequestId);
                    frame.WriteInt64(append.PreviousIndex);
                    frame.WriteInt64(append.PreviousTerm);
                    frame.WriteInt64(append.LeaderCommit);
                    if (version >= 2) frame.WriteInt64(append.ReadContext);
                    frame.WriteInt32(append.Entries.Count);
                    for (var i = 0; i < append.Entries.Count; i++)
                    {
                        var entry = append.Entries[i];
                        frame.WriteInt64(entry.Index);
                        frame.WriteInt64(entry.Term);
                        if ((version == 1 && entry.Kind == LogEntryKind.Configuration) || (version < 3 && entry.Kind == LogEntryKind.Membership)) throw new InvalidDataException("Configuration entries require v2.");
                        frame.WriteByte((byte)entry.Kind);
                        frame.WriteInt32(entry.Command.Length);
                        frame.Write(entry.Command.Span);
                    }
                    break;
                case AppendResponse append:
                    frame.WriteInt64(append.RequestId);
                    frame.WriteByte(append.Success ? (byte)1 : (byte)0);
                    frame.WriteInt64(append.MatchIndex);
                    frame.WriteInt64(append.ConflictIndex);
                    frame.WriteInt64(append.ConflictTerm);
                    if (version >= 2) frame.WriteInt64(append.ReadContext);
                    break;
                case PreVoteRequest preVote:
                    frame.WriteInt64(preVote.Round); frame.WriteInt64(preVote.LastLogIndex); frame.WriteInt64(preVote.LastLogTerm);
                    break;
                case PreVoteResponse preVote:
                    frame.WriteInt64(preVote.Round); frame.WriteInt64(preVote.ProspectiveTerm); frame.WriteByte(preVote.Granted ? (byte)1 : (byte)0);
                    break;
                case TimeoutNowRequest timeout: frame.WriteInt64(timeout.RequestId); break;
                case TimeoutNowResponse timeout:
                    frame.WriteInt64(timeout.RequestId); frame.WriteByte(timeout.Accepted ? (byte)1 : (byte)0); break;
                case SnapshotChunkRequest chunk:
                    frame.WriteInt64(chunk.RequestId); frame.WriteString(chunk.StorageId);
                    frame.WriteInt64(chunk.Offset); frame.WriteInt64(chunk.TotalLength); frame.WriteInt32(unchecked((int)chunk.Checksum));
                    frame.WriteByte(chunk.Final ? (byte)1 : (byte)0);
                    frame.WriteInt32(metadata.Length); frame.Write(metadata);
                    frame.WriteInt32(chunk.Data.Length); frame.Write(chunk.Data.Span);
                    break;
                case SnapshotChunkResponse chunk:
                    frame.WriteInt64(chunk.RequestId); frame.WriteString(chunk.StorageId); frame.WriteInt64(chunk.NextOffset);
                    frame.WriteByte(chunk.Success ? (byte)1 : (byte)0); frame.WriteByte(chunk.Complete ? (byte)1 : (byte)0);
                    break;
                case PeerDirectoryRequest or PeerDirectoryResponse:
                    frame.WriteInt32(directory.Length); frame.Write(directory);
                    break;
            }
    }

    private static byte Kind(RaftRpc rpc) => rpc switch
    {
        VoteRequest => 1, VoteResponse => 2, AppendRequest => 3, AppendResponse => 4,
        PreVoteRequest => 5, PreVoteResponse => 6, TimeoutNowRequest => 7, TimeoutNowResponse => 8,
        SnapshotChunkRequest => 9, SnapshotChunkResponse => 10, PeerDirectoryRequest => 11, PeerDirectoryResponse => 12,
        _ => throw new InvalidDataException("Unknown RPC.")
    };

    private readonly ref struct WireWriter(IBufferWriter<byte> destination)
    {
        public void WriteByte(byte value) { destination.GetSpan(1)[0] = value; destination.Advance(1); }
        public void WriteInt32(int value) { BinaryPrimitives.WriteInt32LittleEndian(destination.GetSpan(4), value); destination.Advance(4); }
        public void WriteInt64(long value) { BinaryPrimitives.WriteInt64LittleEndian(destination.GetSpan(8), value); destination.Advance(8); }
        public void Write(ReadOnlySpan<byte> value)
        {
            if (value.IsEmpty) return;
            value.CopyTo(destination.GetSpan(value.Length));
            destination.Advance(value.Length);
        }
        public void WriteString(string value)
        {
            WriteInt32(value.Length);
            Encoding.ASCII.GetBytes(value, destination.GetSpan(value.Length));
            destination.Advance(value.Length);
        }
    }

    internal static RpcEnvelope Decode(ReadOnlySpan<byte> bytes, RaftOptions options)
    {
        var reader = new SpanReader(bytes);
        var version = reader.Byte();
        if (version is not 1 and not 2 and not 3) throw new InvalidDataException("Unsupported Raft wire version.");
        var kind = reader.Byte();
        if (version == 1 && kind > 4) throw new InvalidDataException("Message requires v2.");
        if (version < 3 && kind > 10) throw new InvalidDataException("Directory exchange requires wire3.");
        var group = reader.String();
        var from = reader.String();
        var term = reader.Int64();
        if (term < 0) throw new InvalidDataException("Negative RPC term.");
        var exchangeId = version >= 2 ? reader.Int64() : 0;
        if (version >= 2 && exchangeId < 1) throw new InvalidDataException("Invalid exchange ID.");
        RaftRpc message;
        switch (kind)
        {
            case 1: message = new VoteRequest(term, reader.Int64(), reader.Int64()); break;
            case 2: message = new VoteResponse(term, reader.Boolean()); break;
            case 3:
                var requestId = reader.Int64();
                var previous = reader.Int64();
                var previousTerm = reader.Int64();
                var commit = reader.Int64();
                var readContext = version >= 2 ? reader.Int64() : 0;
                var count = reader.Int32();
                if (count < 0 || count > options.MaxAppendEntries) throw new InvalidDataException("Invalid batch count.");
                var entries = new LogEntry[count];
                var size = 0;
                for (var i = 0; i < count; i++)
                {
                    var index = reader.Int64();
                    var entryTerm = reader.Int64();
                    var entryKind = (LogEntryKind)reader.Byte();
                    if (entryKind > (version == 1 ? LogEntryKind.NoOp : version == 2 ? LogEntryKind.Configuration : LogEntryKind.Membership))
                        throw new InvalidDataException("Invalid log entry kind.");
                    var length = reader.Int32();
                    if (length < 0 || length > (entryKind == LogEntryKind.Configuration ? options.MaxAppendBytes : entryKind == LogEntryKind.Membership ? MembershipCodec.MaxRecordBytes : options.MaxCommandBytes))
                        throw new InvalidDataException("Command length exceeds limit.");
                    size = checked(size + length + LogEntry.EncodedOverhead);
                    if (size > (version == 1 ? options.MaxAppendBytes : options.ReplicationByteLimit))
                        throw new InvalidDataException("Batch bytes exceed limit.");
                    entries[i] = new LogEntry(index, entryTerm, reader.Bytes(length), entryKind);
                }
                message = new AppendRequest(term, requestId, previous, previousTerm, commit, entries) { ReadContext = readContext };
                break;
            case 4:
                var response = new AppendResponse(term, reader.Int64(), reader.Boolean(), reader.Int64(), reader.Int64(), reader.Int64());
                message = version >= 2 ? response with { ReadContext = reader.Int64() } : response;
                break;
            case 5: message = new PreVoteRequest(term, reader.Int64(), reader.Int64(), reader.Int64()); break;
            case 6: message = new PreVoteResponse(term, reader.Int64(), reader.Int64(), reader.Boolean()); break;
            case 7: message = new TimeoutNowRequest(term, reader.Int64()); break;
            case 8: message = new TimeoutNowResponse(term, reader.Int64(), reader.Boolean()); break;
            case 9:
                var id = reader.Int64(); var storageId = reader.String();
                var offset = reader.Int64(); var total = reader.Int64(); var crc = unchecked((uint)reader.Int32()); var final = reader.Boolean();
                var metadataLength = reader.Int32();
                if (metadataLength < 0 || metadataLength > (version == 3 ? MembershipCodec.MaxDirectoryBytes : 280_000)) throw new InvalidDataException("Invalid snapshot metadata length.");
                var metadata = metadataLength == 0 ? null : StorageCodecV2.Snapshot(reader.Bytes(metadataLength).ToArray());
                var dataLength = reader.Int32();
                if (dataLength < 0 || dataLength > options.SnapshotChunkBytes || offset < 0 || total < 0 || offset > total - dataLength)
                    throw new InvalidDataException("Invalid snapshot chunk bounds.");
                var data = reader.Bytes(dataLength).ToArray();
                if (Crc32C.Compute(data) != crc) throw new InvalidDataException("Snapshot chunk CRC32C differs.");
                message = new SnapshotChunkRequest(term, id, storageId, offset, total, crc, final, metadata, data);
                break;
            case 10:
                message = new SnapshotChunkResponse(term, reader.Int64(), reader.String(), reader.Int64(), reader.Boolean(), reader.Boolean());
                break;
            case 11:
            case 12:
                var directoryLength = reader.Int32();
                if (directoryLength is < 1 or > MembershipCodec.MaxDirectoryBytes || term != 0)
                    throw new InvalidDataException("Invalid directory exchange.");
                var directory = MembershipCodec.DecodeDirectory(reader.Bytes(directoryLength));
                message = kind == 11 ? new PeerDirectoryRequest(directory) : new PeerDirectoryResponse(directory);
                break;
            default: throw new InvalidDataException("Unknown Raft message kind.");
        }
        if (!reader.IsEmpty) throw new InvalidDataException("Trailing bytes in Raft frame.");
        return new(group, from, message) { Version = version, ExchangeId = exchangeId };
    }

    internal static async ValueTask<RpcEnvelope> ReadAsync(Stream stream, byte[] header, RaftOptions options, CancellationToken token)
    {
        await stream.ReadExactlyAsync(header, token).ConfigureAwait(false);
        var length = BinaryPrimitives.ReadInt32LittleEndian(header);
        if (length < 18 || length > options.MaxFrameBytes) throw new InvalidDataException("Invalid Raft frame length.");
        var buffer = ArrayPool<byte>.Shared.Rent(length);
        try
        {
            await stream.ReadExactlyAsync(buffer.AsMemory(0, length), token).ConfigureAwait(false);
            // Decoded commands own their data; none reference this rented network buffer.
            return Decode(buffer.AsSpan(0, length), options);
        }
        finally { ArrayPool<byte>.Shared.Return(buffer); }
    }

    private ref struct SpanReader(ReadOnlySpan<byte> bytes)
    {
        private ReadOnlySpan<byte> _remaining = bytes;
        public bool IsEmpty => _remaining.IsEmpty;
        public ReadOnlySpan<byte> Bytes(int length)
        {
            if (length < 0 || length > _remaining.Length) throw new InvalidDataException("Truncated Raft frame.");
            var value = _remaining[..length];
            _remaining = _remaining[length..];
            return value;
        }
        public byte Byte() => Bytes(1)[0];
        public bool Boolean() => Byte() switch { 0 => false, 1 => true, _ => throw new InvalidDataException("Invalid Boolean.") };
        public int Int32() => BinaryPrimitives.ReadInt32LittleEndian(Bytes(4));
        public long Int64() => BinaryPrimitives.ReadInt64LittleEndian(Bytes(8));
        public string String()
        {
            var length = Int32();
            if (length is < 1 or > 128) throw new InvalidDataException("Invalid identifier length.");
            var value = Bytes(length);
            foreach (var c in value)
                if (!char.IsAsciiLetterOrDigit((char)c) && c is not (byte)'-' and not (byte)'_')
                    throw new InvalidDataException("Invalid identifier bytes.");
            return Encoding.ASCII.GetString(value);
        }
    }
}

/// <summary>Rented contiguous frame, returned only after the asynchronous write completes.</summary>
internal sealed class PooledFrame : IDisposable, IBufferWriter<byte>
{
    private byte[] _buffer;
    private readonly int _limit;
    private int _written;
    private bool _disposed;
    public PooledFrame(int maxFrameBytes)
    {
        _limit = checked(maxFrameBytes + 4);
        _buffer = Array.Empty<byte>();
    }
    public ReadOnlyMemory<byte> Memory => _buffer.AsMemory(0, _written);
    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentOutOfRangeException.ThrowIfNegative(sizeHint);
        var count = Math.Max(1, sizeHint);
        if (count < 0 || count > _limit - _written) throw new InvalidDataException("Frame exceeds configured limit.");
        if (_buffer.Length - _written < count)
        {
            var next = ArrayPool<byte>.Shared.Rent(Math.Min(_limit, Math.Max(_written + count, Math.Max(512, _buffer.Length * 2))));
            _buffer.AsSpan(0, _written).CopyTo(next);
            if (_buffer.Length != 0) ArrayPool<byte>.Shared.Return(_buffer);
            _buffer = next;
        }
        return _buffer.AsMemory(_written, Math.Min(_buffer.Length, _limit) - _written);
    }
    public Span<byte> GetSpan(int sizeHint = 0) => GetMemory(sizeHint).Span;
    public void Advance(int count)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (count < 0 || count > Math.Min(_buffer.Length, _limit) - _written)
            throw new ArgumentOutOfRangeException(nameof(count));
        _written += count;
    }
    public void Dispose()
    {
        _disposed = true;
        var buffer = Interlocked.Exchange(ref _buffer, Array.Empty<byte>());
        if (buffer.Length != 0) ArrayPool<byte>.Shared.Return(buffer);
    }
}
