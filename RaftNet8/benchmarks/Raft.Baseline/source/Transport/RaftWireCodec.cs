using System.Buffers;
using System.Buffers.Binary;
using System.Text;

namespace Raft.Transport;

internal readonly record struct RpcEnvelope(string GroupId, string From, RaftRpc Message);

internal static class RaftWireCodec
{
    internal static PooledFrame Encode(string groupId, string from, RaftRpc rpc, int maxFrameBytes)
    {
        var frame = new PooledFrame(maxFrameBytes);
        try
        {
            frame.WriteInt32(0); // length prefix, patched after serialization
            frame.WriteByte(1); // protocol version; deliberately incompatible with the historical protocol
            frame.WriteByte(rpc switch { VoteRequest => 1, VoteResponse => 2, AppendRequest => 3, AppendResponse => 4, _ => throw new InvalidDataException("Unknown RPC.") });
            frame.WriteString(groupId);
            frame.WriteString(from);
            frame.WriteInt64(rpc.Term);
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
                    frame.WriteInt32(append.Entries.Count);
                    for (var i = 0; i < append.Entries.Count; i++)
                    {
                        var entry = append.Entries[i];
                        frame.WriteInt64(entry.Index);
                        frame.WriteInt64(entry.Term);
                        frame.WriteByte(entry.IsNoOp ? (byte)1 : (byte)0);
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
                    break;
            }
            frame.PatchLength();
            return frame;
        }
        catch { frame.Dispose(); throw; }
    }

    internal static RpcEnvelope Decode(ReadOnlySpan<byte> bytes, RaftOptions options)
    {
        var reader = new SpanReader(bytes);
        if (reader.Byte() != 1) throw new InvalidDataException("Unsupported Raft wire version.");
        var kind = reader.Byte();
        var group = reader.String();
        var from = reader.String();
        var term = reader.Int64();
        if (term < 0) throw new InvalidDataException("Negative RPC term.");
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
                var count = reader.Int32();
                if (count < 0 || count > options.MaxAppendEntries) throw new InvalidDataException("Invalid batch count.");
                var entries = new LogEntry[count];
                var size = 0;
                for (var i = 0; i < count; i++)
                {
                    var index = reader.Int64();
                    var entryTerm = reader.Int64();
                    var noOp = reader.Boolean();
                    var length = reader.Int32();
                    if (length < 0 || length > options.MaxCommandBytes) throw new InvalidDataException("Command length exceeds limit.");
                    size = checked(size + length + LogEntry.EncodedOverhead);
                    if (size > options.MaxAppendBytes) throw new InvalidDataException("Batch bytes exceed limit.");
                    entries[i] = new LogEntry(index, entryTerm, reader.Bytes(length), noOp);
                }
                message = new AppendRequest(term, requestId, previous, previousTerm, commit, entries);
                break;
            case 4:
                message = new AppendResponse(term, reader.Int64(), reader.Boolean(), reader.Int64(), reader.Int64(), reader.Int64());
                break;
            default: throw new InvalidDataException("Unknown Raft message kind.");
        }
        if (!reader.IsEmpty) throw new InvalidDataException("Trailing bytes in Raft frame.");
        return new(group, from, message);
    }

    internal static async ValueTask<RpcEnvelope> ReadAsync(Stream stream, byte[] header, RaftOptions options, CancellationToken token)
    {
        await stream.ReadExactlyAsync(header, token).ConfigureAwait(false);
        var length = BinaryPrimitives.ReadInt32LittleEndian(header);
        if (length < 18 || length > options.MaxAppendBytes + 1024) throw new InvalidDataException("Invalid Raft frame length.");
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
internal sealed class PooledFrame : IDisposable
{
    private byte[] _buffer = ArrayPool<byte>.Shared.Rent(512);
    private readonly int _limit;
    private int _written;
    public PooledFrame(int maxFrameBytes) => _limit = checked(maxFrameBytes + 4);
    public ReadOnlyMemory<byte> Memory => _buffer.AsMemory(0, _written);
    private Span<byte> Reserve(int count)
    {
        if (count < 0 || count > _limit - _written) throw new InvalidDataException("Frame exceeds configured limit.");
        if (_buffer.Length - _written < count)
        {
            var next = ArrayPool<byte>.Shared.Rent(Math.Min(_limit, Math.Max(_written + count, _buffer.Length * 2)));
            _buffer.AsSpan(0, _written).CopyTo(next);
            ArrayPool<byte>.Shared.Return(_buffer);
            _buffer = next;
        }
        var span = _buffer.AsSpan(_written, count);
        _written += count;
        return span;
    }
    public void WriteByte(byte value) => Reserve(1)[0] = value;
    public void WriteInt32(int value) => BinaryPrimitives.WriteInt32LittleEndian(Reserve(4), value);
    public void WriteInt64(long value) => BinaryPrimitives.WriteInt64LittleEndian(Reserve(8), value);
    public void Write(ReadOnlySpan<byte> value) => value.CopyTo(Reserve(value.Length));
    public void WriteString(string value)
    {
        RaftOptions.ValidateId(value, nameof(value));
        WriteInt32(value.Length);
        Encoding.ASCII.GetBytes(value, Reserve(value.Length));
    }
    public void PatchLength() => BinaryPrimitives.WriteInt32LittleEndian(_buffer, _written - 4);
    public void Dispose()
    {
        var buffer = Interlocked.Exchange(ref _buffer, Array.Empty<byte>());
        if (buffer.Length != 0) ArrayPool<byte>.Shared.Return(buffer);
    }
}
