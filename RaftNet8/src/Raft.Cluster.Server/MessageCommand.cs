using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;

namespace Raft.Cluster.Server;

internal enum MessageOperation : byte { Append = 1, Create = 2, RegisterEndpoint = 3 }
internal sealed record MessageCommand(MessageOperation Operation, Guid RequestId, string Name,
    EntityMode Mode, ReadOnlyMemory<byte> Payload)
{
    private const int HeaderBytes = 28;
    private const int Magic = 0x31434c52;

    internal byte[] Encode()
    {
        ClusterLimits.ValidateName(Name);
        ClusterLimits.ValidatePayloadLength(Payload.Length);
        if (RequestId == Guid.Empty || !Enum.IsDefined(Operation) || !Enum.IsDefined(Mode))
            throw new ArgumentException("Invalid message command.");
        var nameBytes = Encoding.UTF8.GetByteCount(Name);
        var bytes = new byte[checked(HeaderBytes + nameBytes + Payload.Length)];
        BinaryPrimitives.WriteInt32LittleEndian(bytes, Magic);
        bytes[4] = (byte)Operation;
        RequestId.TryWriteBytes(bytes.AsSpan(5, 16));
        bytes[21] = (byte)Mode;
        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(22), (ushort)nameBytes);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(24), Payload.Length);
        Encoding.UTF8.GetBytes(Name, bytes.AsSpan(HeaderBytes, nameBytes));
        Payload.Span.CopyTo(bytes.AsSpan(HeaderBytes + nameBytes));
        return bytes;
    }

    internal static MessageCommand Decode(ReadOnlyMemory<byte> memory)
    {
        var bytes = memory.Span;
        if (bytes.Length < HeaderBytes || BinaryPrimitives.ReadInt32LittleEndian(bytes) != Magic)
            throw new InvalidDataException("Not a cluster message command (format cluster-v1).");
        var nameBytes = BinaryPrimitives.ReadUInt16LittleEndian(bytes[22..]);
        var length = BinaryPrimitives.ReadInt32LittleEndian(bytes[24..]);
        if (nameBytes is < 1 or > ClusterLimits.MaxEntityNameBytes || length is < 0 or > ClusterLimits.MaxPayloadBytes ||
            bytes.Length != HeaderBytes + nameBytes + length)
            throw new InvalidDataException("Invalid cluster command lengths.");
        var value = new MessageCommand((MessageOperation)bytes[4], new Guid(bytes.Slice(5, 16)),
            new UTF8Encoding(false, true).GetString(bytes.Slice(HeaderBytes, nameBytes)),
            (EntityMode)bytes[21], memory.Slice(HeaderBytes + nameBytes, length));
        ClusterLimits.ValidateName(value.Name);
        if (value.RequestId == Guid.Empty || !Enum.IsDefined(value.Operation) || !Enum.IsDefined(value.Mode))
            throw new InvalidDataException("Invalid cluster command fields.");
        return value;
    }

    internal static byte[] Hash(ReadOnlyMemory<byte> encoded) => SHA256.HashData(encoded.Span);
}

internal sealed record RequestOutcome(byte[] Hash, long MessageId, string? Error);
