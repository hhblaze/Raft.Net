using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;

namespace Raft.Cluster.Server;

internal enum MessageOperation : byte
{
    Append = 1, Create = 2, RegisterEndpoint = 3, BootstrapSecurity = 4, IssueToken = 5,
    RevokeToken = 6, ConfigureEntity = 7, RegisterManagementEndpoint = 8, AdminIntent = 9, AdminResult = 10,
    UpdateToken = 11, CancelAdmin = 12, CancelResult = 13
}
internal sealed record MessageCommand(MessageOperation Operation, Guid RequestId, string Name,
    EntityMode Mode, ReadOnlyMemory<byte> Payload, CommandAuthorization? Authorization = null)
{
    private const int HeaderBytes = 28;
    private const int Magic = 0x31434c52;

    private const int V2Magic = 0x32434c52;
    internal byte[] Encode()
    {
        ClusterLimits.ValidateName(Name);
        ClusterLimits.ValidatePayloadLength(Payload.Length);
        if (RequestId == Guid.Empty || !Enum.IsDefined(Operation) || !Enum.IsDefined(Mode))
            throw new ArgumentException("Invalid message command.");
        var nameBytes = Encoding.UTF8.GetByteCount(Name);
        var prefix = Authorization is null && Operation <= MessageOperation.RegisterEndpoint ? 0 : 28;
        var bytes = new byte[checked(prefix + HeaderBytes + nameBytes + Payload.Length)];
        if (prefix != 0)
        {
            BinaryPrimitives.WriteInt32LittleEndian(bytes, V2Magic);
            if (Authorization is { } auth)
            {
                auth.TokenId.TryWriteBytes(bytes.AsSpan(4, 16));
                BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(20), auth.AuthorizedAtUnixMilliseconds);
            }
        }
        var inner = bytes.AsSpan(prefix);
        BinaryPrimitives.WriteInt32LittleEndian(inner, Magic);
        inner[4] = (byte)Operation;
        RequestId.TryWriteBytes(inner.Slice(5, 16));
        inner[21] = (byte)Mode;
        BinaryPrimitives.WriteUInt16LittleEndian(inner[22..], (ushort)nameBytes);
        BinaryPrimitives.WriteInt32LittleEndian(inner[24..], Payload.Length);
        Encoding.UTF8.GetBytes(Name, inner.Slice(HeaderBytes, nameBytes));
        Payload.Span.CopyTo(inner[(HeaderBytes + nameBytes)..]);
        return bytes;
    }

    internal static MessageCommand Decode(ReadOnlyMemory<byte> memory)
    {
        if (memory.Length >= 28 && BinaryPrimitives.ReadInt32LittleEndian(memory.Span) == V2Magic)
        {
            var id = new Guid(memory.Span.Slice(4, 16));
            var at = BinaryPrimitives.ReadInt64LittleEndian(memory.Span[20..]);
            if (id != Guid.Empty && at < 1) throw new InvalidDataException("Invalid command authorization time.");
            return DecodeInner(memory[28..], true) with { Authorization = id == Guid.Empty ? null : new(id, at) };
        }
        return DecodeInner(memory, false);
    }
    private static MessageCommand DecodeInner(ReadOnlyMemory<byte> memory, bool v2)
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
        if (value.RequestId == Guid.Empty || !Enum.IsDefined(value.Operation) || !Enum.IsDefined(value.Mode) ||
            (!v2 && value.Operation > MessageOperation.RegisterEndpoint))
            throw new InvalidDataException("Invalid cluster command fields.");
        return value;
    }

    internal static byte[] Hash(ReadOnlyMemory<byte> encoded)
    {
        // Admission timestamps change on a retry; identity and business content do not.
        if (encoded.Length >= 28 && BinaryPrimitives.ReadInt32LittleEndian(encoded.Span) == V2Magic)
        {
            using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
            hash.AppendData(encoded.Span[..20]);
            hash.AppendData(encoded.Span[28..]);
            return hash.GetHashAndReset();
        }
        return SHA256.HashData(encoded.Span);
    }
}

internal sealed record RequestOutcome(byte[] Hash, long MessageId, string? Error);
