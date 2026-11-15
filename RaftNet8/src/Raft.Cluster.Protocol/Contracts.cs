using System.Text;

namespace Raft.Cluster;

public enum EntityMode : byte { Standard = 0, HighSpeedCache = 1 }

public sealed record ClientEndpoint(string NodeId, Uri Address)
{
    public ClientEndpoint Validate()
    {
        ClusterLimits.ValidateName(NodeId);
        if (NodeId.Length > 128 || NodeId.Any(character => !char.IsAsciiLetterOrDigit(character) && character is not '-' and not '_'))
            throw new ArgumentException("Node IDs use 1..128 ASCII letters, digits, '-' or '_'.");
        if (!Address.IsAbsoluteUri || Address.AbsoluteUri.Length > 2048 || Address.Scheme is not ("http" or "https") ||
            !string.IsNullOrEmpty(Address.UserInfo) || Address.AbsolutePath != "/" ||
            !string.IsNullOrEmpty(Address.Query) || !string.IsNullOrEmpty(Address.Fragment))
            throw new ArgumentException("A client address must be an absolute HTTP(S) origin without a path or credentials.");
        return this;
    }
}

public sealed record ClusterTopology(string GroupId, string? LeaderId, long Term,
    long ConfigurationIndex, long DirectoryIndex, bool IsAuthoritative, IReadOnlyList<ClientEndpoint> Endpoints);
public sealed record EntityInfo(string Name, EntityMode Mode, long EntityId, long LastMessageId);
public sealed record ClusterMessage(long MessageId, int PayloadLength, ReadOnlyMemory<byte> Data);
public sealed record MessageBatch(EntityMode Mode, IReadOnlyList<ClusterMessage> Messages);
public sealed record ClusterError(string Code, string Message, string? LeaderId = null);

public sealed class ClusterRequestException(string code, string message) : InvalidOperationException(message)
{
    public string Code { get; } = code;
}

public static class ClusterLimits
{
    public const int MaxPayloadBytes = 16_000_000;
    public const int MaxCommandBytes = MaxPayloadBytes + 1024;
    public const int MaxEntityNameBytes = 256;
    public const int MaxEntities = 4096;
    public const int MaxEndpoints = 1024;
    public const int MaxBatchCount = 128;
    public const int MaxBatchBytes = 32 * 1024 * 1024;
    public const int BatchMagic = 0x31424352; // RCB1, little endian
    public const string ApiPath = "/raft-cluster/v1";

    public static void ValidateName(string name)
    {
        if (string.IsNullOrWhiteSpace(name) || Encoding.UTF8.GetByteCount(name) > MaxEntityNameBytes ||
            name.Any(char.IsControl) || name.Any(char.IsSurrogate))
            throw new ArgumentException("Names must contain 1..256 UTF-8 bytes without control/surrogate characters.", nameof(name));
    }

    public static void ValidatePayloadLength(long length)
    {
        if (length is < 0 or > MaxPayloadBytes) throw new ArgumentOutOfRangeException(nameof(length), $"Maximum payload: {MaxPayloadBytes} bytes.");
    }
}
