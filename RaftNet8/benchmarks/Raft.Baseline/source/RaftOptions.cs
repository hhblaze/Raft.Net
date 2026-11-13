namespace Raft;

/// <summary>Fixed membership and explicit memory/network limits for one node.</summary>
public sealed record RaftOptions
{
    public required string NodeId { get; init; }
    public string GroupId { get; init; } = "default";
    public required IReadOnlyList<string> Members { get; init; }
    public TimeSpan TickInterval { get; init; } = TimeSpan.FromMilliseconds(25);
    public int HeartbeatTicks { get; init; } = 2;
    public int ElectionMinTicks { get; init; } = 12;
    public int ElectionMaxTicks { get; init; } = 24;
    public TimeSpan RpcTimeout { get; init; } = TimeSpan.FromMilliseconds(200);
    public int MaxAppendEntries { get; init; } = 128;
    public int MaxAppendBytes { get; init; } = 256 * 1024;
    public int MaxCommandBytes { get; init; } = 64 * 1024;
    public int MaxPendingProposals { get; init; } = 1024;
    public int LogCacheEntries { get; init; } = 1024;
    public long LogCacheBytes { get; init; } = 8 * 1024 * 1024;

    internal RaftOptions Freeze()
    {
        ValidateId(NodeId, nameof(NodeId));
        ValidateId(GroupId, nameof(GroupId));
        ArgumentNullException.ThrowIfNull(Members);
        var members = Members.ToArray();
        if (members.Length is < 1 or > 1024 || members.Distinct(StringComparer.Ordinal).Count() != members.Length ||
            !members.Contains(NodeId, StringComparer.Ordinal))
            throw new ArgumentException("Membership must contain self exactly once, with 1..1024 unique members.");
        foreach (var member in members) ValidateId(member, nameof(Members));
        if (TickInterval < TimeSpan.FromMilliseconds(1) || TickInterval > TimeSpan.FromSeconds(10) ||
            HeartbeatTicks < 1 || ElectionMinTicks <= HeartbeatTicks * 2L ||
            ElectionMaxTicks <= ElectionMinTicks || ElectionMaxTicks > 1_000_000 ||
            RpcTimeout <= TimeSpan.Zero || RpcTimeout.TotalMilliseconds >= TickInterval.TotalMilliseconds * ElectionMinTicks)
            throw new ArgumentOutOfRangeException(nameof(ElectionMinTicks), "Election and RPC deadlines must allow heartbeat delivery.");
        if (MaxAppendEntries is < 1 or > 4096 || MaxCommandBytes is < 1 or > 16 * 1024 * 1024 ||
            MaxAppendBytes < MaxCommandBytes + LogEntry.EncodedOverhead || MaxAppendBytes > 64 * 1024 * 1024 ||
            MaxPendingProposals is < 1 or > 1_000_000 || LogCacheEntries < 0 || LogCacheBytes < 0)
            throw new ArgumentOutOfRangeException(nameof(MaxAppendBytes), "Invalid batch, admission, or cache limits.");
        return this with { Members = Array.AsReadOnly(members) };
    }

    internal static void ValidateId(string id, string parameter)
    {
        if (string.IsNullOrEmpty(id) || id.Length > 128)
            throw new ArgumentException("IDs must contain 1..128 ASCII letters, digits, '-' or '_'.", parameter);
        foreach (var c in id)
            if (!char.IsAsciiLetterOrDigit(c) && c is not '-' and not '_')
                throw new ArgumentException("IDs must contain 1..128 ASCII letters, digits, '-' or '_'.", parameter);
    }
}
