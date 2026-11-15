namespace Raft.Cluster;

[Flags]
public enum ClusterPermission
{
    None = 0, Discover = 1, Publish = 2, ReadMetadata = 4, ReadPayload = 8, CreateEntity = 16,
    Observe = 32, ManageEntities = 64, ManageNodes = 128, ManageTokens = 256,
    All = Discover | Publish | ReadMetadata | ReadPayload | CreateEntity | Observe | ManageEntities | ManageNodes | ManageTokens
}

public sealed record EntitySettings(bool WritesPaused = false, int MaxPayloadBytes = ClusterLimits.MaxPayloadBytes,
    long? MaxRetainedBytes = null)
{
    public EntitySettings Validate()
    {
        if (MaxPayloadBytes is < 0 or > ClusterLimits.MaxPayloadBytes || MaxRetainedBytes < 0)
            throw new ArgumentException("Invalid entity limits.");
        return this;
    }
}

public sealed record TokenGrant(string Name, ClusterPermission Permissions, IReadOnlyList<string> Entities, DateTimeOffset ExpiresAt)
{
    public TokenGrant Validate()
    {
        ClusterLimits.ValidateName(Name);
        if (Permissions == ClusterPermission.None || (Permissions & ~ClusterPermission.All) != 0 || Entities.Count is < 1 or > 256)
            throw new ArgumentException("Invalid token permissions.");
        foreach (var name in Entities) if (name != "*") ClusterLimits.ValidateName(name);
        return this;
    }
}
public sealed record ClientTokenInfo(Guid TokenId, TokenGrant Grant, bool Revoked, long Revision);
public sealed record IssuedClientToken(ClientTokenInfo Token, string? Secret);
public sealed record AdminPeer(string NodeId, string Host, int Port)
{
    public AdminPeer Validate()
    {
        _ = new ClientEndpoint(NodeId, new Uri("http://127.0.0.1/")).Validate();
        if (string.IsNullOrWhiteSpace(Host) || Host.Length > 253 || Port is < 1 or > 65535 ||
            Host.Any(c => !char.IsAsciiLetterOrDigit(c) && c is not '.' and not '-' and not ':'))
            throw new ArgumentException("Invalid peer host or port.");
        return this;
    }
}
public sealed record ManagedNodeEndpoint(string NodeId, Uri Address);
public sealed record AddClusterNode(AdminPeer Peer, ClientEndpoint ClientEndpoint, ManagedNodeEndpoint ManagementEndpoint);
public sealed record CreateManagedEntity(string Name, EntityMode Mode);
public sealed record NodeJoinCapability(string NodeId, string GroupId, string ApplicationVersion, bool RequireClientTokens,
    bool SecureTransport, bool HasVotingRole, AdminPeer? Peer, ClientEndpoint ClientEndpoint, ManagedNodeEndpoint ManagementEndpoint);
public sealed record AdminOperationRequest(string Kind, string? NodeId = null, AddClusterNode? Add = null);
public sealed record AdminOperation(Guid OperationId, AdminOperationRequest Request, string State,
    long AcceptedIndex, string Actor, string? Result = null, string? Error = null)
{
    public bool CancelRequested { get; init; }
    public string? CancelError { get; init; }
    public string? MembershipStage { get; init; }
    public bool OwnsClientRegistration { get; init; }
    public bool OwnsManagementRegistration { get; init; }
}
public sealed record AdminOperationView(AdminOperation? Operation, System.Text.Json.JsonElement? Membership);
public sealed record AdminAuditEntry(long Index, Guid OperationId, string Actor, string Action, string Target, string? Error);
public sealed record CommandAuthorization(Guid TokenId, long AuthorizedAtUnixMilliseconds);
public sealed record AdminEntity(EntityInfo Entity, EntitySettings Settings)
{
    /// <summary>Optional observed catalog-interval rate; never a sum of replicated copies.</summary>
    public double? MessagesPerSecond { get; init; }
}
public sealed record MessagePreview(long MessageId, int PayloadLength, string Base64, bool Truncated);
/// <summary>Metadata collected behind one authorization/read barrier; contains no payload or bearer secret.</summary>
public sealed record AdminOverview(AdminNodeStatus Status, IReadOnlyList<ManagedNodeEndpoint> Directory,
    IReadOnlyList<AdminEntity> Entities, IReadOnlyList<AdminOperation> Operations, DateTimeOffset? CredentialExpiresAt,
    NodeDiagnostics? LeaderDiagnostics);
public sealed record AdminPeerStatus(string NodeId, bool IsVoter, long MatchIndex, long NextIndex,
    string ReplicationMode, int InFlightRequests, long InFlightBytes, long SnapshotOffset);
public sealed record AdminNodeStatus(string NodeId, string GroupId, string ApplicationVersion, string Role,
    long Term, string? LeaderId, long CommitIndex, long LastLogIndex, long LastApplied, long ConfigurationIndex,
    IReadOnlyList<string> Voters, IReadOnlyList<string> Learners, IReadOnlyList<AdminPeerStatus> Peers,
    long UncommittedBytes, long? SnapshotIndex, string? Fault);
public sealed record NodeResources(string HostId, string? VolumeId, DateTimeOffset ObservedAt,
    long WorkingSetBytes, long PrivateBytes, long GcHeapBytes, long CachedPayloadBytes,
    long? HostTotalMemoryBytes, long? HostAvailableMemoryBytes, long? VolumeTotalBytes, long? VolumeAvailableBytes,
    long? LogBytes, long? MessageBytes, long? ImageBytes, DateTimeOffset? FilesObservedAt, int ProcessId = 0, int SampleIntervalSeconds = 1);
public sealed record LatencyHistogram(IReadOnlyList<double> UpperBoundsMilliseconds, IReadOnlyList<long> Counts);
public sealed record NodeDiagnostics(AdminNodeStatus Status, NodeResources Resources, long SuccessfulPublications,
    long PublishedBytes, long FailedRequests, LatencyHistogram PublishLatency, string Epoch)
{
    public IReadOnlyDictionary<string, CoreMetric> CoreMetrics { get; init; } = new Dictionary<string, CoreMetric>();
}
/// <summary>For durations Total is milliseconds; for counters it is the summed instrument value.</summary>
public sealed record CoreMetric(long Samples, double Total);
public static class Administration
{
    public const string ApiPath = "/raft-cluster/admin/v1";
    public const string ApplicationVersion = "cluster-v2";
    public const int MaxPageSize = 100;
}
