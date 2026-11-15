using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text.Json;
using DBreeze.Transactions;
using DBreeze.Utils;

namespace Raft.Cluster.Server;

public enum ClusterLedgerVersion { V1 = 1, V2 = 2 }
internal sealed record StoredClientToken(ClientTokenInfo Info, byte[] Hash);
internal sealed record BootstrapSecurity(StoredClientToken Token, bool RequireClientTokens);
internal sealed record AdminResult(Guid OperationId, string? Result, string? Error, string? State = null);
internal sealed record AdminCancelResult(Guid OperationId, string Error);
internal sealed record SecurityConfiguration(bool Initialized, bool RequireClientTokens);

public sealed partial class MessageLedger
{
    public ClusterLedgerVersion Version { get; }
    private static readonly byte[] SecurityKey = 6.ToIndex("security");
    private static readonly byte[] ActiveAdminKey = 6.ToIndex("admin");
    private SecurityConfiguration _security = new(false, false);
    private AdminOperation? _activeAdmin;
    private IReadOnlyList<ManagedNodeEndpoint> _managementEndpoints = Array.Empty<ManagedNodeEndpoint>();
    private readonly object _credentialGate = new();
    private readonly Dictionary<Guid, StoredClientToken> _credentials = new();
    private readonly Queue<Guid> _credentialOrder = new();
    private long _credentialEpoch;
    public bool SecurityInitialized => Volatile.Read(ref _security).Initialized;
    public bool RequiresClientTokens => Volatile.Read(ref _security).RequireClientTokens;
    public AdminOperation? ActiveAdminOperation => Volatile.Read(ref _activeAdmin);

    private static MessageOperation Kind(ReadOnlyMemory<byte> bytes) =>
        bytes.Length >= 33 && BinaryPrimitives.ReadInt32LittleEndian(bytes.Span) == 0x32434c52
            ? (MessageOperation)bytes.Span[32] : (MessageOperation)bytes.Span[4];
    private static bool IsAdministrative(ReadOnlyMemory<byte> bytes) => bytes.Length >= 5 && Kind(bytes) > MessageOperation.RegisterEndpoint;
    private void RefreshAdministration(bool alreadyLocked = false)
    {
        (SecurityConfiguration security, AdminOperation? active, IReadOnlyList<ManagedNodeEndpoint> endpoints) ReadState(Transaction tx)
        {
            var security = Select<SecurityConfiguration>(tx, SecurityKey) ?? new(false, false);
            var active = Select<Guid?>(tx, ActiveAdminKey);
            return (security, active.HasValue ? Select<AdminOperation>(tx, 9.ToIndex(active.Value.ToByteArray())) : null,
                Array.AsReadOnly(Range<ManagedNodeEndpoint>(tx, 11, null, ClusterLimits.MaxEndpoints).ToArray()));
        }
        var state = alreadyLocked ? _database.Read(ReadState) : Read(ReadState);
        Volatile.Write(ref _security, state.security);
        Volatile.Write(ref _activeAdmin, state.Item2);
        Volatile.Write(ref _managementEndpoints, state.endpoints);
        lock (_credentialGate) { _credentialEpoch++; _credentials.Clear(); _credentialOrder.Clear(); }
    }
    public EntitySettings GetEntitySettings(string name)
    {
        ClusterLimits.ValidateName(name);
        return Read(tx => Select<EntitySettings>(tx, 7.ToIndex(name)) ?? new());
    }
    public IReadOnlyList<AdminEntity> GetAdminEntities()
    {
        var entities = GetEntities();
        return Read(tx => entities.Select(entity => new AdminEntity(entity, Select<EntitySettings>(tx, 7.ToIndex(entity.Name)) ?? new())).ToArray());
    }
    public MessagePreview? PreviewMessage(string name, long messageId, int maximumBytes = 4096)
    {
        ClusterLimits.ValidateName(name);
        if (messageId < 1 || maximumBytes is < 1 or > 4096) throw new ArgumentOutOfRangeException(nameof(messageId));
        return Read(tx =>
        {
            var entity = Entity(tx, name);
            if (entity is null) return null;
            var header = tx.Select<byte[], int>(Table, 2.ToIndex(entity.EntityId, messageId));
            if (!header.Exists) return null;
            ClusterLimits.ValidatePayloadLength(header.Value);
            var row = tx.Select<byte[], byte[]>(Table, 3.ToIndex(entity.EntityId, messageId));
            if (!row.Exists) throw new InvalidDataException("Retained payload is missing.");
            // GetValuePart reads at most the preview prefix; never materialize the complete16MB body here.
            var length = Math.Min(header.Value, maximumBytes);
            var preview = length == 0 ? Array.Empty<byte>() : row.GetValuePart(0, (uint)length);
            Interlocked.Increment(ref _payloadReads);
            return new MessagePreview(messageId, header.Value, Convert.ToBase64String(preview), header.Value > maximumBytes);
        });
    }
    internal StoredClientToken? GetCredential(Guid id)
    {
        while (true)
        {
            long epoch;
            lock (_credentialGate)
            {
                if (_credentials.TryGetValue(id, out var cached)) return cached;
                epoch = _credentialEpoch;
            }
            var credential = Read(tx => Select<StoredClientToken>(tx, 8.ToIndex(id.ToByteArray())));
            if (credential is not null) credential = credential with { Info = credential.Info with { Grant = credential.Info.Grant with
                { Entities = Array.AsReadOnly(credential.Info.Grant.Entities.ToArray()) } } };
            lock (_credentialGate)
            {
                if (epoch != _credentialEpoch) continue;
                if (credential is not null && _credentials.TryAdd(id, credential)) _credentialOrder.Enqueue(id);
                while (_credentials.Count > 1024) _credentials.Remove(_credentialOrder.Dequeue());
                return credential;
            }
        }
    }
    public ClientTokenInfo? GetToken(Guid id) => GetCredential(id)?.Info;
    public IReadOnlyList<ClientTokenInfo> GetTokens(int count = Administration.MaxPageSize, Guid? after = null) =>
        Read(tx => Range<StoredClientToken>(tx, 8, after?.ToByteArray(), count).Select(item => item.Info).ToArray());
    public AdminOperation? GetAdminOperation(Guid id) => Read(tx => Select<AdminOperation>(tx, 9.ToIndex(id.ToByteArray())));
    public IReadOnlyList<AdminOperation> GetAdminOperations(long afterIndex = 0, int count = Administration.MaxPageSize) => Read(tx =>
        Range<Guid>(tx, 12, afterIndex, count).Select(id => Select<AdminOperation>(tx, 9.ToIndex(id.ToByteArray()))!).ToArray());
    public IReadOnlyList<AdminOperation> GetRecentAdminOperations(int count = Administration.MaxPageSize) => Read(tx =>
        Recent<Guid>(tx, 12, count).Select(id => Select<AdminOperation>(tx, 9.ToIndex(id.ToByteArray()))!).ToArray());
    public IReadOnlyList<AdminAuditEntry> GetRecentAudit(int count = Administration.MaxPageSize) => Read(tx => Recent<AdminAuditEntry>(tx, 10, count));
    public IReadOnlyList<AdminAuditEntry> GetAudit(long afterIndex = 0, int count = Administration.MaxPageSize) =>
        Read(tx => Range<AdminAuditEntry>(tx, 10, afterIndex, count));
    // Routing metadata changes only with administrative commits/restores. Diagnostics must not queue behind application fsync.
    public IReadOnlyList<ManagedNodeEndpoint> GetManagementEndpoints() => Volatile.Read(ref _managementEndpoints);

    private static IReadOnlyList<T> Range<T>(Transaction tx, int type, object? after, int count)
    {
        if (count is < 1 or > ClusterLimits.MaxEndpoints) throw new ArgumentOutOfRangeException(nameof(count));
        var prefix = type.ToIndex();
        var start = after switch { long index => type.ToIndex(index), byte[] bytes => type.ToIndex(bytes), _ => prefix };
        var result = new List<T>(Math.Min(count, 100));
        foreach (var row in tx.SelectForwardStartFrom<byte[], byte[]>(Table, start, after is null))
        {
            if (!row.Key.AsSpan().StartsWith(prefix) || result.Count == count) break;
            result.Add(JsonSerializer.Deserialize<T>(row.Value) ?? throw new InvalidDataException("Invalid administration record."));
        }
        return result;
    }
    private static IReadOnlyList<T> Recent<T>(Transaction tx, int type, int count)
    {
        if (count is < 1 or > Administration.MaxPageSize) throw new ArgumentOutOfRangeException(nameof(count));
        var prefix = type.ToIndex();
        var values = new List<T>(count);
        foreach (var row in tx.SelectBackwardStartFrom<byte[], byte[]>(Table, (type + 1).ToIndex(), false))
        {
            if (!row.Key.AsSpan().StartsWith(prefix) || values.Count == count) break;
            values.Add(JsonSerializer.Deserialize<T>(row.Value) ?? throw new InvalidDataException("Invalid administration history."));
        }
        values.Reverse();
        return values;
    }
    internal CommandAuthorization Authenticate(string? bearer, ClusterPermission permission, string entity, DateTimeOffset now)
    {
        if (bearer is null || bearer.Length > 160) throw new ClusterRequestException("unauthorized", "A bearer token is required.");
        var parts = bearer.Split('.');
        if (parts.Length != 3 || parts[0] != "rc2" || !Guid.TryParseExact(parts[1], "N", out var id))
            throw new ClusterRequestException("unauthorized", "Invalid bearer token.");
        byte[] secret;
        try { secret = Convert.FromBase64String(parts[2]); }
        catch (FormatException) { throw new ClusterRequestException("unauthorized", "Invalid bearer token."); }
        var credential = GetCredential(id);
        if (secret.Length != 32 || credential is null || credential.Info.Revoked || credential.Info.Grant.ExpiresAt <= now ||
            !CryptographicOperations.FixedTimeEquals(credential.Hash, SHA256.HashData(secret)))
            throw new ClusterRequestException("unauthorized", "Invalid or expired bearer token.");
        if (!Allows(credential.Info, permission, entity)) throw new ClusterRequestException("forbidden", "Insufficient permissions.");
        return new(id, now.ToUnixTimeMilliseconds());
    }
    private static bool Allows(ClientTokenInfo token, ClusterPermission permission, string entity) =>
        (token.Grant.Permissions & permission) == permission &&
        (token.Grant.Entities.Contains("*", StringComparer.Ordinal) || entity.Length == 0 && permission == ClusterPermission.Discover ||
            entity.Length > 0 && token.Grant.Entities.Contains(entity, StringComparer.Ordinal));
    private static bool Permitted(Transaction tx, CommandAuthorization auth, ClusterPermission permission, string entity)
    {
        var stored = Select<StoredClientToken>(tx, 8.ToIndex(auth.TokenId.ToByteArray()));
        return stored is not null && !stored.Info.Revoked &&
            stored.Info.Grant.ExpiresAt.ToUnixTimeMilliseconds() > auth.AuthorizedAtUnixMilliseconds &&
            Allows(stored.Info, permission, entity);
    }
    private static string? AuthorizeApplication(Transaction tx, MessageCommand command)
    {
        var permission = command.Operation switch
        {
            MessageOperation.Append => ClusterPermission.Publish,
            MessageOperation.Create => ClusterPermission.CreateEntity,
            MessageOperation.IssueToken or MessageOperation.RevokeToken or MessageOperation.UpdateToken => ClusterPermission.ManageTokens,
            MessageOperation.ConfigureEntity => ClusterPermission.ManageEntities,
            MessageOperation.AdminIntent or MessageOperation.CancelAdmin => ClusterPermission.ManageNodes,
            _ => ClusterPermission.None
        };
        if (permission == ClusterPermission.None) return null;
        var admin = command.Operation > MessageOperation.RegisterEndpoint;
        if (command.Authorization is null)
            return admin || Select<SecurityConfiguration>(tx, SecurityKey)?.RequireClientTokens == true ? "unauthorized" : null;
        return Permitted(tx, command.Authorization, permission, admin && command.Operation != MessageOperation.ConfigureEntity ? "" : command.Name) ? null : "forbidden";
    }
    private static T Decode<T>(MessageCommand command) =>
        JsonSerializer.Deserialize<T>(command.Payload.Span) ?? throw new InvalidDataException("Missing administrative command body.");
    private static string? ApplyAdministration(Transaction tx, LogEntry entry, MessageCommand command)
    {
        string? error = null;
        switch (command.Operation)
        {
            case MessageOperation.BootstrapSecurity:
            {
                if (Select<SecurityConfiguration>(tx, SecurityKey)?.Initialized == true) { error = "already_initialized"; break; }
                var bootstrap = Decode<BootstrapSecurity>(command);
                bootstrap.Token.Info.Grant.Validate();
                if (bootstrap.Token.Hash.Length != 32 || bootstrap.Token.Info.TokenId == Guid.Empty ||
                    bootstrap.Token.Info.Grant.Permissions != ClusterPermission.All || !bootstrap.Token.Info.Grant.Entities.Contains("*"))
                    throw new InvalidDataException("Invalid bootstrap grant.");
                Insert(tx, 8.ToIndex(bootstrap.Token.Info.TokenId.ToByteArray()), bootstrap.Token with { Info = bootstrap.Token.Info with { Revision = entry.Index } });
                Insert(tx, SecurityKey, new SecurityConfiguration(true, bootstrap.RequireClientTokens));
                break;
            }
            case MessageOperation.IssueToken:
            {
                var stored = Decode<StoredClientToken>(command);
                stored.Info.Grant.Validate();
                if (stored.Hash.Length != 32 || stored.Info.TokenId == Guid.Empty) throw new InvalidDataException("Invalid token.");
                var key = 8.ToIndex(stored.Info.TokenId.ToByteArray());
                if (Select<StoredClientToken>(tx, key) is not null) { error = "token_exists"; break; }
                Insert(tx, key, stored with { Info = stored.Info with { Revision = entry.Index } });
                break;
            }
            case MessageOperation.UpdateToken:
            {
                var update = Decode<ClientTokenInfo>(command);
                update.Grant.Validate();
                var key = 8.ToIndex(update.TokenId.ToByteArray());
                var stored = Select<StoredClientToken>(tx, key);
                if (stored is null) { error = "token_not_found"; break; }
                if (stored.Info.Revoked) { error = "token_revoked"; break; }
                Insert(tx, key, stored with { Info = stored.Info with { Grant = update.Grant, Revision = entry.Index } });
                break;
            }
            case MessageOperation.RevokeToken:
            {
                var id = Guid.Parse(command.Name);
                var key = 8.ToIndex(id.ToByteArray());
                var stored = Select<StoredClientToken>(tx, key);
                if (stored is null) error = "token_not_found";
                else Insert(tx, key, stored with { Info = stored.Info with { Revoked = true, Revision = entry.Index } });
                break;
            }
            case MessageOperation.ConfigureEntity:
                if (Entity(tx, command.Name) is null) error = "entity_not_found";
                else Insert(tx, 7.ToIndex(command.Name), Decode<EntitySettings>(command).Validate());
                break;
            case MessageOperation.RegisterManagementEndpoint:
            {
                var endpoint = Decode<ManagedNodeEndpoint>(command);
                _ = new ClientEndpoint(endpoint.NodeId, endpoint.Address).Validate();
                Insert(tx, 11.ToIndex(endpoint.NodeId), endpoint);
                break;
            }
            case MessageOperation.AdminIntent:
            {
                if (Select<Guid?>(tx, ActiveAdminKey) is not null) { error = "membership_busy"; break; }
                var request = Decode<AdminOperationRequest>(command);
                if (request.Kind is not ("add" or "remove" or "transfer" or "snapshot")) throw new InvalidDataException("Unknown administrative action.");
                var operation = new AdminOperation(command.RequestId, request, "Accepted", entry.Index, command.Authorization!.TokenId.ToString("N"))
                {
                    OwnsClientRegistration = request.Add is { } add && !tx.Select<byte[], byte[]>(Table, 5.ToIndex(add.Peer.NodeId)).Exists,
                    OwnsManagementRegistration = request.Add is { } addManagement && !tx.Select<byte[], byte[]>(Table, 11.ToIndex(addManagement.Peer.NodeId)).Exists
                };
                Insert(tx, 9.ToIndex(command.RequestId.ToByteArray()), operation);
                Insert(tx, 12.ToIndex(entry.Index), command.RequestId);
                Insert(tx, ActiveAdminKey, command.RequestId);
                break;
            }
            case MessageOperation.AdminResult:
            {
                var result = Decode<AdminResult>(command);
                var key = 9.ToIndex(result.OperationId.ToByteArray());
                var operation = Select<AdminOperation>(tx, key);
                if (operation is null) throw new InvalidDataException("Unknown administrative result.");
                if (operation.State != "Accepted") return null;
                if (result.State is not (null or "Completed" or "Cancelled" or "Failed")) throw new InvalidDataException("Invalid operation result state.");
                Insert(tx, key, operation with { State = result.State ?? (result.Error is null ? "Completed" : "Failed"), Result = result.Result, Error = result.Error, CancelRequested = false });
                if (result.State == "Cancelled" && operation.Request.Add is { } cancelled)
                {
                    if (operation.OwnsClientRegistration && tx.Select<byte[], byte[]>(Table, 5.ToIndex(cancelled.Peer.NodeId)).Exists)
                    {
                        tx.RemoveKey<byte[]>(Table, 5.ToIndex(cancelled.Peer.NodeId));
                        var count = tx.Select<byte[], int>(Table, EndpointCountKey);
                        tx.Insert<byte[], int>(Table, EndpointCountKey, Math.Max(0, count.Value - 1));
                        tx.Insert<byte[], long>(Table, DirectoryKey, entry.Index);
                    }
                    if (operation.OwnsManagementRegistration) tx.RemoveKey<byte[]>(Table, 11.ToIndex(cancelled.Peer.NodeId));
                }
                if (Select<Guid?>(tx, ActiveAdminKey) == result.OperationId) tx.RemoveKey<byte[]>(Table, ActiveAdminKey);
                break;
            }
            case MessageOperation.CancelAdmin:
            {
                var id = Decode<Guid>(command);
                var key = 9.ToIndex(id.ToByteArray());
                var operation = Select<AdminOperation>(tx, key);
                if (operation is null || operation.State != "Accepted" || operation.Request.Kind != "add" || operation.CancelError is not null) error = "too_late";
                else Insert(tx, key, operation with { CancelRequested = true, CancelError = null });
                break;
            }
            case MessageOperation.CancelResult:
            {
                var result = Decode<AdminCancelResult>(command);
                var key = 9.ToIndex(result.OperationId.ToByteArray());
                var operation = Select<AdminOperation>(tx, key);
                if (operation is not null && operation.State != "Cancelled") Insert(tx, key, operation with { CancelRequested = false, CancelError = result.Error });
                break;
            }
            default: throw new InvalidDataException("Unsupported administrative command.");
        }
        Insert(tx, 10.ToIndex(entry.Index), new AdminAuditEntry(entry.Index, command.RequestId,
            command.Authorization?.TokenId.ToString("N") ?? "host", command.Operation.ToString(), command.Name, error));
        return error;
    }
}
