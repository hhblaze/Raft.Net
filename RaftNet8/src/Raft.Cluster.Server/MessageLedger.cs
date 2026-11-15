using System.Buffers.Binary;
using System.Text.Json;
using DBreeze.Transactions;
using DBreeze.Utils;
using Raft.Storage;

namespace Raft.Cluster.Server;

/// <summary>Durable application ledger. Standard history is independent of the compactable Raft log.</summary>
public sealed class MessageLedger : IRaftBatchStateMachine, ICheckpointStateMachine, ISnapshotStateMachine, IDisposable
{
    private const string Table = "cluster-v1";
    private static readonly byte[] FormatKey = 6.ToIndex("format");
    private static readonly byte[] EntityCountKey = 6.ToIndex("entities");
    private static readonly byte[] DirectoryKey = 6.ToIndex("directory");
    private static readonly byte[] EndpointCountKey = 6.ToIndex("endpoints");
    private readonly ReaderWriterLockSlim _applicationGate = new();
    private readonly DBreezeGenerationStateMachine _database;
    private readonly object _cacheGate = new();
    private readonly Dictionary<(long Entity, long Id), LinkedListNode<CacheEntry>> _cache = new();
    private readonly LinkedList<CacheEntry> _lru = new();
    private readonly long _cacheLimit;
    private long _cacheBytes, _payloadReads;
    private sealed record CacheEntry(long Entity, long Id, byte[] Payload);
    private sealed record EndpointView(long Index, IReadOnlyList<ClientEndpoint> Endpoints);
    private EndpointView? _endpoints;
    private readonly object _outcomeGate = new();
    private readonly Dictionary<Guid, RequestOutcome> _outcomes = new();
    private readonly Queue<Guid> _outcomeOrder = new();

    public MessageLedger(string directory, long cacheBytes = 32 * 1024 * 1024)
    {
        if (cacheBytes is < 0 or > 1024L * 1024 * 1024) throw new ArgumentOutOfRangeException(nameof(cacheBytes));
        _cacheLimit = cacheBytes;
        _database = new(directory, "raft-cluster-v1", Table, [Table], ApplyCommand);
        try
        {
            _database.Read(tx =>
            {
                var format = tx.Select<byte[], string>(Table, FormatKey);
                if (format.Exists && format.Value != "raft-cluster-v1") throw new InvalidDataException("Incompatible cluster ledger format.");
                // An election no-op may checkpoint an otherwise empty ledger before its first business command.
                if (!format.Exists && tx.SelectForward<byte[], byte[]>(Table).Any(row => row.Key.Length > 0 && row.Key[0] is >= 1 and <= 6))
                    throw new InvalidDataException("Missing cluster ledger identity.");
                return true;
            });
            WarmCache();
        }
        catch { _database.Dispose(); throw; }
    }

    public AppliedCheckpoint Checkpoint => _database.Checkpoint;
    public long LastApplied => Checkpoint.Index;
    public long CachedPayloadBytes { get { lock (_cacheGate) return _cacheBytes; } }
    public long PayloadReads => Interlocked.Read(ref _payloadReads);
    public ValueTask ApplyAsync(LogEntry entry, CancellationToken token) => ApplyBatchAsync([entry], token);
    public ValueTask ApplyBatchAsync(IReadOnlyList<LogEntry> entries, CancellationToken token)
    {
        _applicationGate.EnterWriteLock();
        try
        {
            var result = _database.ApplyBatchAsync(entries, token);
            if (entries.Any(entry => !entry.IsNoOp && entry.Command.Length >= 5 && entry.Command.Span[4] == (byte)MessageOperation.RegisterEndpoint))
                Volatile.Write(ref _endpoints, null);
            return result;
        }
        finally { _applicationGate.ExitWriteLock(); }
    }
    private T Read<T>(Func<Transaction, T> read)
    {
        _applicationGate.EnterReadLock();
        try { return _database.Read(read); }
        finally { _applicationGate.ExitReadLock(); }
    }
    public ValueTask CreateSnapshotAsync(SnapshotBuildContext context, string path, CancellationToken token) => _database.CreateSnapshotAsync(context, path, token);
    public ValueTask PrepareSnapshotAsync(SnapshotImage image, long maxBytes, CancellationToken token) => _database.PrepareSnapshotAsync(image, maxBytes, token);
    public async ValueTask InstallSnapshotAsync(SnapshotImage image, CancellationToken token)
    {
        await _database.InstallSnapshotAsync(image, token).ConfigureAwait(false);
        Volatile.Write(ref _endpoints, null);
        lock (_outcomeGate) { _outcomes.Clear(); _outcomeOrder.Clear(); }
        lock (_cacheGate) { _cache.Clear(); _lru.Clear(); _cacheBytes = 0; }
        WarmCache();
    }

    private static T? Select<T>(Transaction tx, byte[] key)
    {
        var row = tx.Select<byte[], byte[]>(Table, key);
        return row.Exists ? JsonSerializer.Deserialize<T>(row.Value) : default;
    }
    private static void Insert<T>(Transaction tx, byte[] key, T value) => tx.Insert<byte[], byte[]>(Table, key, JsonSerializer.SerializeToUtf8Bytes(value));
    private static EntityInfo? Entity(Transaction tx, string name) => Select<EntityInfo>(tx, 1.ToIndex(name));

    private static void ApplyCommand(Transaction tx, LogEntry entry)
    {
        var command = MessageCommand.Decode(entry.Command);
        var requestKey = 4.ToIndex(command.RequestId.ToByteArray());
        if (tx.Select<byte[], byte[]>(Table, requestKey).Exists) return; // Same ID/body conflicts are returned by the caller, never fail-stop application.
        tx.Insert<byte[], string>(Table, FormatKey, "raft-cluster-v1");
        long id = 0;
        string? error = null;
        if (command.Operation == MessageOperation.RegisterEndpoint)
        {
            var endpoint = JsonSerializer.Deserialize<ClientEndpoint>(command.Payload.Span)?.Validate()
                ?? throw new InvalidDataException("Missing client endpoint.");
            if (endpoint.NodeId != command.Name) throw new InvalidDataException("Client endpoint identity mismatch.");
            var endpointKey = 5.ToIndex(endpoint.NodeId);
            var exists = tx.Select<byte[], byte[]>(Table, endpointKey).Exists;
            var count = tx.Select<byte[], int>(Table, EndpointCountKey);
            if (!exists && count.Exists && count.Value >= ClusterLimits.MaxEndpoints) error = "endpoint_limit";
            else
            {
                Insert(tx, endpointKey, endpoint);
                if (!exists) tx.Insert<byte[], int>(Table, EndpointCountKey, (count.Exists ? count.Value : 0) + 1);
                tx.Insert<byte[], long>(Table, DirectoryKey, entry.Index);
            }
        }
        else
        {
            var entity = Entity(tx, command.Name);
            if (entity is null)
            {
                var count = tx.Select<byte[], int>(Table, EntityCountKey);
                if ((count.Exists ? count.Value : 0) >= ClusterLimits.MaxEntities) error = "entity_limit";
                else
                {
                    entity = new(command.Name, command.Operation == MessageOperation.Create ? command.Mode : EntityMode.Standard, entry.Index, 0);
                    tx.Insert<byte[], int>(Table, EntityCountKey, (count.Exists ? count.Value : 0) + 1);
                }
            }
            if (entity is not null)
            {
                if (command.Operation == MessageOperation.Create && entity.Mode != command.Mode) error = "entity_mode_conflict";
                else if (command.Operation == MessageOperation.Append)
                {
                    if (entity.LastMessageId == long.MaxValue) error = "message_id_exhausted";
                    else
                    {
                        id = entity.LastMessageId + 1;
                        if (entity.Mode == EntityMode.HighSpeedCache && entity.LastMessageId > 0)
                        {
                            tx.RemoveKey<byte[]>(Table, 2.ToIndex(entity.EntityId, entity.LastMessageId));
                            tx.RemoveKey<byte[]>(Table, 3.ToIndex(entity.EntityId, entity.LastMessageId));
                        }
                        tx.Insert<byte[], int>(Table, 2.ToIndex(entity.EntityId, id), command.Payload.Length);
                        tx.Insert<byte[], byte[]>(Table, 3.ToIndex(entity.EntityId, id), command.Payload.ToArray());
                        entity = entity with { LastMessageId = id };
                    }
                }
                if (error is null) Insert(tx, 1.ToIndex(entity.Name), entity);
            }
        }
        Insert(tx, requestKey, new RequestOutcome(MessageCommand.Hash(entry.Command), id, error));
    }

    internal RequestOutcome? CachedOutcome(Guid requestId)
    { lock (_outcomeGate) return _outcomes.GetValueOrDefault(requestId); }
    internal RequestOutcome? Outcome(Guid requestId)
    {
        var cached = CachedOutcome(requestId);
        if (cached is not null) return cached;
        var result = Read(tx => Select<RequestOutcome>(tx, 4.ToIndex(requestId.ToByteArray())));
        if (result is not null)
            lock (_outcomeGate)
            {
                if (_outcomes.TryAdd(requestId, result)) _outcomeOrder.Enqueue(requestId);
                while (_outcomes.Count > 1024) _outcomes.Remove(_outcomeOrder.Dequeue());
            }
        return result;
    }
    public EntityInfo? GetEntity(string name)
    {
        ClusterLimits.ValidateName(name);
        return Read(tx => Entity(tx, name));
    }
    public IReadOnlyList<EntityInfo> GetEntities() => Read(tx =>
    {
        var prefix = 1.ToIndex();
        var result = new List<EntityInfo>();
        foreach (var row in tx.SelectForwardStartFrom<byte[], byte[]>(Table, prefix, true))
        {
            if (!row.Key.AsSpan().StartsWith(prefix)) break;
            if (result.Count == ClusterLimits.MaxEntities) throw new InvalidDataException("Entity catalog exceeds its bound.");
            result.Add(JsonSerializer.Deserialize<EntityInfo>(row.Value) ?? throw new InvalidDataException("Invalid entity."));
        }
        return (IReadOnlyList<EntityInfo>)result;
    });
    public (long Index, IReadOnlyList<ClientEndpoint> Endpoints) GetEndpoints()
    {
        var published = Volatile.Read(ref _endpoints);
        if (published is not null) return (published.Index, published.Endpoints);
        return Read(tx =>
    {
        var prefix = 5.ToIndex();
        var endpoints = new List<ClientEndpoint>();
        foreach (var row in tx.SelectForwardStartFrom<byte[], byte[]>(Table, prefix, true))
        {
            if (!row.Key.AsSpan().StartsWith(prefix)) break;
            if (endpoints.Count == ClusterLimits.MaxEndpoints) throw new InvalidDataException("Endpoint directory exceeds its bound.");
            endpoints.Add(JsonSerializer.Deserialize<ClientEndpoint>(row.Value)!.Validate());
        }
        var index = tx.Select<byte[], long>(Table, DirectoryKey);
        var view = new EndpointView(index.Exists ? index.Value : 0, endpoints.AsReadOnly());
        Volatile.Write(ref _endpoints, view);
        return (view.Index, view.Endpoints);
    });
    }

    public MessageBatch ReadBatch(string name, long afterMessageId, bool idsOnly = false,
        int maxCount = ClusterLimits.MaxBatchCount, int maxBytes = ClusterLimits.MaxBatchBytes)
    {
        ClusterLimits.ValidateName(name);
        if (afterMessageId < 0 || maxCount is < 1 or > ClusterLimits.MaxBatchCount ||
            maxBytes is < ClusterLimits.MaxPayloadBytes or > ClusterLimits.MaxBatchBytes) throw new ArgumentOutOfRangeException(nameof(afterMessageId));
        return Read(tx =>
        {
            var entity = Entity(tx, name) ?? throw new ClusterRequestException("entity_not_found", $"Entity '{name}' does not exist.");
            var messages = new List<ClusterMessage>();
            if (afterMessageId == long.MaxValue) return new MessageBatch(entity.Mode, messages);
            var prefix = 2.ToIndex(entity.EntityId);
            long total = 0;
            foreach (var row in tx.SelectForwardStartFrom<byte[], int>(Table, 2.ToIndex(entity.EntityId, afterMessageId), false))
            {
                if (!row.Key.AsSpan().StartsWith(prefix)) break;
                if (row.Key.Length != prefix.Length + 8) throw new InvalidDataException("Invalid message key.");
                // DBreeze numeric encodings are signed, order-preserving big endian.
                var id = BinaryPrimitives.ReadInt64BigEndian(row.Key.AsSpan(prefix.Length)) ^ long.MinValue;
                var length = row.Value;
                ClusterLimits.ValidatePayloadLength(length);
                if (messages.Count == maxCount || (!idsOnly && total + length > maxBytes)) break;
                messages.Add(new(id, length, idsOnly ? ReadOnlyMemory<byte>.Empty : Payload(tx, entity, id)));
                total += length;
            }
            return new MessageBatch(entity.Mode, messages);
        });
    }

    public ClusterMessage? GetMessage(string name, long messageId)
    {
        ClusterLimits.ValidateName(name);
        if (messageId <= 0) throw new ArgumentOutOfRangeException(nameof(messageId));
        return Read(tx =>
        {
            var entity = Entity(tx, name);
            if (entity is null) return null;
            var row = tx.Select<byte[], int>(Table, 2.ToIndex(entity.EntityId, messageId));
            return row.Exists ? new ClusterMessage(messageId, row.Value, Payload(tx, entity, messageId)) : null;
        });
    }

    private byte[] Payload(Transaction tx, EntityInfo entity, long id)
    {
        if (entity.Mode == EntityMode.HighSpeedCache)
            lock (_cacheGate)
                if (_cache.TryGetValue((entity.EntityId, id), out var cached))
                { _lru.Remove(cached); _lru.AddFirst(cached); return cached.Value.Payload; }
        var row = tx.Select<byte[], byte[]>(Table, 3.ToIndex(entity.EntityId, id));
        if (!row.Exists) throw new InvalidDataException("Committed message payload is missing.");
        var payload = row.Value;
        Interlocked.Increment(ref _payloadReads);
        if (entity.Mode == EntityMode.HighSpeedCache && payload.Length <= _cacheLimit)
            lock (_cacheGate)
            {
                if (_cache.TryGetValue((entity.EntityId, id), out var current)) return current.Value.Payload;
                while (_lru.Last is { } tail && (_cacheBytes + payload.Length > _cacheLimit || _cache.Count >= 128))
                { _lru.RemoveLast(); _cache.Remove((tail.Value.Entity, tail.Value.Id)); _cacheBytes -= tail.Value.Payload.Length; }
                var node = _lru.AddFirst(new CacheEntry(entity.EntityId, id, payload));
                _cache.Add((entity.EntityId, id), node); _cacheBytes += payload.Length;
            }
        return payload;
    }
    private void WarmCache()
    {
        foreach (var entity in GetEntities().Where(entity => entity.Mode == EntityMode.HighSpeedCache && entity.LastMessageId > 0).Take(128))
            Read(tx =>
            {
                var row = tx.Select<byte[], int>(Table, 2.ToIndex(entity.EntityId, entity.LastMessageId));
                if (row.Exists && row.Value <= _cacheLimit - CachedPayloadBytes) _ = Payload(tx, entity, entity.LastMessageId);
                return true;
            });
    }
    public void Dispose() => _database.Dispose();
}
