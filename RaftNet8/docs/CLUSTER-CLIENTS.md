# External cluster clients and named entities

The optional **Raft.Cluster.Server**, **Raft.Cluster.Client**, and **Raft.Cluster.Protocol** net8.0 libraries provide a message service above the embedded consensus library. One Raft group has one leader across all entities. Clients are never voters or learners. The core, wire3, raft8-v4 and pinned DBreeze version are unchanged. Use fresh application directories: the new `cluster-v1` ledger is not a migration of an arbitrary application or of legacy entities.

## Start

Run commands from `RaftNet8/`. The [human solution](../HumanTest/Raft.HumanTest.sln) includes the external-client demo. Each node owns different log/application/image directories and exposes different peer/client ports.

```sh
dotnet run --project HumanTest/Raft.Cluster.Demo -c Release -- node n1 --root D:/Temp/RaftNet8/manual
dotnet run --project HumanTest/Raft.Cluster.Demo -c Release -- node n2 --root D:/Temp/RaftNet8/manual
dotnet run --project HumanTest/Raft.Cluster.Demo -c Release -- node n3 --root D:/Temp/RaftNet8/manual
# Separate client processes:
dotnet run --project HumanTest/Raft.Cluster.Demo -c Release -- publish --entity events --text Hello
dotnet run --project HumanTest/Raft.Cluster.Demo -c Release -- subscribe --entity events --after 0
```

Default demo group: `movies`; peer ports6101–6109, client ports7101–7109. `--peer-base-port` and `--client-base-port` override them. Keep bootstrap voters, group and initial endpoints identical on original and joining nodes. `--voters 5` selects a five-original-voter lab; nodesn4/n5 in the three-original-voter lab start with `JoiningNode=true`. The console's `add n4 [operation-guid]`, `remove n1 [operation-guid]`, `status`, `snapshot`, and `quit` commands are sample administration, not an anonymous HTTP administration API.

For embedding, `RaftClusterNode.StartAsync(RaftClusterNodeOptions)` owns the Raft node/store, message ledger, peer TCP transport and HTTP gateway. Pass wire3 options, an absolute data directory, a local PeerDescriptor and ClusterServerOptions. `ConfigureMessageLimits` raises byte budgets for16MB bodies while preserving the configured consensus timers and batching delays. For an existing node use `RaftClusterServer.StartAsync` with the node's MessageLedger, matching GroupId and byte limits configured before construction. Do not attach a message gateway to an unrelated application state machine.

## Client API

```csharp
using Raft.Cluster;
using Raft.Cluster.Client;

await using var client = new RaftClusterClient(new()
{
    GroupId = "movies",
    SeedEndpoints = [new Uri("http://127.0.0.1:7101/"),
                     new Uri("http://127.0.0.1:7102/"),
                     new Uri("http://127.0.0.1:7103/")]
});
client.TopologyChanged += topology => Console.WriteLine(topology.LeaderId);
long id = await client.AddLogEntryAsync(new byte[] { 1, 2, 3 }, "events", requestId: Guid.NewGuid());
await client.CreateEntityAsync("latest", EntityMode.HighSpeedCache);
await client.AddLogEntryAsync(new byte[] { 4 }, "latest");
MessageBatch batch = await client.ReadBatchAsync("events", afterMessageId: 0);
using var output = new MemoryStream();
bool found = await client.CopyMessageToAsync("events", id, output);
```

The first append atomically creates a missing Standard entity. Existing modes cannot change. Counters start at1 independently per entity, never reset, and are unrelated to technical Raft indices. Creation and publication are deterministic replicated commands; reads do not create entities.

Success follows voter-quorum durability and leader application, without waiting for every follower or external subscriber. Retries use the same request Guid and identical command. The ID is global within the ledger, including create/endpoint requests. A conflicting body/entity/type returns `ClusterRequestException` with `Code=request_conflict`. Results are persisted with data/checkpoint and survive leader changes/snapshots. A timeout/cancellation stops waiting and may leave a committed message; persist the caller's request ID in a local outbox when retrying across process crashes. Default generated IDs only cover retries within that API call.

Memory payloads must remain unchanged until completion. Stream overloads accept a declared length or read to EOF up to the ceiling; streams remain caller-owned. One message is buffered for retry, not the whole file. Network redirects/retries happen before response-body processing; a broken streamed download leaves partial output. Rewind/truncate/discard the partial chunk before retrying.

## Subscriptions and durable positions

```csharp
// savedId comes from the subscriber application's own durable database.
await client.SubscribeAsync("events", savedId, (entity, id, data) =>
{
    // In ONE local transaction: apply business effects/store data and save id.
    // Commit that transaction BEFORE returning true.
    return true;
}, token);

await client.SubscribeIdsAsync("events", savedId, (entity, id) =>
{
    Console.WriteLine($"Committed {entity}/{id}");
    return true;
}, token);
```

Async handlers return `ValueTask<bool>`. Callbacks run sequentially per subscription. True advances the process-local cursor; false or an exception retries the same buffered message after500ms by default. Handler failures also raise `SubscriptionCallbackFailed`; diagnostic event exceptions are isolated. Cancellation terminates the subscription. Handlers must complete/cooperate with their own cancellation; the SDK cannot terminate arbitrary user code.

After a process restart supply the application's last committed cursor. There is **no persistent ClientId, cluster consumer offset, or Raft write for subscription ACKs**. Standard delivery begins strictly after that position, at10001 for after10000. Reconnect inside a process uses the latest successful position. At-least-once delivery means a pre-ack crash may repeat work unless effects and cursor are atomic/idempotent. ID-only reads touch message headers, not payloads. Receiving uses bounded batches, with callbacks per message.

## Disk, RAM and snapshots

| Mode | Payload retention | Reconnect |
| --- | --- | --- |
| Standard | Every message indefinitely on disk | All messages strictly above cursor |
| HighSpeedCache | Latest payload on disk and in a bounded read cache | Available latest above cursor; gaps are expected |

Both modes persist counters and request outcomes. Cache is not RAM-only. Its latest-only policy applies to the application ledger: the technical Raft log still contains old cache commands until snapshot/compaction. SnapshotIntervalEntries defaults to0 (manual); schedule snapshots or configure an interval when reclaiming technical history. Deleted DBreeze payload blocks may remain physically allocated/reusable; this is not secure erasure. An overwritten cache message cannot be fetched by old ID. A failed callback can retry its buffered cache value during the process lifetime; after restart only the current value is guaranteed. Cache defaults to32MiB/128entries; startup warms a fitting set, with other values loaded on demand. Persistent idempotence results have no TTL, including cache mode, so disk metadata still grows. No entity deletion, ID reset, automatic retention or orphan-file-chunk collection is provided.

One physical application table (`cluster-v1`) uses ToIndex prefixes:1 entity catalog,2 message headers,3 payloads,4 request results,5 client endpoints,6 format/counters/directory frontier. The helper's `applied` checkpoint shares that physical table. All mutations and the index/term checkpoint commit atomically in one synchronous transaction. No transaction escapes or crosses await. Reads are bounded key ranges with namespace checks; ID-only never materializes payload rows. The generation application owns its engine and allows independent snapshot replay.

Standard history is application state, independent of retained technical log entries. Snapshots preserve it together with cache latest values, counters, outcomes and endpoint metadata at the same applied fence; the existing membership wrapper includes membership service state. A new empty node receives a checked image and subsequent log suffix and can serve retained history. Generation construction copies/replays the application database and archives it without compression. Budget for source data, every replica's log and application database, replay generations, images, temporary installs and download output. Default image64GiB/unpacked128GiB must be increased for80GB histories. The larger file stress command does not itself run an80GB snapshot test.

## Topology and hot membership

PeerDescriptor addresses are peer routes, not client origins. A separate replicated application directory holds advertised HTTP(S) origins. The SDK periodically refreshes it (default1s); this is eventual polling, not immediate server push. Topology includes group, term, configuration index and endpoint-directory index. Follower responses supply bootstrap hints; a fresh leader ReadIndex makes authoritative responses. Older/non-authoritative views cannot replace an authoritative client view. `KnownEndpoints` is read-only.

Call `server.AddNodeAsync(peer, clientEndpoint, operationId, token)` on the leader. It first commits the future client address, then invokes the existing journaled Add. An interruption after registration leaves a harmless inactive address until the same Add is retried. The address is advertised when configuration gives the node a role. The wrapper does not weaken serialization of membership operations. If a raw `node.AddNodeAsync` is used, register the client origin explicitly with `RegisterClientEndpointAsync(endpoint, requestId)`. Old bootstrap settings never overwrite newer persisted origins automatically. Address changes use a fresh registration request ID.

Leader removal still uses `node.RemoveNodeAsync(nodeId, operationId, token)`. A redirected administrative caller must contact the successor with the same ID; see [Hot Membership](HOT-MEMBERSHIP.md). The refreshed client view excludes removed roles. Ordinary data requests reconnect automatically. At least one known seed/DNS bootstrap must survive an offline interval: a disconnected client cannot discover entirely unknown addresses when all old origins disappear.

## Limits and access control

| Resource | Default / ceiling |
| --- | --- |
| Logical payload |16,000,000 bytes; envelope fits within core16MiB |
| Batch |128 records and32MiB included payload |
| Server admission |4 HTTP operations;256 connections;16KiB request headers |
| SDK admission |4 requests;8 active subscription buffers; waiting calls remain caller-owned |
| Catalog |4096 entities;1024 registered client endpoints; wire3 also bounds historical peer IDs |
| Value cache |32MiB/128entries |
| Poll/retry |1s topology;100ms empty poll;500ms handler retry |
| Operation deadline |30s SDK/server; caller cancellation may be earlier |

Count limits multiplied by maximum payload size are component memory bounds, not total process RSS. Request encoding, Raft log ownership, replicas, HTTP, DBreeze and GC add copies/overhead. Increasing concurrency multiplies live buffers. The host raises peer and uncommitted byte windows to at least64MiB but does not alter election/RPC timers. Tune timers for actual large-body disk/network tails; demo timers (5–7s elections,2sRPC) are sample choices.

Plain HTTP and peer TCP are for a trusted network. `ClusterServerOptions.ConfigureHttpServer` configures Kestrel HTTPS/certificates; `Authorize(HttpContext, token)` supplies the host's access policy. Use a caller-owned HttpClient for credentials/client certificates while preserving server chain/name validation. Peer mTLS is separate and still binds certificate to node ID. The directory never distributes keys or trust roots. No anonymous client membership administration endpoint is exposed. [Kestrel endpoint configuration](https://learn.microsoft.com/en-us/aspnet/core/fundamentals/servers/kestrel/endpoints?view=aspnetcore-8.0) documents host HTTPS configuration.

## Examples and validation

The [compiled file example](../HumanTest/Raft.Cluster.Demo/FileMessages.cs) publishes bounded chunks, then a manifest in another entity, and retrieves ordered IDs into a new file with SHA-256 validation. The [scenario](../HumanTest/Raft.Cluster.Demo/Program.cs) demonstrates ID-only callbacks and atomic local cursor/data transactions. Compression/encryption belong to application code; the server stores opaque transformed bytes.

```sh
dotnet run --project HumanTest/Raft.Cluster.Demo -c Release -- scenario --root D:/Temp/RaftNet8
dotnet run --project HumanTest/Raft.Cluster.Demo -c Release -- upload --file movie.bin --chunk-entity chunks --metadata-entity movies
dotnet run --project HumanTest/Raft.Cluster.Demo -c Release -- download --manifest 1 --file new-movie.bin
# Explicit, real data; no sparse files and no automatic deletion of experiment results:
dotnet run --project HumanTest/Raft.Cluster.Demo -c Release -- stress-file --gib 80 --voters 3 --root D:/Temp/RaftNet8
dotnet run --project HumanTest/Raft.Cluster.Demo -c Release -- benchmark --rounds 5 --requests 512 --root D:/Temp/RaftNet8
```

The stress test preflights `(2*voters+2)*fileBytes+16GiB` for logs/application replicas/source/output. Other concurrent usage or snapshot copies require additional room. Reports distinguish small scenarios,16MB message tests, and opt-in large-file tests. Measurements use actual HTTP, full retained message history and process-wide allocations across in-process replicas; do not compare these numbers directly with the accepted embedded-core benchmark's different business transaction.
