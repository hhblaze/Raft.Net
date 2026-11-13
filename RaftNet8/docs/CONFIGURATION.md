# Configuration and diagnostics

`RaftOptions` is immutable. Construction freezes/copies membership lists. Recovered v2 membership is authoritative; supplied `Members` bootstrap a new store and never rewrite persisted membership. IDs use 1..128 ASCII letters/digits/`-`/`_`. Each directory is bound to node/group identity.

| Option | Default | Unit / contract |
| --- | ---: | --- |
| `NodeId` / `GroupId` | required / `default` | Stable node/group identities |
| `Members` | required | Bootstrap voters; normally 3 or 5 |
| `Learners` | empty | Bootstrap non-voters, disjoint from voters |
| `KnownPeers` | union with voters/learners | Provisioned transport IDs, including future/retired peers; max 1024 total |
| `MinimumVoters` | 1 | Refuse removal below this count |
| `MaxLearnerPromotionLag` | 0 | Maximum leader-tail minus acknowledged match, in entries |
| `TickInterval` | 25 ms | Logical tick cadence, 1 ms..10 s |
| `HeartbeatTicks` | 1 | Heartbeat interval in ticks |
| `ElectionMinTicks` / `ElectionMaxTicks` | 12 / 24 | Random timeout in `[min,max)` ticks; default 300..600 ms |
| `RpcTimeout` | 200 ms | Positive per-exchange deadline below minimum election timeout and within runtime timer range |
| `WireVersion` | 2 | 1 or 2; v1 requires pre-vote off/no snapshot directory, and rejects ReadIndex/transfer/membership controls |
| `EnablePreVote` | true | Prospective election before term increment |
| `MaxAppendEntries` | 128 | Entries per log/proposal/replication/application batch; 1..4096 |
| `MaxAppendBytes` | 256 KiB | Application/query encoded-byte budget and legacy wire1 budget; max64MiB |
| `MaxReplicationBytes` | null / automatic1MiB+2688B | Wire2 packet budget: auto min(4×MaxAppendBytes +21×MaxAppendEntries,MaxInFlightBytes−64,64MiB). Explicit >=max command+21B and fits peer window/64MiB. Wire1 retains MaxAppendBytes and rejects this override. |
| `MaxProposalBatchBytes` | null / automatic | Local durable proposal group only: auto min(4×MaxAppendBytes +21×MaxAppendEntries, MaxUncommittedBytes,64MiB), default1MiB+2688B; explicit >=max command+21B and <=uncommitted budget/64MiB. Application remains bounded by MaxAppendBytes; wire2 uses MaxReplicationBytes. Count still uses MaxAppendEntries. |
| `MaxCommandBytes` | 64 KiB | Command payload; max 16 MiB |
| `ProposalBatchDelay` | 2 ms | Count-limited collection window outside the owner; <=half heartbeat/1s. Zero disables automatic collection delays. |
| `BulkProposalBatchDelay` | null / automatic8ms | Byte-limited groups: auto min(4×small window, one heartbeat,1s), shortened when no other admitted work is waiting for quorum/application. Explicit0 disables bulk waiting; explicit values <=one heartbeat/1s. Single-voter cap1ms. |
| `EnablePipelining` | true | Multiple bounded flights in Replicate mode |
| `CoalesceCommitNotifications` | true | Modern wire2: piggyback ordinary idle commit notification on next data/heartbeat; read/config/transfer/snapshot/retirement controls retain their send paths. False attempts the prior immediate idle broadcast. Log/commit persistence and client ACK are unchanged. |
| `MaxInFlightMessages` | 4 | Per-peer flights; 1..256 |
| `MaxInFlightBytes` | 4 MiB | Per-peer accounted replication window (entry bytes+64 allowance/flight); >=max batch+64, <=1 GiB |
| `MaxUncommittedBytes` | 64 MiB | Durable uncommitted budget; >=max batch, <=1 TiB |
| `MaxPendingProposals` / `MaxPendingReads` | 1024 / 1024 | Admission until applied/rejected/faulted; each max 1,000,000 |
| `LogCacheEntries` / `LogCacheBytes` | 1024 / 8 MiB | Both limits apply; count zero disables cache |
| `SnapshotDirectory` | null | Exclusive absolute image/staging directory; requires v2 store/snapshot application |
| `SnapshotIntervalEntries` | 0 | Applied entries since published image; 0 means manual creation only |
| `SnapshotRetainEntries` | 128 | Retained log tail; 0..1,000,000 |
| `SnapshotRetainedImages` | 2 | Newest images plus current/transfer/prepared pins; 2..64 |
| `SnapshotChunkBytes` | 64 KiB | Payload; 1 KiB..4 MiB |
| `SnapshotBytesPerSecond` | 16 MiB/s | Per-node outbound snapshot bandwidth; >=1 KiB/s |
| `MaxSnapshotBytes` | 64 GiB | Archived image bound; >=chunk size |
| `MaxSnapshotUnpackedBytes` | 128 GiB | Extracted generation bound; >=image bound |

Minimum election ticks must be **at least 10× heartbeat ticks**. Choose deadlines from storage/network/GC tails, including admitted backlog. The comparison benchmark uses identical longer deadlines for slow disk experiments, not a default tuning prescription.

`MaxAppendBytes` must fit `MaxCommandBytes + 21` entry bytes. Keep limits compatible across peers and retained data. Configuration metadata is separately bounded (1024 IDs). Frames, inbound connections/handlers and pending exchanges are bounded. Caller tasks awaiting admission and application-owned query/cache objects are outside the node's memory budget. Cache/uncommitted byte accounting counts payload+21 entry bytes; replication accounting adds64 per flight. CLR object/dictionary/frame/pool-bucket overhead is additionally bounded by count/frame limits, not an exact managed-heap byte budget.

Wire2 defaults now permit a larger packet than the application transaction. Frames and decoded-handler reservations use the replication bound; the existing peer byte window still applies. Older wire2 binaries/configurations reject larger packets. During mixed binary upgrades explicitly set MaxReplicationBytes=262144 (or the old peer MaxAppendBytes) on every new node; keep it through the rolling upgrade. Raise limits only with compatible settings everywhere, using a coordinated stop/configure/restart if send/receive limits cannot be changed separately. Wire bytes and storage records are unchanged.

Custom `ILogStore` implementations should expose `ILogStoreProgress.UncommittedBytes` for precise admission. `RaftStorageAdapter` supplies it; the v1 fallback scans its uncommitted suffix. After uncertain failure dispose/recover the store instead of retrying against its failed instance.

## Observability

`RaftNode.Status` is an immutable published view with configuration, peer match/next/flights, snapshot/install progress and fault. It is diagnostic, not a read fence. `Metrics` and storage metrics expose proposal/RPC/transaction/cache counters.

Pass caller-owned `ILoggerFactory` through the five-argument constructor or eight-argument logging overload:

```csharp
var node = await RaftNode.CreateAsync(options, log, transport, machine,
    clock: null, random: null, cancellationToken: token, loggerFactory: loggerFactory);
```

Original overloads remain unambiguous, including null clocks. Role/configuration/image changes use structured Information logs, faults Error, failed RPCs Debug. Payloads are excluded.

Meter/ActivitySource: **`Raft.Net8`**. Dimensions are `raft.group`, `raft.node`, and where relevant `raft.peer`/`raft.outcome`.

| Instrument | Meaning |
| --- | --- |
| `raft.term`, `raft.role` | Term and numeric `RaftRole` |
| `raft.commit.index`, `raft.applied.index` | Commit/application frontier |
| `raft.replication.lag` | Leader tail minus acknowledged peer match |
| `raft.proposals.pending`, `raft.reads.pending` | Admitted/pending requests |
| `raft.uncommitted.bytes` | Uncommitted budget usage |
| `raft.snapshot.index`, `raft.snapshot.installing.index` | Published/incoming included index |
| `raft.snapshot.send.offset`, `raft.snapshot.receive.offset` | Stream offsets in bytes |
| `raft.elections`, `raft.pre_votes` | Election/pre-vote attempts |
| `raft.failures`, `raft.rpc.failures` | Fail-stop/transport attempt counts |
| `raft.snapshot.send.bytes`, `raft.snapshot.receive.bytes` | Attempted/received bytes including retries/duplicates |
| `raft.proposal.duration` | Caller latency including admission/quorum/apply; seconds, outcome-tagged |
| `raft.persistence.duration` | Durable ILogStore write including queue; seconds |
| `raft.storage.commit.duration` | DBreeze Commit including disk flush; seconds, not raw syscall timing |
| `raft.apply.duration` | Application batch completion; seconds |
| `raft.diagnostics.callback.failures` | Isolated diagnostic callback failures |

Proposal spans contain index, term, payload length and outcome. Listener-disabled paths skip timing/histogram work. Gauge registration uses weak node references and unregisters on disposal. Exporters and log sinks belong to the host.


The bounded proposal channel now has a separate asynchronous collection deadline task. The consensus owner continues processing control/RPC events while commands gather; only that owner drains commands and assigns log indices. The change signal is bounded and carries no command payloads. Leadership/transfer/memory checks still run on the owner before persistence.

BulkProposalBatchDelay is nullable: auto min(8×ProposalBatchDelay, one heartbeat,1second), normally16ms for byte-limited groups while committed application work remains. Count-limited groups keep ProposalBatchDelay (default2ms); single-voter windows cap at1ms. When the committed application pipeline empties, the collector uses the small window to avoid idle waiting. Set BulkProposalBatchDelay=TimeSpan.Zero to disable the intentional bulk wait. Explicit bulk values must fit one heartbeat/one second. This trades bounded intentional collection latency for group amortization; wire/application/count/admission/durability limits remain unchanged.

The automatic wire2 budget includes21B of metadata headroom per count slot, matching the local payload-group intent without fragmentation solely for entry metadata. Default maximum frame is1052288B including protocol allowance. Pool bucket/object overhead is additional; a frame just above1MiB can use a2MiB pool bucket. Count and peer-byte reservations remain bounded.

Coalescing defers only the commit-only notification to caught-up idle peers. Leader quorum commit and local apply still precede client success. A follower can learn/apply the new frontier one logical heartbeat later; storage/network/owner backlog can add lag. Periodic heartbeats are unchanged. ReadIndex on the leader remains the freshness contract. The setting is ignored for wire1/legacy storage.
