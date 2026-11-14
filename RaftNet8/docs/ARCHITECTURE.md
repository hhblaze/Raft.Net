# Core, durability and snapshot design

Authorities: [Raft Figure 2](https://raft.github.io/raft.pdf), [Ongaro's dissertation](https://github.com/ongardie/dissertation/blob/master/online.pdf), and the [single-server membership correction](https://groups.google.com/g/raft-dev/c/t4xj6dJTP6E). Assume non-Byzantine nodes and storage honoring DBreeze Commit. Commands must apply deterministically.

## Core and runtime

`Core/RaftCore.Step` maps immutable `(RaftState, CoreEvent)` to `(State, Effects)`. Correlated log reads/writes, timeout samples and snapshot completions resume immutable action records, not executable callbacks. Queries request bounded ranges/terms/configuration; the core never preloads a full log. Stale completion IDs cannot release another operation.

`RaftDriver` executes effects. `RaftNode` has a bounded control channel plus a separately bounded proposal channel with coalesced notifications, an injected ticker/random provider and an ordered application worker. Controls retain owner ordering without fragmenting queued proposal batches. `RaftStorageAdapter` runs synchronous log transactions on one dedicated background thread per adapter, through a bounded2-operation channel, so durable flushes do not block the shared ThreadPool. Published cache reads use nonblocking TryEnter and no DB I/O; misses/busy caches go to that worker. Network exchanges run outside the owner and post correlated responses. Slow disk can delay the next owner event; batching amortizes this without early acknowledgments.

Peers move Probe → Replicate → Snapshot. Replicate tracks optimistic next indices and count/byte bounded flights. A cumulative valid acknowledgment reclaims earlier flights. Rejection/unreach collapses to probe; stale IDs do nothing. Only eligible current voters count for commit/contact/read quorums. Independent keepalive appends maintain contact during throttled snapshots.

## Checked v2/v3 storage

The v2/v3 formats below remain compatibility modes. New hot-membership groups select explicit wire3/UnifiedTableV4; see [the service-state and failure contract](HOT-MEMBERSHIP.md).

Private tables: `raft8/v2/meta`, `raft8/v2/log`, `raft8/v2/config`. One engine/folder represents one node/group. Cached metadata includes term/vote/commit, first/tail index/term, byte frontier, indexed configuration, known members, snapshot and install intent. Cache limits apply to both count and bytes.

Existing constructors select these v2 tables. Explicit `DBreezeLogLayout.UnifiedTableV3` stores entries and other checked values in physical table `raft8/v3`: `1.ToIndex(metadataKey)`, `2.ToIndex(logIndex)`, `3.ToIndex(configurationIndex)`. Ordered prefix/range operations and deletions stay inside each logical namespace; metadata and appended entries share one physical-table commit. Hard state and frontier are packed in one RF3M manifest with exact length/version/CRC32C checks, reducing two key updates to one; the other checked record bodies are unchanged. Earlier unreleased v3 prototypes are rejected before cleanup. Initialization writes incompatible-reader guards; existing v1/v2 data requires offline copy migration. No public wire change is involved. Full snapshot-log replacement visits retained keys within the log prefix in bounded memory; its work grows with the retained log rather than resetting an independent table root.

Entry records include magic/version, length, CRC32C, index, term, kind, payload length and header CRC32C. Metadata/checkpoints use checked envelopes. Hardware CRC has a portable implementation. CRC detects accidental corruption, not malicious tampering.

```mermaid
sequenceDiagram
    participant Core
    participant Driver
    participant DB as DBreeze
    participant Peer
    Core->>Driver: Persist(id, state/entries/frontier)
    Driver->>DB: reserve tables; ordered atomic mutations
    DB->>DB: Commit and durable flush
    DB-->>Driver: success
    Driver->>Core: StorageCompleted(id)
    Core-->>Driver: dependent response/send/apply
    Driver-->>Peer: durable acknowledgment
```

Atomic batches combine append/suffix replacement with hard state/frontier. Single-voter append+commit and follower suffix+commit can share one Commit. Metadata/cache publication follows success. Unchanged hard state/heartbeats do not write.

Local proposal persistence has its own byte limit: MaxProposalBatchBytes=null defaults to four MaxAppendBytes budgets plus21B per allowed entry, capped by the uncommitted budget and64MiB. The same immutable group is split into existing byte/count-bounded wire frames and application batches. This permits one durable local append for several replication frames without changing per-command quorum/apply acknowledgment. Commit-only partial byte accounting can use already-published cached entries only before any log mutation in the transaction; otherwise it reads the checked DB range.

The ordered application loop also owns a background thread. It starts synchronous DBreeze callbacks there and waits for genuinely asynchronous callbacks without installing a synchronization context or blocking ThreadPool workers. Completed ValueTasks are consumed exactly once. Its coalesced bounded signal, snapshot/apply gate and supervised completion remain; application exceptions still fail-stop. With RaftStorageAdapter, a started node uses one log I/O thread plus one application thread.

Startup streams retained records in bounded memory, checking continuity, index/term/kind/length/CRC, configuration indexes and the durable frontier. The frontier covers previously promised durable entries, potentially beyond commitIndex. Promised/interior corruption is fatal; only an unpromised incomplete tail beyond that frontier is discardable. A Commit exception may follow a flush: poison the backend and fail-stop, then recover to determine the result.

## Journaled membership and owned topology

CoreTopology shares rarely changed options, configuration, membership projection and transfer-failure metadata behind one immutable reference. Ordinary reducer steps copy the hot state without copying extra membership fields. A topology update creates another owned value; it never introduces live storage, DNS, sockets, locks or callbacks into Core.

Wire3 uses service log entries for one durable logical operation. The coordinator starts only after intent commitment, advances learner/catch-up/promotion or transfer/removal, and retains a local reservation for the first uncommitted service record. Correlated store completions publish the committed projection and pending frontier; suffix rollback releases replaced reservations. The previous configuration/current-term gate and latest-in-log voter quorum still control all role changes. API wait cancellation affects the waiter; explicit Add cancellation is another logged transition with cleanup.

Raft8-v4 uses one primary table with ToIndex prefixes1=metadata/RF4M,2=log,3=config,4=sparse service index,5=terminal results. Ordinary writes retain the small materialized directory/operation by reference and do not scan result history. Indexed point reads and128-row snapshot pages bound materialization. Old readers are fenced out by format guards.

Runtime publishes committed PeerDescriptor routes without node restart. Transport exchanges bounded trusted hints at connection/revision changes; session revision caching keeps repeated discovery off ordinary sends. DNS and connection retirement run outside the consensus owner. Hints never alter voter configuration or commit. mTLS authorizes the certificate/ID before introducing unknown senders; the host owns PKI.

V4 image wrapping streams terminal history alongside the application and checked directory/active state at the same applied fence. A prepared image stages history by immutable image ID. The durable install intent spans application generation activation and log publication; completing it switches the history pointer atomically with log/frontier/snapshot. Recovery repeats preparation safely. Bounded maintenance collects obsolete staging and avoids an unchanged commit when nothing is collectible.

## Atomic application

`DBreezeStateMachine` reserves all declared business tables plus checkpoint before reads, checks contiguous application, applies commands and commits effects plus CRC `(index,term)` together. No-op/config records advance checkpoint without invoking the handler. `LastApplied` publication follows Commit. Startup validates the application checkpoint against the durable log/image. Every application/storage execution failure is fatal regardless of exception type.

External payments/emails require an application outbox/deduplication protocol; a Raft acknowledgment cannot make those external effects atomic. Canceled commands have unknown outcomes. Stable request IDs and transactional deduplication belong to the application.

## Snapshots

DBreeze read transactions are not assumed to provide MVCC. `DBreezeGenerationStateMachine` owns a separate application root and a short gate for live reads/applies. Snapshot creation seeds an independent closed generation from the previous immutable image, replays committed records through a pinned `(applied index,term,config)` fence, then archives with bounded buffers. The handler therefore executes again during replay. This consumes extra disk/CPU without holding the live gate for the build. The first image needs the uncompacted prefix; subsequent images replay the delta.

An exclusive repository owns SHA-256 images/descriptors, incoming data/progress and private builds. Retention keeps at least two recent images plus current/transfer/prepared pins, pausing admission during collection. Archive/generation paths reject traversal, reparse points and alternate streams; file count/unpacked bytes are bounded.

```mermaid
sequenceDiagram
    participant Leader
    participant Receiver
    participant Log
    participant App
    Leader->>Receiver: authorized term/metadata + offset/CRC chunk
    Receiver->>Receiver: write/flush chunk; persist progress
    Receiver-->>Leader: durable next offset
    Receiver->>Receiver: SHA-256 verification / closed generation
    Receiver->>Log: durable install intent
    Receiver->>App: switch checked generation pointer at batch boundary
    App-->>Receiver: application index/term active
    Receiver->>Log: atomic image/frontier/config/commit; clear intent
    Receiver-->>Leader: installed
```

Authorization/term adoption precedes staging. Chunk flushing/hash/extraction/preparation run outside the owner; an unverified image never gets an install intent. During activation, the core processes higher terms/votes but suppresses campaigning/normal appends. Existing suffix survives only when its boundary term matches the image.

Startup finishes a durable intent before elections, validates the published image even with an existing app generation, and restores an application behind compaction. A removed node recovers files while remaining Removed. Process-kill tests cover progress/image publication, intent, generation pointer and final log publication: recovery yields the prior complete app or a recoverable new one.

## Versions

V1 `raft8/meta`/`raft8/log` and wire 1 remain compatibility paths. They cannot represent the integrity frontier, configuration entry kinds, snapshots or new RPCs. V2 is explicit; mixed wire versions fail closed. `RaftStorageMigration.CopyV1ToV2` exclusively opens/copies stopped data, preserves business tables and writes checked v2 records to a fresh destination. See the [runbook](OPERATIONS.md) for deployment/checkpoint handling. Historical pre-.NET-8 formats need application-specific migration.

Published log-cache entries and metadata share a short publication lock separate from the serialized storage/flush lock. Cache-only readers never acquire the storage lock or open a transaction. They may read the previous durable prefix during a later write; future entries still wait for the worker. Publish state/frontier/cache invalidation/new entries together after Commit, and publish poisoning/disposal under the same cache lock. Lock order is storage then cache, never the reverse. Count/byte limits are unchanged.

Proposal collection runs as an asynchronous signal/deadline worker outside the consensus owner; it reads immutable published status and bounded queue metadata only. The owner drains the one payload channel, applies current leadership/backpressure checks and executes the reducer. It does not wait for collection timers. See specification020 for the adaptive small/bulk windows and ownership invariants.

One reusable timer per node wakes the bounded metadata channel. Its callback never accesses the reducer or command bytes; the worker rechecks the current queue/deadline, so a stale callback cannot force an early group. Deadline evaluation and ready reservation use the same metadata gate. The reservation lasts through the complete owner-side drain, avoiding duplicate notifications from an obsolete wave. Ordinary deadline expiry no longer creates/cancels a linked CTS or throws an exception. Collection windows and all admission/batch limits are unchanged.

Proposal admission takes an immediately available semaphore slot without creating a linked CTS. Only the bounded asynchronous wait path links caller cancellation to node shutdown. Both tokens are checked before admission and under the queue gate before ownership transfers; failure releases the slot. Once accepted, caller cancellation only stops waiting for the result, preserving the existing uncertain-outcome contract.

After durable writes, the storage adapter reads DBreeze's cached UncommittedBytes frontier through ILogStoreProgress instead of constructing recovery/membership DTOs. The getter takes the storage gate and opens no transaction. Full recovery and compatibility fallbacks retain their existing behavior; frontier publication still follows Commit.

Wire2 replication bytes are independent from application bytes: MaxReplicationBytes auto1MiB+2688B, MaxAppendBytes256KiB. The reducer/codec/replication queries/frame reservation use the former; application/snapshot replay queries retain the latter. Existing per-peer flight bytes still bound the pipeline. Limits must be compatible across peers; see the frame-limit upgrade procedure. No encoding/storage field changes.

The collection worker can observe other admitted proposals awaiting quorum or application, including uncommitted work; it never reads reducer state. Bulk auto min(4×small,one heartbeat,1s), normally8ms, shortens when no other admitted work remains. Follower aggregation handles only already-queued plain contiguous fresh same-term frames, has no gathering delay, and releases individual correlated replies after one bounded durable write. Specifications021,024,025 record regression evidence and bounds.

CoalesceCommitNotifications defaults true for wire2 with modern storage. Ordinary idle commit frontiers piggyback on the next data packet or scheduled heartbeat; follower apply can lag by that logical heartbeat plus I/O/network/owner delay. Term/vote/log/commit persistence and quorum-plus-leader-application client completion are unchanged. Read contexts, new configuration, transfer, snapshot progress and retirement retain immediate/control send paths. False restores the prior attempt to send an extra idle commit RPC; a peer with an existing flight can still await later traffic. Wire1/legacy storage retain prior behavior. See specification027 and the majority-only leader-death regressions.
