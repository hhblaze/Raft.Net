---
type: Storage Contract
title: DBreeze durable log and application generations
description: Checked v2/v3 layouts, prefixed single-table storage, atomic effects/checkpoint and offline copy migration.
status: stable
sources:
  - resource: ../../../RaftNet8/src/Raft/Storage/DBreezeLogStorage.cs
  - resource: ../../../RaftNet8/src/Raft/Storage/DBreezeGenerationStateMachine.cs
  - resource: ../../../RaftNet8/docs/ARCHITECTURE.md
  - resource: ../skill/DBreeze_skill.md
---

# Checked storage ownership and recovery

Recommended new storage is explicit DBreezeLogLayout.UnifiedTableV3: one physical raft8/v3 table with1.ToIndex(metadataKey),2.ToIndex(logIndex),3.ToIndex(configurationIndex). Entries/identity/configuration/snapshots retain v2 checked bodies; one RF3M manifest packs hard state and durable frontier with CRC32C. Unreleased prior v3 prototypes are rejected before cleanup; v2 bytes remain unchanged. Every forward/backward/suffix/clear range remains inside its namespace; full log replacement visits retained keys in bounded memory. Existing DBreezeLogStorage constructors still select v2 private tables `raft8/v2/meta`, `raft8/v2/log`, `raft8/v2/config`. Layout changes require offline migration; malformed/mixed/legacy data is never reinitialized as empty v3. Each directory binds node/group identity; recovered membership is authoritative. Metadata/frontiers and immutable entries are cached with count/byte budgets. Path constructors own engines; supplied engines remain caller-owned unless transferred. RaftStorageAdapter owns its backend and one dedicated background I/O thread with a bounded2-operation channel. Published cache reads use nonblocking TryEnter with no DB I/O; misses/busy caches use the worker. No await inside a transaction; reserve all write tables before reads.

CRC32C entries include length/index/term/kind, header CRC and complete-record CRC. Checked metadata carries durable frontier/byte totals, hard state, configuration, snapshot and install intent. Startup streams/validates retained continuity and configuration indexes in bounded memory. Previously promised durable data can extend past commitIndex: corruption there is fatal. Only an unpromised incomplete tail beyond the frontier can be discarded. A failed/uncertain commit poisons the backend and fail-stops the node, preserving the original exception.

Batch mutations atomically combine append/suffix and hard state/frontier updates. Single-voter append+commit and follower suffix+commit use one DBreeze Commit where possible. Cache publication follows Commit. Unchanged heartbeats/hard state do not write. Ordinary appends/commit-only writes no longer scan the LRU for invalidation; suffix replacement/compaction still invalidate the relevant entries. Term ranges use cached entry/metadata terms, with one lazy transaction for misses. Committing through the tail uses its already-checked total-byte frontier instead of rereading entry headers. Random list/model/actual-store and cache rollback/compaction/reopen tests validate these paths.

# Application and snapshots

User preference recorded2026-11-13: favor one physical DBreeze table with disjoint numeric type/index prefixes, e.g.1.ToIndex(entityId),2.ToIndex(compositeKey). Use ordered byte encodings and bounded prefix/range queries/deletions. Compare flat prefixes to nested roots for new flat schemas; preserve explicit migration/version checks. See the [project supplement](../skill/DBreeze_project_patterns.md); the upstream downloaded skill remains unchanged.

DBreezeStateMachine commits declared business effects and CRC checkpoint `(index,term)` in one transaction and publishes LastApplied afterward. No-op/configuration records advance checkpoint but skip business handler. Use a separate DBreezeGenerationStateMachine application root for snapshots; handlers write only their supplied transaction because independent replay invokes them again. Materialize queries within Read; no lazy rows escape.

Do not assume DBreeze read transactions are MVCC. Snapshot creation builds a closed independent generation seeded from the previous immutable image, replayed to a pinned committed/applied fence. It uses extra disk/CPU while live apply continues. SHA-256 image/descriptor catalog, CRC/offset chunks and flushed resume progress are bounded. Retention keeps newest images plus current/transfer/prepared pins. Generation/archive paths, unpacked size and file counts are checked under exclusive owner locks.

Activation order: verified/prepared image → durable log install intent → atomic checked application generation pointer → atomic log/snapshot/config/frontier/commit publication and intent removal. Startup completes intent before elections and restores an app behind compaction from its checked image. Retain a suffix only on a matching included term. A removed node can recover image files without voting/campaigning. Real process kills cover each intent/generation/publication boundary.

# Compatibility

CopyV2ToV3/CLI --v2-to-v3 physically copies a stopped database into a fresh destination, preserving files and business tables inside the database folder. Separate application/image roots must be copied separately while stopped. A bounded raw-record conversion preserves checked values, bootstrap/current membership, known peers, snapshots/intents; complete verification and a flushed manifest precede removing the incomplete marker. Old readers refuse the target. A previous completed v1-to-v2 target can be migrated again. Use the coordinated stopped-group runbook; never run both copies of one identity or return to stale source after new writes. V2/v3 use unchanged wire2. See [spec017](../sdd/017-unified-log-layout.md).

DBreezeRaftStorage is the explicit v1 compatibility backend; bytes remain pinned, but it lacks v2 integrity/config/snapshot support. CopyV1ToV2 requires stopped source engines and a fresh separate destination. A read-only format preflight requires an existing v1 identity/state before constructing the v1 reader, which otherwise initializes missing markers. Unknown/historical databases are rejected without inserting Raft metadata or creating a target. Valid initialized-empty v1 databases still migrate. Physical business tables are preserved, v2 data/manifest verified, incomplete targets refused and migrated targets sealed against old binaries. Application-specific checkpoints require explicit adaptation; the helper adds a verified term to index-only checkpoints. See the [runbook](../../../RaftNet8/docs/OPERATIONS.md).

Published log-cache entries and metadata share a short publication lock separate from the serialized storage/flush lock. Cache-only readers never acquire the storage lock or open a transaction. They may read the previous durable prefix during a later write; future entries still wait for the worker. Publish state/frontier/cache invalidation/new entries together after Commit, and publish poisoning/disposal under the same cache lock. Lock order is storage then cache, never the reverse. Count/byte limits are unchanged.
