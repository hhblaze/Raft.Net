# Unified checked DBreeze log layout

Implemented during goal016; final performance/delivery validation is still in progress. Target net8.0, DBreeze1.142.2026.1016-beta. No deployed/user database is changed by this work.

## Persistent representation

DBreezeLogLayout.SeparateTablesV2 remains the default of existing constructors and CreateInMemory overloads. The explicit UnifiedTableV3 overload is recommended for new databases. It uses one physical table, raft8/v3, with DBreeze ordered composite keys:

| Prefix | Key components | Value |
| --- | --- | --- |
| 1.ToIndex(...) | metadata string key | RF3M packed hard state/frontier; existing checked identity/snapshot/configuration/install-intent records |
| 2.ToIndex(...) | ascending long log index | existing CRC-protected complete entry/header/payload |
| 3.ToIndex(...) | ascending long configuration index | existing checked configuration-index record |

Use DBreeze converters, not platform-endian numeric keys. Namespace0 and4+ are reserved. Every forward/backward range and clear stays within its prefix; partial header reads must not materialize payloads. Full snapshot-log replacement deletes the log namespace in bounded memory, with work proportional to retained entries; it must preserve metadata and independent configuration handling. Cache count/byte bounds remain unchanged.

One physical table reduces DBreeze's multi-table commit work. No durable promise is removed: term/vote and the complete appended log/frontier are atomic; CRC/header lengths, integrity recovery, configuration rules, commit/application ordering and disk flush remain. The common benchmark business transaction is unchanged for every participant.

## Ownership and compatibility

RaftStorageAdapter owns one background thread for synchronous DBreeze I/O, a bounded2-operation channel and its backend. It never adjusts the global ThreadPool. Published immutable cache reads may complete inline only after a nonblocking TryEnter; a busy cache or any missing data falls back to the worker. No transaction/await is performed in the cache path. Reads must honor cancellation/disposal/poisoning. Core remains a pure reducer over events/effects.

V3 initialization atomically writes incompatible state guards into v1/v2 namespaces. Readers of those formats refuse the database. V3 refuses legacy identities/data and malformed/missing v3 identity; no automatic layout conversion or reinterpretation as an empty group. Wire2 is unchanged: physical storage layouts are local, not a new RPC version. Existing v1 golden bytes and API signatures remain supported explicitly.

An initial nested-root v3 prototype existed only in ignored benchmark/test directories. It was never published or used for user data. Its binaries/source and exploratory measurements are retained under .artifacts/winner/nested-v3-*; the final flat-prefixed reader rejects that experimental representation. Final reports must identify the prefixed schema and binary hashes, not reuse nested-prototype measurements.

## Offline migration

CopyV2ToV3 and Raft.Migrate --v2-to-v3 require a stopped source and a fresh destination outside it. Validate the source's existing format/identity without initializing it; hold read-only file handles during physical copy; refuse reparse points/alternative tables outside the source. Validate/recover records on the copy, convert checked values in bounded ascending batches, verify every retained v3 record/configuration/frontier, seal old destination readers and flush a manifest before removing the incomplete marker. Failed/canceled destinations remain unusable. Do not delete the source or infer an application schema.

Physical files/business tables inside the database folder are copied. Separate application roots and snapshot repositories must be copied/backed up separately while the whole node/group is stopped. Stored identity, bootstrap/current configuration, known peers, snapshots/intents and checked application data are preserved. A completed v1-to-v2 copy can be migrated again to v3. Supported upgrade runbook uses a coordinated stopped-group switch; never run both copies of one node identity or roll back to a stale source after accepting new writes.

## Evidence

Property oracle32 seeds x160 atomic operations on both layouts; prefix forward/backward/suffix/clear isolation; corruption and all write boundaries; migration business/configuration/compaction/file preservation and interrupted conversion; actual killed-process log and snapshot/generation boundaries; 3/5-process leader death, rolling restart, learner promotion, transfer/removal/recovery. Full suite passed270/270 on the prefixed implementation before final documentary adjustments.

The user's explicit preference for flat ToIndex type prefixes is recorded in ../skill/DBreeze_project_patterns.md. The upstream DBreeze skill is retained unchanged. Isolated layout probes compare physical transaction work only; use fresh whole-stack measurements to establish throughput results.

## Packed metadata revision (unreleased R3)

One RF3M manifest at1.ToIndex("manifest") now contains term, commit, vote length/vote and the five frontier Int64 fields. Its envelope includes magic, exact record length and CRC32C over all content/header; decoded vote/term/commit validation and recovered log/snapshot/byte-total checks remain unchanged. One DBreeze key update replaces the previous two checked state/frontier updates without weakening the atomic transaction or publication boundary. Entry, identity, configuration, snapshot and intent record formats remain unchanged. SeparateTablesV2 retains its exact old state/frontier bytes.

Older experimental flat/nested V3 directories are rejected before any cleanup. The state key contains a guard that older V3 decoders reject; no separate frontier is written. These prototypes were used solely in ignored owned test/benchmark directories and never shipped. New user data and CopyV2ToV3 use the packed representation. Corruption tests flip every byte, valid-CRC malformed lengths/hard state are rejected, and a prior-prototype fixture proves rejection leaves its unpromised tail untouched. Performance improvement still requires whole-stack evidence.
