# V2 storage and atomic application

Working implementation under autonomous specification 006. New code is not the final enhancement release; elections/transport/configuration/snapshot runtime work remains.

## Supported paths

- DBreezeRaftStorage retains the explicit v1 compatibility path and its bytes. Configuration entries are rejected.
- DBreezeLogStorage is the new checked synchronous backend; RaftStorageAdapter supplies its bounded async I/O channel. This backend must become the recommended demo/new deployment path when the runtime extensions are complete.
- IExtendedRaftStorage exposes recovered configuration, identity, log boundaries, and atomic mutations without placing I/O in the core.

## Record and recovery contract

V2 tables: raft8/v2/meta, raft8/v2/log, raft8/v2/config. Every value has magic/version, declared total length, CRC32C, and a body. Entry body includes index (also checked against its key), term, kind, payload length, a header CRC32C, and payload. Header-only term queries validate header integrity. Full entry reads and startup verify the full checksum.

The frontier contains first/last index, last term, and total/committed encoded byte totals. Hard state, entries, configuration index, and frontier share one DBreeze transaction. A successful operation requests OS disk flush through DBreeze Commit before completion is returned. Cache publication follows Commit.

Recovery scans retained records in bounded memory, validates contiguous indices, nondecreasing terms, commit/frontier/snapshot agreement, checksums, configuration index completeness, and byte totals. Any bad/missing promised record fails startup, even above local commitIndex. Only rows beyond the atomically committed durable frontier can be discarded: they were never acknowledged. Local commitIndex alone is not a safe truncation boundary.

Configuration is active on append and indexed in the same transaction; conflicting suffix replacement rolls it back. V2 recovery owns configuration authority. Runtime identity is bound to recovered group/node; v1 also binds its fixed voters.

Local snapshot publication validates committed index/term/configuration and retains a configured tail. Download/install/atomic application-generation activation is still unfinished and must not be advertised as implemented.

## Application

DBreezeStateMachine reserves the complete application write set and commits deterministic effects plus a CRC-protected (index,term) checkpoint in the same transaction. No-op/configuration entries advance checkpoints without business effects. Errors require recovery; an after-commit error cannot cause replay of an already checkpointed effect. Node construction/application checks checkpoint term as well as index.

The demo now writes lastAppliedTerm alongside values, request IDs, and lastApplied in one transaction. Offline CompleteIndexOnlyCheckpoint upgrades known legacy checkpoints against a committed log boundary; the copy migrator upgrades the built-in demo checkpoint.

## Migration

CopyV1ToV2 requires the source application offline and a nonexisting destination outside the source. It validates identity/exclusive availability, rejects reparse points/external or memory tables, read-locks source files, copies all physical files (including application/nested data) and records hashes, converts the log in bounded batches, and reopens v2 for validation.

Incomplete file/database markers prohibit opening a partial target. Completion atomically seals the old v1 identity in the target and marks v2 ready, then removes only the tool's own incomplete marker. The original folder remains available. This is an offline coordinated upgrade, not a mixed-version rolling activation. Historical 2018 databases with missing durable election state are not automatically reconstructible by this v1-to-v2 tool.

## Group commit and liveness

ProposalBatchDelay bounds coalescing delay (default 1 ms), combined with existing count/byte limits. Single-voter append+commit and follower suffix+commit are one atomic mutation batch in v2. Legacy stores execute the same ordered mutation effects before completion, with their original durability semantics.

Runtime rejects RpcTimeout values beyond the .NET timer range. Unexpected worker exit faults the node; detached RPC task faults are observed. Shutdown releases proposals already collected into a delayed batch.

## Evidence so far

- 13 integrity/storage tests: standard CRC vectors, hardware/portable agreement, every-byte corruption, key swap, missing promised rows, safe unpromised tail, atomic commit, configuration rollback, snapshot metadata retention, identity, migration-required guard.
- 3 migration tests: physical application data retained, source retained, old target binary rejected, cancellation leaves an unopenable incomplete target, unsafe paths rejected.
- 10 separate-process kill cases across every instrumented Raft/application persistence boundary (plus the existing two).
- Atomic application unit tests for index+term, no-ops/configuration, before/after-commit failure/recovery, incorrect checkpoint term.
- Deadline/worker-exit regressions failed before the correction (supervision-before.trx).
- Latest full suite at foundation checkpoint: 128 passing tests, zero skipped. Raw evidence: RaftNet8/.artifacts/autonomous/test-results/autonomous-foundation.trx.

Remaining full objective is in 006; do not stop for phase approval or claim completion.
# Migration format preflight regression

Final review reproduced acceptance of an unrelated DBreeze database as empty v1: probing through the ordinary v1 constructor initialized its missing identity in the source. `migration-unknown-before.trx` failed before the correction. Copy migration now reads/validates the existing v1 marker/state before constructing the v1 reader. Unknown/historical sources are rejected with no Raft identity/state inserted and no target created; an explicitly initialized empty v1 store still migrates. No consensus/core changes are involved. The final suite includes both regressions.
