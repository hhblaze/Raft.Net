# Autonomous enhancement delivery

Status: implemented and locally validated: 221 tests, 42,000 seeded scenarios, process recovery/membership, core coverage, human demo, compatibility and 36-case performance comparison. See RaftNet8/docs/VALIDATION.md and benchmarks/results/enhancement-2026-11-12.md for executed evidence and limits. Supersedes every per-phase approval gate in 003–005 and earlier memory. The user explicitly requested taking control, applying reasonable improvements, comparing the previous implementation and iterating without phase questions. **Keep .NET 8.**

## Required result

Complete all ten enhancement goals from the attachment: pure deterministic core; durability/integrity/recovery; pre-vote/check-quorum; transfer; ReadIndex; learners and corrected single-server changes; bounded batching/pipelining/group commit; consistent resumable snapshots; realistic deterministic fault/history tests; measured performance and operational guidance. Optional leases stay off; joint consensus and follower forwarding are optional.

Keep existing public APIs through additive adapters. Preserve explicit v1 readers/fixtures, but introduce v2 where CRC, entry kinds, configuration, snapshot metadata, and new messages require it. Provide copy-based offline migration and refuse unsafe mixed-version activation. Do not silently reinterpret old data or treat local commitIndex as proof that a corrupt durable tail is discardable.

## Implementation order and invariants

1. Versioned DBreeze records: length, index, term, kind and CRC32C; checked hard state and durable frontier in the same transaction; full bounded-memory recovery verification; fail on promised/interior corruption, salvage only unpromised incomplete tail. Bind runtime identity/configuration. Add copy migration and persistence kill points.
2. Atomic durable application checkpoint(index,term) and effects with reusable DBreeze integration; bounded-delay/count/byte proposal group commit and atomic store mutation batches.
3. V2 protocol and election refinements; no persistent receiver changes for pre-vote; recent-leader suppression; transfer catches up a voter, sends TimeoutNow, aborts within timeout. Add explicit ReadIndex contexts/current-term-commit/applied fences.
4. Active-on-append configuration with indexed recovery/rollback. Distinguish voters/learners; one pending change; current-term commit before changes (2015 correction); bounded-lag promotion and post-commit leader removal.
5. Per-peer Probe/Replicate/Snapshot windows and multiplexed transport; bounded inflight and uncommitted bytes. Add TLS/mTLS stream/authentication hooks, structured logs/metrics/traces, immutable validation.
6. Snapshot creation outside apply. Validate DBreeze consistency rather than assuming MVCC. A separate DBreeze replay generation, seeded from the last complete snapshot and replayed through a pinned committed/applied fence, is an acceptable deterministic point-in-time mechanism if live backup semantics cannot establish one. It trades disk/CPU for isolation and must remain memory bounded. Stage, verify, resume/throttle chunks, and journal atomic activation before compaction.
7. Single-seed simulator with delayed/asymmetric networking, skew, pending/flushed disk states and torn writes; every-step invariants, history linearizability, randomized properties, thousands-of-seed runner/nightly CI, actual 3/5-node OS-kill/transfer/config tests and coverage.
8. Benchmark 1/3/5-node workloads, small/large payloads, batching/pipelining toggles, durable application writes and individual p50/p99. Compare against baseline 8b991bd and archived code where meaningful; label weaker historical durability. Iterate on failures and measured bottlenecks; update demo, README, runbooks, usage skill, and OKF.

## Validation discipline

Run focused safety tests as behavior changes, then full tests/integration/stress/benchmarks. Record failing-before evidence and authority for safety changes. Preserve the 95-test Phase 1 baseline while updating fixtures explicitly for selected format versions. Never claim completion from green narrow checks; audit every requirement against current code and results.

No additional approval is required for these implementation steps. Do not commit, push, publish, or change user deployment data without instruction.
