# .NET 8 resurrection specification

Historical initial scope/evidence. Superseded by [006 autonomous enhancement](006-autonomous-enhancement.md) and specifications 007–012. Current supported features and commands are in [the solution README](../../../RaftNet8/README.md); the earlier exclusions and phase approval gates below no longer describe the current implementation.

Status: implemented and locally validated on 2026-11-12. See [the evidence](../okf/validation.md) and [the detailed protocol contract](002-protocol-contract.md).

## Outcome

An independently buildable .NET 8 Raft library, using DBreeze 1.142.2026.1016-beta, with deterministic consensus tests, real persistence and TCP tests, bounded concurrency and caches, benchmarks, and an interactive human demo. Legacy projects and pre-existing modifications remain artifacts. Copy legacy source into the new solution as an excluded reference baseline; replace unsafe internals rather than preserving their wire/API assumptions.

## Supported protocol

Implement fixed-membership, non-Byzantine Raft: randomized elections, persistent terms/votes, log freshness voting, AppendEntries consistency, conflict hints, a current-term election no-op, current-term quorum commits, ordered application, leader redirection information, a quorum-backed read barrier, and leader check-quorum. Membership and node/group identity are persisted to prevent accidental reconfiguration of a data directory.

Log compaction, InstallSnapshot, dynamic membership, pre-vote, automatic client deduplication, authentication, and legacy wire/storage compatibility are outside this resurrection's initial API. Local application reads alone are not linearizable. Fixed membership and an uncompacted disk log must be clear in the README and usage skill. These are explicit supported-scope boundaries, not claims that all extensions in the Raft paper are implemented.

## Architecture and resource bounds

Historical baseline: Phase 1 supersedes the core/I/O architecture below with a pure immutable reducer and effect driver; see [004](004-phase1-core-separation.md) and [005](005-phase1-review.md). The fixed-membership protocol and stored bytes remain compatible.

- Deterministic, synchronous consensus core, advanced by logical ticks and RPCs. One asynchronous node loop owns it.
- Short synchronous DBreeze transactions; cache durable metadata and bounded log entries. Atomic conflict-suffix replacement; monotonically ascending writes and bounded range reads. No full-log load at startup.
- Separate serialized application worker, checkpoint supplied by the application. Replay from the application's checkpoint through durable commit index after restart. Fail closed on storage/application errors.
- Bounded proposal admission and event queue; batch proposal writes and follower replication. At most one current AppendEntries RPC per peer. Correlation IDs prevent delayed replies from regressing replication progress.
- TCP transport uses reusable per-peer connections, bounded binary frames, pooled buffers, cancellation, and explicit timeout handling. Restrict it to trusted networks; it does not authenticate member IDs.

## Acceptance evidence

1. Release solution builds for net8.0 with nullable analysis and warnings as errors.
2. Deterministic tests exercise one/three/five nodes; split votes; duplicate votes; stale and higher terms; log freshness; Figure 8/current-term commit restriction; committed-prefix protection; matching prefixes with extra suffixes; reordered replies; partitions/rejoins; restarts; and seeded fault schedules with safety invariants.
3. DBreeze disk tests verify durable votes, term-only updates, atomic suffix replacement, bounded cache behavior, identity checks, and replay. No unchanged heartbeat writes.
4. Async node/TCP tests verify client completion after apply, backpressure/cancellation, fail-closed application failure, shutdown, actual socket replication and failover, and malformed-frame rejection.
5. Concurrent-load tests verify ordered, identical state machines and bounded progress under lagging followers.
6. Human-test folder contains its own `.sln`, interactive commands, and a scripted failover/rejoin scenario. Run that scenario and record its result.
7. BenchmarkDotNet covers batches, disk versus memory storage, replication/catch-up, and allocations. Run a smoke benchmark and record scope; never present a smoke run as capacity planning.
8. Root guidance, OKF memory, DBreeze reference skill, and `_docs/raft_skill.md` exist and match the delivered API.

All eight acceptance areas are covered by the delivered code, 71 passing tests, a passing human scenario, 26 benchmark smoke cases, warmed benchmark reports, and validated skills/memory. CI is configured for Windows/Linux but has not been run remotely. This release's explicit exclusions above remain unsupported features, not unverified implementations.

## Sources

- [Raft extended paper](https://raft.github.io/raft.pdf), especially Figure 2, §5.2–5.4, §8.
- [DBreeze instructions](https://github.com/hhblaze/DBreeze/blob/master/Documentation/LLM/DBreeze_skill.md), copied to `../skill/DBreeze_skill.md` on 2026-11-12.
- [OKF v0.2 specification](https://github.com/GoogleCloudPlatform/open-knowledge-format/blob/main/SPEC.md), local reference `../okf/OKF-SPEC.reference.txt`.
