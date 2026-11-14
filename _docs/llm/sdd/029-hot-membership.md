# Journaled hot membership, wire3 and raft8-v4

Status: implemented and validated2026-11-14; authorized by the user on the same date. User approved the plan and requested implementation. No deployed clusters exist; no format converter is required for this feature. Keep net8.0/DBreeze1.142.2026.1016-beta, old codecs/fixtures, pure Core, persistence-before-reply and atomic application checkpoints.

## Accepted behavior

Replicate PeerDescriptor(ID, host, port) and a single active membership operation. KnownPeers is bootstrap only. AddNodeAsync(peer,Guid,token) owns registration, learner, acknowledged catch-up and promotion; operations continue after caller timeout/cancel, process restart and elections. Another ID receives Busy(activeID), matching repeated ID/content joins the original, conflicting repeated content is rejected. Strong status lookup is by operation ID. Results/history are indexed on disk with bounded runtime memory.

RemoveNodeAsync(id,Guid,token) transfers leadership first when removing self, choosing highest acknowledged match then ordinal ID among eligible active voters. The new leader finishes the persisted intent after current-term readiness. A failed transfer records failure without forced deletion. Explicit cancellation before final promotion removes only the learner created by that operation and completes registry cleanup before releasing the global lane; cancellation after final configuration append is too late. Cancellation never erases a committed entry. Legacy membership/standalone transfer APIs cannot bypass an active operation in the new mode.

Use explicit wire3 and UnifiedTableV4, one primary DBreeze table under numeric prefixes. Add dedicated service log entries, owned immutable management state, indexed service records, and terminal results. Publish registry/operation state only after commit; reserve an uncommitted intent so two local requests cannot race before commit. Restore reservation/state after suffix rollback. All I/O stays in runtime/store/transport. No whole-log preload.

Snapshots include directory, active operation and result history at the same application/log fence. Catch-up of a peer unaware of a new leader uses bounded trusted directory exchange; directory repair never changes voters. mTLS retains chain/name/ID checks, host-owned trust roots and keys. Plain TCP remains trusted-network only. Runtime routes, connections and frame budgets stay bounded and update without existing-node restarts.

Joining processes retain the exact original Members/Learners/InitialPeers and use wire3-only JoiningNode=true. The flag permits self outside the canonical bootstrap configuration and never confers votes. Until a logged learner entry arrives, Role=Removed means outside membership; actual retirement has a directory IsRetired tombstone. Injecting a local learner into C0 is incorrect: snapshots of an earlier log prefix must preserve the original configuration. Recovered/logged membership is authoritative after restart, even if the flag remains set.

Cancellation after promotion append or a non-cancellable/terminal operation throws MembershipCancellationTooLateException with current Status. Repeating a completed cancellation returns Cancelled. Custom application snapshot callbacks receive the inner application's own length and SHA-256 digest, not the outer service-container digest.

## Evidence and delivery

Frozen baseline HEAD e06c4f9: .artifacts/hot-membership/baseline-source-e06c4f9.zip and baseline-runtime/comparison/. Compare actual durable3/5-node64B/16KiB throughput/p99/allocations and during Add; preserve raw rounds and failures. Do not relabel old R18 data as a fresh measurement.

Required: red/green codec/store/core and actual TCP/disk/process tests for new IDs, concurrent operations, elections at every phase, cancel cleanup/too-late, self-removal and failure, isolated old leader, snapshots/compaction and stale-directory repair, TLS/malformed bounds/fail-stop. Retain explicit legacy fixtures; full build/test, HumanTest, BenchmarkDotNet smoke and updated HTML books/runbook/skill/OKF.

## Delivered validation and limits

402/402 tests, zero skips; Release solution build zero warnings/errors. HumanTest scenario,29 BDN Dry cases and the books'20 checks/13 actual process starts passed. Both HTML books now have27 chapters and14 compiled source regions; offline desktop/mobile/print/noJS/widget/source/default/link QA passed. New code remains net8.0 with pinned DBreeze. Legacy source trees/fixtures and old data are preserved. No commit/push/publication.

The fresh-store directory invariant covers voters and learners with live descriptors. It must not be reapplied against original InitialPeers when recovery replaces Members/Learners with the latest logged configuration. A failed overbroad validation experiment is retained in validated-full.trx; final-validated-full.trx is the passing delivery evidence. Snapshot integration compacts all eligible leaders before joining a replica and checks a valid image fence, allowing different leader-local storage IDs.

The [standalone HTML comparison](../../../RaftNet8/benchmarks/results/hot-membership-2026-11-14.html) has52 rounds/425984 samples and raw CSV/summary. Throughput is approximately retained (−0.59%…+2.89%); small-command p99 worsened14.73% on3 voters and30.51% on5 voters. This is an explicit measured limitation, not an all-metrics performance win. All12 Add workloads completed. Exact measured binaries/source archive and the subsequent fresh-store-only validation change are disclosed separately; no measured hot-path code was changed afterward.

Operational limits:1024 historical directory identities including cancelled/retired tombstones, no identity reuse; indexed result history has no TTL; quorum/MinimumVoters and one current-term/configuration gate remain mandatory. Host launches processes, routes clients, supplies PKI and application durability. Wire3/v4 is for fresh groups; no converter or mixed-version rollout is provided. See the [full contract](../../../RaftNet8/docs/HOT-MEMBERSHIP.md), runbook and LLM skill.
