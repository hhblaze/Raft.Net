# Raft.Net working guidance

## Scope

- Develop the supported implementation under `RaftNet8/`, targeting **net8.0 only**.
- The user reaffirmed .NET 8 and then explicitly revoked per-phase approval gates: take ownership of all reasonable enhancements, iterate implementation/tests/benchmarks, and do not ask for phase approvals. Follow `_docs/llm/sdd/006-autonomous-enhancement.md`. Older approval-gate notes are historical.
- `Raft/`, `RaftStandard/`, `Deployment/`, and `Docu/` are historical artifacts. Preserve existing user changes there. `RaftNet8/legacy-source/` is the copied source baseline, excluded from compilation.
- Pin DBreeze to **1.142.2026.1016-beta** (the published NuGet version of 1.142). Do not silently substitute an older stable release.
- Read `_docs/llm/sdd/001-resurrection.md`, `_docs/llm/okf/index.md`, and `_docs/llm/skill/DBreeze_skill.md` before changing consensus or storage. Update specifications and memory with decisions, evidence, and remaining limitations.

## Correctness

- Use the [Raft paper, Figure 2 and sections 5–8](https://raft.github.io/raft.pdf) as the authority. Persist term and vote before responding; persist log entries before acknowledging them.
- A quorum is `activeConfiguration.Voters.Count / 2 + 1`, counting self only if a voter. Configuration is the latest entry in the log, including uncommitted entries. Permit one server/role change at a time, only after the prior config and a current-term entry commit (the 2015 single-server correction). Restore the previous configuration on suffix rollback. Provisioned `KnownPeers` are transport identities, never voters by themselves.
- Keep `Raft.Core` a pure reducer over immutable owned state and explicit events. No storage interfaces, clocks, random providers, tasks, locks, threads, or executable I/O callbacks belong there. Correlated read/write/timeout completions release dependent effects. Do not preload the log to simulate purity.
- Commit by quorum counting only entries in the leader's current term. Append a no-op on election. Reject stale responses and never replace a committed entry.
- Apply committed entries in order. Acknowledgment to clients follows local application. Application checkpoints and effects must be atomic or replay-safe. Cancellation/timeouts do not imply that a submitted command was not committed.
- Validate requests before queue admission. Never classify an execution/storage exception as invalid input based on its exception type; uncertain persistence failures must fail-stop and preserve the original exception for the caller and Completion.
- Keep deterministic fault tests for elections, restarts, partitions, message duplication/reordering, conflict repair, and application failures. New safety fixes need a regression reproducing the failure.

## DBreeze and performance

- The user accepted commit `d44147effdb20271854dd758cc6f9073543a554d` as the new performance baseline, including Hot Add/Remove, on 2026-11-14. Use fresh paired comparisons against it for subsequent work; e06/R18/R19 and older competitors are historical unless requested. See `RaftNet8/benchmarks/baselines/hot-membership-2026-11-14.json`. Preserve the historical measurements and their FAIL results; user acceptance does not imply every old numeric gate passed.
- Follow the downloaded [DBreeze skill](_docs/llm/skill/DBreeze_skill.md). Reuse engines, use short synchronous transactions, and never await inside a transaction. Reserve all write tables before dependent reads.
- Follow the [project DBreeze preferences](_docs/llm/skill/DBreeze_project_patterns.md): when appropriate, prefer one physical table with numeric ToIndex type/index prefixes and bounded composite-key ranges; compare with nested roots rather than defaulting to them.
- Cache hard state and the log tail with bounded memory. Batch ascending log writes and replication. Use key ranges for catch-up and suffix deletion. Heartbeats must not write unchanged state.
- Keep proposal collection deadlines outside the consensus owner. Its signal worker may read immutable published status/queue metadata, never reducer state; only the owner drains command bytes and assigns indices. Keep storage→cache lock order, with no I/O/storage lock in published-cache reads.
- Bound client admission, replication batches, frames, and log-cache bytes. Keep a single owner of consensus state; perform network and application I/O outside that owner.
- Benchmark actual behavior and record runtime, storage mode, parameters, and allocation data. Do not describe unmeasured improvements as benchmark results.
- Keep explicit v1 compatibility fixtures green. Required new wire/storage versions need migration tools, rejection of unsafe mixed deployments, and documented upgrade procedures.
- Exception authorized by the user for spec029: fresh wire3/raft8-v4 groups need no old-data converter because no deployed clusters exist. Preserve wire1/2 and storage v1/v2/v3 fixtures/modes, explicitly reject mixed versions/old directories, and document that boundary.
- Wire3 hot membership uses one durable logical operation, caller Guid idempotence and a replicated PeerDescriptor directory. Caller wait cancellation never cancels the operation. Explicit Add cancellation commits learner cleanup before freeing the lane; too-late errors include current Status. Persisted leader removal transfers first and never forcibly removes after confirmed transfer failure. Read the [contract](RaftNet8/docs/HOT-MEMBERSHIP.md) and [spec029](_docs/llm/sdd/029-hot-membership.md) before changing it.
- Wire3 joining processes keep identical original Members/Learners/InitialPeers and use JoiningNode=true to start outside that configuration. Never inject self into bootstrap Learners; only a logged entry assigns its role. Historical prefix snapshots must preserve the original configuration.

## Validation and delivery

- User-provided experiment root: `D:\Temp\RaftNet8` (2026-11-14). Keep new large databases, raw traces, process logs and benchmark scratch data there; repository sources and reviewable reports remain here. Use checked owned subdirectories for cleanup, never delete other experiment/user data. Set `RAFT_EXPERIMENT_ROOT` for new client tests and `RAFT_BENCHMARK_ROOT` for benchmark storage.
- The external client/message contract is [spec032](_docs/llm/sdd/032-cluster-clients-and-entities.md). The subscriber owns its durable cursor in a local effects+cursor transaction; no durable consumer ClientId or cluster ACK offsets. Standard retains all history, HighSpeedCache retains its latest durable payload; both are snapshotted application state. Preserve the accepted embedded-core baseline while measuring the optional HTTP service separately.
- Dashboard/native security follow [spec033](_docs/llm/sdd/033-cluster-dashboard.md) and [the runbook](RaftNet8/docs/CLUSTER-DASHBOARD.md). New message hosts use cluster-v2 with an outer application-version peer preamble, preserving core wire3/v4. Never mix applications or migrate live/in place. Token secrets/host keys never enter Raft state; permissions and quotas apply in log order. Browser service credentials stay backend-only. Measure overhead against fresh3fafaa2 HTTP workload; do not substitute old embedded/competitor numbers.

- From `RaftNet8/`: `dotnet restore Raft.Net8.sln`, `dotnet build Raft.Net8.sln -c Release --no-restore`, `dotnet test Raft.Net8.sln -c Release --no-build`.
- Run the demo's automated scenario and BenchmarkDotNet smoke run after changes affecting them. See the solution README for exact commands.
- Keep `_docs/raft_skill.md` aligned with the public API and demonstrated examples. State protocol/storage compatibility and operational limits explicitly.
- Do not commit, push, publish packages, or alter user data unless requested.
