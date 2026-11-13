# Phase 1 review: .NET 8 and core/I/O separation

Date: 2026-11-12. Baseline: 8b991bd. Scope: [004](004-phase1-core-separation.md), following the user's explicit decision to retain .NET 8.

## Summary

- Kept net8.0, existing SDK policy, assembly/public API, and exact dependency versions.
- Centralized package versions; 53 resolved package/hash entries remain unchanged.
- Extracted a pure reducer over immutable state/events and explicit effects.
- Moved bounded log reads, durable writes, timeout sampling, networking, and application outside the core.
- Added correlated storage/timeout completions and explicit durable-before-dependent-action ordering.
- Retained bounded proposal batching, single-inflight replication, and separate ordered application.
- Added asynchronous log-store construction, injectable clock/random, and legacy interface shims.
- Introduced pooled IBufferWriter serialization with byte-for-byte v1 compatibility fixtures.
- Fixed the demonstrated persistence ArgumentException fail-stop defect in a separate regression/fix step.
- Passed the original 71 cases and 24 new cases: **95 tests**, no failures/skips.
- Passed build, TCP/disk scenario, extracted README application, binary/API compatibility checks, and 26 benchmark smoke cases.
- No protocol/storage version changed, historical projects changed, commit created, or package published.

## File-level change list and code

The linked files contain the full new code. Existing-file diffs are available with git diff; a complete review patch is retained locally under RaftNet8/.artifacts/phase1/review.patch. That patch is relative to HEAD and also includes the still-uncommitted Phase 0 report/memory.

| Group | Files |
| --- | --- |
| Pure core | New [CoreModel.cs](../../../RaftNet8/src/Raft/Core/CoreModel.cs); rewritten [RaftCore.cs](../../../RaftNet8/src/Raft/Core/RaftCore.cs) |
| Runtime/services | New [RaftDriver.cs](../../../RaftNet8/src/Raft/Runtime/RaftDriver.cs), [SystemServices.cs](../../../RaftNet8/src/Raft/Runtime/SystemServices.cs), [Abstractions.cs](../../../RaftNet8/src/Raft/Abstractions.cs); updated [RaftNode.cs](../../../RaftNet8/src/Raft/RaftNode.cs) |
| Storage boundary | New [ILogStore.cs](../../../RaftNet8/src/Raft/Storage/ILogStore.cs), [RaftStorageAdapter.cs](../../../RaftNet8/src/Raft/Storage/RaftStorageAdapter.cs). DBreezeRaftStorage encoding and database operations are unchanged. |
| API/wire compatibility | Updated [Protocol.cs](../../../RaftNet8/src/Raft/Protocol.cs) and [RaftWireCodec.cs](../../../RaftNet8/src/Raft/Transport/RaftWireCodec.cs); no RPC kinds/version changes |
| Dependencies | New [Directory.Packages.props](../../../RaftNet8/Directory.Packages.props); version attributes moved from library/test/benchmark csproj files; all five package lock files regenerated for central management |
| New tests | [CoreTransitionTests.cs](../../../RaftNet8/tests/Raft.Tests/CoreTransitionTests.cs), [AsyncDriverTests.cs](../../../RaftNet8/tests/Raft.Tests/AsyncDriverTests.cs), [CompatibilityTests.cs](../../../RaftNet8/tests/Raft.Tests/CompatibilityTests.cs) |
| Existing tests/benchmarks | [FailureAndBatchTests.cs](../../../RaftNet8/tests/Raft.Tests/FailureAndBatchTests.cs) adds fail-stop regressions; [ReplicationTests.cs](../../../RaftNet8/tests/Raft.Tests/ReplicationTests.cs) restarts after direct fixture writes; new CoreAlias.cs files route existing test/benchmark harnesses through the inline effect executor |
| Guidance | [README](../../../RaftNet8/README.md), [AGENTS](../../../AGENTS.md), [usage skill](../../raft_skill.md), SDD/OKF memory and validation records |

## Architecture and compatibility

RaftCore.Step takes an immutable RaftState and a CoreEvent and returns a new state plus effects. An immutable agenda represents continuation data; it holds no delegate or live I/O service. Prior state snapshots remain valid for replay. Timeouts are sampled by runtime IRandom and supplied as events. Bounded term/range/replication queries return owned data; the full log is never materialized in core state.

Each read/write/timeout effect has a monotonic correlation ID. A persist completion advances durable term/vote/tail/commit metadata, then releases its continuation. Wrong-kind, duplicate, and stale completions cannot release another pending effect. Storage failure is terminal; the core retains immutable failure information while the runtime preserves and propagates the original exception object.

RaftNode's bounded channel remains the sole runtime owner. RaftDriver executes each event's dependency chain asynchronously outside the reducer. RaftStorageAdapter has a bounded I/O channel and runs synchronous DBreeze calls entirely on a worker, with no await inside a transaction. Once an operation starts, cancellation does not abandon its commit outcome. The existing independent application worker consumes durable commit targets and acknowledges clients only after application.

This phase deliberately preserves serial completion of each event's dependencies. It does not yet overlap consensus transitions during pending disk work, group fsync, or pipeline several AppendEntries per peer. The immutable implementation adds state/continuation allocation costs; this phase claims architectural separation and tested behavior, not a speedup. Phase 6 must measure and optimize those costs.

Public LogEntry remains an owned reference type. Replication reuses existing entry/payload references and encoding adds no per-entry wrappers, but decoding and cache misses still materialize owned entries/commands. Eliminating those allocations requires an additive batch-ownership fast path or a breaking public representation change. Preserve compatibility now; evaluate the additive path and retained-buffer accounting in Phase 6. Bounded term queries currently use existing header reads; conflict-prefix prefetch may read more headers than the old early-exit loop and needs measurement before optimization.

The original four-argument constructor and IRaftTransport/IRaftStateMachine/IRaftBatchStateMachine remain. New fundamental interfaces use default legacy bridges, including explicit interface implementations. CreateAsync supports ILogStore, ITransport, IStateMachine, IClock, and IRandom. Store ownership transfers to the node only after successful construction; transport ownership remains external.

Snapshot metadata/query/publication contracts and later-phase event types are reserved extension points. The v1 adapter returns no snapshot and rejects publication; the core rejects snapshot recovery, membership changes, and transfer. These are explicitly unsupported operations, not partial snapshot/configuration implementations.

## Separate behavior correction: F1 fail-stop

Before the fix, the new regression suite produced **two failures and two passes**: IOException failures stopped the node, while ArgumentException failures returned to the RPC caller but left Completion pending. Both before-write and uncertain-after-write cases reproduced the defect.

The fix validates malformed requests before admission and removes the exception-type catch around execution. Any persistence/application execution failure now reaches the existing node fail-stop path. Tests additionally persist a vote, inject an ArgumentException after commit, halt, reopen disk storage, and verify that a different candidate cannot obtain that vote in the same term.

This is a failure-policy correction, not a new election rule. Authority: durable term/vote before responding and one durable vote per term, Figure 2 / paper §5.2. A storage error with an uncertain outcome cannot justify continuing with potentially stale volatile state. The original exception reaches both the caller and Completion. Invalid requests still leave the node running, as the existing malformed-request tests require.

One original replication test wrote directly into a running test store. It now restarts that fixture before exercising the same assertions, because the new core receives recovered metadata explicitly. No assertion or fault seed was removed.

## Executed checks and reproduction

Windows; .NET SDK 8.0.428, runtime 8.0.34; DBreeze 1.142.2026.1016-beta; BenchmarkDotNet 0.13.12.

| Check | Result |
| --- | --- |
| Locked restore and Release solution build | Passed, zero build warnings/errors |
| Full xUnit suite | **95 passed**, zero failed/skipped; original 71 behavior cases retained |
| Fail-stop regression before correction | 2 failed / 2 passed; red evidence retained separately |
| Pure transition tests | Replay and immutable history, persistence fences, stale/wrong-kind completions, terminal failure, bounded historical reads, owned membership/request lists, explicit future-operation rejection |
| Async driver tests | Held writes suppress election messages/RPC completion; errors stop progress; injected ticker/random and legacy application contract work |
| Wire/storage fixtures | Five captured v1 frame cases through pooled and arbitrary IBufferWriter output; exact identity/hard-state/log bytes unchanged |
| Public API comparison | **272** baseline exported type/member signatures preserved; zero missing/changed |
| Core dependency inspection | **997** compiled core methods plus fields inspected; zero forbidden direct I/O/clock/random/thread/task dependencies |
| Unchanged binary client | Compiled against baseline assembly, then executed against current assembly without recompiling the client; explicit legacy transport/batch interfaces passed |
| README application | Extracted, compiled, executed: Applied index 2; count=1 |
| Human scenario | Actual three-node TCP, separate disk engines, isolation/failover/heal/restart/checkpoints/catch-up: SCENARIO PASS |
| Benchmark smoke | **26/26** cases have measurements/statistics across five exported JSON reports; no benchmark failures |

The unchanged client SHA256 is 2BD210BB23A09EAC28E6C4F7BCA4A29E3749E4C290A841B8C4B6F6A97A314D9B. API/dependency inspections are structural checks, not a proof of semantic equivalence; the behavioral tests and client execution provide additional evidence.

Run from RaftNet8:

~~~powershell
dotnet restore Raft.Net8.sln --locked-mode
dotnet build Raft.Net8.sln -c Release --no-restore
dotnet test Raft.Net8.sln -c Release --no-build
dotnet run --project HumanTest/Raft.Demo -c Release --no-build -- --scenario
dotnet test Raft.Net8.sln -c Release --no-build --filter 'FullyQualifiedName~FaultScheduleTests'
dotnet run --project benchmarks/Raft.Benchmarks -c Release --no-build -- --filter '*' --job Dry
~~~

Existing simulation seeds remain 7, 19, 73, 431, 2026, 5417, 19391, 65537. The filter above replays them; a configurable thousands-of-seeds runner remains Phase 8.

Raw local evidence is in .artifacts/phase1: test-results/phase1-final.trx; test-results/phase1-failstop-before.trx; human-scenario.log; benchmark-final.log and benchmark-final/results; api-purity-audit.txt; isolated baseline/client/API-audit sources and binaries.

Fresh generated-project restores could not reach NuGet's vulnerability feed (NU1900). The benchmark/example and final offline verification use a process-scoped NuGetAudit=false with already cached, pinned packages. No repository audit/security setting was changed; no vulnerability-scan claim is made. Initial failed benchmark restore output is retained. Dry-job timing includes cold start and is not a throughput/p50/p99 result.

## Safety argument and remaining work

The reducer preserves static majority calculation, unique voting, freshness ordering, term adoption, stale-message checks, current-term no-op/commit rule, matched-prefix follower commit bound, committed-prefix protection, conflict repair, and per-peer response correlation. These remain covered by the original tests and eight fault seeds.

The refactor makes persistence dependencies explicit: no dependent vote/request/reply/apply notification before its successful completion. State and payload ownership survive delayed effects without reading mutable caller lists. Application still advances checkpoints in order and completes clients after apply. Fail-stop correction prevents further participation after uncertain persistence errors.

Known follow-ups from discovery remain: F2 storage/runtime identity binding, F3 CRC/length/full recovery hygiene, F4 atomic checkpoint term and reusable integration, and F6 timer range/send supervision. They are not silently treated as solved. DBreeze still requests OS disk flush through its existing commit path; this work adds no hardware power-loss guarantee.

Phase 2 must implement the versioned integrity/migration and recovery design, index+term atomic apply, group commit, and exhaustive crash-boundary tests. Later phases still cover pre-vote/transfer, ReadIndex/optional leases, learners/configuration, throughput, snapshots, full simulation/history checking, observability, secure transport hooks, and operational runbooks. Workflow remains inactive under .github/_workflows; no Linux/remote CI run was observed. Default TCP remains plaintext.

**Phase 1 is complete. Await explicit approval before starting Phase 2.** The overall enhancement objective is not complete.
