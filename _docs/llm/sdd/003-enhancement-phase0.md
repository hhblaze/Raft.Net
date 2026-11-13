# RaftNet8 enhancement: Phase 0 discovery

Date: 2026-11-12. Baseline: **8b991bd**. Status: **discovery complete; awaiting approval for Phase 1**.

Gate update: the user subsequently approved Phase 1 while retaining .NET 8. The original discovery recommendation below is preserved as history; [005](005-phase1-review.md) records the completed Phase 1 and next approval gate.

## Outcome and change boundary

The supported solution builds cleanly, all **71 tests pass**, and the three-node TCP/disk demonstration passes. The fixed-membership implementation contains the central Figure 2 safeguards, but it does not yet provide the requested extensions or a pure core.

Discovery reproduced a fail-stop defect, a storage/runtime identity mismatch, delayed detection of an injected interior log gap, and an accepted RPC deadline that the runtime rejects. A rejoining isolated node also disrupts a healthy leader because pre-vote is absent. These results have different implications; they are distinguished below.

No library, test, project, transport, storage-format, or workflow source was changed. This report and linked knowledge notes are the deliverables. Diagnostic programs and results are isolated under the ignored RaftNet8/.artifacts/phase0 directory. No commits, pushes, or package publication were performed.

The attachment explicitly requires: **“Stop and present this report before proceeding to Phase 1.”** No Phase 1 work is included.

## Scope, evidence, and assumptions

The repository review covered the supported library, all current test/harness/demo/benchmark sources, project and package configuration, historical consensus/storage/transport sources and DTOs, documentation, copied legacy baseline, and deployment metadata. The baseline inventory contains 191 tracked source/configuration/documentation files with 132 distinct content hashes, excluding generated/vendor directories and binary artifacts. Duplicate copied source was compared as a baseline; historical binaries were inventoried rather than all decompiled. The pinned DBreeze assembly was inspected separately for commit/flush behavior.

This is a source review plus executed checks, not a formal proof or a hardware power-loss certification. Historical .NET Framework/.NET Standard projects were not rebuilt. No Linux run, new coverage measurement, or new performance comparison was performed during Phase 0.

Assumptions for the proposed work:

- Crash/recovery faults and an asynchronous network; no Byzantine peers. A successful client write means quorum durability and local application.
- Keep the exact DBreeze 1.142.2026.1016-beta dependency unless separately approved. Verify its net8.0 assembly on the selected newer runtime before relying on it.
- Preserve the current RaftNet8 public API with adapters and, where necessary, obsolete shims. Archived 1.x has a separate compatibility boundary.
- Lease reads remain off; follower forwarding and joint consensus are optional extensions, not prerequisites for the required ReadIndex and single-server workflow.
- Application commands are deterministic and application-defined. Cancellation after admission does not prove that a command was not committed.

## 1. Projects, dependencies, and execution status

| Area | Current state |
| --- | --- |
| Supported solution | [RaftNet8/Raft.Net8.sln](../../../RaftNet8/Raft.Net8.sln): library, xUnit tests, crash harness, BenchmarkDotNet, demo |
| Human test | [HumanTest/Raft.HumanTest.sln](../../../RaftNet8/HumanTest/Raft.HumanTest.sln): interactive and scripted three-node disk/TCP demonstration |
| Build configuration | [Directory.Build.props](../../../RaftNet8/Directory.Build.props:3): net8.0, C# 12, nullable, implicit usings, warnings as errors, deterministic builds, package lock files |
| SDK | [global.json](../../../RaftNet8/global.json:3) selects 8.0.400/latestFeature; observed SDK 8.0.428, runtime 8.0.34 |
| Library | Assembly Raft.Net8, namespace Raft, version 2.0.0; exact DBreeze [1.142.2026.1016-beta] |
| Tests/benchmarks | Microsoft.NET.Test.Sdk 17.12.0, xUnit 2.9.3, VS runner 3.0.1; BenchmarkDotNet 0.13.12 |
| Dependency management | Versions currently live in individual csproj files; no Directory.Packages.props |
| Historical projects | Raft targets net48, with an older release configuration; RaftStandard targets netstandard2.0 and links historical sources. DBreeze references are 1.95 and 1.94.0 respectively. Deployment contains older packages/build scripts. These are artifacts, outside the supported solution. |
| Copied baseline | RaftNet8/legacy-source is excluded from compilation. Dormant historical UDP/protobuf files must not be confused with compiled transport code. |
| Automation | Workflow is tracked at [.github/_workflows/raft-net8.yml](../../../.github/_workflows/raft-net8.yml), not .github/workflows, the [required workflow directory](https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax#about-yaml-syntax-for-workflows). It is currently inactive as an Actions workflow. Its contents describe Windows/Linux validation, not evidence those jobs ran. |

Executed from RaftNet8:

~~~powershell
dotnet restore Raft.Net8.sln --locked-mode
dotnet build Raft.Net8.sln -c Release --no-restore
dotnet test Raft.Net8.sln -c Release --no-build --logger 'trx;LogFileName=phase0-baseline.trx' --results-directory .artifacts/phase0/test-results
dotnet run --project HumanTest/Raft.Demo -c Release --no-build -- --scenario
~~~

Restore succeeded; Release build had **zero warnings/errors**; tests had **71 passed, zero failed/skipped**. The scenario reported SCENARIO PASS after minority rejection, replacement leader election, healing, stop/start, application checkpoint recovery, and catch-up. This is a repeat of those checks on the current baseline, not just a quotation of the earlier validation record.

### Conflicts requiring an explicit choice

| Conflict | Options and recommendation |
| --- | --- |
| Root [AGENTS.md](../../../AGENTS.md:5) and the initial request require net8.0 only; the new request requires latest LTS/current. | As of this review, .NET 10 is latest LTS; .NET 11 is current STS; .NET 8 support ended November 10, 2026. Recommend **.NET 10 LTS** in Phase 1. Alternatives: retain net8.0 or multi-target net8.0/net10.0 with corresponding validation cost. Keep directory/assembly naming for API compatibility initially. [Official support policy](https://dotnet.microsoft.com/en-us/platform/support/policy/dotnet-core). |
| AGENTS and [001](001-resurrection.md)/[002](002-protocol-contract.md) describe fixed membership and explicitly exclude several newly requested features. AGENTS also says future membership requires joint consensus. | Supersede these scope restrictions with approved new specifications. Implement corrected single-server changes as requested; joint consensus remains optional. Preserve old specs as the history of the fixed-membership release. |
| Phase 1 says preserve behavior; confirmed defects require behavior changes. | First extract and characterize behavior. Put fixes in distinct reviewable diffs with failing regressions; do not conceal fixes in the refactor. The fail-stop fix is highest priority after extraction, or a separate prerequisite fix if explicitly preferred. |
| Phase 3 requires a 10:1 minimum election/heartbeat ratio; cross-cutting guidance mentions 5:1. | Recommend the stricter **10:1** rule and updated defaults in Phase 3. Existing defaults are 6:1. Phase 1 retains timing behavior. |
| Workflow was moved under _workflows. | Preserve its current inactive state in discovery. Decide separately whether Phase 8 should activate PR/nightly Actions; do not silently undo this repository choice. |

## 2. Current node architecture

~~~mermaid
flowchart LR
    C[Client proposals / RPC handlers] --> Q[Bounded event channel]
    T[PeriodicTimer] --> Q
    Q --> D[Single consensus driver]
    D --> R[RaftCore]
    R -->|synchronous reads / commits| S[DBreeze Raft storage]
    D -->|outbound tasks| N[Persistent TCP connections]
    N -->|responses| Q
    D -->|committed target| A[Separate ordered apply worker]
    A -->|bounded log reads| S
    A --> M[Application state machine]
    M --> DB[DBreeze application tables]
~~~

**Roles and elections.** [RaftCore.cs:49](../../../RaftNet8/src/Raft/Core/RaftCore.cs:49) advances Follower/Candidate/Leader through logical ticks. Campaign persists incremented term and self-vote before sending requests (78–94). Election appends a current-term no-op (96–112). Votes compare last term before last index and persist the candidate before granting (161–175). A leader checks recent responding peers over ElectionMaxTicks and steps down without quorum (56–69). There is no pre-candidate round, learner, or transfer state.

**Replication.** Per-peer next/match positions and a single correlated in-flight AppendEntries request drive catch-up (205–250, 269–276). Conflict hints avoid decrementing one index at a time. Leaders advance commit by a voting majority only for current-term entries (253–261). Followers validate the previous entry, preserve committed entries, and cap commit at the prefix matched in that request (177–203).

**Concurrency and locks.** [RaftNode.cs:48](../../../RaftNet8/src/Raft/RaftNode.cs:48) freezes options and creates bounded admission and event channels. The driver is the core's sole owner; socket handlers and outbound tasks exchange work through the channel. A separate worker serially applies committed entries (252–285). Lifecycle and connection semaphores, storage locks, concurrent dictionaries, and Interlocked operations still exist outside the core. Storage serializes synchronous operations, including reads shared by the driver and apply worker. Transactions contain no await.

Client proposals are gathered from adjacent queued work and appended in count/byte-bounded batches (177–221), with no configurable group-commit delay. Admission precedes copying the caller's command. Completion follows local application; cancellation limits the caller's wait while admitted work may still commit. Checkpoint replay runs from LastApplied through durable CommitIndex.

**The core is not pure.** It owns IRaftStorage, reads and mutates it synchronously, and samples an internal System.Random (default Random.Shared). Although repeatable with seeded random, fixed storage, and fixed delivery order, it is not the requested event/effect state machine. DBreeze work can block the consensus driver. The separate apply worker already prevents arbitrary application callbacks from running inside the core; this useful separation should be retained.

**Timers.** PeriodicTimer ticks are coalesced through _tickQueued; a saturated channel can omit ticks ([RaftNode.cs:237](../../../RaftNet8/src/Raft/RaftNode.cs:237)). These count processed logical ticks, not accurately elapsed wall time under load. That primarily affects availability now; future leases must use a separately justified monotonic-clock/drift model.

## 3. Persistence, formats, and fsync

[DBreezeRaftStorage.cs](../../../RaftNet8/src/Raft/Storage/DBreezeRaftStorage.cs) supports an owned disk engine or a supplied/shared engine. One physical folder is one engine; the current fixed table names hold one Raft group/node in that engine alongside optional application tables.

| Location | Key/value format |
| --- | --- |
| raft8/meta: identity | UTF-8 schema marker raft8-v1, group ID, node ID, sorted fixed members, separated by newlines (57–58) |
| raft8/meta: state | Little-endian term:8, commit index:8, vote byte length:4, UTF-8 vote bytes (324–344) |
| raft8/log | DBreeze ordered positive long index key; value is little-endian term:8, no-op flag:1, opaque command bytes (299–322) |
| Application checkpoint | Provided by IRaftStateMachine.LastApplied; only an index in the current contract |
| Snapshot metadata/data | Absent; no last-included index/term, configuration snapshot, install state, or compaction |

There is **no Raft-level CRC or declared record-length envelope**. DBreeze supplies its own transactional storage machinery, which must not be confused with end-to-end validation of the Raft record and checkpoint.

Hard-state mutations use a committed synchronous transaction; cached state changes follow successful commit (92–110). Suffix replacement reserves the log table and deletes/inserts atomically before updating cache/tail (118–166). Committed suffix replacement is rejected. Term/commit regressions and conflicting votes are rejected. Unchanged metadata does not write.

Reads use bounded ascending ranges, a count/byte-bounded LRU, cached hard state/tail, and header-only nine-byte term reads while a transaction is alive (168–297). Startup checks identity, hard state, tail, and existence of the commit row, rather than scanning the whole log (52–89). Consequently, interior corruption can be discovered later.

### Verified disk-flush path

The exact pinned net8.0 DBreeze.dll was inspected through reflection/IL:

~~~text
SHA256 A4FD6D59492DE81BBAB6C22A67B69F741B1E6C9906D23EE7BC68EA602FE90FDA
MVID   325255af-4412-407c-bbd1-4b898d35fc43
Transaction.Commit -> TransactionsCoordinator.Commit
single-table commit -> LTrie / storage layer -> FSR.Commit
FSR.FlushFile -> FSR.NET_Flush
FSR.NET_Flush -> Stream.Flush -> RandomAccess.FlushToDisk(SafeFileHandle)
~~~

The multi-table coordinator uses transactional prepare/finish operations and a transaction journal, including a journal commit before completing tables. This establishes an explicit **OS flush request** in the pinned disk backend. It does not establish the behavior of every disk controller or prove recovery at every instruction boundary. The in-memory backend intentionally provides no process-restart durability.

Current core methods call storage synchronously before dependent success responses/election messages. This ordering is implicit in call order and the IRaftStorage “durable on return” contract; the future asynchronous effect executor must encode it explicitly. Grouped client appends already share a log transaction, but there is no independent bounded-delay fsync scheduler; hard-state/commit updates can be separate transactions.

### DBreeze application integration

The library accepts opaque bytes and an application callback; it does not impose the demo's JSON schema or supply generic application deduplication. [KeyValueMachine.cs:9](../../../RaftNet8/HumanTest/Raft.Demo/KeyValueMachine.cs:9) demonstrates SetCommand(RequestId, Key, Value), source-generated JSON, deterministic application, and request-ID deduplication.

The demo reserves demo/checkpoint, demo/values, and demo/requests, then commits effects, deduplication entries, and lastApplied **index in the same transaction** (52–66). The in-memory view and reported index advance afterward. This is a sound atomic checkpoint example, but **checkpoint term is missing**, and the library can only verify that the callback advances the reported index. It cannot enforce arbitrary application-side atomicity. The deduplication table and log have no retention policy.

Catch-up currently replays retained log ranges. There is no snapshot creation/stream/install path and no compaction. Neither an ordinary DBreeze read transaction nor a live directory copy has been established as a consistent multi-table point-in-time snapshot. Phase 7 must validate a supported backup/generation mechanism against the exact dependency; the DBreeze skill's read-visibility guidance is not an MVCC snapshot guarantee.

## 4. Transport and serialization

[TcpRaftTransport.cs](../../../RaftNet8/src/Raft/Transport/TcpRaftTransport.cs) reuses one outbound TCP connection per peer. A semaphore spans the entire request/response exchange (18–30), so the transport itself serializes per-peer round trips. Enabling several core in-flight requests alone will not implement pipelining.

The listener bounds concurrent connections, dispatches handlers, validates group/member IDs and request direction, and applies a fixed 30-second server exchange deadline (98–146). Client deadlines use RpcTimeout. Failures close/recreate connections; counters exist, but error context is largely discarded.

[RaftWireCodec.cs:14](../../../RaftNet8/src/Raft/Transport/RaftWireCodec.cs:14) writes a four-byte little-endian frame length followed by version 1, RPC kind, bounded group/sender identifiers, term, and message fields. Four kinds exist: VoteRequest/Response and AppendRequest/Response. Append requests include correlation ID, previous index/term, leader commit, and count/length-delimited entries. Readers reject oversized frames, invalid kinds/versions/IDs/counts, invalid command lengths, and trailing bytes before accepting messages.

Frames use pooled buffers and Memory/Span. Decode materializes owned commands before returning receive buffers to the pool; this ownership is necessary, although it allocates per entry. The encoder is a custom pooled writer rather than an IBufferWriter contract. No version negotiation or snapshot/read/config messages exist.

The supplied transport opens raw NetworkStream connections. IRaftTransport permits a replacement implementation, but this implementation has no stream factory/TLS/mTLS hook or peer authentication. Member IDs are routing validation, not authentication; trusted networking is the documented current assumption.

Historical 1.x uses Biser DTOs over its separate SPROT/TCP framing and signal protocol. It is incompatible with raft8 wire v1 and raft8-v1 storage. These are three distinct generations when considering a future version 2, not a single legacy/v2 transition.

## 5. Safety findings, risks, and counterexamples

Priorities below are engineering priorities, not security vulnerability scores.

| ID / priority | Evidence and consequence | Required response |
| --- | --- | --- |
| F1 / high: persistence failure does not always fail-stop | [RaftNode.cs:151–153](../../../RaftNet8/src/Raft/RaftNode.cs:151) catches ArgumentException around all of core.HandleRequest, including storage calls. A wrapper throwing ArgumentException once from SaveHardState propagated the error to the caller, but the node remained running and granted a later vote. | Separate input validation from execution; every storage/app execution failure must halt the node, regardless of exception type. Test uncertain post-write failures as well as failures before mutation. This is a confirmed failure-policy defect and potential threat to election/log safety, not a demonstrated split-brain trace. |
| F2 / high: runtime configuration can bypass persisted identity binding | Storage validates against its own options ([storage:57–86](../../../RaftNet8/src/Raft/Storage/DBreezeRaftStorage.cs:57)); [node:48–57](../../../RaftNet8/src/Raft/RaftNode.cs:48) and [core:29–36](../../../RaftNet8/src/Raft/Core/RaftCore.cs:29) never verify those options agree. A storage object for default/n1/{n1,n2,n3} was accepted by a runtime for different/n1/{n1,n4,n5}. | Bind bootstrap identity/configuration to recovered storage metadata, with one authority. Otherwise configuration mistakes can undermine quorum overlap. This is a confirmed startup guard hole, not a counterexample under correctly configured fixed membership. |
| F3 / high durability gap: no complete record-integrity recovery contract | [storage:52–89](../../../RaftNet8/src/Raft/Storage/DBreezeRaftStorage.cs:52), [299–344](../../../RaftNet8/src/Raft/Storage/DBreezeRaftStorage.cs:299). Deliberately deleting index 2 from committed entries 1..3 allowed reopen with commit=3/tail=3; a subsequent range read failed with “Log gap at index 2.” Valid-looking payload corruption has no Raft CRC to detect it. | Versioned envelopes/segment validation and explicit recovery policy. Committed or promised-durable corruption must fail-stop/require replica repair. Do not infer that a tail is safely discardable solely from local commitIndex; it may be globally committed. The fixture is injected logical corruption, not proof DBreeze produces torn committed rows. |
| F4 / high contract gap: checkpoint term/atomicity not enforced generally | [Protocol.cs:54–64](../../../RaftNet8/src/Raft/Protocol.cs:54), [node:55–57](../../../RaftNet8/src/Raft/RaftNode.cs:55), [demo:52–74](../../../RaftNet8/HumanTest/Raft.Demo/KeyValueMachine.cs:52). Demo atomically stores index/effects, but not term. A custom machine can advance an index independently of effects; startup validates bounds only. | Provide a durable index+term checkpoint contract and DBreeze integration helper; validate replay/snapshot boundary consistency. Preserve the old API through an adapter with explicit guarantees and limitations. |
| F5 / medium availability: isolated follower disrupts healthy leader | [core:78–94](../../../RaftNet8/src/Raft/Core/RaftCore.cs:78), [147–174](../../../RaftNet8/src/Raft/Core/RaftCore.cs:147). A three-node probe kept n1/n2 healthy in term 1, isolated n3 until term 7, then delivered its valid vote request: n1 stepped down. | Implement pre-vote and recent-leader suppression with transfer exceptions. CheckQuorum already exists. This is an expected basic-Raft availability weakness, not an election-safety violation. |
| F6 / medium availability: options accept unsupported timer range; send faults can escape supervision | [options:31–39](../../../RaftNet8/src/Raft/RaftOptions.cs:31) accepted a 50-day RPC timeout below a very large legal election timeout; CancellationTokenSource.CancelAfter rejects it. [node:305–306](../../../RaftNet8/src/Raft/RaftNode.cs:305) is outside the send try/catch; [298](../../../RaftNet8/src/Raft/RaftNode.cs:298) removes completed tasks without reporting their faults. SuperviseAsync (121–132) lacks a final cleanup path if its awaits fault. | Validate supported deadline ranges and supervise all effect failures/completions. The accepted invalid option/runtime rejection is reproduced; stalled peer progress or incomplete shutdown is inferred from these control paths, not measured as a separate cluster failure. |

Further risks and missing guarantees:

- Sync disk work and coalesced ticks can delay heartbeats/elections; no heartbeat/load SLO or fsync tail distribution has been established.
- Admission, batch/frame sizes, and cache memory are bounded, but there is no explicit total inherited/uncommitted-log-byte limit. Log/dedup disk growth is unlimited without snapshots/retention.
- A successful logged ReadBarrier is a quorum-backed, applied no-op fence; local reads alone are not linearizable. ReadIndex is absent. Preserve the existing fence API while adding fresh-context read rounds.
- Existing RaftStatus/NodeMetrics/StorageMetrics are snapshots/counters, not ILogger, Meter instrumentation, or Activity traces. Missing errors make diagnosis and durability audits harder.
- The simulator and tests are strong starting points but do not establish the requested invariants across dynamic configuration, snapshots, leases, or unsynced disk loss.

### Safeguards verified in the supported baseline

No additional direct Figure 2 safety violation was reproduced under correct fixed membership and a durable, uncorrupted store. Reviewed protections include durable term/vote adoption, repeat grants to the same candidate, unique-voter counting, log freshness, stale-term rejection before resetting timers, correlation filtering, committed-prefix protection, matched-prefix follower commit bounds, and current-term-only leader quorum commitment. Targeted regressions in [ElectionTests](../../../RaftNet8/tests/Raft.Tests/ElectionTests.cs), [ReplicationTests](../../../RaftNet8/tests/Raft.Tests/ReplicationTests.cs), and [002](002-protocol-contract.md) exercise these rules. This statement is bounded by the reviewed model and tests, not a proof of all executions.

### Historical implementation: do not reuse its safety assumptions

Archived [Raft/RaftNode.cs:40](../../../Raft/RaftNode.cs:40) and 934–952 keep only a volatile last-voted term, not durable candidate identity. [StateLog.cs:160](../../../Raft/StateMachine/StateLog.cs:160) recovers node term from the log tail, losing term-only updates. [RaftNode.cs:720](../../../Raft/RaftNode.cs:720) changes leader/timing information before checking heartbeat term. [StateLog.cs:502–515](../../../Raft/StateMachine/StateLog.cs:502) checkpoints application completion in a separate transaction from arbitrary user effects; delayed persistence also weakens acknowledgment durability. These are reasons to preserve the archive as reference and reject an unqualified mixed-version upgrade.

## 6. Tests, simulation, and performance coverage

| Existing coverage | Boundary of that evidence |
| --- | --- |
| Election, stale/duplicate messages, log freshness, Figure 8 commit rule, conflict repair, committed-prefix protection, check-quorum | Static membership; no pre-vote, transfer, configuration, or snapshot state |
| Disk term/vote restart, suffix atomicity, bounded cache and reads, malformed records/options/wire | No exhaustive persistence-completion ordering, per-record CRC, or torn-tail recovery suite |
| Runtime apply ordering, batching, backpressure/cancellation, 2,000 concurrent proposals, injected storage/application failures | Failure injection misses the ArgumentException classification defect and all-boundary crash schedules |
| Two separate-process crash tests | One exits after commits without disposal; one kills during an uncommitted multi-table transaction. These do not emulate hardware power loss or kill every persistence boundary. |
| Real TCP failover/restart and demo disk checkpoints | Three nodes; integration node stops are graceful. No five-node OS-kill/membership/transfer cluster scenario. |
| Eight seeded five-node fault schedules | Seeds 7, 19, 73, 431, 2026, 5417, 19391, 65537; 500 fault steps each, then healing |

[Simulation.cs](../../../RaftNet8/tests/Raft.Tests/Simulation.cs) contains SimCluster: real in-memory DBreeze, synchronous persistence, symmetric partitions, drop/duplicate/reorder scheduling, and retained-engine core restart. It has separate PRNG instances for scenario/network/node decisions, not a single recorded source. It lacks scheduled network delay, asymmetric links, per-node skew, simulated flush completions, unsynced-write loss, and torn disk writes.

Its safety checker tracks one observed leader per term, monotonic terms/commit, commit bounds, leader completeness on first observation, and agreement on committed entries (110–140). It is not a complete every-step checker for leader append-only, all log-matching pairs, applied state-machine safety, or configuration safety. There is no client-history linearizability checker, property-testing dependency, core coverage report, or several-thousand-seed nightly runner.

Replay the existing fixed seed set:

~~~powershell
dotnet test Raft.Net8.sln -c Release --no-build --filter 'FullyQualifiedName~FaultScheduleTests' --logger 'console;verbosity=normal'
~~~

There is no current arbitrary seed/range CLI; adding it is planned, not an available command.

The isolated audit probes can be rerun in this workspace:

~~~powershell
dotnet run --project .artifacts/phase0/probes/Probes.csproj -c Release
~~~

They are diagnostic scratch code, not newly added committed regression tests. Their observed output is retained in .artifacts/phase0/probe-results.txt and summarized in F1/F2/F3/F5/F6.

Existing [benchmarks](../../../RaftNet8/benchmarks/README.md) cover count-batched disk/memory appends, cached/range/header reads, three-node memory-core replication, pooled codec, and three-node TCP with 256 concurrent 256-byte proposals. Prior recorded short jobs and 26 dry cases are useful execution/allocation baselines, but **not individual p50/p99 request latency**, 1/3/5-node comparisons, large commands, pipelining toggles, or a correctness-equivalent comparison with archived 1.x. No new benchmark numbers are claimed here.

## 7. Nondeterminism and purity inventory

| Source | Location and required treatment |
| --- | --- |
| Random sampling inside core | [core:21,33,279–283](../../../RaftNet8/src/Raft/Core/RaftCore.cs:21). Move sampling to driver/simulator IRandom and record sampled timeout in an event. A seed alone does not remove hidden random-state mutation from the transition function. |
| Storage reads, writes, recovery/failure outcomes | Core owns IRaftStorage and performs commits during transitions. Replace with owned recovered state/bounded log views, explicit read/persist effects, and completion/failure events. |
| Clock/tick delivery outside core | Node PeriodicTimer and tick coalescing. Inject ITicker/IClock into driver; simulate per-node time explicitly. Core currently has no direct wall-clock call. |
| Tasks, network, locks, scheduling outside core | Node workers/RPC tasks, TCP semaphore/socket completion order, DBreeze storage lock. Preserve one owner and express observed ordering as events; never expose an uncompleted durable mutation through a dependent reply. |
| Application behavior | Arbitrary callbacks, command contents, DBreeze and external effects. Require deterministic commands and atomic/replay-safe effects; simulator supplies a controlled machine. Demo request IDs are generated outside consensus. |
| Historical sources | TimeMaster uses wall time/tasks; old RaftNode uses Random, operation locks, thread-dependent sampling, and DateTime. These remain archive code, not code to pull into the new core. |

## 8. Proposed work plan and blast radius

Risk order is **foundations and durability first; then election/transfer; reads; learners/configuration; throughput; snapshots**, with simulation expanded alongside every phase. “High” means changes across core/driver/storage/transport or persisted compatibility, not a calendar estimate. Every phase ends with a report, passing checks, remaining risks, and an approval gate.

### Goal-to-change map

| Goal | Concrete work / phase | Blast radius and primary invariant |
| --- | --- | --- |
| 9. Pure deterministic core | Phase 1 event/state/effect model, recovered-state bootstrap, channel driver, ILogStore/ITransport/IStateMachine adapters, injected ticker/random | **Very high:** core/node/storage seams and test harness. Preserve all Figure 2 behavior and durable-before-send ordering. |
| 8. Persistent hygiene | Separate F1/F2/F6 regressions; Phase 2 versioned length/CRC32C records, explicit durable completions, checkpoint index+term, safe recovery and bounded group commit | **Very high:** disk schema, storage, apply integration, migration/harness. Never acknowledge volatile state or silently lose committed/promised state. |
| 10. Simulation/tests | Characterization in Phase 1; simulated disk/network/clock and new invariants cross-cutting; Phase 8 history checker, properties, real clusters, coverage and seed runs | **High:** test architecture/CI; exposes all invariants and client-visible consistency. |
| 1. Pre-vote/CheckQuorum | Phase 3 pre-candidate round, recent-leader suppression, preserve and strengthen existing quorum checks, stricter timing defaults | **High:** roles/elections/messages/options; election safety plus availability on rejoin. |
| 2. Leadership transfer | Phase 3 drain proposal admission, catch up eligible voter, TimeoutNow, bypass pre-vote only for authorized transfer, bounded abort | **High:** public API/core/RPCs; no split authority or permanently closed admission. |
| 3. Linearizable reads | Phase 4 term-bound fresh quorum contexts, batching, current-term commit gate, applied-index fence; optional forwarding/stale API | **High:** core/driver/API/wire; linearizability with no write per read. Leases off by default. |
| 4. Learners | Phase 5 replicated config distinguishes learners/voters; replicate to learners without votes/elections/quorum weight; promotion lag bound | **Very high with goal 5:** configuration recovery/peer progress/snapshot metadata. |
| 5. Membership changes | Phase 5 corrected single-server algorithm, active-on-append config, suffix rollback, one pending change, current-term committed entry gate, self-removal after commit | **Very high:** quorum calculation/election/log/recovery/API. Maintain required quorum overlap through leader changes. |
| 6. Batching/pipelining | Phase 2 bounded fsync batching; Phase 6 per-peer Probe/Replicate/Snapshot, bounded inflight count/bytes, correlated transport multiplexing, uncommitted-byte admission | **High:** progress/transport/memory lifetime; reordered responses must not acknowledge nonexistent prefixes. |
| 7. Snapshots | Phase 7 consistent DBreeze snapshot generation, manifest(index,term,config,digests), retained tail, resumable CRC chunks, staged durable install, throttling | **Very high:** state machine/storage/recovery/transport/config. Atomic old-or-new state after every crash. |

### Phase 1: reviewable modernization and extraction

Choose the framework explicitly, then add central package management, update SDK/tool paths/lock files, and validate the pinned DBreeze asset. Nullable and warnings-as-errors already exist; preserve them. Keep protocol bytes and disk contents unchanged during extraction.

Introduce explicit Tick, MessageReceived, ClientProposal, ReadRequest, StorageCompleted, SnapshotCompleted, ConfigChange, and TransferLeadership event types as needed; unsupported future operations remain explicitly unsupported until their phase. Emit Send/Persist/Apply/Respond/timer effects. Core code must not call clocks, random providers, tasks, locks, sockets, or disk. Driver IRandom samples election deadlines and supplies values as input; time and durable I/O arrive as events.

Model durable versus pending state and ordered persistence batches. A persisted-completion token releases only effects that depend on that batch. Reordered/stale completions cannot release later votes, log acknowledgments, or application work prematurely. Storage reads that are not in bounded owned core state also require explicit effects/completions; an apparently pure method must not hide an ILogStore query.

Adapt existing public IRaftStorage, IRaftTransport, and state-machine APIs; keep existing async/cancellation semantics. Make payload/buffer ownership explicit, and introduce IBufferWriter serialization without changing wire bytes. Characterize fault/shutdown paths before separating behavior fixes. Initial metrics/logging/TLS injection seams can be added without activating new consensus behavior.

Gate: all 71 baseline tests and demo still pass; byte-for-byte wire/store fixtures unchanged; event replay produces identical state/effects; completion-order tests show no dependent send before durable completion; all workers terminate and errors reach Completion. Keep the newly demonstrated defects as separate failing regression/fix diffs rather than mixing them into extraction.

### Phase 2: persistence and application checkpoint

Fix the failure classification and identity authority with targeted regressions before new durability features. For DBreeze, design logical record/segment envelopes with length+CRC32C rather than automatically adding a second WAL. Version and migrate any new schema. Validate contiguous indexes, terms, record integrity, checkpoint term, and snapshot boundary on recovery.

Tail salvage is allowed only where the storage protocol proves an incomplete, unpromised tail can be discarded. Local commitIndex alone is insufficient: a locally uncommitted entry may already be committed elsewhere. Fail closed for interior, committed, or acknowledged durable corruption unless safe replica repair is explicitly performed before participating again.

Store application effects, deduplication where needed, and checkpoint(index,term) in one short DBreeze transaction. Group contiguous compatible persistence effects by count/bytes and bounded maximum delay; publish completion only after DBreeze Commit returns. Never move a live DBreeze transaction across await/threads.

Gate: process death before/after every injected persistence step; uncertain completion, term/vote, suffix, checkpoint, grouped fsync, and migration fixtures; no acknowledgment escapes its durable fence. Test actual DBreeze process recovery in addition to simulated unsynced loss. Hardware/controller power-loss behavior remains a separate deployment validation.

### Phase 3: elections and transfer

Add pre-vote with proposed-term semantics that bypass the current generic higher-term adoption rule. Receivers do not mutate persistent state for a pre-vote and reject while recently hearing a valid leader. Preserve check-quorum and define its response/freshness window. Set heartbeat at most one tenth minimum election timeout.

Transfer only to an eligible caught-up voter: stop new proposals, replicate the admitted tail, send TimeoutNow, allow immediate real election, and abort/reopen admission within one election timeout. Test cancellation/timeout and target/leader crashes. Keep lease-specific voting rules disabled unless leases are enabled later.

Gate: replay F5 with stable healthy term; isolated leader steps down; asymmetric/partial partitions, delayed old votes, transfer to lagging target, failed transfer, and old leader recovery. Authority: dissertation §9.6 and §3.10; Figure 2 persistence/election constraints remain in force.

### Phase 4: reads

Add ReadIndex without removing the logged ReadBarrier. Require a committed current-term entry, record the read fence, obtain a fresh majority acknowledgment with a unique term/leadership-generation-bound context, and wait for application through the fence. Batch simultaneous readers. Old heartbeat responses cannot satisfy a new round.

Gate: histories under partition, stale leadership, leader changes, delayed apply, canceled readers, and request duplication pass the linearizability checker. Optional leases require an explicit monotonic-clock/drift/safety-margin contract and separate skew/expiration tests; default off. Dissertation §6.4/§6.4.1.

### Phase 5: learners and corrected single-server changes

Represent voter/learner configuration in the log/recovered state and snapshots. Activate a configuration on append, recompute it after suffix rollback/restart, and reject overlapping pending changes. Require a committed entry in the leader's term before any new change, including after election with inherited uncommitted configuration. This condition is essential: use the [author's 2015 correction](https://groups.google.com/g/raft-dev/c/t4xj6dJTP6E/m/d2D9LrWRza8J), not the dissertation's uncorrected single-server algorithm.

AddLearner → bounded-lag catch-up → PromoteToVoter changes one voter at a time; RemoveNode respects minimum configured size and leader removal steps down after commit. A joining learner needs a bootstrap/config authority, not an edited local members array. Joint consensus remains a separately scoped option.

Gate: reproduce competing configurations across even-sized clusters and leader changes, rollback active uncommitted configs correctly, prevent learner votes/campaigns, reject concurrent/unsafe requests, preserve quorum-intersection safety through promotion/self-removal. Include the published membership counterexamples.

### Phase 6: throughput and memory

Add bounded progress windows and transport request/response correlation with Probe/Replicate/Snapshot transitions. Validate duplicate/reordered success/rejection and retransmission semantics before raising concurrency. Bound inflight bytes per peer, retained payload ownership, and total uncommitted bytes; reject/backpressure before copying oversized admitted work. Keep apply separate and batch DBreeze application transactions.

Gate: slow/failing follower and slow application cannot cause unbounded memory or starve healthy quorum heartbeats; pooled data remains valid until all consumers finish. Benchmark 1/3/5 nodes with small and large commands, batching/pipelining independently enabled, real disk/network and durable application effects. Record throughput, allocations/GC, individual end-to-end p50/p99, and fsync latency separately.

Use baseline 8b991bd and archived 1.x through an isolated adapter where comparable. Label the latter's weaker durability/consensus semantics; do not call unsafe acknowledgments equivalent performance. Preserve raw environment/results and do not substitute normalized iteration means for per-request percentiles.

### Phase 7: snapshot creation, install, and compaction

First prove a consistent DBreeze snapshot mechanism with concurrent writes to multiple tables and checkpoint/index/term/config. Investigate supported backup/generation semantics against the pinned implementation; do not assume ordinary read visibility means MVCC. If no safe online mechanism exists, present alternatives and costs before choosing one. Creation must run outside the apply loop after a bounded consistent cut is established.

Include manifest/version/identity/config and whole-image digest; stream bounded chunks with offset and CRC, resumable transfer identity, and bandwidth limits. Stage into a separate generation; validate and durably finalize before atomic activation. Do not assume open DBreeze files can be renamed on Windows. Crash recovery must choose one complete generation and reconcile checkpoint/log/config. Install may require a controlled follower application handoff; snapshot creation must not stop normal application.

Gate: far-behind followers, overlap with normal replication, disconnect/resume, duplicate chunks, leader/term changes, corrupt chunks/image, and crash at each install/activation/compaction boundary. Keep the configured log tail and never compact ahead of a durable usable snapshot.

### Phase 8 and cross-cutting delivery

Build a single-seed event scheduler with recorded random choices, simulated skewed clocks, delayed/asymmetric links, and disks distinguishing pending writes from flushed state. Crash loses unsynced writes and injects torn tails at modeled boundaries. Print seed, schedule, and reduced failure trace. Replay identical events against the pure core and real driver adapters where possible.

Check every listed invariant after every event: election safety, leader append-only, log matching, leader completeness, state-machine safety, committed-entry preservation, monotonic terms, configuration safety. Add a client-history linearizability checker with cancellation/timeout indeterminacy and property-based storage/config/codec tests. Run a small deterministic PR set and several thousand seeds **per scenario** nightly, with explicit seed-start/count/scenario controls. Publish the actual invocation when the runner exists.

Add real 3/5-node DBreeze/TCP OS-kill tests, rolling restart with transfer, membership/learner workflows, and core coverage reports. Activate CI only after the _workflows choice is resolved; do not claim configured jobs have executed.

Across phases add structured ILogger events, Meter instruments for role/term/index/lag/elections/snapshots/proposal and fsync latency, and proposal Activity spans. Validate immutable options at startup with units and supported runtime ranges. Supply transport stream/TLS/mTLS hooks and cancellation/error supervision. Keep README, Mermaid architecture, configuration reference, human demo, _docs/raft_skill.md, SDD, and OKF aligned. Add operational runbooks for node replacement, membership, snapshot restore, and upgrades.

## 9. Compatibility and migration gates

Phase 1 preserves raft8 wire v1 and raft8-v1 disk formats. New pre-vote/transfer/read/config/snapshot messages and integrity/checkpoint metadata require deliberate versioning later; the current strict decoder cannot accept unknown RPC kinds.

Before enabling such features, choose and test a capability/activation protocol for a rolling raft8 upgrade, or require a coordinated offline upgrade. A dual reader alone does not make mixed quorums safe. Provide a copy-based offline migrator with validation, backups, crash-safe publication, and refusal of ambiguous identity/config/checkpoints. Preserve hard state, committed prefix, index/term checkpoints, and application data; do not rewrite a live data folder in place.

Archived 1.x did not durably record all necessary election/checkpoint information. Generic lossless live migration cannot be promised. A validated application snapshot into a new cluster may be necessary; inspect concrete deployed data before selecting a strategy. Migration tools must reject uncertainty rather than invent lost term/vote state.

## 10. Phase 0 delivery and safety argument

Changed tracked files: this report; a new OKF enhancement-discovery concept; links in the OKF index and dated work log. Full new-file content is in the linked repository files; no implementation diff exists in this phase.

Checks: fresh locked restore, clean Release build, 71 passing tests, passing TCP/disk scenario, exact dependency flush inspection, and five isolated diagnostic probes. Existing benchmark reports were reviewed, not rerun or presented as new results. Source citations refer to baseline 8b991bd.

No consensus transition, public API, persisted byte, or wire byte was changed, so this phase introduces no change to the implemented invariants. The report records known defects and missing guarantees instead of treating green tests as proof of safety.

Remaining work is Phases 1–8 above. **Approval requested for Phase 1, including the framework choice; recommended target: .NET 10 LTS.** Keep defect fixes distinct from the behavior-preserving extraction. Each later phase requires its own approval.

## Primary references

- [Raft extended paper](https://raft.github.io/raft.pdf): Figure 2 and §5 safety rules.
- [Ongaro dissertation](https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf): §3.10 transfer, chapter 4 membership, chapter 5 compaction, §6.4 reads, §9.6 disruptive rejoin/pre-vote.
- [Dissertation errata](https://github.com/ongardie/dissertation) and [single-server membership correction](https://groups.google.com/g/raft-dev/c/t4xj6dJTP6E/m/d2D9LrWRza8J).
- [DBreeze skill retained in this repository](../skill/DBreeze_skill.md), plus the exact pinned package assembly/lock files inspected above.
- [Microsoft .NET support policy](https://dotnet.microsoft.com/en-us/platform/support/policy/dotnet-core), checked 2026-11-12.
