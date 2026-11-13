---
type: Benchmark Record
title: Three-way Raft performance comparison
description: Pinned .NET8 DotNext adapter, acknowledgment/recovery evidence and separate fresh workload matrices.
status: stable
sources:
  - resource: ../sdd/015-dotnext-comparison.md
  - resource: ../../../RaftNet8/benchmarks/Raft.Comparison/README.md
  - resource: ../../../RaftNet8/benchmarks/Raft.Comparison/DotNextAuditTrail.cs
  - resource: ../../../RaftNet8/benchmarks/Raft.Comparison/AdapterChecks.cs
  - resource: ../../../RaftNet8/benchmarks/results/dotnext-comparison-2026-11-13.html
  - resource: ../../../RaftNet8/benchmarks/results/dotnext-summary-2026-11-13.json
---

# Contract and evidence

User requested current/base/DotNext comparison and a new HTML. Delivered and verified on2026-11-13. .NET8 remains mandatory; downloaded nupkg inspection on2026-11-12 confirms last5.x DotNext.Net.Cluster5.26.2/net8.0 at39ff4fe882e68a5ea90818b41adb97cdc3ec60d8, latest6.12.1/net10.0 only. Exact dependencies are locked in the isolated Raft.Comparison project, not added to the production library.

Stock MemoryBasedStateMachine is a disk WAL. AutoFlush source invokes FlushToDisk on partition flush and append awaits it; IntegrityCheck=true, caching OnCommit,65536 records/partition, no compaction. Do not replace this with experimental WriteAheadLog5.x or ConsensusOnlyState without re-auditing append durability.

Composition adapter delegates native operations, stages owned payloads in native apply before cache eviction, reads only metadata afterward to preserve actual empty-entry index/term, and performs the identical ComparisonMachine business transaction. Its applied frontier/waiters follow DBreeze. Early draft reread payloads after OnCommit eviction; corrected before final runs. Smoke data from that draft is excluded.

Adapter checks passed: blocked DB transaction cannot ACK; genuine empty metadata,15-entry byte bound, repeated commit, canceled pending waiter, durable term/tail and reopened/replayed business state. Locked main restore, zero-warning Release build and221/221 regression tests passed. Existing ComparisonCluster gains only optional retained-root access so successful rounds can stop, reopen/verify all business databases and safely delete their own GUID data. Failed data remains for diagnosis.

# Measurement state

Final process exited0 after short144/144 and long72/72 measurements,48 short and24 batched long cases,3 rounds each. Both raw files have Complete=true and Failure=null; every successful round was closed/reopened/verified. Raw/log/source audit/test outputs are under RaftNet8/.artifacts/dotnext-comparison/. Partial matrices and old smoke are not final evidence.

The new Python renderer validates exact cases/rounds and recomputes each round from individual samples, then creates a new standalone Russian HTML/CSV/summary. Reporting defaults to3/5 voters and16KiB. Short/long are separate; median throughput/allocation, pooled nearest-rank latency, observed min/max are not CI. Full measured sample count368,640.

Independent raw recomputation/CSV parity and all measured binary hashes passed. Offline Chromium verified all72 table rows, both overview tables and all four charts with baseline/DotNext references; p50/filter/reset/sorting,390px,print and no-JS fallback passed without network/runtime errors. Explicit zero-percent reference line added. Python raw/saved-summary regeneration is byte-identical; saved summary permits HTML regeneration after raw artifacts are removed. QA images are in .artifacts/dotnext-comparison/qa/.

# Limits to retain

DotNext adapter is measured code, not default sample or production integration. Native file WAL vs Raft.Net DBreeze WAL, native wire aggregation (not limited by our MaxAppendBytes), fixed in-memory bootstrap configuration,25-30ms vs25ms heartbeat and weaker v1 baseline guarantees prevent claiming equal internals/recovery contracts. Common collector/business bounds and native protocol are explicitly described. No new algorithm claim, no latest6.x measurement or cross-host/TLS/hardware durability promise.

The earlier40% large-payload multi-voter regression was not fixed by this task. Fresh current source/binary provenance is recorded; baseline DLL matches the earlier report, current DLL includes prior late corrections and differs from earlier measured hash. Fresh three-way results must be used together rather than grafting the old matrix onto DotNext. Long16KiB current pipeline is approximately21%/38% below DotNext and44%/46% below baseline on3/5 nodes. Do not describe the appearance of a third participant as a performance fix.

Existing BDN compatibility smoke also passed: one Current-3n-64B-Pipeline Dry case, exit0; short-iteration warnings retained and numbers excluded from comparison. See .artifacts/dotnext-comparison/bdn-smoke.log.
