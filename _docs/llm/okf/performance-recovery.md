---
type: Engineering Record
title: e06 performance recovery
description: Validated runtime improvements and fresh evidence, with two open p99 gates.
status: draft
sources:
  - resource: ../sdd/031-performance-recovery.md
  - resource: ../../../RaftNet8/benchmarks/results/performance-recovery-e06-2026-11-14.html
---

# Status, 2026-11-14

**Performance acceptance remains OPEN: 14/16 cells.** Throughput and allocation pass16/16; p99 passes14/16. Do not mark spec031 complete, hide bad observations, or ask for phase approvals. The user authorized autonomous iteration. No commit, push or publication.

Final source: release-source.zip SHA256 **16690FDAB4563C603FC91EC9B41FAB4F2BCAC0A83E545E0046D33828D7F3E07F**. Live/tested/frozen DLL SHA256 **91FD2E34E822614B182E3257F797D0CAD29A0E856E4FF1318207EE8E87E3CF75**. Runtime/CWD are release-runtime and release-cwd under RaftNet8/.artifacts/performance-recovery/.

# Requested lower-load recheck, 2026-11-14 18:20 local

User requested an immediate repeat of long5n16KiB Pipeline while the computer had less background load. Same three frozen DLLs, same H-drive data/workload/timers,10 balanced rounds/30 fresh processes/122880 latencies; no code changes, rebuild or profiler. Results are separate from the original300-observation matrix: [focused HTML](../../../RaftNet8/benchmarks/results/p99-recheck-5n-16k-pipeline-2026-11-14.html), JSON/CSV alongside; raw recheck-low-load-20261114-182029.

Fresh e06/before/current median throughput706.87/682.75/680.75 commands/s; pooled p99227.6949/215.2042/212.2702ms. Current/e06: throughput−3.695%, p99−6.774%, allocation−0.032%; this single new cohort passes all three working tolerances. Absolute current p99 is slightly worse than its previous204.89ms (+3.60%); e06 rose from101.61ms. Thus the earlier+101.7% relative gap did not recur, but long delays remain in every version; do not claim that lower CPU load eliminated them or uniquely identified their cause. CPU load was not independently measured. Keep the original14/16/FAIL data unchanged and do not infer15/16 for the full matrix from this isolated recheck. Independent raw-hash/numeric and offline desktop/mobile/noJS/SVG QA passed.

# Retained implementation

- Hold proposal-ready reservation through the complete owner drain; evaluate deadline/reservation under one metadata gate. Only the owner drains bytes and assigns indices.
- One reusable timer wakes the bounded change channel. Recheck the real oldest proposal/deadline after every wake, including stale callbacks. No per-wait linked CTS/normal timeout exception. Original2ms/8ms/1ms windows, bounds and durability/CRC remain.
- Immediate proposal admission uses an available semaphore slot without linked CTS. Only the bounded waiting path links caller/lifetime tokens. Check both before admission and under the queue gate. Cancellation after ownership still only stops waiting; shutdown/backpressure tests pass.
- DBreezeLogStorage implements ILogStoreProgress using the cached durable byte frontier. The adapter polls it instead of materializing Recover/MembershipView after writes. No DB query/transaction; repeat polling allocates0 in the regression. Preserve full recovery, legacy fallback, dedicated workers and storage→cache lock order.
- Pure Core/CoreTopology and direct ordinary TCP completion are unchanged. No extra inactive membership history/directory queries. Preserve net8.0, pinned DBreeze1.142.2026.1016-beta, Hot Add/Remove API and wire3/raft8-v4.

# Evidence

Final full suite **410/410**, zero skips,2m31s (validation/admission-full.trx). This includes four producers continuing through Add and automatic current-leader removal on3/5 voters with stable operation IDs; existing node objects remain and active durable applications agree. HumanTest and29 BDN Dry cases pass.36 fresh single-voter/unbatched controls completed; four full Add workloads all completed before commands ended. Dry/Add smoke are functional evidence, not capacity claims.

The [HTML](../../../RaftNet8/benchmarks/results/performance-recovery-e06-2026-11-14.html), CSV/JSON compare fresh e06c4f95edd6836c9a8782ecad2542bef298fa32 / frozen R19 b6ba58a (production source unchanged from bfcc523) / final working tree.300 fresh processes,691200 latencies,5–10 paired rounds, unchanged business/checkpoint transaction, H-drive data,64 clients and original128+512 /2048+4096 workloads. Main raw sets release-five, release-extra-short, release-extra-long. No8b991bd/DotNext rerun. R18/R19 preserved.

Open p99 points, both after ten pairs: short5n64B Batched **+13.09%**; long5n16KiB Pipeline **+101.65%**. Pipeline and16KiB each pass7/8. Smaller throughput losses remain: short3n64B Pipeline−3.63% (7/10 paired losses), long5n64B Pipeline−3.85%. Being within measurement tolerance does not prove absence of systematic loss. Long3n64B Batched+14.84% throughput/−16.32% allocation; long5n64B Batched+20.88%/−21.23% in this cohort. Do not generalize modal gains into universal product claims.

Independent numeric QA recomputes300 raw SHA hashes,48 aggregates,16 decisions and691200 samples. Offline browser QA checks144 SVG values, filters, desktop/mobile widths, print and no-JavaScript fallback; screenshots visually inspected. Reports contain PerformanceAccepted=false.

# Diagnostics and rejected experiments

Main optimizations from e06 remain. Packet fragmentation is measured: one5n64B Batched profile gave e0662.1 entries/append versus current-v342.1/current-v440.9. Mode frequencies vary even with the same DLL. Same-e06-DLL calibration30 observations produced throughput deltas around±2%, p99 up71.6% and allocation up9.5% in some points. Staged e06/current-v3 medians reversed2127/2725 then2992/2961. These do not erase canonical FAIL or establish a unique cause of every historical R18/R19 loss.

Candidate1 (reservation/frontier) passed408 tests but only10/16 cells after360 observations. Timer-only candidate passed410 tests but12/16 after345 observations (DLL2007AE4C…, source ZIPA1E393CD…). Preserve these candidates and all rounds. The current report archives timer-only results in timer-delivery-report.

Additional probes retain100 observations. Reduced collector notifications did not improve and were rejected. Fast admission versus timer-only, ten paired long runs:3/5-voter throughput+15.3%/+27.9%,5-voter allocation−19.5%, but p99+15.3%; main acceptance uses the separate full final matrix. Flat Core fields and exact old TCP completion failed their isolated controls and remain rejected. Never include them live without new evidence.

Valid early traces diagnostic-v3/traces-v3 show32→0 empty dispatches in16384 commands, with about4800 old CTS waits/4300 timeout expirations. Warmup diagnostics after2048 still JIT506–1272 methods/125–252ms versus34–36/6–13ms after16384; the latter also changes retained log and is not a replacement workload. No global JIT/ThreadPool settings changed.

Final-source diagnostic-release/traces-release covers5n16KiB Pipeline,2048+4096, exact primitive-long QPC fences. e06/before/after process CPU11140.6/11250.0/9843.8ms versus wall5129.4/5118.2/4990.9ms; membership queries0. Stack samples include blocked threads and are NOT CPU time. Store wait durations overlap. This diagnostic did not reproduce the long slow period of the main p99 point; its cause remains unresolved, and profiling is not capacity data. Traces-v1/v2 failed fencing attempts remain invalid evidence.

# Artifact map and caveat

All task-owned material is under RaftNet8/.artifacts/performance-recovery/, except checked transient GUID clusters under .artifacts/winner/data and normal build bin/obj/Python cache outputs. See README.txt. Original investigation remains .artifacts/regression-e06-investigation/. Frozen e06 is under .artifacts/hot-membership/baseline-runtime/comparison/. before-runtime/cwd is frozen R19. after-runtime/cwd is failed candidate1; final-runtime/cwd is historical timer-only; release-runtime/cwd is current. release-controls/release-add-load and validation/admission-* belong to the final DLL. qa is final report QA.

Scratch caveat: an exact-envelope probe reused tcp-runtime and overwrote the early rejected outer-wrapper binary. Early tcp-cwd source, raw observations and original hashes remain; the exact-envelope DLL is also tcp-envelope-runtime. Essential e06/R19/candidate1/timer/final binaries, source archives and all main observations are intact. Do not claim every discarded scratch DLL survives.
