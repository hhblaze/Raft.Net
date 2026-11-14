# Recover e06 performance without losing hot membership

Status: runtime fixes and functional validation delivered; performance acceptance remains OPEN (14/16 cells), authorized 2026-11-14.

Preserve net8.0, DBreeze 1.142.2026.1016-beta, wire3/raft8-v4, all membership APIs and persistence/CRC guarantees. Never increase collection deadlines to improve benchmark scores. Preserve R18/R19 and do not rerun 8b991bd or DotNext.

## Experiment

Use frozen e06c4f95edd6836c9a8782ecad2542bef298fa32 wire2/v3, frozen pre-fix current wire3/v4, and corrected wire3/v4. Include current wire2/v3 controls to separate general runtime changes from the new protocol. Run each measurement in a fresh process, alternate order, retain raw samples, binary/source hashes and the unchanged business transaction. Main matrix: 3/5 voters × 64/16384 bytes × Batched/Pipeline × short (128 warmup/512 requests) and long (2048/4096), concurrency64. Five paired rounds, extend inconclusive cells to ten. One-voter/unbatched are regression controls.

Working per-cell tolerances against fresh e06: throughput >=95%, managed allocations <=105%, p99 <=110%. These are measurement tolerances, not permission to accept systematic loss. Separate capacity timing from diagnostics and long-JIT-warmup experiments. CPU stack sampling must distinguish blocked threads; traces need exact measurement fences.

## Candidate fixes and validation

Evaluate TCP completion topology, duplicate/empty proposal notifications, idle membership scans, and full Recover allocations for a single cached byte counter separately. Retain only changes supported by measurements or direct allocation evidence; preserve rejected experiments. Core remains a pure immutable reducer. All disk effects publish only after Commit and all admission limits remain bounded.

Run the full regression suite, HumanTest, BenchmarkDotNet smoke and membership under load/cancellation/elections/recovery/snapshot tests. Deliver an offline HTML e06/before/after report with every cell, all rounds, honest tolerance failures, provenance and limitations. Update OKF and usage guidance where behavior changes.

Initial read-only investigation is under `.artifacts/regression-e06-investigation/`; implementation experiments and frozen pre-fix runtime/source metadata are under `.artifacts/performance-recovery/`. No single cause of all historical losses, or complete recovery, has been established.

## Implemented runtime changes

Hold the proposal-ready reservation through the complete owner-side group drain. Evaluate deadline and reserve against one metadata snapshot under the same gate. A timer callback only wakes the bounded change channel; it cannot assign indices, drain bytes, inspect the reducer or force a flush. Recheck the actual oldest proposal/deadline after every wake, including stale timer callbacks. One reusable timer per node replaces linked CTS/timer/exception allocation for every wait. The2ms/8ms/1ms deadlines and all byte/count/admission bounds are unchanged.

Expose DBreeze's cached durable byte frontier via ILogStoreProgress and poll it directly from the adapter after a successful write. Retain full recovery for initialization and the old extended/compatibility fallback. Publish after DBreeze Commit and preserve storage→cache lock order.

Candidate1 (reservation+frontier) passed408 tests but only10/16 performance cells after360 measurements. The timer-only candidate passed410 tests and12/16 performance cells after345 measurements; its2007AE4C… DLL and A1E393CD… source archive remain historical evidence.

The final candidate also acquires an immediately available proposal slot without creating a linked CTS. Only the bounded wait path links caller/lifetime cancellation. Check both tokens before admission and under the queue gate; caller cancellation of an accepted command still only cancels waiting. A ten-pair isolated long-series experiment versus the timer-only candidate gave+15.3%/+27.9% throughput on3/5 voters and−19.5% allocation on5 voters, but+15.3% pooled p99 on5 voters. That tradeoff is retained in the evidence, not assumed to meet acceptance. Reducing per-proposal collector wakeups did not improve the isolated controls and was rejected.

The final candidate passed410/410 full tests (zero skips,2m31s), HumanTest and29 BDN Dry cases. This includes continuous writes through Add and automatic current-leader removal on3/5 voters with stable retry operation IDs. Final DLL SHA25691FD2E34E822614B182E3257F797D0CAD29A0E856E4FF1318207EE8E87E3CF75; release-source.zip SHA25616690FDAB4563C603FC91EC9B41FAB4F2BCAC0A83E545E0046D33828D7F3E07F.

Diagnostics found no ordinary membership queries; valid traces show32→0 empty dispatches but about4800 collector deadline waits/4300 cancellation expirations per16384 commands in the old collector. Stack samples include blocked threads; use separate process CPU. Extended warmup diagnostic does not replace canonical timing:2048 warmup still left506–1272 methods/125–252ms JIT during4096 requests, versus34–36/6–13ms after16384. No host JIT/ThreadPool settings were changed.

## Acceptance evidence and remaining work

The offline report `RaftNet8/benchmarks/results/performance-recovery-e06-2026-11-14.html` compares fresh e06 / frozen R19 / corrected runtime:300 fresh-process observations,691200 latencies,5–10 paired rounds per cell. Raw sets `release-five`, `release-extra-short`, `release-extra-long` retain every repeat. Throughput and allocation pass16/16; all-three-metric acceptance passes14/16. The open cells are short5 voters/64B/Batched (p99+13.09%) and long5 voters/16KiB/Pipeline (p99+101.65%). Both already have ten pairs. Do not drop the slow observation or claim full recovery. Pipeline and16KiB each pass7/8 cells. Short3-voter64B Pipeline remains−3.63% throughput after ten pairs and long5-voter64B Pipeline−3.85%; tolerance does not establish absence of systematic loss.

Additional controls include single-voter/unbatched and full Add-under-load cases, plus same-e06-DLL A/B calibration30 observations. Separate version controls demonstrate fragmented packets with wire2/v3 as well as wire3/v4. Flat Core fields and exact old TCP completion did not improve their isolated controls and were rejected. Later favorable modes do not replace failed canonical observations. Full causal attribution of the residual tail latency and smaller throughput losses remains open, without increasing collection windows, changing the workload, or weakening durability. Slow large-command periods occurred across e06 and both wire3 binaries in different observations; this does not prove their unique cause.

Numeric QA independently recomputed all48 aggregates/16 decisions from300 raw SHA-verified files and691200 samples. Offline Chromium QA passed for all144 chart values, filters, desktop/mobile widths, print and no-JavaScript fallback. R18/R19 remain intact. No commit/push/package publication.
