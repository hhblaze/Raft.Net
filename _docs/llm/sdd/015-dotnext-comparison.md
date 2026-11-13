# Three-way .NET 8 Raft comparison

User request: compare current Raft.Net and baseline8b991bd with DotNext.Net.Cluster, and deliver a new HTML report with charts and tables. Existing regressions must remain visible; this task does not silently change either measured Raft.Net implementation.

## Contract

- Target net8.0. NuGet inspection on2026-11-12 identifies DotNext.Net.Cluster5.26.2 (commit39ff4fe882e68a5ea90818b41adb97cdc3ec60d8) as the last5.x package with a net8.0 asset. Latest6.12.1 has only net10.0; disclose its exclusion rather than relabel5.x as latest overall.
- Pin the package/lock dependencies in an isolated benchmark project; use stock DotNext TCP and persistent WAL, with explicitly verified durable flush settings. Never use ConsensusOnlyState as a durable comparison.
- Reuse the unchanged baseline/current cluster and exact DBreeze business application transaction. For DotNext, preserve every command as a separate Raft entry, atomic payload-ring/count/CRC(index,term) checkpoint writes, and client completion after quorum commitment and leader application. Validate all replicas and reopened business state outside measurement. Explain native-WAL versus DBreeze-log differences.
- Same host/volume, TCP loopback,1/3/5 voters,64-byte/16-KiB commands, concurrency64, no compaction; record timer mapping, batch/count/byte/collection limits, GC/runtime/CPU and binary/source hashes. DotNext-native replication behavior must be described without claiming identical internals.
- Fresh short matrix: baseline Unbatched/Batched, current four batching/pipeline combinations, DotNext Unbatched/Batched =48 cases. Warm128,512 measured ×3 rounds. Longer batched matrix: baseline Batched, current Batched/Pipeline, DotNext Batched =24 cases; warm2048,4096 measured ×3 rounds. Rotate implementation order between rounds; run only one cluster at a time. Preserve individual samples and fail on incomplete/incorrect runs.
- Compare medians and observed ranges, nearest-rank pooled p50/p99 and process-wide managed allocations. No BDN amortized mean is passed off as request latency. Keep separate workloads; ranges are not confidence intervals.
- New standalone Russian HTML, embedded chart library/data, filters and sortable complete tables, multi-node emphasis, measured ratios versus both peers, methodological/feature/version limits and raw/CSV/JSON/reproduction links. Preserve the earlier report.
- Verify adapters (durable reopen/application completeness, count/payload/checkpoint consistency, startup/stop), all case coverage, independent recomputation, no browser/network errors, offline/mobile/print views and source/baseline immutability. Run required build/tests after integration. Update OKF and commands.

## Delivery evidence — 2026-11-13

Delivered [the new standalone HTML](../../../RaftNet8/benchmarks/results/dotnext-comparison-2026-11-13.html), two CSV matrices, saved summary and reproducible runner/renderer. The earlier two-way report is preserved.

Both final processes completed successfully: short144/144 and long72/72 round measurements,48/24 distinct cases,368,640 individual samples. All replicas and closed/reopened business databases passed every round. Native append/apply/batch counters and per-round statistics are verified from raw data; independent calculations matched every summary/CSV value. Measured binary hashes still match the files, and both workloads use the same source/dependency/runtime provenance.

Locked solution restore/build passed with zero warnings/errors;221/221 regression tests and the dedicated acknowledgment/replay adapter checks passed. Offline Chromium verified all72 table rows, all four chart datasets against JSON, both ratio references, unfiltered overview tables, filters/p50/reset/sorting,390px layout, printing and no-JS fallback. No runtime/network errors. Raw and saved-summary regeneration produce identical HTML. Screenshots, raw samples, source audit and TRX are under RaftNet8/.artifacts/dotnext-comparison/.

The current-version large-payload multi-voter regression remains: long16KiB pipeline medians are about21%/38% below DotNext on3/5 voters and about44%/46% below baseline. These are whole-stack results for this measured environment, not an isolated algorithm ranking. No consensus/storage optimization, commit, push or publication was performed in this comparison task.

Final compatibility check: the existing BenchmarkDotNet DurableClusterBenchmarks Current-3n-64B-Pipeline Dry case executed successfully (1 case, exit0). Its short-iteration warning is retained in .artifacts/dotnext-comparison/bdn-smoke.log; these functional smoke numbers are excluded from all three-way capacity/latency data.
