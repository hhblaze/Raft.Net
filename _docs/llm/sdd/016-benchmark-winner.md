# Make current Raft.Net the measured winner

Achieved local measured criterion in R18,2026-11-13: make the current .NET8 implementation win among baseline8b991bd, current and DotNext5.26.2, using appropriate modern .NET techniques; no arbitrary fixed percent target; deliver a new HTML report.

## Required result

- Optimize real production paths, retaining pure immutable core/effects, durable-before-ACK, quorum/current-term rules, atomic application checkpoint, bounded caches/admission/frames and compatibility. No weakened integrity, skipped business writes or volatile-only acknowledgments to improve a chart.
- Preserve baseline and DotNext implementations/workloads; retain the earlier report. Reference current binaries/source are snapshotted under .artifacts/winner/ before production changes.
- Investigate actual batch sizes, queries, transaction counts/times, copying, channel scheduling and replication behavior. Diagnostics are separate from uninstrumented capacity measurements.
- Prefer measured improvements to default batched/pipelined operation across1/3/5 voters,64B/16KiB and short/long workloads. Preserve all batching/pipeline toggle cases and losses. Do not pick the best current mode independently per result after measurement or hide latency/allocation regressions.
- Iterate focused red/green correctness tests, benchmarks and fixes; then full build/tests, simulations, real recovery/membership/demo, compatibility and appropriate benchmark smoke. No phase approval gates, commits or publication.
- Final fresh three-way comparison with equivalent durable business work, raw samples/metadata, repeated estimates and a new standalone Russian HTML with complete tables/charts. A partial improvement or one fast case does not prove the requested winner.

## Initial evidence / candidates

Source inspection shows follower MatchEntries always emits ReplaceSuffix even when extending exactly LastIndex+1. That causes suffix accounting/lookup, LRU invalidation and configuration refresh queries for ordinary command replication. Distinguish actual conflicts from pure append, with persisted-boundary and rollback regression checks.

The runtime already uses Channels, but cached log reads still enqueue separate worker operations and proposal batching stops at intervening control messages in the same queue. Previous unmeasured/speculative cache-worker bypass was reverted; any replacement needs ownership/concurrency/recovery evidence and fresh repeatable measurements.

The final R18 comparison passed the predeclared local criterion: fixed Pipeline median throughput exceeds both references at all12points, including8/8distributed points. Five rounds,360measurements,614400 individual latency samples. See RaftNet8/benchmarks/results/winner-r18-2026-11-13.html and the OKF benchmark record for exact source/binary/raw/QA evidence. This does not claim universal/statistical superiority, lower allocations or uniformly better p99.

Final workload protocol is five rounds for each of48 short and24 long configurations, unchanged short128-warmup/512-measure and long2048-warmup/4096-measure,64 closed-loop callers, all references freshly rerun. Fixed primary current mode remains Pipeline everywhere. Audit all12 topology/payload/length combinations against both references by median throughput, include3/5-node subcounts, per-round wins, all raw samples, latency/allocation losses and observed ranges. Do not select a different current mode/round afterward or mix prototype results with the final schema. Five-round median leadership is the defined local measured criterion, not proof of universal/statistical superiority.

Local proposal groups now have a separate production limit (default1MiB+2688B) while wire2 defaults to the same1051264B budget and application remains256KiB; baseline/DotNext collectors retain256KiB. This explicit implementation difference must appear next to results. The shared ComparisonMachine transaction and every business write remain unchanged. Specifications017/018 describe the prefixed layout, bounded group commit, dedicated workers and already-queued cumulative response ordering. Specifications024–027 cover independent wire/application bounds, in-flight feedback and commit-notification coalescing. Final independent data/CSV/source/binary/audit and offline browser/mobile/print/noJS/chart QA passed. All four current and twelve R4/R5/R11 saved-summary outputs regenerate byte-identically.342tests and a fresh1400-run current-reducer fault matrix passed. Allocation loses all12points to both refs; p99 loses three individual reference comparisons. No commits/push/publication were performed.
