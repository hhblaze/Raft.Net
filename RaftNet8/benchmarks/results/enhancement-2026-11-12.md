# Enhancement performance measurements — 2026-11-12

All **36** comparison cases have individual-request measurements and warmed BenchmarkDotNet results: 1/3/5 voters × 64/16384-byte commands × baseline unbatched/batched and all four current batching/pipelining combinations. The complete short matrix has **55,296** measured requests (1536/case); longer batched series add **73,728** requests (4096/case), for **129,024** stored unprofiled caller-latency samples. Both gains and regressions are retained.

## Environment and method

Windows 11 (10.0.26200.9974), .NET SDK 8.0.428, runtime .NET 8.0.34 x64 RyuJIT AVX2, concurrent workstation GC, 12 logical processors visible. BDN could not identify the processor model or change the power plan in this environment. No cross-host/network/TLS capacity claim follows from these single-host loopback results.

Both versions use DBreeze **1.142.2026.1016-beta DISK**, the same atomic business transaction and CRC index/term checkpoint on every node, and one engine/node containing log and a 256-key payload ring. Client completion means quorum durability plus leader apply; all replicas are validated afterward outside timing. Baseline is unchanged commit **8b991bd**, with weaker v1 log integrity/recovery and one-flight wire 1. Current uses checked v2/wire 2. Archived pre-.NET-8 contracts are not equivalent throughput baselines.

Concurrency 64; heartbeat 25 ms; election 5–6 s; RPC 2 s, identical in both versions. Unbatched means one entry per log/replication/apply batch with zero collection delay; current pipeline can independently be on/off. Batched uses 128 entries/256 KiB, current delay 1 ms, baseline drains queued commands. Pipeline window 4. Snapshot compaction is off. Data is on the H-drive workspace; the much slower incomplete C-temp diagnostic run is excluded. Actual binary hashes/data roots/timings are in raw JSON and the summary metadata.

BDN uses 3 warmup/8 measurement iterations, one 128-proposal invocation/iteration and a 128-command setup warmup. Its normalized mean is **amortized time**, not individual latency. Error/outlier/minimum-iteration warnings and 99.9% intervals remain in raw reports. Short latency runs independently warm 128 and measure 512 requests per round, repeated 3 times. Throughput below is median of the 3 rounds; p50/p99 are nearest-rank over all 1536 raw samples, with no outlier removal. Long series independently warm 2048 and measure 4096 once; they are a different retained-log/steady-load workload with no between-round confidence estimate.

## Short individual-request measurements

| Voters | Payload | Baseline batched /s | Current batched /s | Current pipeline /s | Baseline p50 / p99 ms | Current pipeline p50 / p99 ms |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 64 B | 3,877 | 5,165 | 5,704 | 15.78 / 20.73 | 11.42 / 22.20 |
| 1 | 16384 B | 1,077 | 1,647 | 1,615 | 58.26 / 95.38 | 36.90 / 55.26 |
| 3 | 64 B | 2,469 | 2,629 | 2,580 | 23.05 / 37.36 | 22.98 / 33.98 |
| 3 | 16384 B | 1,167 | 722 | 716 | 50.74 / 82.34 | 85.81 / 114.69 |
| 5 | 64 B | 1,516 | 1,950 | 2,322 | 44.03 / 67.67 | 27.67 / 41.29 |
| 5 | 16384 B | 866 | 549 | 517 | 68.96 / 91.56 | 116.54 / 175.18 |

All 36 configurations, including unbatched pipeline and the batched-without-pipeline tails/allocations/ranges, are in [short CSV](enhancement-latency-short-2026-11-12.csv). Managed allocation is process-wide during the timed caller interval across all co-located replicas, not retained heap or per-node memory.

## Longer series

| Voters | Payload | Baseline batched /s | Current batched /s | Current pipeline /s | Baseline p50 / p99 ms | Current pipeline p50 / p99 ms |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 64 B | 3,431 | 4,176 | 4,376 | 17.19 / 25.77 | 15.14 / 21.77 |
| 1 | 16384 B | 1,590 | 1,610 | 902 | 40.65 / 56.08 | 67.94 / 94.64 |
| 3 | 64 B | 2,213 | 2,011 | 2,557 | 29.80 / 34.08 | 24.20 / 36.32 |
| 3 | 16384 B | 1,202 | 742 | 631 | 50.27 / 69.53 | 105.40 / 128.53 |
| 5 | 64 B | 1,471 | 1,351 | 1,099 | 42.11 / 61.53 | 59.81 / 74.02 |
| 5 | 16384 B | 422 | 280 | 474 | 174.87 / 243.47 | 136.11 / 156.29 |

The [long CSV](enhancement-latency-long-2026-11-12.csv) contains all 18 batched configurations. Longer log growth and filesystem/scheduler behavior materially change throughput/tails; compare like workloads. In particular large-command multi-voter paths have regressions and variable pipeline benefit. Small-command improvements are not universal: the BDN 128-command bursts and the longer series can rank configurations differently. These data do not support a blanket claim that the new implementation is faster.

## BenchmarkDotNet allocation and throughput matrix

| Case | Mean μs/proposal | StdDev μs | Managed KiB/proposal |
| --- | ---: | ---: | ---: |
| Baseline-1n-16384B-Batched | 635.49 | 20.20 | 123.20 |
| Baseline-1n-16384B-Unbatched | 7,448.68 | 317.95 | 151.04 |
| Baseline-1n-64B-Batched | 249.78 | 25.33 | 6.35 |
| Baseline-1n-64B-Unbatched | 6,476.98 | 149.23 | 39.46 |
| Baseline-3n-16384B-Batched | 2,231.78 | 167.37 | 347.73 |
| Baseline-3n-16384B-Unbatched | 12,171.60 | 1,546.03 | 459.16 |
| Baseline-3n-64B-Batched | 364.08 | 48.85 | 15.75 |
| Baseline-3n-64B-Unbatched | 10,164.28 | 152.52 | 124.28 |
| Baseline-5n-16384B-Batched | 1,494.87 | 68.48 | 582.92 |
| Baseline-5n-16384B-Unbatched | 16,262.70 | 653.67 | 771.61 |
| Baseline-5n-64B-Batched | 487.02 | 55.29 | 25.48 |
| Baseline-5n-64B-Unbatched | 15,068.42 | 720.95 | 210.38 |
| Current-1n-16384B-Batched | 737.29 | 11.37 | 120.51 |
| Current-1n-16384B-Pipeline | 735.09 | 13.34 | 120.77 |
| Current-1n-16384B-Unbatched | 8,997.05 | 210.09 | 169.85 |
| Current-1n-16384B-UnbatchedPipeline | 8,958.57 | 124.55 | 169.91 |
| Current-1n-64B-Batched | 192.05 | 7.82 | 5.64 |
| Current-1n-64B-Pipeline | 187.88 | 4.16 | 5.63 |
| Current-1n-64B-Unbatched | 8,246.39 | 114.01 | 58.06 |
| Current-1n-64B-UnbatchedPipeline | 8,230.25 | 135.32 | 58.09 |
| Current-3n-16384B-Batched | 1,608.84 | 140.95 | 375.83 |
| Current-3n-16384B-Pipeline | 1,456.66 | 51.33 | 380.46 |
| Current-3n-16384B-Unbatched | 17,519.85 | 300.43 | 729.93 |
| Current-3n-16384B-UnbatchedPipeline | 16,747.81 | 591.35 | 730.65 |
| Current-3n-64B-Batched | 387.81 | 16.83 | 17.88 |
| Current-3n-64B-Pipeline | 400.47 | 33.17 | 16.31 |
| Current-3n-64B-Unbatched | 19,072.93 | 499.46 | 395.28 |
| Current-3n-64B-UnbatchedPipeline | 19,298.59 | 1,996.25 | 390.92 |
| Current-5n-16384B-Batched | 3,889.53 | 113.22 | 607.31 |
| Current-5n-16384B-Pipeline | 3,909.51 | 198.18 | 630.93 |
| Current-5n-16384B-Unbatched | 25,717.99 | 614.31 | 1,249.63 |
| Current-5n-16384B-UnbatchedPipeline | 24,314.08 | 353.75 | 1,247.49 |
| Current-5n-64B-Batched | 470.22 | 28.91 | 27.04 |
| Current-5n-64B-Pipeline | 538.58 | 111.78 | 29.31 |
| Current-5n-64B-Unbatched | 24,691.87 | 530.82 | 703.14 |
| Current-5n-64B-UnbatchedPipeline | 23,247.19 | 262.69 | 674.32 |

The [BDN CSV](enhancement-bdn-2026-11-12.csv) includes throughput derived from the mean, sample count and allocations. Full confidence intervals/GC counts/measurement details are retained in [main 30-case BDN report](../../.artifacts/autonomous/benchmarks/bdn-final/results/Raft.Benchmarks.DurableClusterBenchmarks-report-github.md) and [additional 6-case report](../../.artifacts/autonomous/benchmarks/bdn-unbatched-pipeline/results/Raft.Benchmarks.DurableClusterBenchmarks-report-github.md).

## Iterations and validation

The retained storage optimizations avoid LRU scans on ordinary append/commit, answer known term ranges from cache with one lazy transaction for misses, and use checked total-byte metadata when committing through the tail. Replacement/compaction/reopen and randomized DBreeze/model/list tests protect those results. A separate nonblocking cache-read worker-bypass experiment passed safety tests but showed inconsistent performance (including a slower 5-node long run), so it was reverted; it is not part of these final results. CPU EventPipe traces are diagnostic only: async rundown/ConditionalWeakTable overhead distorts profiled timing.

The earlier microbenchmark generator failed on duplicate aliased Raft return types and executed zero cases; those failed reports are excluded. Corrected generators return the same result references as object, preserving measured work. All 29 micro Dry cases and 6 warmed wire cases passed, checked separately from this durable comparison. Dry numbers are execution checks, not performance evidence.

Late production corrections affect only legacy-wire control rejection and offline migration preflight; measured proposal/storage/wire 2 paths are unchanged. See [validation and coverage](../../docs/VALIDATION.md) and [benchmark commands/limitations](../README.md). Regenerate the summary with `python benchmarks/summarize.py` from RaftNet8; it refuses missing/incomplete case sets.

Raw sources: [90 short measurements](../../.artifacts/autonomous/benchmarks/latency-final.json), [18 unbatched-pipeline measurements](../../.artifacts/autonomous/benchmarks/latency-unbatched-pipeline.json), [long batched](../../.artifacts/autonomous/benchmarks/latency-long-batched.json), [long pipeline](../../.artifacts/autonomous/benchmarks/latency-long-pipeline.json), and [compact summary/metadata](enhancement-summary-2026-11-12.json). Binary/source provenance and weaker baseline durability must accompany reuse of these figures.
