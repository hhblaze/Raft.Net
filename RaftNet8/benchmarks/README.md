# Benchmarks

**Accepted baseline,2026-11-14:** the user accepted the Hot Add/Remove implementation at commit `d44147effdb20271854dd758cc6f9073543a554d`. Use this version for future fresh paired comparisons with membership enabled. [Baseline identity and evidence](baselines/hot-membership-2026-11-14.json) pin the source/runtime and retain the historical limitations. Existing e06/R18/R19 reports are preserved; adoption does not rewrite their measured PASS/FAIL results.

The user's subsequent [lower-load recheck of long5-node16KiB Pipeline](results/p99-recheck-5n-16k-pipeline-2026-11-14.html) uses the same binaries and10 new rounds per version. Fresh e06/current p99:227.69/212.27ms (−6.77%); throughput706.87/680.75 (−3.69%). This new cohort passes the working tolerances, but current absolute p99 did not improve over the previous204.89ms. All30 observations/122880 latencies remain separate; the original full matrix below is unchanged. [JSON](results/p99-recheck-5n-16k-pipeline-2026-11-14.json) / [CSV](results/p99-recheck-5n-16k-pipeline-2026-11-14.csv).

The [e06 / R19 / corrected runtime HTML](results/performance-recovery-e06-2026-11-14.html) records300 fresh measurements /691200 latencies, with5–10 paired rounds per distributed workload. **14/16 cells passed all three historical tolerances.** Throughput and allocation pass16/16. Historical p99 failures: short5-node64B Batched (+13.09%) and long5-node16KiB Pipeline (+101.65%), both after ten pairs. Pipeline and16KiB each pass7/8. Smaller throughput losses (−3.63% short3-node64B Pipeline,−3.85% long5-node64B Pipeline) remain explicit; tolerance alone does not establish absence of systematic loss. User adoption of the new baseline leaves these observations intact. All individual rounds, same-DLL controls, warmup diagnostics and rejected experiments are retained. R18/R19 are preserved; neither8b991bd nor DotNext was remeasured. [CSV](results/performance-recovery-e06-2026-11-14.csv) / [JSON](results/performance-recovery-e06-2026-11-14.json).

Reproduce the fresh-process controls from frozen runtime/source-only directories described in `.artifacts/performance-recovery/README.txt`:

```sh
python -X utf8 -B benchmarks/run_recovery.py --config .artifacts/performance-recovery/release-config.json --out .artifacts/performance-recovery/new-run --variants e06,before,after --cases 3-64-Batched,3-64-Pipeline,3-16384-Batched,3-16384-Pipeline,5-64-Batched,5-64-Pipeline,5-16384-Batched,5-16384-Pipeline --series short,long --rounds 5
python -X utf8 -B benchmarks/render_recovery.py .artifacts/performance-recovery/release-five .artifacts/performance-recovery/release-extra-short .artifacts/performance-recovery/release-extra-long --validation .artifacts/performance-recovery/release-validation-summary.json --out benchmarks/results/performance-recovery-e06-2026-11-14.html
```

Use new output directories. Capacity runs must be separate from profiling/build/tests. `prepare_recovery_diagnostics.py`, `trace_recovery.ps1` and `analyze_recovery_traces.ps1` create isolated diagnostic builds and explicit QPC windows; their stack samples include waiting threads and are not CPU-time estimates. Trace dependencies are cached tools, not new production package references. Queue tracing calls are compiled out of normal builds. The410-test validation, HumanTest,29 Dry cases and four completed Add-under-load smoke cases establish functional evidence, not16/16 performance acceptance.

The latest [R19 HTML report](results/winner-r19-2026-11-14.html) refreshes only the current **wire3/raft8-v4** implementation: 180 new measurements / 307,200 latencies. Baseline and DotNext values are copied unchanged from R18 (another 180 observations / 307,200 latencies), explicitly labelled historical. It preserves R18's four charts, filters, full short/long tables and CSV/JSON, with an additional comparison against our R18 version. These are different-date observations, without paired-win claims. Pipeline exceeds both historical references at12/12 points; our own R18 throughput is higher at11/12 points (largest R19 loss25.5%, largest p99 increase117.2%). All losses remain prominent. See [R19 reproduction](Raft.Comparison/README.md#r19-current-only-refresh).

The [hot-membership HTML report](results/hot-membership-2026-11-14.html) compares wire3/v4 with the frozen immediately preceding HEAD e06c4f9 (wire2/v3), including full Add under load. It contains four charts, per-workload throughput/p99/allocation changes, all individual rounds and measured source/binary identities. This is a separate experiment from the historical competitor comparisons below.

The historical [R18 five-round HTML report](results/winner-r18-2026-11-13.html) records48short+24long configurations,360measurements and614400 individual request latencies. Fixed Pipeline wins median throughput against both baseline8b991bd and DotNext5.26.2 at12/12points, including8/8distributed. All raw slow rounds, paired wins, latency/managed-allocation losses, binary/source hashes and grouping/commit-notification differences remain explicit. Independent numeric/source/CSV and offline desktop/mobile/print/noJS/four-chart QA passed. See [current reproduction commands](Raft.Comparison/README.md).

The earlier [three-round report](results/dotnext-comparison-2026-11-13.html), R4/R5/R11 and all before/after artifacts remain historical evidence. No prior competitor numbers are mixed into R18.

Run from `RaftNet8/` in Release, one workload at a time on an otherwise idle host. Preserve runtime/storage/parameters and raw JSON. [The HTML enhancement report](results/enhancement-2026-11-12.html) presents actual before/after results with charts, filters, sortable tables and limitations; it opens offline directly from disk. The [Markdown report](results/enhancement-2026-11-12.md) and CSV/JSON remain available. Older `2026-11-12.md` reports are the initial v1 resurrection baseline.

Regenerate HTML from the saved summary with `python benchmarks/render_html.py` (standard library only, no raw artifacts required). `python benchmarks/summarize.py` recomputes CSV/JSON/Markdown and HTML from the complete raw measurements. The HTML embeds its data and pinned, MIT-licensed Chart.js; no server, CDN or network is needed to view it.

## Durable cluster comparison

For the new feature comparison, preserve baseline runtime/source ZIP and current runtime in separate directories before measuring. The runner verifies the unchanged business/checkpoint implementation, alternates five HEAD/current pairs per topology/payload, then runs three full-Add rounds. All nodes use separate engines in one benchmark process over real TCP; functional process tests cover separate OS processes. Pipeline, H-drive storage and acknowledgment semantics are identical. Add changes3→4 or5→6 voters; its cost includes extra replication and catch-up.

~~~sh
python -X utf8 -B benchmarks/run_hot_membership.py --baseline .artifacts/hot-membership/baseline-runtime/comparison/Raft.Comparison.dll --source .artifacts/hot-membership/baseline-source-e06c4f9.zip --current .artifacts/hot-membership/current-runtime-delivery/Raft.Comparison.dll --out .artifacts/hot-membership/performance-delivery
python -X utf8 -B benchmarks/render_hot_membership.py .artifacts/hot-membership/performance-delivery --out benchmarks/results/hot-membership-2026-11-14.html
~~~

Use a new output directory for each rerun; completed measurements are never overwritten. Defaults:8192 commands after2048 warmup, concurrency64,52 measurements. Generated HTML is standalone/offline with embedded SVG/data; CSV and summary JSON are beside it. The renderer rejects incomplete experiments and changing runtime/source metadata. No v4 converter or historical competitor rerun is implied.

```sh
# 36 cases: 1/3/5 voters, 64-byte/16-KiB commands, all batching/pipeline combinations
dotnet run --project benchmarks/Raft.Benchmarks -c Release -- --filter '*DurableClusterBenchmarks*' --exporters json markdown --artifacts .artifacts/benchmarks/bdn

# One functional smoke case; Dry is not a capacity result
dotnet run --project benchmarks/Raft.Benchmarks -c Release -- --filter '*DurableClusterBenchmarks*Current-1n-64B-Pipeline*' --job Dry

# Individual caller latency, raw samples, throughput and process-wide allocations
dotnet run --project benchmarks/Raft.Benchmarks -c Release -- --latency --operations 512 --repeats 3 --concurrency 64 --out .artifacts/benchmarks/latency.json
```

The comparison uses the **unchanged eight C# source files from commit 8b991bd**, isolated by an assembly alias in [Raft.Baseline](Raft.Baseline/README.md). Both versions target .NET 8 and DBreeze 1.142.2026.1016-beta. The new version uses raft8-v2/wire2 with pre-vote and checksummed durable frontier; baseline is raft8-v1/wire1 and one replication flight. Its integrity/recovery/feature contract is weaker. The archived pre-.NET-8 projects have different APIs/application/acknowledgment semantics and are not an equivalent throughput baseline.

Both use real loopback TCP, **disk DBreeze**, the same synchronous business transaction/checkpoint implementation and one engine per node containing log and business tables. Every replica stores payloads in a 256-key ring, command count and CRC `(index,term)` checkpoint atomically. No effects are replaced with an in-memory counter. Client success waits for quorum durability and leader application; after measurement all replicas' durable counters/checkpoints are checked. Compaction is off to compare retained logs.

Each topology/payload has baseline Unbatched/Batched and all four current combinations: Unbatched, Batched, Pipeline (batched), UnbatchedPipeline. Unbatched uses one entry per proposal/replication/apply batch and no collection delay. Current application uses128-entry/256KiB bounds; local groups and wire2 default1051264B, with off-owner2ms small/8ms in-flight bulk/1ms single-voter collection. Baseline retains256KiB and drains queued commands. The ordinary BDN/two-way runner defaults to separate-table v2; the final three-way R18 report explicitly selects prefixed v3. Do not merge these storage configurations. Pipeline variants add the bounded four-message window. Pipeline has no network benefit with one node. Concurrency is 64, deliberately a closed-loop saturation workload; it is not an open-loop latency SLO test.

The same timing configuration is used in both versions: 25-ms tick, heartbeat1, election `[200,240)` ticks (5–6 seconds), RPC2 seconds. This accommodates the measured disk/backlog tails, including the deliberately unbatched experiment. It is **not** a recommendation to copy benchmark timeouts into every deployment. The initial temporary-C-drive diagnostic run was much slower and lost baseline leadership with default short deadlines; it was excluded from the final comparison.

Default data parent is `.artifacts/autonomous/benchmarks/data` beneath the process working directory. Set an absolute **`RAFT_BENCHMARK_ROOT`** to select the actual volume; it is inherited by BDN children. Both versions must use the same volume/settings. Every cluster creates/deletes only its own checked GUID child. Keep reports on a separate path from that data root. H-drive workspace and temporary C-drive results in this environment differed substantially, so storage location is part of the result.

`DurableClusterBenchmarks` defaults to warmup3/measurement8, one invocation of **128 proposals** per iteration, concurrency64 and unroll1. Global setup elects a leader and warms 128 commands. Fixed iteration/invocation counts bound log growth; setup/cleanup/replica validation are excluded. BDN reports managed allocations and mean **amortized time per proposal**, which can be converted to throughput. That mean is not the latency experienced by an individual request.

`--latency` starts a fresh cluster for every round, warms128 by default (`--warmup` overrides it), records every request from before submission until its own completion, checks all replicas afterward, and stores raw samples plus nearest-rank p50/p99, throughput and process-wide managed bytes/proposal. Defaults512 requests ×3 rounds produce1536 samples per case, 55,296 across36cases. JSON includes expected count/completion flag, runtime/OS, data root, binary hashes and timings. Partial reports from a failed run are incomplete evidence. `--case Current-3n-64B-Pipeline` filters the latency runner; unmatched filters fail rather than silently report success. The report also includes4096-request series after2048 warmup for batched variants; these are separate workloads.

## Microbenchmarks

```sh
dotnet run --project benchmarks/Raft.Benchmarks -c Release -- --list flat
dotnet run --project benchmarks/Raft.Benchmarks -c Release -- --filter '*WireBenchmarks*' '*AppendBenchmarks*' '*ReadBenchmarks*' '*CoreReplicationBenchmarks*' '*TcpLoadBenchmarks*' --job Dry
dotnet run --project benchmarks/Raft.Benchmarks -c Release -- --filter '*WireBenchmarks*' --job Short --exporters json markdown
dotnet run --project benchmarks/Raft.Benchmarks -c Release -- --filter '*AppendBenchmarks*' --job Short
```

| Class | Measurement / exclusions |
| --- | --- |
| AppendBenchmarks | V2 durable batches1/32/128, payload256, memory/disk; excludes setup |
| ReadBenchmarks | V2 cache-off/on tail/term/catch-up over10,000 entries, memory/disk; owned result allocations included |
| CoreReplicationBenchmarks | Actual reducer/driver, 3 nodes and v2 memory stores, deterministic delivery; excludes sockets/business I/O |
| WireBenchmarks | Explicit wire1/2, pooled heartbeat/32-entry encoding and owned decoding; excludes sockets |
| TcpLoadBenchmarks | 3-node v2 memory/disk TCP,256 proposals; application is a volatile counter, so this is **not** the durable comparison |

Micro write/core/TCP iterations reset their bounded workloads. Read ranges materialize results; decoding copies command bytes before pool return. Minimum-iteration/short-job warnings and inability to change the host power plan must be retained with results. Dry/JIT samples are functional checks only. Run longer jobs with representative payloads, network/storage tails, applications and slow followers before capacity planning. Loopback results do not predict cross-host deployment throughput.
