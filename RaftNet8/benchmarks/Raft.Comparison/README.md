# Raft.Net / baseline / DotNext comparison

Run from RaftNet8/. The standalone .NET8 runner compares baseline8b991bd, current Raft.Net and **DotNext.Net.Cluster 5.26.2**. NuGet package inspection found5.26.2 to be the last5.x/net8.0 package; latest6.12.1 targets net10.0 only and is not measured. The production library remains net8.0 and does not depend on DotNext.

## Current optimization comparison

The completed [R18 five-round report](../results/winner-r18-2026-11-13.html) meets the fixed Pipeline local median-throughput criterion at12/12points over both references, including8/8distributed. It is validated by342tests, a fresh1400-run current-reducer fault matrix, all614400samples, independent numeric/hash/CSV QA and offline browser/chart/mobile/print/noJS QA. Allocations lose all12points to both refs, and individual p99 losses remain explicit. Earlier [R11](../results/winner-r11-2026-11-13.html) and [R5](../results/winner-r5-2026-11-13.html) remain historical evidence. Preserve all completed reports/raw archives; exploratory results are not combined with them.

For a new reproduction, run these sequentially from RaftNet8/ on an otherwise idle machine. Use a fresh output folder/tag; the commands below do not overwrite the archived R4/R5/R11 data:

~~~sh
dotnet restore Raft.Net8.sln --locked-mode
dotnet build Raft.Net8.sln -c Release --no-restore
dotnet test Raft.Net8.sln -c Release --no-build --no-restore --logger 'trx;LogFileName=r18-reproduction-full.trx' --results-directory .artifacts/winner/tests
dotnet run --project benchmarks/Raft.Comparison -c Release --no-build -- --batched-only --unified --warmup 2048 --operations 4096 --repeats 5 --out .artifacts/winner/r18-reproduction/long.json
dotnet run --project benchmarks/Raft.Comparison -c Release --no-build -- --unified --warmup 128 --operations 512 --repeats 5 --out .artifacts/winner/r18-reproduction/short.json
python -B benchmarks/render_winner.py --artifacts .artifacts/winner/r18-reproduction --tag r18-reproduction
# Recreate the same outputs later from the verified summary:
python -B benchmarks/render_winner.py --summary benchmarks/results/winner-r18-reproduction-summary-2026-11-13.json --artifacts .artifacts/winner/r18-reproduction --tag r18-reproduction
~~~

Set RAFT_BENCHMARK_ROOT to the same data volume for every implementation; the recorded work uses .artifacts/winner/data. Successful GUID roots are removed only after stop/reopen validation. Failed roots, raw samples, frozen runtimes/source ZIPs and hashes remain in .artifacts/winner/. Do not modify measured C# or run builds/tests/another benchmark during timing.

Current v3 uses one physical Raft table with1.ToIndex(metadata),2.ToIndex(log),3.ToIndex(configuration), a packed checked RF3M state/frontier manifest and once-initialized incompatible-reader guards. Local groups default1051264 encoded bytes and128 entries. Wire2 now independently permits1051264B including metadata headroom; the shared business application remains256KiB/128 entries, so16KiB commands still apply at most15 per transaction. Baseline/DotNext proposal collectors remain256KiB; baseline wire remains256KiB, and native DotNext networking has independent aggregation. The common ComparisonMachine source/business work is unchanged. This is explicitly a comparison of different production grouping policies, not identical packet sizes.

Current proposal readiness uses an off-owner signal/deadline worker: small2ms, byte-limited up to8ms while other admitted work awaits quorum/application, single voter capped1ms. Deadlines start with the oldest queued command; count/byte/role bounds can end collection. Unbatched modes use count1/delay0. Current followers may coalesce only already-queued plain fresh contiguous frames, preserving original correlated replies after durability. Ordinary idle commit notifications piggyback next data/heartbeat; read/config/transfer/snapshot/retirement send paths remain, as do persisted commit state and client quorum-plus-leader-apply completion. Followers can learn/apply a frontier one logical heartbeat later plus I/O/network/owner delay. This policy and larger frame/pool overhead are part of reused figures.

The fixed audited mode is Pipeline for all1/3/5-node,64B/16KiB,short/long points. A complete final report uses five rounds,48 short+24 long cases and614400 individual latency samples, showing medians, paired wins, all observed ranges, p50/p99 and allocations. The renderer refuses partial/mismatched/instrumented final input. --diagnose is exploratory only; --proposal-batch-bytes 262144 is a current-only local-byte control, not a network-size control. Neither smoke results nor older competitor measurements are mixed into a new comparison.

## Earlier three-round comparison

~~~sh
dotnet restore Raft.Net8.sln --locked-mode
dotnet build benchmarks/Raft.Comparison -c Release --no-restore

# Validity checks: no early ACK, batching, actual no-op metadata, cancel/repeat, WAL/business reopen/replay
dotnet run --project benchmarks/Raft.Comparison -c Release --no-build -- --verify-adapter

# All48 cases: baseline2, current4 and DotNext2 modes x1/3/5 voters x64/16384 bytes
dotnet run --project benchmarks/Raft.Comparison -c Release --no-build -- --warmup 128 --operations 512 --repeats 3 --out .artifacts/dotnext-comparison/short.json

#24 batched cases; independently warmed long workload, also3 rounds
dotnet run --project benchmarks/Raft.Comparison -c Release --no-build -- --batched-only --warmup 2048 --operations 4096 --repeats 3 --out .artifacts/dotnext-comparison/long.json

# Verify complete matrices and recompute CSV/summary/standalone HTML (Python standard library)
python -B benchmarks/render_dotnext.py

# Regenerate from the saved verified summary after removing raw artifacts
python -B benchmarks/render_dotnext.py --summary benchmarks/results/dotnext-summary-2026-11-13.json
~~~

The new report is [dotnext-comparison-2026-11-13.html](../results/dotnext-comparison-2026-11-13.html). It embeds Chart.js/data and opens offline. The earlier two-way enhancement report is preserved. Raw reports and logs live under .artifacts/dotnext-comparison/; successful data GUIDs are deleted only after all nodes have stopped and their business databases have been reopened and checked. Failed data is retained for investigation. RAFT_BENCHMARK_ROOT can select another data volume; use the same volume for every implementation.

Run just one workload/cluster at a time, on an otherwise idle machine. --list lists cases. --case DotNext-3n-16384B-Batched selects one case; --warmup 128 --operations 128 --repeats 1 is a functional smoke, not part of the final matrix. Unmatched filters fail. The renderer refuses partial reports, missing cases/rounds, wrong workload counts and inconsistent raw samples/summary statistics.

## Measured contract

- Real TCP loopback, disk logs and durable DBreeze 1.142.2026.1016-beta business transactions on every node;64 closed-loop callers; fresh clusters per round, rotated case order. Identical zero payload content/length in all implementations, no snapshots/compaction or injected network faults.
- Client completion follows quorum log commitment and leader business apply. Every actual command is a separate Raft entry. The common ComparisonMachine stores a256-key payload ring, command count and CRC(index,term) checkpoint atomically. Post-measure validation checks all replicas and then close/reopens each DBreeze engine to check checkpoint, count, ring size and payload bytes.
- Raft.Net uses its DBreeze log/business engine. DotNext uses stock MemoryBasedStateMachine persistent WAL: AutoFlush, IntegrityCheck=true, caching with OnCommit eviction,65536 records/partition, background compaction never triggered. AutoFlush's partition flush calls FileWriter.FlushToDisk in the pinned source; append waits for it and for index-state persistence. This is not ConsensusOnlyState or an in-memory WAL.
- DotNextAuditTrail delegates native term/vote/append/read/commit to the pinned package, stages payloads in its native apply callback before eviction, and uses metadata-only range reads to include genuine empty entries. It batches the common business transaction and advances its public applied frontier/waiters afterward. It is a **benchmark adapter**, not a new supported production integration or a modification of DotNext's consensus algorithm.
- The DotNext proposal collector uses public WAL batch append, wakes native replication, and completes each batch after the applied committed prefix reaches it. Pending batches can coexist; it does not serialize admission on each batch's quorum round trip. Public force-replication tasks may wait for every peer, while client completion independently follows the quorum/apply frontier. Tasks are observed at validation/shutdown.
- Batched proposal/application limits:128 entries/256KiB with21 bytes of per-entry accounting;16KiB commands admit15 entries. Current/DotNext collection delay1ms; baseline drains queued commands. Unbatched means one local log/application entry and zero collection delay. DotNext's native network aggregation is retained and **does not share Raft.Net's MaxAppendBytes limit**. Thus the isolated label does not prohibit DotNext from transmitting queued entries together.
- Current Raft.Net additionally measures pipelining off/on (window4). DotNext's stock replication is not labelled as an equivalent configurable window. All measured guarantees/configuration differences belong with reused figures.
- Raft.Net tick/heartbeat25ms, election5-6s, RPC2s. DotNext election5000-6000ms, heartbeat threshold.005 (25-30ms), RPC2s. Fixed endpoint membership is prepopulated in DotNext's in-memory configuration storage outside measurement; persistent membership recovery is not tested by these workloads.

## Statistical limits and provenance

Throughput/managed allocation are medians of3 round estimates. Individual request p50/p99 use nearest rank over all1536 short or12288 long samples/configuration, without outlier removal. Min/max is observed between-round throughput, never a confidence interval. The full matrix contains368,640 individual samples. No BenchmarkDotNet amortized mean is used as individual latency.

Managed allocation is process-wide across co-located replicas/adapter tasks during the caller workload. It is not retained heap, per-node RSS or hardware sizing. Local disk/network/scheduler/GC behavior and log growth matter; these data are not a cross-host/TLS capacity or power-loss guarantee. This compares whole stacks with different storage/feature contracts, not algorithmic complexity in isolation.

Raw JSON preserves runtime/GC/OS/CPU count, actual root, binary/current-source/harness/common-application/lock hashes, timings, completeness flags and per-round samples. Diagnostic counters include warmup and post-validation; they are not pure timed-interval counts. Neither old measurements nor smoke cases are merged with fresh results.

Pinned DotNext source: [39ff4fe882e68a5ea90818b41adb97cdc3ec60d8](https://github.com/dotnet/dotNext/tree/39ff4fe882e68a5ea90818b41adb97cdc3ec60d8). Upstream source audit references and precise limits are in the generated HTML and _docs/llm/sdd/015-dotnext-comparison.md.
