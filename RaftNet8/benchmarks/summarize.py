"""Summarize the executed enhancement comparison; standard library only.

Run from RaftNet8/: python benchmarks/summarize.py
Raw artifacts remain authoritative. Missing/incomplete cases fail this script.
"""
import argparse
import csv
import json
import math
import statistics
from pathlib import Path

from render_html import render_report


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8-sig"))


def latency(paths, expected_cases, rounds):
    grouped = {}
    sources = []
    for path in paths:
        document = read_json(path)
        assert document["Complete"], f"Incomplete report: {path}"
        assert len(document["Results"]) == document["ExpectedMeasurements"]
        sources.append({k: v for k, v in document.items() if k != "Results"})
        for row in document["Results"]:
            case = row["Case"]
            key = (case["Nodes"], case["PayloadBytes"], case["Version"], case["Mode"])
            grouped.setdefault(key, []).append(row)
    assert len(grouped) == expected_cases, (len(grouped), expected_cases)
    output = []
    for (nodes, size, version, mode), rows in sorted(grouped.items()):
        assert len(rows) == rounds
        samples = sorted(value for row in rows for value in row["LatencyMilliseconds"])
        output.append({"Nodes": nodes, "PayloadBytes": size, "Version": version, "Mode": mode,
                       "Requests": len(samples),
                       "MedianOpsPerSecond": statistics.median(row["ProposalsPerSecond"] for row in rows),
                       "MinOpsPerSecond": min(row["ProposalsPerSecond"] for row in rows),
                       "MaxOpsPerSecond": max(row["ProposalsPerSecond"] for row in rows),
                       "P50Ms": samples[math.ceil(len(samples) * .50) - 1],
                       "P99Ms": samples[math.ceil(len(samples) * .99) - 1],
                       "AllocatedBytesPerRequest": statistics.median(row["AllocatedBytesPerProposal"] for row in rows)})
    return output, sources


def csv_file(path, rows):
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def default_table(rows):
    result = ["| Voters | Payload | Baseline batched /s | Current batched /s | Current pipeline /s | Baseline p50 / p99 ms | Current pipeline p50 / p99 ms |",
              "| ---: | ---: | ---: | ---: | ---: | ---: | ---: |"]
    for nodes in (1, 3, 5):
        for size in (64, 16384):
            def select(version, mode):
                return next(r for r in rows if (r["Nodes"], r["PayloadBytes"], r["Version"], r["Mode"]) == (nodes, size, version, mode))
            base, batched, pipeline = select("Baseline", "Batched"), select("Current", "Batched"), select("Current", "Pipeline")
            result.append(f"| {nodes} | {size} B | {base['MedianOpsPerSecond']:,.0f} | {batched['MedianOpsPerSecond']:,.0f} | {pipeline['MedianOpsPerSecond']:,.0f} | {base['P50Ms']:.2f} / {base['P99Ms']:.2f} | {pipeline['P50Ms']:.2f} / {pipeline['P99Ms']:.2f} |")
    return "\n".join(result)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--artifacts", type=Path, default=Path(".artifacts/autonomous/benchmarks"))
    parser.add_argument("--output", type=Path, default=Path("benchmarks/results"))
    args = parser.parse_args()
    source, output = args.artifacts, args.output
    short, metadata = latency([source / "latency-final.json", source / "latency-unbatched-pipeline.json"], 36, 3)
    long, long_metadata = latency([source / "latency-long-batched.json", source / "latency-long-pipeline.json"], 18, 1)
    bdn = []
    bdn_metadata = []
    for folder in ("bdn-final", "bdn-unbatched-pipeline"):
        document = read_json(source / folder / "results/Raft.Benchmarks.DurableClusterBenchmarks-report-full-compressed.json")
        bdn_metadata.append(document["HostEnvironmentInfo"])
        for case in document["Benchmarks"]:
            stats = case["Statistics"]
            assert stats is not None, case["Parameters"]
            bdn.append({"Case": case["Parameters"].removeprefix("Case="), "MeanNsPerProposal": stats["Mean"],
                        "StdDevNs": stats["StandardDeviation"], "MedianNs": stats["Median"], "RetainedSamples": stats["N"],
                        "OpsPerSecondFromMean": 1e9 / stats["Mean"],
                        "AllocatedBytesPerProposal": case["Memory"]["BytesAllocatedPerOperation"]})
    assert len(bdn) == len({b["Case"] for b in bdn}) == 36
    output.mkdir(parents=True, exist_ok=True)
    csv_file(output / "enhancement-latency-short-2026-11-12.csv", short)
    csv_file(output / "enhancement-latency-long-2026-11-12.csv", long)
    csv_file(output / "enhancement-bdn-2026-11-12.csv", sorted(bdn, key=lambda b: b["Case"]))
    summary = {"ShortLatency": short, "LongLatency": long, "BenchmarkDotNet": bdn,
               "ShortMetadata": metadata, "LongMetadata": long_metadata, "BenchmarkDotNetEnvironment": bdn_metadata}
    (output / "enhancement-summary-2026-11-12.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    bdn_table = ["| Case | Mean μs/proposal | StdDev μs | Managed KiB/proposal |",
                 "| --- | ---: | ---: | ---: |"]
    for case in sorted(bdn, key=lambda b: b["Case"]):
        bdn_table.append(f"| {case['Case']} | {case['MeanNsPerProposal']/1000:,.2f} | {case['StdDevNs']/1000:,.2f} | {case['AllocatedBytesPerProposal']/1024:,.2f} |")
    text = f"""# Enhancement performance measurements — 2026-11-12

All **36** comparison cases have individual-request measurements and warmed BenchmarkDotNet results: 1/3/5 voters × 64/16384-byte commands × baseline unbatched/batched and all four current batching/pipelining combinations. The complete short matrix has **55,296** measured requests (1536/case); longer batched series add **73,728** requests (4096/case), for **129,024** stored unprofiled caller-latency samples. Both gains and regressions are retained.

## Environment and method

Windows 11 (10.0.26200.9974), .NET SDK 8.0.428, runtime .NET 8.0.34 x64 RyuJIT AVX2, concurrent workstation GC, 12 logical processors visible. BDN could not identify the processor model or change the power plan in this environment. No cross-host/network/TLS capacity claim follows from these single-host loopback results.

Both versions use DBreeze **1.142.2026.1016-beta DISK**, the same atomic business transaction and CRC index/term checkpoint on every node, and one engine/node containing log and a 256-key payload ring. Client completion means quorum durability plus leader apply; all replicas are validated afterward outside timing. Baseline is unchanged commit **8b991bd**, with weaker v1 log integrity/recovery and one-flight wire 1. Current uses checked v2/wire 2. Archived pre-.NET-8 contracts are not equivalent throughput baselines.

Concurrency 64; heartbeat 25 ms; election 5–6 s; RPC 2 s, identical in both versions. Unbatched means one entry per log/replication/apply batch with zero collection delay; current pipeline can independently be on/off. Batched uses 128 entries/256 KiB, current delay 1 ms, baseline drains queued commands. Pipeline window 4. Snapshot compaction is off. Data is on the H-drive workspace; the much slower incomplete C-temp diagnostic run is excluded. Actual binary hashes/data roots/timings are in raw JSON and the summary metadata.

BDN uses 3 warmup/8 measurement iterations, one 128-proposal invocation/iteration and a 128-command setup warmup. Its normalized mean is **amortized time**, not individual latency. Error/outlier/minimum-iteration warnings and 99.9% intervals remain in raw reports. Short latency runs independently warm 128 and measure 512 requests per round, repeated 3 times. Throughput below is median of the 3 rounds; p50/p99 are nearest-rank over all 1536 raw samples, with no outlier removal. Long series independently warm 2048 and measure 4096 once; they are a different retained-log/steady-load workload with no between-round confidence estimate.

## Short individual-request measurements

{default_table(short)}

All 36 configurations, including unbatched pipeline and the batched-without-pipeline tails/allocations/ranges, are in [short CSV](enhancement-latency-short-2026-11-12.csv). Managed allocation is process-wide during the timed caller interval across all co-located replicas, not retained heap or per-node memory.

## Longer series

{default_table(long)}

The [long CSV](enhancement-latency-long-2026-11-12.csv) contains all 18 batched configurations. Longer log growth and filesystem/scheduler behavior materially change throughput/tails; compare like workloads. In particular large-command multi-voter paths have regressions and variable pipeline benefit. Small-command improvements are not universal: the BDN 128-command bursts and the longer series can rank configurations differently. These data do not support a blanket claim that the new implementation is faster.

## BenchmarkDotNet allocation and throughput matrix

{chr(10).join(bdn_table)}

The [BDN CSV](enhancement-bdn-2026-11-12.csv) includes throughput derived from the mean, sample count and allocations. Full confidence intervals/GC counts/measurement details are retained in [main 30-case BDN report](../../.artifacts/autonomous/benchmarks/bdn-final/results/Raft.Benchmarks.DurableClusterBenchmarks-report-github.md) and [additional 6-case report](../../.artifacts/autonomous/benchmarks/bdn-unbatched-pipeline/results/Raft.Benchmarks.DurableClusterBenchmarks-report-github.md).

## Iterations and validation

The retained storage optimizations avoid LRU scans on ordinary append/commit, answer known term ranges from cache with one lazy transaction for misses, and use checked total-byte metadata when committing through the tail. Replacement/compaction/reopen and randomized DBreeze/model/list tests protect those results. A separate nonblocking cache-read worker-bypass experiment passed safety tests but showed inconsistent performance (including a slower 5-node long run), so it was reverted; it is not part of these final results. CPU EventPipe traces are diagnostic only: async rundown/ConditionalWeakTable overhead distorts profiled timing.

The earlier microbenchmark generator failed on duplicate aliased Raft return types and executed zero cases; those failed reports are excluded. Corrected generators return the same result references as object, preserving measured work. All 29 micro Dry cases and 6 warmed wire cases passed, checked separately from this durable comparison. Dry numbers are execution checks, not performance evidence.

Late production corrections affect only legacy-wire control rejection and offline migration preflight; measured proposal/storage/wire 2 paths are unchanged. See [validation and coverage](../../docs/VALIDATION.md) and [benchmark commands/limitations](../README.md). Regenerate the summary with `python benchmarks/summarize.py` from RaftNet8; it refuses missing/incomplete case sets.

Raw sources: [90 short measurements](../../.artifacts/autonomous/benchmarks/latency-final.json), [18 unbatched-pipeline measurements](../../.artifacts/autonomous/benchmarks/latency-unbatched-pipeline.json), [long batched](../../.artifacts/autonomous/benchmarks/latency-long-batched.json), [long pipeline](../../.artifacts/autonomous/benchmarks/latency-long-pipeline.json), and [compact summary/metadata](enhancement-summary-2026-11-12.json). Binary/source provenance and weaker baseline durability must accompany reuse of these figures.
"""
    (output / "enhancement-2026-11-12.md").write_text(text, encoding="utf-8")
    render_report(summary, output / "enhancement-summary-2026-11-12.json", output / "enhancement-2026-11-12.html")
    print(f"Summarized {len(short)} short, {len(long)} long, {len(bdn)} BDN cases; 129024 measured latency samples.")


if __name__ == "__main__":
    main()
