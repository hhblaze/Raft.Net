"""Paired HEAD/wire3 durable TCP measurements. Run one workload at a time.
Requires the frozen comparison runtime and git source ZIP captured before edits.
All generated source-only metadata, raw samples, logs and DBs stay in --out.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import zipfile

ROOT = Path(__file__).resolve().parents[1]

def business_source(text):
    return text.replace("\r\n", "\n").split("internal sealed class ComparisonMachine", 1)[1].split("internal sealed class ComparisonCluster", 1)[0]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--current", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--add-rounds", type=int, default=3)
    parser.add_argument("--operations", type=int, default=8192)
    parser.add_argument("--warmup", type=int, default=2048)
    args = parser.parse_args()
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=False)
    metadata = out / "baseline-cwd"
    with zipfile.ZipFile(args.source.resolve()) as archive:
        for name in archive.namelist():
            if not name.startswith("RaftNet8/"):
                continue
            relative = Path(name).relative_to("RaftNet8")
            if ".." in relative.parts:
                raise ValueError("Unsafe source archive")
            if ((name.endswith(".cs") and (name.startswith("RaftNet8/src/Raft/") or name.startswith("RaftNet8/benchmarks/"))) or
                name == "RaftNet8/benchmarks/Raft.Comparison/packages.lock.json"):
                target = metadata / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(archive.read(name))
    old_business = (metadata / "benchmarks/Raft.Benchmarks/ComparisonCluster.cs").read_text(encoding="utf-8")
    current_business = (ROOT / "benchmarks/Raft.Benchmarks/ComparisonCluster.cs").read_text(encoding="utf-8")
    if business_source(old_business) != business_source(current_business):
        raise AssertionError("The common business/checkpoint implementation changed")
    app_hash = hashlib.sha256(business_source(old_business).encode()).hexdigest()
    data = out / "data"
    environment = dict(os.environ, RAFT_BENCHMARK_ROOT=str(data))
    cases = [f"Current-{nodes}n-{payload}B-Pipeline" for nodes in (3, 5) for payload in (64, 16384)]
    runs = []
    manifest = dict(complete=False, baseline_commit="e06c4f9", rounds=args.rounds, add_rounds=args.add_rounds,
                    operations=args.operations, warmup=args.warmup, concurrency=64,
                    business_source_sha256=app_hash, runs=runs)
    def save():
        (out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    def run(variant, case, round_number):
        stem = f"{variant}-r{round_number}-{case}"
        output = out / (stem + ".json")
        binary = args.baseline.resolve() if variant == "head" else args.current.resolve()
        command = ["dotnet", str(binary), "--case", case, "--operations", str(args.operations),
                   "--warmup", str(args.warmup), "--repeats", "1", "--concurrency", "64", "--out", str(output)]
        command += ["--unified"] if variant == "head" else ["--hot-add" if variant == "add" else "--hot-membership"]
        with (out / (stem + ".log")).open("w", encoding="utf-8") as log:
            completed = subprocess.run(command, cwd=metadata if variant == "head" else ROOT, env=environment,
                                       stdout=log, stderr=subprocess.STDOUT, encoding="utf-8")
        if completed.returncode:
            raise RuntimeError(f"{stem} failed; retained log/raw data")
        result = json.loads(output.read_text(encoding="utf-8-sig"))
        if not result["Complete"] or len(result["Results"]) != 1:
            raise AssertionError(f"Incomplete measurement: {stem}")
        row = result["Results"][0]
        runs.append(dict(variant=variant, case=case, round=round_number, file=output.name))
        save()
        print(f"{stem}: {row['ProposalsPerSecond']:.0f}/s p99={row['P99Milliseconds']:.2f}ms alloc={row['AllocatedBytesPerProposal']}B/op", flush=True)
    save()
    for round_number in range(1, args.rounds + 1):
        for index, case in enumerate(cases):
            order = ["head", "wire3"]
            if (round_number + index) % 2 == 0:
                order.reverse()
            for variant in order:
                run(variant, case, round_number)
    for round_number in range(1, args.add_rounds + 1):
        for case in cases:
            run("add", case, round_number)
    manifest["complete"] = True
    save()
    print(f"COMPLETE {len(runs)} measurements: {out}", flush=True)

if __name__ == "__main__":
    main()
