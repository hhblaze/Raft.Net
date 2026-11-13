"""Verify fresh three-way measurements and render an offline HTML comparison.

From RaftNet8/: python -B benchmarks/render_dotnext.py
Requires complete short.json/long.json; smoke and partial runs are refused.
"""
import argparse
import csv
import hashlib
import html
import json
import math
import re
import statistics
from collections import defaultdict
from pathlib import Path

from render_html import ASSETS, CHART_SHA256, number, payload


DATE = "2026-11-13"
VARIANTS = [
    ("Baseline", "Batched"), ("Current", "Batched"), ("Current", "Pipeline"), ("DotNext", "Batched"),
    ("Baseline", "Unbatched"), ("Current", "Unbatched"), ("Current", "UnbatchedPipeline"), ("DotNext", "Unbatched"),
]
NAMES = {"Baseline": "Базовая", "Current": "Текущая", "DotNext": "DotNext 5.26.2"}
MODES = {"Batched": "Пакеты", "Pipeline": "Пакеты + pipeline",
         "Unbatched": "Без пакетов", "UnbatchedPipeline": "Без пакетов + pipeline"}


def quantile(samples, fraction):
    return samples[math.ceil(len(samples) * fraction) - 1]


def summarize(path, long, repeats=3):
    raw = json.loads(path.read_text(encoding="utf-8-sig"))
    variants = VARIANTS[:4] if long else VARIANTS
    expected = {(n, p, v, m) for n in (1, 3, 5) for p in (64, 16384) for v, m in variants}
    operations, warmup = (4096, 2048) if long else (512, 128)
    if (not raw["Complete"] or raw["Failure"] is not None or raw["Repeats"] != repeats
            or raw["WarmupOperationsPerRound"] != warmup or raw["RequestedOperations"] != operations
            or raw["ExpectedMeasurements"] != len(expected) * repeats or len(raw["Results"]) != len(expected) * repeats
            or raw["Concurrency"] != 64 or raw["DotNextPackage"] != "5.26.2"):
        raise ValueError(f"Incomplete or unexpected workload: {path}")
    grouped = defaultdict(list)
    for r in raw["Results"]:
        c = r["Case"]
        key = (c["Nodes"], c["PayloadBytes"], c["Implementation"], c["Mode"])
        if key not in expected or r["Operations"] != operations or r["Concurrency"] != 64:
            raise ValueError(f"Unexpected case: {key}")
        samples = sorted(r["LatencyMilliseconds"])
        if len(samples) != operations or any(not math.isfinite(s) or s <= 0 for s in samples):
            raise ValueError(f"Invalid individual samples: {key}")
        checks = [(r["ProposalsPerSecond"], operations / r["Seconds"]),
                  (r["P50Milliseconds"], quantile(samples, .5)),
                  (r["P99Milliseconds"], quantile(samples, .99)), (r["MaxMilliseconds"], samples[-1])]
        if any(not math.isclose(a, b, rel_tol=1e-10, abs_tol=1e-10) for a, b in checks):
            raise ValueError(f"Inconsistent round summary: {key}")
        if c["Implementation"] == "DotNext":
            diagnostic = r["Diagnostics"]
            limit = min(128, 256 * 1024 // (c["PayloadBytes"] + 21)) if c["Mode"] == "Batched" else 1
            if (diagnostic is None or diagnostic["AppendedEntries"] != warmup + operations
                    or diagnostic["AppliedCommands"] != (warmup + operations) * c["Nodes"]
                    or not 1 <= diagnostic["MaxApplyBatch"] <= limit):
                raise ValueError(f"Native adapter diagnostics differ: {key}")
        grouped[key].append(r)
    if set(grouped) != expected:
        raise ValueError("Case coverage differs")
    rows = []
    for key in sorted(expected, key=lambda k: (k[0], k[1], VARIANTS.index((k[2], k[3])))):
        n, p, v, m = key
        rounds = sorted(grouped[key], key=lambda r: r["Round"])
        if [r["Round"] for r in rounds] != list(range(1, repeats + 1)):
            raise ValueError(f"Missing/duplicate rounds: {key}")
        samples = sorted(s for r in rounds for s in r["LatencyMilliseconds"])
        rates = [r["ProposalsPerSecond"] for r in rounds]
        rows.append({"Nodes": n, "PayloadBytes": p, "Implementation": v, "Mode": m, "Rounds": repeats, "Requests": len(samples),
                     "Ops": statistics.median(rates), "MinOps": min(rates), "MaxOps": max(rates),
                     "P50Ms": quantile(samples, .5), "P99Ms": quantile(samples, .99),
                     "AllocatedKiB": statistics.median(r["AllocatedBytesPerProposal"] for r in rounds) / 1024,
                     "RoundOps": rates, "RoundP99Ms": [r["P99Milliseconds"] for r in rounds]})
    metadata = {k: v for k, v in raw.items() if k != "Results"}
    metadata["RawFile"] = path.name
    metadata["RawSha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
    metadata["DotNextDiagnostics"] = [
        {"Case": str(r["Case"]), "Round": r["Round"], "Diagnostics": r["Diagnostics"]}
        for r in raw["Results"] if r["Diagnostics"] is not None]
    return rows, metadata


def reference(row, rows, implementation):
    mode = "Unbatched" if row["Mode"].startswith("Unbatched") else "Batched"
    return next(r for r in rows if (r["Nodes"], r["PayloadBytes"], r["Implementation"], r["Mode"])
                == (row["Nodes"], row["PayloadBytes"], implementation, mode))


def difference(row, rows, implementation):
    return (row["Ops"] / reference(row, rows, implementation)["Ops"] - 1) * 100


def change(value):
    return f'<td class="numeric {"gain" if value >= 0 else "loss"}">{"+" if value >= 0 else "−"}{number(abs(value), 1)} %</td>'


def headers():
    return "<tr>" + "".join(
        f'<th scope="col" aria-sort="none"><button type="button" data-sort="{key}">{label}<span aria-hidden="true"> ↕</span></button></th>'
        for key, label in [
            ("Nodes", "Узлы"), ("PayloadBytes", "Команда"), ("Implementation", "Реализация"), ("Mode", "Режим"),
            ("Ops", "Команд/с ↑"), ("BaseDelta", "Δ к базе, % ↑"), ("DotDelta", "Δ к DotNext, % ↑"),
            ("P50Ms", "p50, мс ↓"), ("P99Ms", "p99, мс ↓"), ("AllocatedKiB", "Выделено, КиБ/ком. ↓"),
            ("MinOps", "Мин.–макс., команд/с"), ("Requests", "Запросов всего")
        ]) + "</tr>"


def rows_html(rows):
    output = []
    for r in rows:
        cells = (f'<td class="numeric">{r["Nodes"]}</td><td class="nowrap">{payload(r["PayloadBytes"])}</td>'
                 f'<td>{NAMES[r["Implementation"]]}</td><td class="nowrap">{MODES[r["Mode"]]}</td>'
                 f'<td class="numeric strong">{number(r["Ops"])}</td>')
        for version in ("Baseline", "DotNext"):
            cells += '<td class="numeric muted">—</td>' if r is reference(r, rows, version) else change(difference(r, rows, version))
        cells += (f'<td class="numeric">{number(r["P50Ms"], 2)}</td><td class="numeric">{number(r["P99Ms"], 2)}</td>'
                  f'<td class="numeric">{number(r["AllocatedKiB"], 2)}</td>'
                  f'<td class="numeric">{number(r["MinOps"])}–{number(r["MaxOps"])}</td>'
                  f'<td class="numeric">{number(r["Requests"])}</td>')
        output.append("<tr>" + cells + "</tr>")
    return "\n".join(output)


def overview(rows):
    result = []
    for n in (1, 3, 5):
        for p in (64, 16384):
            selected = [next(r for r in rows if (r["Nodes"], r["PayloadBytes"], r["Implementation"], r["Mode"]) == (n, p, v, m))
                        for v, m in VARIANTS[:4]]
            base, current, pipeline, dotnext = selected
            cells = f'<th scope="row">{n}</th><td>{payload(p)}</td>'
            cells += "".join(f'<td class="numeric">{number(r["Ops"])}</td>' for r in selected)
            cells += change(difference(pipeline, rows, "Baseline")) + change(difference(dotnext, rows, "Baseline"))
            cells += change(difference(pipeline, rows, "DotNext"))
            result.append("<tr>" + cells + "</tr>")
    return "\n".join(result)


def findings(short, long):
    output = []
    for label, rows in (("Короткие", short), ("Длинные", long)):
        for nodes in (3, 5):
            group = [r for r in rows if r["Nodes"] == nodes and r["PayloadBytes"] == 16384]
            base = next(r for r in group if r["Implementation"] == "Baseline" and r["Mode"] == "Batched")
            current = next(r for r in group if r["Implementation"] == "Current" and r["Mode"] == "Pipeline")
            dotnext = next(r for r in group if r["Implementation"] == "DotNext" and r["Mode"] == "Batched")
            ratio = difference(current, rows, "DotNext")
            output.append(f'<li><strong>{label}, {nodes} {"узла" if nodes == 3 else "узлов"}, 16 КиБ:</strong> база {number(base["Ops"])}, '
                          f'текущая + pipeline {number(current["Ops"])}, DotNext {number(dotnext["Ops"])} команд/с. '
                          f'Текущая + pipeline относительно DotNext: {"+" if ratio >= 0 else "−"}{number(abs(ratio), 1)} %.</li>')
    return "\n".join(output)


def validate_summary(summary, repeats=3):
    for name, long in (("Short", False), ("Long", True)):
        rows = summary[name]
        variants = VARIANTS[:4] if long else VARIANTS
        expected = {(n, p, v, m) for n in (1, 3, 5) for p in (64, 16384) for v, m in variants}
        actual = {(r["Nodes"], r["PayloadBytes"], r["Implementation"], r["Mode"]) for r in rows}
        meta = summary[name + "Metadata"]
        if actual != expected or len(rows) != len(expected) or not meta["Complete"] or meta["Failure"] is not None:
            raise ValueError(f"Invalid saved summary coverage: {name}")
        for r in rows:
            rates = r["RoundOps"]
            if (r["Requests"] != (4096 if long else 512) * repeats or r["Rounds"] != repeats or len(rates) != repeats
                    or any(not math.isfinite(rate) or rate <= 0 for rate in rates)
                    or r["Ops"] != statistics.median(rates) or r["MinOps"] != min(rates) or r["MaxOps"] != max(rates)
                    or not 0 < r["P50Ms"] <= r["P99Ms"] or r["AllocatedKiB"] < 0
                    or any(not math.isfinite(v) for v in r.values() if isinstance(v, (int, float)))):
                raise ValueError(f"Invalid saved summary statistics: {name}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifacts", type=Path, default=Path(".artifacts/dotnext-comparison"))
    parser.add_argument("--output", type=Path, default=Path("benchmarks/results"))
    parser.add_argument("--summary", type=Path, help="Regenerate from a previously verified saved summary, without raw artifacts.")
    args = parser.parse_args()
    if args.summary is None:
        short, short_meta = summarize(args.artifacts / "short.json", False)
        long, long_meta = summarize(args.artifacts / "long.json", True)
        summary = {"Short": short, "Long": long, "ShortMetadata": short_meta, "LongMetadata": long_meta}
    else:
        summary = json.loads(args.summary.read_text(encoding="utf-8-sig"))
        short, long, short_meta, long_meta = [summary[k] for k in ("Short", "Long", "ShortMetadata", "LongMetadata")]
    validate_summary(summary)
    for key in ("BinarySha256", "PackageLockSha256", "CurrentSourceSha256", "HarnessSourceSha256",
                "CommonApplicationSourceSha256", "Runtime", "ProcessorCount", "DataRoot", "ServerGC"):
        if short_meta[key] != long_meta[key]:
            raise ValueError(f"Workloads used different provenance/environment: {key}")
    output = args.output
    output.mkdir(parents=True, exist_ok=True)
    summary_name = f"dotnext-summary-{DATE}.json"
    (output / summary_name).write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    for name, rows in (("short", short), ("long", long)):
        with (output / f"dotnext-{name}-{DATE}.csv").open("w", encoding="utf-8", newline="") as file:
            fields = [key for key in rows[0] if not key.startswith("Round") or key == "Rounds"]
            writer = csv.DictWriter(file, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
    css = re.search(r"<style>(.*?)</style>", (ASSETS / "report.html").read_text(encoding="utf-8"), re.DOTALL).group(1)
    library = (ASSETS / "chart.umd.js").read_bytes()
    if hashlib.sha256(library).hexdigest() != CHART_SHA256:
        raise ValueError("Pinned Chart.js asset differs")
    document = (ASSETS / "threeway.html").read_text(encoding="utf-8")
    replacements = {
        "CSS": css, "SHORT_OVERVIEW": overview(short), "LONG_OVERVIEW": overview(long),
        "HEAD": headers(), "SHORT_ROWS": rows_html(short), "LONG_ROWS": rows_html(long), "FINDINGS": findings(short, long),
        "DATA": json.dumps(summary, ensure_ascii=False, separators=(",", ":")).replace("<", "\\u003c"),
        "CHART_JS": re.sub(r"^//# sourceMappingURL=.*$", "", library.decode("utf-8"), flags=re.MULTILINE),
        "LICENSE": html.escape((ASSETS / "Chart.js.LICENSE.md").read_text(encoding="utf-8")),
        "SAMPLES": number(sum(r["Requests"] for r in short + long)),
        "SOURCE_HASH": hashlib.sha256((output / summary_name).read_bytes()).hexdigest(),
        "PERIOD": html.escape(short_meta["StartedAtUtc"] + " — " + long_meta["UpdatedAtUtc"]),
    }
    for key, value in replacements.items():
        token = f"@@@{key}@@@"
        if token not in document:
            raise ValueError(f"Missing template token {token}")
        document = document.replace(token, value)
    if re.search(r"@@@\w+@@@", document):
        raise ValueError("Unresolved template tokens")
    target = output / f"dotnext-comparison-{DATE}.html"
    target.write_text(document, encoding="utf-8")
    print(f"Verified/rendered {len(short)} short + {len(long)} long cases, {sum(r['Requests'] for r in short+long):,} samples; {target} ({target.stat().st_size:,} bytes).")


if __name__ == "__main__":
    main()
