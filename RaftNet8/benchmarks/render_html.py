"""Render the dated benchmark summary as a self-contained, offline HTML report.

From RaftNet8/: python benchmarks/render_html.py
No packages, raw benchmark artifacts, network access or server are required.
"""
import argparse
import hashlib
import html
import json
import math
import re
from pathlib import Path


DATE = "2026-11-12"
ASSETS = Path(__file__).resolve().parent / "report_assets"
CHART_SHA256 = "e4cf4d144b222634f2e64ff707cd57b953f8de4b65b231cd5ac6bde114648e4d"
MODES = {"Unbatched": "Без пакетов", "Batched": "Пакеты",
         "Pipeline": "Пакеты + pipeline", "UnbatchedPipeline": "Без пакетов + pipeline"}
VERSIONS = {"Baseline": "Базовая", "Current": "Текущая"}


def number(value, decimals=0):
    return f"{value:,.{decimals}f}".replace(",", "\u202f").replace(".", ",")


def payload(size):
    return "64 Б" if size == 64 else "16 КиБ"


def bdn_rows(summary):
    rows = []
    for source in summary["BenchmarkDotNet"]:
        match = re.fullmatch(r"(Baseline|Current)-([135])n-(64|16384)B-(\w+)", source["Case"])
        if match is None:
            raise ValueError(f"Unknown BDN case: {source['Case']}")
        version, nodes, size, mode = match.groups()
        rows.append({**source, "Version": version, "Nodes": int(nodes), "PayloadBytes": int(size), "Mode": mode})
    return rows


def validate(summary):
    pairs = {("Baseline", "Unbatched"), ("Baseline", "Batched"),
             *(('Current', mode) for mode in MODES)}
    full = {(nodes, size, version, mode) for nodes in (1, 3, 5) for size in (64, 16384)
            for version, mode in pairs}
    long = {key for key in full if key[3] in ("Batched", "Pipeline")}
    for name, rows, expected, requests in (
        ("ShortLatency", summary["ShortLatency"], full, 1536),
        ("LongLatency", summary["LongLatency"], long, 4096),
        ("BenchmarkDotNet", bdn_rows(summary), full, None),
    ):
        keys = {(r["Nodes"], r["PayloadBytes"], r["Version"], r["Mode"]) for r in rows}
        if len(rows) != len(expected) or keys != expected:
            raise ValueError(f"Missing/duplicate/unexpected cases in {name}")
        for row in rows:
            if any(not math.isfinite(v) for v in row.values() if isinstance(v, (int, float))):
                raise ValueError(f"Non-finite metric in {name}")
            if requests is not None and (row["Requests"] != requests or row["P99Ms"] < row["P50Ms"]
                                         or not 0 < row["MinOpsPerSecond"] <= row["MedianOpsPerSecond"] <= row["MaxOpsPerSecond"]):
                raise ValueError(f"Invalid latency metrics in {name}")
            if requests is None and (row["RetainedSamples"] < 1 or row["MeanNsPerProposal"] <= 0
                                    or not math.isclose(row["OpsPerSecondFromMean"], 1e9 / row["MeanNsPerProposal"])):
                raise ValueError("Invalid BDN metrics")


def find(rows, nodes, size, version, mode):
    return next(r for r in rows if (r["Nodes"], r["PayloadBytes"], r["Version"], r["Mode"])
                == (nodes, size, version, mode))


def delta(row, rows, metric):
    if row["Version"] == "Baseline":
        return None
    base_mode = "Unbatched" if row["Mode"] in ("Unbatched", "UnbatchedPipeline") else "Batched"
    baseline = find(rows, row["Nodes"], row["PayloadBytes"], "Baseline", base_mode)
    return (row[metric] / baseline[metric] - 1) * 100


def change_cell(value):
    if value is None:
        return '<td class="numeric muted">—</td>'
    sign = "+" if value >= 0 else "−"
    status = "gain" if value >= 0 else "loss"
    return f'<td class="numeric {status}">{sign}{number(abs(value), 1)} %</td>'


def table_head(bdn=False):
    columns = [("Nodes", "Узлы"), ("PayloadBytes", "Команда"), ("Version", "Версия"), ("Mode", "Режим")]
    if bdn:
        columns += [("RetainedSamples", "N после фильтра BDN"), ("MeanUs", "Среднее, мкс/ком."),
                    ("StdDevUs", "StdDev, мкс"), ("Throughput", "Команд/с ↑"), ("Delta", "Δ к базе, % ↑"),
                    ("AllocationKiB", "Выделено, КиБ/ком. ↓")]
    else:
        columns += [("Requests", "Запросы"), ("Throughput", "Команд/с ↑"), ("Delta", "Δ к базе, % ↑"),
                    ("P50Ms", "p50, мс ↓"), ("P99Ms", "p99, мс ↓"),
                    ("AllocationKiB", "Выделено, КиБ/ком. ↓"), ("MinOpsPerSecond", "Мин.–макс., команд/с")]
    return "<tr>" + "".join(
        f'<th scope="col" aria-sort="none"><button type="button" data-sort="{key}">{label}<span aria-hidden="true"> ↕</span></button></th>'
        for key, label in columns) + "</tr>"


def full_rows(rows, bdn=False):
    output = []
    metric = "OpsPerSecondFromMean" if bdn else "MedianOpsPerSecond"
    for r in rows:
        cells = (f'<td class="numeric">{r["Nodes"]}</td><td class="nowrap">{payload(r["PayloadBytes"])}</td>'
                 f'<td>{VERSIONS[r["Version"]]}</td><td class="nowrap">{MODES[r["Mode"]]}</td>')
        if bdn:
            cells += (f'<td class="numeric">{r["RetainedSamples"]}</td>'
                      f'<td class="numeric">{number(r["MeanNsPerProposal"] / 1000, 2)}</td>'
                      f'<td class="numeric">{number(r["StdDevNs"] / 1000, 2)}</td>')
        else:
            cells += f'<td class="numeric">{number(r["Requests"])}</td>'
        cells += f'<td class="numeric strong">{number(r[metric])}</td>' + change_cell(delta(r, rows, metric))
        if not bdn:
            cells += f'<td class="numeric">{number(r["P50Ms"], 2)}</td><td class="numeric">{number(r["P99Ms"], 2)}</td>'
        allocation = r["AllocatedBytesPerProposal" if bdn else "AllocatedBytesPerRequest"] / 1024
        cells += f'<td class="numeric">{number(allocation, 2)}</td>'
        if not bdn:
            interval = "—" if r["Requests"] == 4096 else f'{number(r["MinOpsPerSecond"])}–{number(r["MaxOpsPerSecond"])}'
            cells += f'<td class="numeric nowrap">{interval}</td>'
        output.append("<tr>" + cells + "</tr>")
    return "\n".join(output)


def workload_rows(summary):
    output = []
    for nodes in (1, 3, 5):
        for size in (64, 16384):
            cells = f'<th scope="row">{nodes}</th><td>{payload(size)}</td>'
            for source in ("ShortLatency", "LongLatency"):
                rows = summary[source]
                base = find(rows, nodes, size, "Baseline", "Batched")
                current = find(rows, nodes, size, "Current", "Pipeline")
                cells += (f'<td class="numeric">{number(base["MedianOpsPerSecond"])}</td>'
                          f'<td class="numeric">{number(current["MedianOpsPerSecond"])}</td>'
                          + change_cell(delta(current, rows, "MedianOpsPerSecond")))
            output.append("<tr>" + cells + "</tr>")
    return "\n".join(output)


def render_report(summary, summary_path, output_path):
    validate(summary)
    library = (ASSETS / "chart.umd.js").read_bytes()
    if hashlib.sha256(library).hexdigest() != CHART_SHA256:
        raise ValueError("Vendored Chart.js differs from the pinned 4.4.8 asset")
    # Source-map requests are unnecessary: the delivered report is one offline file.
    chart = re.sub(r"^//# sourceMappingURL=.*$", "", library.decode("utf-8"), flags=re.MULTILINE)
    document = (ASSETS / "report.html").read_text(encoding="utf-8")
    replacements = {
        "WORKLOAD_ROWS": workload_rows(summary),
        "SHORT_HEAD": table_head(), "SHORT_ROWS": full_rows(summary["ShortLatency"]),
        "LONG_ROWS": full_rows(summary["LongLatency"]),
        "BDN_HEAD": table_head(True), "BDN_ROWS": full_rows(bdn_rows(summary), True),
        "SUMMARY_JSON": json.dumps(summary, ensure_ascii=False, separators=(",", ":")).replace("<", "\\u003c"),
        "SUMMARY_SHA256": hashlib.sha256(summary_path.read_bytes()).hexdigest(),
        "CHART_JS": chart.replace("</script", "<\\/script"),
        "CHART_LICENSE": html.escape((ASSETS / "Chart.js.LICENSE.md").read_text(encoding="utf-8")),
    }
    for key, value in replacements.items():
        token = f"@@@{key}@@@"
        if token not in document:
            raise ValueError(f"Missing template token {token}")
        document = document.replace(token, value)
    if re.search(r"@@@\w+@@@", document):
        raise ValueError("Unresolved report template tokens")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(document, encoding="utf-8")
    return output_path


def main():
    directory = Path(__file__).resolve().parent / "results"
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--summary", type=Path, default=directory / f"enhancement-summary-{DATE}.json")
    parser.add_argument("--output", type=Path, default=directory / f"enhancement-{DATE}.html")
    args = parser.parse_args()
    summary = json.loads(args.summary.read_text(encoding="utf-8-sig"))
    path = render_report(summary, args.summary, args.output)
    print(f"Rendered {path} ({path.stat().st_size:,} bytes; 36 short, 18 long, 36 BDN cases).")


if __name__ == "__main__":
    main()
