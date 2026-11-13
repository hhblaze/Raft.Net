# Offline benchmark report assets

The three-way renderer also uses these assets: ../render_dotnext.py plus threeway.html. Its separate output is results/dotnext-comparison-2026-11-13.html, preserving the earlier enhancement report. Styles are shared with report.html; Chart.js/data are embedded. Regenerate from complete raw matrices or use --summary benchmarks/results/dotnext-summary-2026-11-13.json to use the previously verified saved summary without raw artifacts.

`report.html` is the editable template for `../render_html.py`. The generated report embeds the data, styles, scripts and Chart.js; opening it needs neither a server nor the adjacent files. CSV/JSON/raw-source links remain optional provenance links into the repository.

Vendored chart library: [Chart.js 4.4.8 UMD](https://cdn.jsdelivr.net/npm/chart.js@4.4.8/dist/chart.umd.js), [upstream source](https://github.com/chartjs/Chart.js/tree/v4.4.8). MIT terms are in `Chart.js.LICENSE.md` and embedded in the report. The original download is preserved; only the source-map comment is omitted from the generated HTML.

SHA-256 of `chart.umd.js`: `e4cf4d144b222634f2e64ff707cd57b953f8de4b65b231cd5ac6bde114648e4d`.

Regenerate from the checked-in summary with `python benchmarks/render_html.py` from `RaftNet8/`. No third-party Python packages are used. To also recompute CSV/JSON/Markdown from complete raw measurements, use `python benchmarks/summarize.py`.
