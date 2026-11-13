# Readable performance comparison

The user requested the existing comparison as HTML with tables and charts. This changes presentation, not measured data or production code.

- Deliver `RaftNet8/benchmarks/results/enhancement-2026-11-12.html` as a self-contained Russian-language report that opens offline from disk.
- Source every quantitative result from `enhancement-summary-2026-11-12.json`: 36 short, 18 long, 36 BenchmarkDotNet cases. Preserve regressions, full mode coverage and baseline provenance.
- Keep short/long/BDN workloads separate. Clearly distinguish individual p50/p99 from amortized BDN time, observed three-round ranges from confidence intervals, and managed allocations from retained/per-node memory.
- Include workload comparison, filterable charts and sortable detailed tables with explicit units/direction; retain methodological/environment limitations and links to original evidence.
- Keep regeneration independent of ignored raw artifacts via a standard-library renderer and pinned, licensed, embedded Chart.js. Recomputing summaries also refreshes HTML.
- Verify data parity, offline loading, filter/sort behavior, desktop/mobile layout, printing and absence of runtime errors. Record the result in OKF. No new .NET implementation or benchmark run is necessary.

Delivered and verified: all90 table rows and four chart datasets matched JSON in offline Chromium; filters, p50, reset, sorting, 390-pixel layout, printing and no-JS tables passed without browser errors or network requests. Raw-source regeneration reproduced HTML and left all original report data unchanged. Visual QA screenshots are in `RaftNet8/.artifacts/report-qa/`; production code was untouched.
