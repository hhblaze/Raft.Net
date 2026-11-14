# R19 current-only benchmark refresh

Status: complete and validated2026-11-14; requested by the user on the same date.

Create the same human-facing report as winner-r18-2026-11-13.html using fresh measurements of the current Raft implementation. The user explicitly requested no new baseline or DotNext measurements. Preserve R18 files and reference values.

Use current wire3/raft8-v4, .NET8 and pinned DBreeze, all R18 topology/payload/mode combinations and five rounds. Short: current24 cases ×512 commands after128 warmup; long: current12 cases ×4096 after2048 warmup. Concurrency64, same H-drive data parent, unchanged durable business/checkpoint transaction, real TCP and per-round close/reopen validation. Freeze runtime and source; no production C# changes or concurrent benchmarks/tests while timed.

Combine180 fresh measurements/307200 latencies with exactly180 historical reference measurements/307200 latencies from R18. Show the source and date distinction prominently in HTML, tables, legends, JSON and documentation. These are historical controls, not paired rounds. Do not compute paired-win counts against old observations. Add a separate fixed-Pipeline comparison with our R18 implementation using its existing values.

Keep the R18 four interactive charts, filters/sorting, complete short/long tables, p50/p99, throughput medians/ranges and managed allocations. Standalone offline HTML, CSVs and a self-contained summary capable of regeneration without raw artifacts. Explicitly record source/binary/archive/reference hashes, actual dates/environment and validation evidence. No performance target, library optimization, commits or pushes are part of this request.

Verify all fresh raw statistics, unchanged historical rows, embedded data/CSV, immutable runtime/source provenance, offline browser charts/filters/mobile/print/noJS and deterministic regeneration. Update benchmark readmes and OKF memory. The pre-existing402-test functional validation remains historical; do not label it as newly executed R19 testing.

## Delivered evidence

Report: RaftNet8/benchmarks/results/winner-r19-2026-11-14.html; tagged summary and short/long CSVs beside it. Exactly180 new Current-only measurements/307200 latencies, with180 historical R18 reference observations/307200 latencies. All new rounds complete and validated after DB reopen. Source/runtime/reference snapshots and logs under .artifacts/winner/r19. No source/runtime workload changes, old-data reruns or production C# edits.

Pipeline medians exceed both historical reference stacks at12/12 points (8/8 distributed). Against our own R18 Current, R19 throughput is lower at11/12 points, worst−25.523%; largest pooled p99 increase+117.154%. These cross-date observations are explicit and do not establish code causality; all rows/ranges/allocations/p99 losses remain visible.

Independent numeric/source/binary/hash/reference-row/CSV/embedded-data QA passed. Four charts and filters/sorting validated in offline browser;390px mobile, print and noJS fallback passed. Visual inspection and byte-identical regeneration from summary passed. Readmes/OKF updated. No commits or pushes.
