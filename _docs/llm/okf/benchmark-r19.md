---
type: Engineering Record
title: R19 benchmark refresh
description: Current wire3/v4 measurements with explicitly reused R18 baseline and DotNext observations.
status: stable
sources:
  - resource: ../sdd/030-r19-report.md
  - resource: ../../../RaftNet8/benchmarks/run_r19.py
  - resource: ../../../RaftNet8/benchmarks/render_r19.py
---

# Accepted scope

User requested R19 in the R18 HTML format and explicitly waived new baseline/DotNext measurements. Measure current only, use wire3/v4, preserve all old files. No new optimization goal or C# changes. Current HEAD bfcc52307edd916154a5de91b1ee9a8bd5aa69da. Fresh restore/build passed without warnings; pinned DBreeze/net8 retained.

New artifacts root: RaftNet8/.artifacts/winner/r19/. New orchestrator freezes runtime/current-source.zip and a byte-exact R18 summary snapshot, verifies the unchanged ComparisonMachine body against the frozen R18 source ZIP, then runs long60 and short120 current-only measurements sequentially. Uses the exact R18 H-drive data root. New307200 latency samples; reused307200 reference samples; combined72 configurations/360 rounds/614400 samples. Existing R18 summary SHA2562EAE83262DCBDBCCAF5241F94991CCDD2B92792CC9A4E68CDC39C9749481EE60.

Historical comparisons are explicitly unpaired. New render_r19.py reuses the R18 layout/assets but preserves old metadata separately, validates current-only v4 coverage and all pooled latency/round stats, copies reference rows unchanged, removes paired-win claims, and includes a separate12-point Pipeline R19-vs-R18-current table. Summary v2 embeds the entire byte-exact historical summary as text for provenance/offline regeneration. No old report or raw numeric data is edited.

Preflight source fingerprint in this first executing orchestrator used WindowsPath case-insensitive ordering (8A798E…); the C# runner uses ordinal full Windows paths (AF2BD4…). Both refer to the same source. Renderer independently reconstructs canonical C# hashes from the immutable source archive and records preflight order explicitly; it never rewrites raw measurements. Future orchestrator runs use ordinal ordering (fixed helper only, no C# changes). Harness fingerprint DE18D7… matches both orders. Verify both archive and recorded metadata, do not mistake order-dependent fingerprints for different production source.

Measurements and report QA are complete; no active timed run remains. Numeric/browser QA evidence is under r19/qa; no production files touched.

# Delivered2026-11-14

Public report **RaftNet8/benchmarks/results/winner-r19-2026-11-14.html**, with winner-r19-summary / short / long files. 180 fresh measurements,307200 new latencies; historical references copied exactly (180 observations/307200 latencies),72 combined rows/360 main observations/614400 samples. R18 Current is additionally compared in a separate12-point Pipeline table; no extra measurements were performed. Source flags all Current, hot membership enabled, fixed membership, no active Add.

Commit **bfcc52307edd916154a5de91b1ee9a8bd5aa69da**; measured Raft.Net8.dll **518E02CDE99E5B7C54CB5D540FEF6106822C392D9833C5E269C6FFF67107CA28**; current C# source canonical **AF2BD44F100348AF70B2369B0573BCFEBC66E9D63B2CDBD28069F6C57B698E8C**; harness **DE18D7BD7442251D569A1FBBECEA394152AFED34DB3E8C39064F749D9A3D0360**; source archive **E9201C05786EC429948F58AD99EF79E847DD7D36ACB60D6CB9AC3A4C23B06452**. Reference summary snapshot SHA remains2EAE83262DCBDBCCAF5241F94991CCDD2B92792CC9A4E68CDC39C9749481EE60. Both preflight/order-dependent and canonical hashes retained, raw data untouched.

Pipeline medians exceed both archived baseline/DotNext at **12/12**, distributed **8/8**. Relative to OUR R18, R19 throughput is lower **11/12**, worst **−25.523%** (long1n64B); largest p99 increase **+117.154%** (long5n16KiB). These are different-date observations, not paired fresh controls; no causal code-regression/win claim. Warned prominently at report start; all12 our-version comparisons open by default, all full matrices and allocations retained. This request was reporting, not optimization; do not silently change code or pick faster rounds.

Independent qa/verify_numbers.py passed: every fresh pool/round statistic, untouched36 historical rows,72 CSV/embedded rows, archive C# source hashes and measured DLL hashes. Browser qa/verify.cjs passed all4 Chart.js datasets/deltas, filters, sorting, long-mode bounds,1600px/390px, print199509B, noJS48+24 rows, offline/no HTTP/errors. Screenshots visually reviewed. Saved-summary regeneration byte-identical. R18 report/raw/reference files preserved; no production src/HumanTest/tests/legacy tree changes. Fresh build zero warnings/errors; prior402 tests accurately labelled historical, no redundant regression rerun. No commit/push. Report task complete.
