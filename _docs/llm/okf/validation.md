---
type: Validation Record
title: Autonomous enhancement validation
description: Executed safety, recovery, history, coverage, compatibility and performance checks with explicit limits.
status: stable
sources:
  - resource: ../../../RaftNet8/docs/VALIDATION.md
  - resource: ../../../RaftNet8/benchmarks/results/enhancement-2026-11-12.html
  - resource: ../../../RaftNet8/benchmarks/results/enhancement-2026-11-12.md
  - resource: ../../../RaftNet8/benchmarks/results/dotnext-comparison-2026-11-13.html
  - resource: ../../../RaftNet8/tests/Raft.Tests/
  - resource: ../../../.github/_workflows/raft-net8.yml
---

# Current evidence

Current R18 candidate:342/342 full tests, zero skips(r18-full.trx, byte-identical passing alias r18-full-fixed.trx), zero-warning Release/locked restore, adapter, human TCP/disk scenario and BDN Dry passed. The initial338/340 run is preserved as r18-full-initial.trx/log; two manually stopped-clock tests needed an explicit final heartbeat after coalescing, and majority-only failover adds two cases. New commit notification regressions12/12, combined ACK ordering14/14 pass; before implementation8fail/2pass on original ten-case set. A fresh current-reducer matrix passed1400runs/4378652events(seed10001..10100,seven scenarios,3/5voters,3000steps):14795writes,9643reads,14685crashes/torn tails,782storage failures,397snapshot installs. Artifacts r18-simulation.json/log. This is a new matrix, not a rerun of the historical42000run baseline. Final R18 is complete:12/12 fixed Pipeline median-throughput points over both refs,8/8distributed,360measurements/614400samples/5rounds. Independent raw/source/binary/CSV/audit and offline/mobile/print/noJS/chart QA passed; current and all older reports regenerate byte-identically. This local measured winner criterion does not imply lower allocation, universally better p99 or cross-host capacity.

Locked restore and Release solution build passed with zero warnings/errors. The final **221/221** native-coverage suite passed, no skips: `.artifacts/autonomous/release-coverage/final-release-full-coverage.trx`. Reducer coverage is **96.75% lines /91.53% branches**; the whole reported Core, including generated records, is **80.29% /88.36%** (1043/1299 lines,744/842 branches). Do not substitute reducer coverage for all Core.

The actual-reducer single-PRNG simulator passed **42,000 runs /132,010,316 events** (3000seeds ×7scenarios ×3/5 voters) with every-event invariants/durable ordering and bounded real-time history checking. It includes438,927 writes,280,606 reads,440,342 crashes/torn tails,22,442 uncertain disk failures and11,954 installs. A further896-run/2,817,880-event check passed. That matrix predates later runtime/storage/frame-budget/commit-notification changes. The current342-test suite and separate1400-run R18 matrix provide newer evidence; do not present the42000-run result as rerun on the current source.

Real3/5-process tests cover abrupt leader kill, snapshot catch-up past compaction, rolling voter transfer/restart and learner promotion/removal/restart. Other tests kill at transaction, commit, image, intent, generation and log-publication boundaries. Multi-chunk tests force sufficiently large images. Actual TCP/mTLS and certificate-ID rejection, randomized store/model/reopen operations, serialization/configuration properties and invalid histories provide independent evidence for modeled assumptions.

All272 original API members/signatures/enum values and v1 wire/database golden fixtures remain. The unchanged compiled original client passed with the new library and updated runtime dependencies; SHA-256 `2BD210BB23A09EAC28E6C4F7BCA4A29E3749E4C290A841B8C4B6F6A97A314D9B`. The README durable example and human snapshot/membership scenario passed. Canonical/router/DBreeze skills passed skill-creator validation using workspace-only PyYAML. Legacy artifacts/source references remain unchanged.

# Regressions and measurement results

The separate2026-11-13 DotNext comparison passed216 fresh round measurements (48short/24long cases x3),368,640 individual samples, native-adapter acknowledgment/replay checks and all-replica/DBreeze reopen validation. Locked main restore/build and221 tests passed before measurement; independent statistics/CSV/binary/source provenance and offline browser/72-row/four-chart/reference/filter/sort/mobile/print/no-JS validation passed. Raw and saved-summary regeneration are identical. See [comparison memory](dotnext-comparison.md) for version/storage/adapter constraints and the remaining current-version regressions. No production consensus/storage optimization was made.

Red/green tests reproduced/fixed original all-type persistence fail-stop, delayed TimeoutNow rejection, removed snapshot-node restart, unnecessary cached-term DB reads and unknown-database migration initialization. New wire1 controls fail clearly while legacy writes/barriers work. The final migration preflight accepts initialized-empty v1 and refuses unknown formats without adding Raft identity/state or creating a target.

Performance report covers **36** durable configurations, warmed BDN, **129,024** individual unprofiled latency samples,29 successful micro Dry cases and6 warmed wire cases. Summaries are reproducible and reject incomplete inputs. Matrices distinguish short128/512-command bursts from longer4096-command series. Byte allocation is process-wide across co-located replicas; means are amortized throughput time, never individual latency. Both gains/regressions are retained; loopback/storage/workload-specific data are not capacity promises.

The user-requested [offline HTML report](../../../RaftNet8/benchmarks/results/enhancement-2026-11-12.html) adds Russian-language tables, workload comparison, four filterable charts, p50/p99 selection and sortable metrics. `python benchmarks/render_html.py` regenerates it from the saved summary without raw artifacts; `summarize.py` refreshes all formats. Chart.js4.4.8 is pinned, licensed and embedded. Chromium offline checks verified all90 table rows and chart datasets against JSON, filter/reset/sort behavior, 390-pixel layout, printing, no-JavaScript fallback and no runtime/network errors. Raw-evidence regeneration preserved the original JSON/CSV/Markdown hashes and reproduced identical HTML. This was a presentation-only task; no .NET code changes or new performance measurements. Visual QA images are under `RaftNet8/.artifacts/report-qa/`.

The slow incomplete C-temp run and failed initial BDN generator reports are excluded. The alias ambiguity was fixed without changing measured work. BDN CLI now exits nonzero on failed/missing reports. The nonblocking cache-worker bypass experiment was reverted after inconsistent unprofiled results; speculative/profiled gains are not shipped claims. See spec013.

# Evidence limits

This is extensive regression/model evidence, not formal verification or hardware power-loss proof. Only Windows executed locally. The active GitHub Actions workflow was removed at the user's request on 2026-11-13 to avoid requiring token workflow scope; the retained `_workflows` template is inactive, and remote CI has not run. Native collection excludes raw child processes; OS clusters run separately to avoid artificial profiler/process contention. Baseline8b991bd has weaker v1 integrity/recovery/features. Historical pre-.NET-8 APIs/data are not an equivalent benchmark or automatically migratable format.
