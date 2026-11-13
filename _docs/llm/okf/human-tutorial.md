---
type: Documentation
title: Human HTML tutorials in Russian and English
description: Offline books, compiled C# companion, storage modes and operational-limit validation.
status: stable
sources:
  - resource: ../sdd/028-human-tutorial.md
  - resource: ../../../RaftNet8/docs/human/tutorial/index.html
  - resource: ../../../RaftNet8/docs/human/tutorial/book-en.html
  - resource: ../../../RaftNet8/docs/human/tutorial/book-ru.html
  - resource: ../../../RaftNet8/docs/human/tutorial/examples/TutorialNode/
  - resource: ../../../RaftNet8/src/Raft/RaftOptions.cs
---

# Deliverable

User requested comprehensive human books in RU and ENG under RaftNet8/docs/human/tutorial/. Two complete offline HTML editions have26 chapters, matching chapter anchors,14 C# regions taken directly from the compiled companion, a language index, searchable contents, copy/theme/print controls, quorum and accounted-payload calculators, mobile navigation and complete no-JavaScript prose/code. No CDN or reading-time server/dependency is required. Print QA produced51-page English and54-page Russian PDFs; those QA files are ignored artifacts, not the requested deliverables.

build.py and content_en.py/content_ru.py are the standard-library-only authoring sources. Run python -X utf8 -B docs/human/tutorial/build.py from RaftNet8/. book-manifest.json records example source/project/lock hashes. Source/external links are references, not required reading-time assets. README and the canonical integration skill link to the books.

# Companion contract

examples/TutorialNode is a separate net8.0 executable referencing src/Raft. It inherits central pinned DBreeze1.142.2026.1016-beta and checked lock files; it is not added to the main solution. Run n1/n2/n3 in separate terminals with common options, loopback6300..6302 and default .data/tutorial/<node>/ roots. --single selects a distinct single-voter group. --memory supplies both a memory log and memory application, no images; no database root is created. CLI switches belong to the example, not RaftOptions.

Disk mode uses explicit UnifiedTableV3 and separate DBreezeGenerationStateMachine application/image roots. Values and durable first request results share book/data under1.ToIndex(key)/2.ToIndex(requestId); helper checkpoint is book/checkpoint in the same atomic transaction. Duplicate IDs preserve the original result; conflicting payload reuse is reported without a second effect. IDs/results grow without automatic retention, explicitly documented. Helpers handle no-op/configuration checkpoint advancement. Callbacks use only the supplied transaction and are replay-safe. Proposals wait for quorum plus leader apply; get uses ReadIndex followed by that leader's app. local is stale-capable. Completion is supervised while stdin is idle; transport closes before node/log/app.

Explicit sample timers are25ms tick, heartbeat2, election[40,80) ticks, RPC500ms,15s console deadline. These are distinguished from library defaults. Existing human demo supplies runnable learner/promotion/transfer/removal scenarios; the new integration recipes compile but require an actual host's provisioned context. New TLS recipe preserves platform checks plus SHA256 certificate-to-ID binding; it is compiled, not represented as a separately executed TLS deployment.

# Storage and limits

Books distinguish disk persistence from a bounded cache and total RSS. Memory voters lose term/vote/log and cannot safely rejoin under a forgotten old identity; RAM mode is disposable lab use. Compacted disk logs require a compatible image/app. Backups keep log, application root and repository coherent; no force-quorum reset is offered. V3 uses one primary changing table plus small incompatible-reader guards and DBreeze system/rollback files, not one total physical file.

Source-backed defaults: 64KiB command,128 entries,256KiB apply/replay, automatic1051264B local/wire2,1052288B frame body plus4B length prefix,4messages/4MiB per peer,64MiB uncommitted,1024 admissions and1024entries/8MiB cache. Accounted entry overhead21B differs from checked disk header37B and physical DBreeze overhead. Bulk auto is4x2ms=8ms, shortened when all admitted work is queued; single voter1ms. Fixed stale8x/16ms wording in CONFIGURATION.md and an older skill paragraph incorrectly giving modern wire the application byte limit. No production consensus/storage behavior changed.

# Executed validation

- Locked main restore and main/companion Release builds passed with zero warnings. NuGet vulnerability feed was unreachable (NU1900 as error); process-local -p:NuGetAudit=false allowed cached pinned restore. No audit suppression was saved to source/config.
- Existing full tests:342/342, no skips, .artifacts/human-tutorial/tests/tutorial-full-with-key-access.trx. Initial sandbox340/342 result tutorial-full.trx is retained: two TLS certificate creation cases needed OS Windows key-store access and passed when rerun with it. No test assertion/production code was changed for those failures.
- verify_examples.py final run:13 checks,11 process starts,14.31seconds. Raw logs/data/result: .artifacts/human-tutorial/examples-20261113-205316-237456/. Tests cover actual three-process TCP/disk election, original-result dedup/conflict, Cyrillic round-trip, snapshot publication, abrupt leader kill, disk catch-up, minority write/read rejection, whole-group restart and dedup persistence, RAM reset/no database root.
- Cyrillic regression initially failed in examples-20261113-205142-921766/: Windows default redirected input decoded UTF-8 incorrectly. The new companion now explicitly uses no-BOM UTF-8 stdin/stdout, rebuilt cleanly and passed the added case. An initial12-check ASCII-only pass remains in examples-20261113-203228-700574/.
- One retry run examples-20261113-205238-383137/ hit listener10013 after ephemeral-range port selection; logs retained. Verifier now probes10000..29999, below common ephemeral ranges, so an earlier node is less likely to consume a later listener's port. It stops only its own processes, cleans up on initialization failure and never deletes sample data.
- Existing HumanTest scenario passed, .artifacts/human-tutorial/demo.log; its generated data is demo-20261113/ under the same ignored root.
- Independent static QA verifies all206 HTML links/fragments, both26-chapter anchor sets,14 exact source regions per edition, source/project/lock hashes,29 actual option defaults/derived frame budgets, UTF-8 and byte-identical regeneration. Script .artifacts/human-tutorial/qa/verify_html.py.
- Offline Chromium QA verifies both editions, copy/theme/quorum/calculator/language-anchor/filter controls,390px mobile without document overflow, no-JS content, print PDFs and zero external HTTP requests/JS errors. Script/report/screenshots: .artifacts/human-tutorial/qa/verify_browser.cjs and browser-result.json. Desktop, mobile, limits, quorum and dark disk screenshots visually reviewed. PDFium checked every printed page for blank/clipped text (none), rendered cover/application/limits/final-reference samples; English application and Russian limits/reference pages visually reviewed. print-result.json records51/54 pages.

These are documentation/example checks plus existing regression evidence, not new throughput measurements, formal verification or a cross-host deployment certification. No commits/pushes, public package changes, live data changes or legacy-tree edits were made.
