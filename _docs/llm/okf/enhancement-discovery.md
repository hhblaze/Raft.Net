---
type: Engineering Audit
title: RaftNet8 enhancement discovery and approval gate
description: Phase 0 evidence, confirmed risks, scope conflicts, and a plan for the requested Raft extensions.
status: draft
generated: { by: codex/gpt-6, at: 2026-11-12T13:38:27Z }
sources:
  - resource: ../sdd/003-enhancement-phase0.md
  - resource: ../../../RaftNet8/src/Raft/RaftNode.cs
  - resource: ../../../RaftNet8/src/Raft/Core/RaftCore.cs
  - resource: ../../../RaftNet8/src/Raft/Storage/DBreezeRaftStorage.cs
  - resource: https://dotnet.microsoft.com/en-us/platform/support/policy/dotnet-core
---

# Current task boundary

The new enhancement request supersedes the initial fixed-membership scope, but explicitly requires a Phase 0 report and approval before Phase 1. [The complete report](../sdd/003-enhancement-phase0.md) is the authoritative discovery record. No implementation changes were made during discovery. Do not start Phases 1–8 without the corresponding user approval.

Baseline 8b991bd: fresh locked restore and Release build passed with zero warnings/errors; all 71 tests and the actual TCP/disk human scenario passed on Windows. This is not a formal proof, Linux validation, new benchmark comparison, or core coverage report.

# Findings to retain

- A persistence ArgumentException during incoming RPC execution is treated as invalid input; the node returns the error but continues. Fail-stop policy is incomplete. Fix separately with a failing regression.
- A storage object and runtime can have different group/member options. Bind runtime identity/configuration to recovered metadata rather than independently trusting two option sets.
- An injected missing committed interior log row passes startup checks and fails on later range read. There is no Raft CRC/length envelope or complete recovery contract. This fixture does not prove DBreeze naturally corrupts committed rows.
- The pinned DBreeze disk assembly calls RandomAccess.FlushToDisk through its commit path. Do not repeat the unsupported claim that fsync is absent. Process crash tests do not certify hardware power-loss behavior.
- The demo atomically commits effects, deduplication, and checkpoint index, but not checkpoint term. Arbitrary application callbacks remain responsible for atomic/replay-safe effects.
- CheckQuorum and logged read barriers exist. Pre-vote, transfer, ReadIndex, learners, membership changes, snapshots, true pipelining, simulated disk loss, and history linearizability checking do not.
- The core owns storage and Random; it is repeatable under controlled inputs but not pure. Tick coalescing and I/O scheduling belong outside the proposed pure transition function.
- Unsupported RPC timer ranges are accepted; send-task failures can escape normal supervision. Classify this as an availability/validation risk, not an established consensus counterexample.

# Decisions awaiting approval

Update: the user approved continuation while explicitly retaining **.NET 8**. Phase 1 is now authorized under [004](../sdd/004-phase1-core-separation.md); do not retarget. The remaining phase gates still apply.

Phase 1 subsequently completed: see [phase1](phase1.md) and [005](../sdd/005-phase1-review.md). F1 is fixed; other discovery risks and later features remain. Approval for Phase 2 is still required.

- Recommend .NET 10 LTS under the new latest-LTS request; root AGENTS and the initial request still say net8.0 only. Explicitly present the conflict. Retain the exact DBreeze version until separately decided.
- Preserve current wire/storage bytes during Phase 1; isolate behavior fixes from extraction. Later required changes need versioning, a tested migration path, and capability activation or an offline upgrade.
- Corrected single-server membership changes replace the earlier joint-only restriction if approved. Include the 2015 current-term-commit correction and active-on-append configuration rollback/recovery.
- Lease reads default off. Never assume an ordinary DBreeze read transaction or live file copy yields a safe nonblocking multi-table snapshot.
- Workflow is currently in .github/_workflows, inactive. Do not silently activate it or treat prior CI configuration statements as evidence of execution.

The report maps all ten goals to phases, concrete components, blast radius, required regressions, compatibility work, and approval gates. Existing 001/002 specs describe the previous release and have not yet been rewritten.
