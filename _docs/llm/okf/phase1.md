---
type: Engineering Record
title: Phase 1 deterministic core separation
description: Approved .NET 8 retention, pure reducer architecture, compatibility evidence, fail-stop correction, and next gate.
status: stable
generated: { by: codex/gpt-6, at: 2026-11-12T15:32:49Z }
verified: { by: process:codex-phase1-validation, at: 2026-11-12T15:40:30Z }
sources:
  - resource: ../sdd/004-phase1-core-separation.md
  - resource: ../sdd/005-phase1-review.md
  - resource: ../../../RaftNet8/src/Raft/Core/RaftCore.cs
  - resource: ../../../RaftNet8/src/Raft/Runtime/RaftDriver.cs
  - resource: ../../../RaftNet8/tests/Raft.Tests/CoreTransitionTests.cs
---

# Decision and gate

The user approved Phase 1 with **“.NET8 should stay”**. Keep net8.0. Phase 1 is complete; [the review](../sdd/005-phase1-review.md) records changes, tests, safety argument, and limitations. Phase 2 still requires approval. Do not mark the overall ten-goal enhancement complete.

# Architecture

Raft.Core holds immutable state and a pure event/effect reducer. I/O interfaces, tasks, clocks, random providers, and original mutable exception objects stay in the runtime. A monotonic effect ID correlates bounded read, durable write, and timeout sample completions. State changes and dependent actions cannot cross an incomplete persistence fence.

RaftDriver executes effects outside the core under the node's single-owner channel. RaftStorageAdapter uses a bounded channel worker for synchronous transactions. Separate application and client admission semantics remain. Phase 1 serializes dependencies; true disk overlap/group commit and multi-inflight replication are future work.

Existing APIs and explicit IRaft* implementations use default interface bridges. New integrations can use CreateAsync and ILogStore/ITransport/IStateMachine/IClock/IRandom. Wire v1 and raft8-v1 database values are unchanged. Snapshot metadata and future event types reject unsupported operations; they do not provide snapshot/configuration features.

# Evidence

- Locked restore and zero-warning Release build; 95 passing tests, no skips, including all original 71 cases.
- Fresh actual TCP/disk scenario and extracted README application passed.
- 272 baseline API type/member signatures preserved; an unchanged baseline-compiled binary client passed.
- 997 compiled core methods plus fields inspected with no forbidden direct dependencies.
- All 26 benchmark Dry cases executed with nonempty measurement/statistics reports. These are execution checks, not capacity/latency claims.
- All 53 resolved dependency version/hash entries remain unchanged after central package management.
- F1 was reproduced with red tests, then fixed: validate inputs before admission; every execution/storage failure halts. Original errors propagate; uncertain durable votes survive restart and prevent a second candidate receiving the vote.

# Remaining

F2 identity binding, F3 integrity/recovery, F4 checkpoint term, and F6 deadline/supervision remain explicit follow-ups. All later feature phases remain pending. The workflow is inactive in .github/_workflows; only Windows was executed. Offline generated-project validation needed process-scoped NuGetAudit=false because the feed was unreachable; project security settings were not changed.
