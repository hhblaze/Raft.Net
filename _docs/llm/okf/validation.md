---
type: Validation Record
title: .NET 8 resurrection validation
description: Executed build, safety, crash, TCP, load, example, and benchmark checks with explicit limits.
status: stable
generated: { by: codex/gpt-6, at: 2026-11-12T12:02:56Z }
verified: { by: process:codex-local-validation, at: 2026-11-12T12:02:56Z }
sources:
  - id: results
    resource: ../../../RaftNet8/benchmarks/results/2026-11-12.md
  - resource: ../../../RaftNet8/tests/Raft.Tests/
  - resource: ../../../RaftNet8/scripts/verify.ps1
  - resource: ../../../.github/workflows/raft-net8.yml
---

# Observed result

On 2026-11-12, locked restore and the complete .NET 8 Release solution build succeeded with zero build warnings/errors. All **71 tests passed**, without skipped tests. The scripted human scenario passed on real TCP with three separate disk engines, and the README example compiled/executed as a separate application. All **26** benchmark cases passed a Dry execution check; warmed short jobs covered wire encoding/decoding, disk/memory append batching, and real TCP load.[^results]

# Evidence boundaries

Tests include persistent votes/term-only recovery, Figure 8 commitment, prefix protection, loss/duplication/reordering/partitions/restarts across eight seeded schedules, 2,000 concurrent proposals, application/checkpoint semantics, storage/application faults, TCP framing and failover, and abrupt process exit/kill. They are regression and stress evidence, not formal verification. CI was added for Windows and Linux; only Windows was executed in this environment.

The measurements reduced wire-encoding allocation from 96/128 bytes for heartbeat/AppendEntries to 32/32 bytes. Timing uncertainty is retained in the reports. Dry numbers are not performance estimates; short disk/TCP runs are not capacity planning. See [the benchmark record](../../../RaftNet8/benchmarks/results/2026-11-12.md) for parameters, exclusions, and raw summary tables.

The original five modified legacy project/package files remain user-owned changes. All 57 copied historical source/reference files were hash-compared with their originals and match byte for byte. The supported solution never compiles them.

[^results]: Executed checks and retained benchmark summaries
