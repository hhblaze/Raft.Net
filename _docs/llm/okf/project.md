---
type: Project
title: Raft.Net .NET 8 resurrection
description: User authorization, supported tree, pinned dependencies and reference integrity.
status: stable
sources:
  - resource: ../sdd/006-autonomous-enhancement.md
  - resource: ../../../RaftNet8/Directory.Packages.props
  - resource: https://api.nuget.org/v3-flatcontainer/dbreeze/index.json
---

# Authorization and scope

The user reaffirmed **.NET 8** and explicitly revoked all phase approval gates: take ownership of reasonable changes, iterate tests/benchmarks/fixes and do not ask for phase approvals. Specifications 003–005 and old gate language are historical. Active direction is [006](../sdd/006-autonomous-enhancement.md), covering all ten requested enhancements; optional leases/follower forwarding/joint consensus are outside the chosen implementation.

On2026-11-13 the user asked to remember their DBreeze schema preference: one physical table can store different types using numeric ToIndex prefixes and composite keys. Apply when appropriate, retaining bounded namespace ranges and versioned migration. The [supplementary skill](../skill/DBreeze_project_patterns.md) records it without editing the upstream reference.

Supported solution: `RaftNet8/Raft.Net8.sln`; human solution: `RaftNet8/HumanTest/Raft.HumanTest.sln`. Legacy Raft/, RaftStandard/, Deployment/, Docu/ and excluded legacy-source/ remain historical artifacts with pre-existing user changes preserved. Do not commit/push/publish or alter deployed data without instruction. No subagent/delegation requirement applies to this task.

The assembly is Raft.Net8, namespace Raft, net8.0 only. DBreeze **[1.142.2026.1016-beta]** is the requested 1.142 release's exact NuGet version and .NET 8 asset, not a guessed 1.142.0 or older stable substitute. Logging abstractions 8.0.3 use net8.0 assets. All projects use central package versions and checked lock files. Original public signatures/v1 golden bytes remain; v2 changes require offline copy migration and coordinated wire activation.

# Working commands and references

Run locked restore, Release solution build, tests/Raft.Tests and the demo scenario from RaftNet8/. The [README](../../../RaftNet8/README.md), [runbook](../../../RaftNet8/docs/OPERATIONS.md), [usage skill](../../raft_skill.md) and [validation record](validation.md) describe current behavior. Active CI is `.github/workflows/raft-net8.yml`, not the historical inactive `_workflows` copy. Remote CI has not run here.

Local restricted environment uses process-local DOTNET_CLI_HOME under TEMP, NUGET_PACKAGES at USERPROFILE/.nuget/packages and NuGetAudit=false only when its vulnerability feed is unreachable. Do not commit audit suppression or accidentally restore into an empty CLI_HOME package cache. Windows mTLS/profiled tests need OS-backed temporary key access; approved dotnet-test escalation provides it. Avoid heavy tests during final benchmark timing.

# Reference integrity

Downloaded 2026-11-12 and retained without edits:

- DBreeze skill SHA-256 `5F2FE71EF3A4D15636297C2682515C512391240627FB62E1B85E44ABE3D627FE`; title v1.139, APIs compiled/tested against 1.142 net8.0.
- OKF specification SHA-256 `26AA5DA029278939F914E578107242D9607D4F2DC5FE153272B82F9ED1030101`, version 0.2. Concept metadata uses type/sources[].resource; index/log keep reserved roles.
- Benchmark baseline `8b991bd` has eight unmodified C# files in benchmarks/Raft.Baseline/source. Text was compared with git show after normalizing only line endings. The wrapper changes assembly identity for aliasing; original behavior/bugs are not patched for better comparison.

Benchmark winner goal completed with R18: fixed Pipeline12/12local median-throughput points over both baseline8b991bd and DotNext5.26.2/net8,8/8distributed,5rounds/360measurements/614400samples. Report RaftNet8/benchmarks/results/winner-r18-2026-11-13.html;342tests and fresh1400-run fault matrix/HTML QA passed. All allocation points and somep99 comparisons still lose; no universal/remote-capacity claim. PureCore/durability/clientACK preserved; independent frame/application limits and coalesced follower commit notifications/opt-out/upgrade constraints are in configuration/runbook/skill.
