---
type: Project
title: Raft.Net .NET 8 resurrection
description: Supported implementation boundaries and dependency choices.
status: stable
generated: { by: codex/gpt-6, at: 2026-11-12T12:02:56Z }
verified: { by: process:codex-local-validation, at: 2026-11-12T12:02:56Z }
sources:
  - id: nuget
    resource: https://api.nuget.org/v3-flatcontainer/dbreeze/index.json
  - resource: https://github.com/GoogleCloudPlatform/open-knowledge-format/blob/main/SPEC.md
---

# Scope

The user requested .NET 8 only, DBreeze 1.142, correctness work, serious tests, benchmarks, efficient resource use, a human demo solution, and persistent LLM guidance. The active solution is `RaftNet8/Raft.Net8.sln`; legacy projects remain artifacts. Existing modifications to old project/package files were present before this work and must be preserved.

# Dependency

The NuGet V3 version index read on 2026-11-12 lists `1.142.2026.1016-beta` as the 1.142 release. Pin it exactly; do not infer `1.142.0` or silently select 1.138 stable.[^nuget]

# References

See [the specification](../sdd/001-resurrection.md), [the DBreeze skill](../skill/DBreeze_skill.md), and [the audit](consensus-audit.md). The supported API is documented in [the usage skill](../../raft_skill.md). Commands live in the solution README; tested results are recorded in [validation](validation.md) and the [work log](log.md).

# Reference integrity

Downloaded 2026-11-12 and retained without edits:

- DBreeze skill SHA-256: `5F2FE71EF3A4D15636297C2682515C512391240627FB62E1B85E44ABE3D627FE`. Its title is v1.139; used APIs were compiled/tested against the requested 1.142 net8.0 assembly.
- OKF specification SHA-256: `26AA5DA029278939F914E578107242D9607D4F2DC5FE153272B82F9ED1030101`, version 0.2. Concept frontmatter uses `type`, `sources[].resource`, and optional machine verification events; `index.md`/`log.md` retain their reserved roles.

[^nuget]: https://api.nuget.org/v3-flatcontainer/dbreeze/index.json
