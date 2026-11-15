# Raft.Net knowledge bundle

OKF v0.2. Concept IDs are paths relative to this directory, without `.md`.

- [Project and migration](project.md) — goals, source boundaries, dependencies, and commands.
- [Consensus audit](consensus-audit.md) — legacy findings and replacement invariants.
- [Enhancement discovery](enhancement-discovery.md) — historical Phase 0 findings and original plan; approval gates were revoked.
- [Phase 1](phase1.md) — historical pure core/effect baseline and compatibility evidence; its later-phase gate was revoked.
- [Autonomous progress](autonomous-progress.md) — current full-scope work; supersedes historical approval gates.
- [Storage](storage.md) — table schema, ownership, caching, and transaction rules.
- [Runtime](runtime.md) — queues, application, cancellation, failure propagation, and TCP.
- [Validation](validation.md) — executed checks and measurement limits.
- [DotNext comparison](dotnext-comparison.md) — pinned external implementation, benchmark adapter, fresh measurements and comparability limits.
- [Benchmark winner work](benchmark-winner.md) — completed R18 local winner criterion, accepted/reverted experiments, provenance and operational limits.
- [Human tutorials](human-tutorial.md) — English/Russian offline HTML books, compiled three-process C# example, disk/RAM semantics and limits.
- [Hot membership](hot-membership.md) — delivered wire3/v4 journaled hot add/remove, validation and measured p99 limitations.
- [R19 report](benchmark-r19.md) — current-only benchmark refresh with explicitly reused R18 reference data.
- [Accepted hot-membership baseline](performance-recovery.md) — user-accepted d44147e, runtime improvements, preserved e06 controls and latency limits.
- [External clients and entities](cluster-clients.md) — retained message streams, local transactional subscriber cursors, large-message APIs and endpoint discovery.
- [Work log](log.md) — dated implementation and validation evidence.

The upstream specification is stored as `OKF-SPEC.reference.txt`; it is a reference, not a concept document. Specifications live in `../sdd/`; the original DBreeze skill lives in `../skill/`.

Current working direction: [autonomous enhancement](../sdd/006-autonomous-enhancement.md). The user revoked phase approval gates; keep .NET 8 and continue the complete enhancement scope without phase questions.
