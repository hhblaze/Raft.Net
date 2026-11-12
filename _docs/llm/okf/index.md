# Raft.Net knowledge bundle

OKF v0.2. Concept IDs are paths relative to this directory, without `.md`.

- [Project and migration](project.md) — goals, source boundaries, dependencies, and commands.
- [Consensus audit](consensus-audit.md) — legacy findings and replacement invariants.
- [Storage](storage.md) — table schema, ownership, caching, and transaction rules.
- [Runtime](runtime.md) — queues, application, cancellation, failure propagation, and TCP.
- [Validation](validation.md) — executed checks and measurement limits.
- [Work log](log.md) — dated implementation and validation evidence.

The upstream specification is stored as `OKF-SPEC.reference.txt`; it is a reference, not a concept document. Specifications live in `../sdd/`; the original DBreeze skill lives in `../skill/`.
