# Work log

## 2026-11-12

- **Validation**: Locked restore and Release build passed with zero build warnings/errors; all 71 tests passed. The TCP/disk human scenario and extracted README example passed. All 26 benchmark cases executed; warmed wire/append/TCP reports are retained. Skills, OKF structure, links, and original-source hashes were checked. See [validation](validation.md).
- **Implementation**: Added a net8.0-only solution, deterministic consensus core, DBreeze hard state/log storage, bounded cache/admission/batching, separate application worker, persistent framed TCP transport, checkpoints/read barriers, and an interactive human-test solution.
- **Correction**: A failure-injection test exposed lifetime cancellation masking an incoming RPC's storage error. Queued request completion now propagates the actual error; cancellation only governs caller waiting/queue admission.
- **Performance**: Added batch application and header-only DBreeze term reads. Warmed measurements show 32-byte heartbeat and AppendEntries encoding allocations after removing LINQ/enumerator allocations; timing limitations are retained in reports.
- **Guidance**: Added root AGENTS, SDD/protocol contracts, OKF concepts, the full requested upstream DBreeze skill plus skill entrypoints, and the canonical `_docs/raft_skill.md`. Explicitly documented fixed membership, no snapshots/compaction, trusted-network transport, and new wire/storage/API compatibility boundaries.

- Inspected legacy projects and found pre-existing local project/package modifications.
- Retrieved DBreeze reference skill and OKF v0.2 specification from the user-supplied URLs.
- Queried NuGet V3: requested 1.142 is `1.142.2026.1016-beta`, including a net8.0 assembly.
- Recorded initial safety findings and the implementation specification before changing the supported code.
- Historical projects were preserved, and 57 source/reference files were copied into the excluded baseline. No package was published and no commit/push was performed.
