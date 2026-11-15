---
type: Implementation Record
title: External cluster clients and named message entities
description: User-owned subscriber cursors, full retained history, cache values, bounded payload streams and separate endpoint discovery.
status: stable
sources:
  - resource: ../sdd/032-cluster-clients-and-entities.md
  - resource: ../../../RaftNet8/docs/CLUSTER-CLIENTS.md
---

The user's new description is authoritative; legacy documentation is reference only. One Raft group, arbitrary server count, one leader for all entities; external clients do not vote. New net8.0 protocol/server/client projects expose a separate HTTP(S) client port. Peer wire3/raft8-v4 and36 production source files remain unchanged from the accepted d44147e baseline (project-relative ordinal normalized source fingerprint8851CD78E3A5042138F12E367805DDBB50CA51F28119347DB9EF0EACC29E58E5). Rebuilt DLL hashes can differ by build path; do not replace historical baseline hashes.

Standard retains full payload history on disk independently of compactable Raft log entries. HighSpeedCache retains only the latest payload on disk plus bounded RAM read caching, with gaps allowed. Each entity has a persistent independent counter. First append creates Standard; cache requires explicit creation. No deletion, mode change or ID reset. Stable request Guids provide durable idempotence and conflict rejection; results have no TTL, including in cache, so metadata grows.

Subscriber application saves effects/data and cursor together in its own transaction, commits, then returns true. Restart supplies that saved ID as exclusive afterMessageId. No persistent ClientId, server-side cursor or Raft subscription ACK commands. Payload and ID-only callbacks are serial per subscription; false/exception retries the same buffered record. At least once is explicit. Cache may lose overwritten data across client restarts. SDK process-local positions support reconnect, not durable application effects.

Logical payload ceiling16,000,000B leaves envelope room in core16MiB. Stream overloads buffer a bounded message for retries; big files use generic chunk entities and a separate manifest entity. Compression/encryption belong to application code. Retrieval by entity+ID streams to caller output; rollback partial chunks before retry. Full Standard history/counters/results/endpoints are part of generation snapshots. Default64GiB image limit is inadequate for80GB histories; account for every replica, log, application, replay, image, install and output copy.

Endpoint origins are replicated application metadata, distinct from peer routes. The host Add wrapper registers before journaled Add; addresses advertise when a role appears. Raw Add needs explicit client registration. SDK verifies GroupId, refreshes topology every second by default and protects authoritative views from older hints. Bootstrap requires at least one surviving known origin or stable DNS/out-of-band update. HTTPS/Authorize/credentials belong to the host/client, separate from peer mTLS.

Experiments use user-provided D:\Temp\RaftNet8 (about1.34TiB free when first checked); new scratch data belongs there. Sources and reviewable results remain in the repo. `RAFT_EXPERIMENT_ROOT` routes all test scratch there; the test helper validates ownership before deletion. Native killed-child handle release is probed before opening an engine; no DBreeze recovery failures are retried/suppressed. Test collections run sequentially to avoid oversubscribing shared IO/wall-clock elections. Disk/process sample/fixture profiles were adjusted; embedded core defaults/source are unchanged.

Final:435/435 tests, zero skips, locked restore/Release build zero warnings; embedded HumanTest and29 BDN Dry cases pass. Independent node/client processes pass killed leader, reconnect, Hot Add without PID changes, automatic leader removal and manifest-file SHA-256. A real3221225472-byte file used202 chunks; ID-only delivery and download SHA-256 passed, and all3 databases independently reopened at checkpoint207/term1 with202 chunks/1manifest. No80GB or3GiB-image test was run. Both30-chapter RU/EN books pass offline links/anchors,13 compiled-source hashes and deterministic regeneration; skill validation passes.

The [validation manifest](../../../RaftNet8/docs/CLIENT-VALIDATION-2026-11-15.json) preserves raw paths and historical failed development runs. Final [HTML benchmark](../../../RaftNet8/benchmarks/results/cluster-clients-2026-11-15.html) contains20 final fresh clusters/10240 measured commands and retains all3 series/30720 individual latencies. Final measured hashes match delivered DLLs. HTTP/full retained history on D differs from the accepted embedded-core workload; do not invent R18/R19 deltas or claim the small cache/frame changes produced a systematic throughput win. Final median3n rates≈462/375 and5n≈308/285 commands/s for64B/16KiB; p99/allocations remain visible. This is initial service characterization, not a capacity guarantee.
