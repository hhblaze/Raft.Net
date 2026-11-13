# Consistent streamed snapshots

Status: implemented; final 221-test suite, broad seeded simulation, multi-chunk disk/TCP and OS-process recovery validation passed. Current architecture and operational documentation describe the delivered behavior.

## Consistency and creation

Ordinary DBreeze read transactions are not assumed to provide MVCC snapshot isolation (the upstream DBreeze skill explicitly warns against that assumption). Create a separate application generation from the previous complete image and replay a pinned immutable committed prefix through an atomic applied(index,term) fence. The live application continues applying. Only one builder runs; compaction waits for the completed image. Incoming installation cancels/joins a builder before mutating its log source.

Use a closed replay engine to produce a bounded-memory image. A manifest binds application format and checkpoint. Images and descriptors are immutable, fsynced, and SHA-256 checked before publication. Local snapshot publication validates the exact committed boundary and configuration, then compacts while retaining a configurable tail. No file copy of an open live DBreeze engine is used.

## Reception and activation

The pure core first validates/adopts the leader term durably and refreshes leader contact. The runtime then stages bounded chunks outside the consensus owner, validating offset/length/CRC and fsyncing data before acknowledging progress. Persist progress for resume after disconnect/restart. A failed transfer never changes the active application. Whole-image verification and extraction run outside the owner. Repeat final polls until complete; each chunk carries matching metadata so stale snapshots can be acknowledged without installation.

For an accepted image newer than local commit:

1. Persist an install intent (metadata and suffix-retention decision) in the Raft DBreeze transaction.
2. Pause local apply between batches; prepare/open a separate verified application generation.
3. Atomically replace the durable generation pointer, then publish the in-process application reference. Keep the original exception on failure.
4. Report the correlated activation completion to the pure core; atomically publish snapshot/log/frontier/configuration/commit and remove the intent.

During this interval the core can process ticks/votes and persist higher terms, but suppresses campaigns and rejects normal AppendEntries. A committed image remains valid if leadership changes. Startup finishes a durable install intent idempotently before starting elections; if the application checkpoint is behind a published snapshot, restore that immutable image before replay. Thus a crash before/after either publication cannot cause applying old log entries to a new application generation.

Retain a local suffix only when its entry at the snapshot boundary has the same term. Otherwise discard the uncommitted local tail. Never install an image at/below local commit. Configuration at the boundary is in the image; a retained later configuration stays active through the configuration index. A resumed/new leader can use the same immutable image ID, while term and request IDs still reject stale control messages.

## Limits and failure model

Bound chunk bytes, image bytes, unpacked bytes, archive entry count, inflight chunks (one per snapshot peer), and streaming bandwidth. Validate archive relative paths and reject reparse/path traversal. Use closed distinct generations on Windows; never rename an open DBreeze database. File fsync + DBreeze commit ordering is tested for process crash, without claiming hardware power-loss certification or directory-fsync guarantees unavailable through the portable .NET API. Recovery fails closed when a referenced durable image is missing/corrupt.

Required validation: continued writes during replay; compaction/restart; lagging learner recovery; interrupted/resumed chunks; checksum/path rejection; leader change during streaming; actual process kills at each intent/generation/log publication boundary; application effects/checkpoint agreement after recovery. Authority: Raft dissertation chapter 5 and the committed-prefix/state-machine-safety invariants.

Evidence: snapshot storage/retention 9 cases, runtime 7 cases (real 3/5-node TCP/disk catch-up, missing application restoration, receiver restart, leader change, slow-bandwidth contact), 15 actual child-process kill boundaries. Full 193/193 passed in `snapshots-refined-full.trx`. Each completed image has a format/checkpoint manifest; extraction bounds sizes/counts and rejects path traversal/alternate streams. Prepared generations are serialized separately from live apply. Repository/application owner locks prohibit concurrent directory owners.

Retention runs outside the core/owner and pauses new snapshot creation/reception admission while collecting. Pin the current image, every peer's active transfer and the prepared incoming image; retain at least the latest two additional catalog candidates. Delete descriptors before unreferenced images so a crash leaves only harmless orphan data. Restart can resume a received image rename interrupted before descriptor publication by verifying the completed file against durable incoming progress.
