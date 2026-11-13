# Published cache reads during storage I/O

Status: implemented R6 candidate after R5 completed unchanged. Regression red run:6 expected failures/4 passes; after the fix47 focused checks and299 full tests passed, zero-warning build, human scenario, native adapter and BDN Dry passed. Whole-stack targeted measurement is running; no isolated throughput gain is yet claimed. Draft/base hashes remain under RaftNet8/.artifacts/winner/r6-draft/.

## Problem

TryQueryCached currently attempts the same monitor that serializes the complete DBreeze transaction/flush. It returns quickly when busy, but queues the read on the storage worker even when the application only needs older, already durable and cached entries. This creates a potential dependency between application of a committed prefix and an unrelated later append. A whole-stack throughput gain has not been established.

## Proposed ownership

- Retain the existing storage monitor for every DB transaction, mutation, ordinary query, recovery and disposal. Do not await inside a transaction or perform any I/O in the cache path.
- Introduce a short cache-publication monitor. Lock order is storage then cache; a cache-only read never acquires the storage monitor. Fast reads use nonblocking TryEnter and retain the worker fallback for contention/misses.
- Protect cache dictionary, LRU, byte counts and hits with the cache monitor. Publish hard state, frontier, snapshot/intent, configuration, known peers, invalidation and new immutable entries together after successful DBreeze Commit, under that monitor.
- Existing published entries may be read during a pending write, linearizing before its publication. Never expose new/replaced entries before Commit. A future range beyond the published frontier while a write is pending falls back to the worker, preserving existing deferred-read behavior.
- Publish poisoning and disposal under the cache monitor too. Preserve the original exception; neither a cache hit nor a previous published state may hide an already published failure. Keep engine disposal outside the short cache monitor.
- Memory bounds, CRC/frontier/storage representation, quorum/current-term rules, application checkpoint and acknowledgment semantics remain unchanged. Do not copy the entire cache on every write or add an unbounded secondary cache.

## Required evidence

First apply only the regression changes to the unchanged production implementation. A committed cached entry must complete inline while a subsequent append is deliberately paused before commit; current code is expected to fail that assertion. Pending future-entry reads must still wait and return the newly published entry afterward.

Cover suffix replacement and compaction on v2/v3: old entries/snapshot are available consistently before commit, replacement/compaction and metadata appear together afterward. Concurrent replication reads must never combine terms from two suffix generations. Retain cancellation, disposal, original-failure identity, all write-boundary/process-kill/model/recovery tests.

After correctness passes, compare actual full-stack throughput/latency/allocations against both unchanged references. A successful inline-read assertion alone is not a benchmark result. Retain all previous R4/R5 reports and raw data; use a distinct R6 artifact/report name if this experiment is retained.
