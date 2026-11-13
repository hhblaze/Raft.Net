# Comparison baseline

`source/` is the unmodified C# library source from repository commit `8b991bd` (`RaftNet8/src/Raft`). Only the assembly name/project wrapper differs, so the benchmark can load it alongside the new library using an assembly alias. Both use .NET 8 and DBreeze 1.142.2026.1016-beta. These files are historical benchmark artifacts, not a supported second implementation. The repository's BSD license applies.

This is the implementation immediately before the requested enhancements. Its v1 storage has weaker integrity/recovery checks, no snapshots/membership/pre-vote, and serialized one-flight replication. The benchmark supplies the same atomic durable application transaction and checkpoint encoding to both versions. Batching-off means one entry per proposal/replication/apply batch; the old version never pipelines. Report that distinction with any speed comparison. Archived pre-.NET-8 projects have different APIs, durability and acknowledgment contracts and are not equivalent throughput baselines.

Verify provenance from the repository root: for each `source/<path>`, compare its text to `git show 8b991bd:RaftNet8/src/Raft/<path>`. Do not edit these reference sources to improve results.
