# Raft.Net working guidance

## Scope

- Develop the supported implementation under `RaftNet8/`, targeting **net8.0 only**.
- `Raft/`, `RaftStandard/`, `Deployment/`, and `Docu/` are historical artifacts. Preserve existing user changes there. `RaftNet8/legacy-source/` is the copied source baseline, excluded from compilation.
- Pin DBreeze to **1.142.2026.1016-beta** (the published NuGet version of 1.142). Do not silently substitute an older stable release.
- Read `_docs/llm/sdd/001-resurrection.md`, `_docs/llm/okf/index.md`, and `_docs/llm/skill/DBreeze_skill.md` before changing consensus or storage. Update specifications and memory with decisions, evidence, and remaining limitations.

## Correctness

- Use the [Raft paper, Figure 2 and sections 5–8](https://raft.github.io/raft.pdf) as the authority. Persist term and vote before responding; persist log entries before acknowledging them.
- A quorum is `members.Count / 2 + 1`, including self once. Membership is fixed and persisted; changes require a future joint-consensus implementation.
- Commit by quorum counting only entries in the leader's current term. Append a no-op on election. Reject stale responses and never replace a committed entry.
- Apply committed entries in order. Acknowledgment to clients follows local application. Application checkpoints and effects must be atomic or replay-safe. Cancellation/timeouts do not imply that a submitted command was not committed.
- Keep deterministic fault tests for elections, restarts, partitions, message duplication/reordering, conflict repair, and application failures. New safety fixes need a regression reproducing the failure.

## DBreeze and performance

- Follow the downloaded [DBreeze skill](_docs/llm/skill/DBreeze_skill.md). Reuse engines, use short synchronous transactions, and never await inside a transaction. Reserve all write tables before dependent reads.
- Cache hard state and the log tail with bounded memory. Batch ascending log writes and replication. Use key ranges for catch-up and suffix deletion. Heartbeats must not write unchanged state.
- Bound client admission, replication batches, frames, and log-cache bytes. Keep a single owner of consensus state; perform network and application I/O outside that owner.
- Benchmark actual behavior and record runtime, storage mode, parameters, and allocation data. Do not describe unmeasured improvements as benchmark results.

## Validation and delivery

- From `RaftNet8/`: `dotnet restore Raft.Net8.sln`, `dotnet build Raft.Net8.sln -c Release --no-restore`, `dotnet test Raft.Net8.sln -c Release --no-build`.
- Run the demo's automated scenario and BenchmarkDotNet smoke run after changes affecting them. See the solution README for exact commands.
- Keep `_docs/raft_skill.md` aligned with the public API and demonstrated examples. State protocol/storage compatibility and operational limits explicitly.
- Do not commit, push, publish packages, or alter user data unless requested.
