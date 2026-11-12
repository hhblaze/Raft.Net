---
name: raft-net8
description: Integrate and operate this repository's .NET 8 fixed-membership Raft library with DBreeze storage, ordered state-machine application, and TCP transport.
---

# Use Raft.Net8

Use the supported code in `RaftNet8/src/Raft/`, built by `RaftNet8/Raft.Net8.sln`. The library targets net8.0 and pins DBreeze **1.142.2026.1016-beta**. The older `Raft/`, `RaftStandard/`, deployment packages, and copied `legacy-source/` tree describe an incompatible historical API. Read the [API and contracts](../RaftNet8/README.md) and [complete working demo](../RaftNet8/HumanTest/Raft.Demo/DemoCluster.cs) when integrating.

## Construct a node

1. Create `RaftOptions` with a unique `NodeId`, a shared `GroupId`, and the same fixed `Members` on every node. IDs contain ASCII letters/digits, `-`, or `_`. Give each node its own data folder and endpoint; use an odd-sized quorum for fault tolerance.
2. Create `DBreezeRaftStorage(path, options)`, an `IRaftStateMachine`, and an `IRaftTransport`. `TcpRaftTransport(options, endpoints)` needs endpoints for all members, including self.
3. Create `RaftNode(options, storage, transport, machine)`, await `StartAsync()`, then connect inbound TCP with `transport.Start(node.HandleAsync)`. Only the node's loop accesses the consensus core. Custom transports route requests to `HandleAsync(from, request, token)` and return the corresponding response from the requested peer.
4. Submit `ProposeAsync(command, token)` to the leader. A follower throws `NotLeaderException`, whose `LeaderId` can be null. A successful `ProposalResult` identifies the committed/applied index and term. Observe `node.Completion` to surface storage/application faults.
5. Dispose transport, node, then any caller-owned DBreeze engine. The node owns its storage; a storage constructed from an existing engine leaves that engine open unless `ownsEngine: true` was supplied. The path constructor owns its engine.

Do not persist business data in the library's `raft8/meta` or `raft8/log` tables. The storage binds one engine/folder to one group/node/member set and rejects identity changes on reopen. Membership changes require a future joint-consensus implementation; editing the configured list on a live or persisted cluster is not supported.

## Implement the application

`IRaftStateMachine.LastApplied` is the checkpoint for the application's actual reconstructed or durable state. `ApplyAsync(LogEntry, token)` is serialized, receives **no-ops too**, and must advance that checkpoint to `entry.Index`. Ignore a no-op's business effects while still advancing the checkpoint. The host applies only durable committed entries, in index order, outside the consensus loop. It acknowledges proposals after local application; followers can still lag.

Persist effects and the checkpoint atomically, or make replay idempotent. A fresh volatile application reports zero and rebuilds by replaying the retained committed log; a durable application reports its durable checkpoint. The host rejects a checkpoint beyond commit. Optional `IRaftBatchStateMachine.ApplyBatchAsync` receives a bounded ordered committed range and must advance through its last entry; use it to amortize durable application writes. See [KeyValueMachine.cs](../RaftNet8/HumanTest/Raft.Demo/KeyValueMachine.cs) for shared-engine transactions, batch checkpointing, and durable request deduplication.

Canceled/timed-out requests may still commit. Reuse a stable request ID on retry and deduplicate inside the replicated state machine when the application requires it; the library does not deduplicate commands. Keep the input memory unchanged until `ProposeAsync` finishes. Application callbacks and custom transports must honor cancellation for prompt disposal.

## Reads, timing, and resource bounds

- For a linearizable local application read, first await `ReadBarrierAsync(token)` on the leader, then read the application's concurrency-safe state. Each barrier commits/applies a current-term no-op. Status and ordinary local reads can be stale; a former leader isolated from quorum cannot serve a valid new barrier.
- Default ticks are 25 ms, heartbeats 50 ms, elections randomized in `[300, 600)` ms, and RPC timeout 200 ms. Tune for measured network/storage tail latency. RPC timeout must remain below minimum election timeout.
- Defaults: 128 entries / 256 KiB per batch, commands up to 64 KiB, 1,024 admitted proposals, and a cache bounded by both 1,024 entries and 8 MiB of encoded entry bytes. Count zero disables the log cache. Keep limits compatible across nodes; do not lower command limits below retained data.
- `Metrics` exposes admitted proposal count, in-flight RPC count, and transport failures. Storage exposes transaction/cache counters. Admission remains occupied while an already-appended command is pending, even if its caller cancels waiting.
- Use the downloaded [DBreeze instructions](llm/skill/DBreeze_skill.md) for application storage: one reusable engine, short synchronous transactions, no `await` in transaction scope, reserve complete write sets before dependent reads, enumerate/load values before disposal.

## Supported boundaries

Fixed membership, full retained logs, non-Byzantine nodes, and trusted-network TCP. No InstallSnapshot/compaction, live membership changes, pre-vote, TLS/authentication, automatic client routing, or compatibility with the old API/wire/database. Monitor disk growth. Memory storage is explicitly volatile. Checking member/group IDs is not authentication. Do not claim formal verification or production capacity from the included tests or smoke benchmarks.

## Verify an integration

From `RaftNet8/`, run locked restore, Release build/tests, then `dotnet run --project HumanTest/Raft.Demo -c Release -- --scenario`. The [human-test README](../RaftNet8/HumanTest/README.md) lists interactive failure commands. Use [BenchmarkDotNet workloads](../RaftNet8/benchmarks/README.md) for measurements and preserve workload/runtime context. When changing the library itself, read [AGENTS.md](../AGENTS.md), the [specification](llm/sdd/001-resurrection.md), and [project memory](llm/okf/index.md), then update them with tested evidence.
