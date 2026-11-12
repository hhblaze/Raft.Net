---
type: Architecture
title: Consensus host and application pipeline
description: Single-owner consensus, bounded queues, independent application, and trusted-network TCP.
status: stable
generated: { by: codex/gpt-6, at: 2026-11-12T12:02:56Z }
verified: { by: process:codex-local-validation, at: 2026-11-12T12:02:56Z }
sources:
  - id: node
    resource: ../../../RaftNet8/src/Raft/RaftNode.cs
  - resource: ../../../RaftNet8/src/Raft/Core/RaftCore.cs
  - resource: ../../../RaftNet8/src/Raft/Transport/TcpRaftTransport.cs
---

# State ownership

The deterministic core handles explicit ticks and typed RPCs; it has no application/network callbacks. The async host owns it through a bounded event channel, batches queued proposals, and dispatches RPCs with deadlines outside the core. A separate application worker reads committed ranges, applies them serially or through `IRaftBatchStateMachine`, and completes pending client proposals after local application.[^node]

# Contracts that must survive future edits

- Cancellation ends the caller's wait; an already-durable proposal retains its admission slot until applied or rejected. The host copies command memory after admission. Callers keep input memory unchanged until completion.
- On leadership loss, fail uncommitted pending proposals while allowing already-committed proposals to finish application. A failure/timeout remains an ambiguous client outcome; replicated request IDs are needed for retry deduplication.
- No-op entries participate in application checkpoints. `ReadBarrierAsync` commits/applies a fresh no-op and can precede a concurrency-safe local application read.
- A storage/application exception faults the node's observable `Completion` task. Queue cancellation must not mask the underlying error of an already-enqueued incoming RPC.
- Per-peer AppendEntries correlation IDs prevent stale replies from regressing progress. Failed sends retry on ticks, not recursively.
- Pooled TCP buffers remain rented until I/O finishes; decoded log commands own their bytes. Frame sizes/counts and inbound connections are bounded. Group/member IDs are checked, but there is no authentication or TLS.

[^node]: RaftNode and its failure/batch/runtime tests
