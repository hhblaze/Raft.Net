---
type: Engineering Audit
title: Legacy consensus safety and replacement design
description: Evidence from the historical implementation and invariants required of its successor.
status: stable
generated: { by: codex/gpt-6, at: 2026-11-12T12:02:56Z }
verified: { by: process:codex-local-validation, at: 2026-11-12T12:02:56Z }
sources:
  - id: raft
    resource: https://raft.github.io/raft.pdf
  - resource: ../../../Raft/RaftNode.cs
  - resource: ../../../Raft/StateMachine/StateLog.cs
---

# Findings

- `RaftNode.LastVotedTermId` is a volatile field, assigned by `ParseCandidateRequest` without durable vote storage. It also tracks a term rather than the candidate voted for. Restart can permit a second vote in one term; retransmitted requests from the same candidate cannot be handled correctly.
- `StateLog` restores `NodeTerm` from the final log entry's term. Term changes without appending a log entry are lost on restart.
- `ParseLeaderHeartbeat` assigns the leader and arrival time before rejecting old terms. Stale messages can change leader tracking and election timing.
- The separate heartbeat/log-suggestion protocol differs substantially from the Figure 2 AppendEntries consistency and commit boundary. Replace it with one term-checked RPC and a matched-prefix commit bound.
- Delayed persistence can acknowledge entries before durable storage. The supported disk implementation must acknowledge only committed DBreeze transactions.

# Required invariants

Persist term/vote before any response or election RPC. Vote only once per term, with repeat grants to the same candidate. Compare candidate last term first, then index. Protect committed prefixes. Restrict leader quorum-counted commitment to current-term entries. Use a no-op after election to establish earlier-entry commitment. Followers cap commit at the prefix matched by that specific AppendEntries request. Ignore stale correlation IDs, and count each configured member once.[^raft]

The supported core implements these rules with persistent hard state, typed RequestVote/AppendEntries RPCs, fixed identity/membership, and bounded per-peer progress. [The protocol contract](../sdd/002-protocol-contract.md) maps the rules to regressions. [Validation](validation.md) records the passing tests and the remaining feature boundaries.

[^raft]: https://raft.github.io/raft.pdf
