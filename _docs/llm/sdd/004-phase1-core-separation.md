# Phase 1: .NET 8 and deterministic core separation

Status: complete; [Phase 1 review and evidence](005-phase1-review.md). Approved by the user's Phase 0 reply: **“.NET8 should stay”**.

## Scope and compatibility

Keep net8.0, SDK policy, public API signatures, DBreeze 1.142.2026.1016-beta, wire v1, and raft8-v1 storage. Centralize existing package versions without upgrading them. Nullable, implicit usings, deterministic builds, and warnings as errors already apply.

Extract fixed-membership Figure 2 behavior into an internal pure reducer: state and an explicit event produce new state and effects. State snapshots are immutable; no storage, clock, random, transport, task, lock, or callback is reachable from the reducer. Timeout samples arrive in completion events from the injected runtime IRandom. Bounded storage queries return owned results through correlated completion events. Never hydrate the full log.

The reducer may suspend on one external effect. Its immutable continuation specifies what follows the result; it never captures an executable I/O delegate. A mismatched completion cannot advance state or release replies. Durable metadata changes only after successful persistence completion. Send/response/application effects cannot cross an incomplete persistence dependency.

The channel-based node driver remains the single owner, executing effects through adapters outside the reducer. Phase 1 serializes an event through its dependencies, retaining current protocol ordering; concurrent disk scheduling/group commit belongs to Phase 2 and replication pipelining to Phase 6. Separate ordered application remains intact.

Add ILogStore, ITransport, IStateMachine, IClock/ITicker, and IRandom seams while retaining existing IRaft* contracts and constructors. The default log-store adapter runs short synchronous DBreeze operations on a bounded channel worker. It never awaits inside a transaction. Legacy simulation/benchmark adapters can execute effects inline without putting I/O inside the core.

Use IBufferWriter<byte> for pooled framing while preserving exact bytes. Share already owned immutable LogEntry references when replicating; retain command ownership when pooled receive memory is returned.

## Reviewable change groups

1. Central package management and byte/API characterization.
2. Pure state/event/effect model, storage/runtime adapters, and driver integration; unchanged protocol rules.
3. Separate regression/fix changes for demonstrated failure classification and effect supervision where the new executor requires them. Record any additional behavioral fix explicitly; do not hide it in extraction.
4. Updated architecture/API/operational limits, knowledge, tests, demo, and benchmark smoke evidence.

Future pre-vote, transfer, ReadIndex, membership, integrity schema, and snapshots remain unsupported in Phase 1. Their event/storage extension points must not masquerade as implemented features. The Phase 0 plan remains the full objective.

## Validation gate

- Existing 71 tests and TCP/disk demonstration pass.
- Deterministic replay from identical immutable state/events yields identical state/effects without mutating prior state.
- Hold persistence and read completions; verify dependent votes, appends, replies, and apply notifications are withheld. Reject stale completion IDs.
- Characterize existing RPC bytes and storage values, successful/failing requests, and application/cancellation semantics.
- Verify no forbidden dependencies inside core sources and no unbounded log preload.
- Run locked restore, zero-warning Release build, full tests, human scenario, and all benchmark smoke cases. Report execution separately from performance claims.

Authority: Figure 2, paper §5.2–5.4; hard state/log durability precedes externally visible dependent actions. No election or quorum algorithm change is intended.

Stop at the Phase 1 gate and request approval for Phase 2.
