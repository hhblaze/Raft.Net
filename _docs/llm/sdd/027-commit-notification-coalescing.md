# Piggyback idle commit notifications

Status: implemented R18, full342tests and1400-run current-reducer matrix plus all integration checks passed. New12notification cases cover both layouts and majority-only failover; initial8fail/2pass reproduces extra idle commits, then all pass. Complete final R18 five-round comparison achieves12/12fixed Pipeline median points over both refs,8/8distributed; independent data/browser/hash QA passed. R17 diagnostic5n showed about270 append and268 hard-state updates per4096 commands,818/s(instrumented). Larger packets and collection timers alone did not resolve the cost. Source/runtime/raw R17 are archived separately.

## Cause and permitted behavior

After every quorum commit the current leader immediately sends a new empty Append to idle caught-up peers. A follower may fsync that commit frontier separately before the next data append arrives. The [Raft paper](https://raft.github.io/raft.pdf), Figure2 and section5.3, carries the commit index in future AppendEntries, including idle heartbeats; it does not require an extra immediate empty RPC for each advancement. Clients already wait for durable quorum log replication and leader application, not application on every follower.

Add CoalesceCommitNotifications(defaulttrue) for modern wire2/configuration-capable storage. On ordinary commit advancement, send immediately only to peers needing data, a read context, snapshot progress or retirement acknowledgment. Preserve immediate broadcasts for uncommitted/new configuration, leadership transfer and read controls; keep the existing unconditional scheduled heartbeat. Opt-out and wire1 retain prior behavior. No durability suppression: term/vote/log/commit persistence, CRC/frontier, quorum counting, local application and client completion remain unchanged. Only commit-only network notification is deferred, normally until next data or one logical heartbeat; storage/network/owner delays may add lag.

Implement an immutable boolean on CoreAction.SendAll, with pure filtering by owned state. No timer/task/callback/I/O enters Core. Ordinary proposal/election/read/config/retirement send paths keep their existing behavior. Document follower apply lag and explicit opt-out.

## Required regressions

Before behavior changes, test modern v2/v3 with manually controlled ticks: after a successful proposal, all follower logs are durable but no extra empty commit RPC is sent until a tick, next data or ReadIndex. Each must propagate the frontier without needing unrelated progress. Test opt-out immediate behavior. Kill the acknowledged leader before the delayed notification; a survivor with the durable entry must establish a new current-term commit and apply the old command exactly once. Config/transfer/snapshot/full fault and wire compatibility tests remain required.

Run full safety/integration, diagnostic actual writes and fresh references before any final matrix. Preserve all earlier measurements and original common application work. No speed claim from fewer messages alone.

Final R18 evidence:342tests/integration and fresh1400-run fault matrix passed; complete five-round report meets fixed Pipeline12/12median throughput points over both references,8/8distributed. See winner-r18-2026-11-13.html and OKF benchmark record. Memory/p99 losses and operational follower-lag/frame-limit trade-offs remain explicit.
