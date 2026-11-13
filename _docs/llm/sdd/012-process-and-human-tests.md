# Separate-process clusters and human demo

Status: implemented and validated: real 3/5-process kill, snapshot catch-up, rolling transfer/restart, learner promotion and removal/restart passed. The updated human scenario also passed; full suite 221/221.

Provide a one-node worker mode in the human-test executable: real TCP endpoint, separate v2 Raft DBreeze log, generation-based application, snapshot repository and atomic dedup/checkpoint. JSON lines on a local stdin/stdout control channel expose status, set/read/stale-read, transfer, add/promote/remove, snapshot and orderly quit. No diagnostic chatter is emitted on this channel. This is a test harness, not a public unauthenticated management server.

Tests launch 3/5 voting processes plus a provisioned spare, force-terminate the leader (TerminateProcess on Windows, SIGKILL equivalent on Unix), elect/write, compact before rejoining, restart using the same disk directories, roll every voter with leadership transfer, add/catch up/promote/transfer/remove and verify values/roles/checkpoints/faults. Capture stderr and include worker state in failures. Close processes and verify owned paths before cleanup.

The interactive demo should use the same supported v2 generation application and show elections, configuration, read fences, transfer, snapshot progress and restart. Preserve legacy project/source artifacts and the old shared-engine key/value example as a compatibility fixture; new generation folders are separate and must not silently reinterpret an old data directory. Document offline v1 migration and the explicit application adaptation needed to opt into snapshots.
# Regression found by process testing

The 3/5-process scenario reproduced a restart failure after removal on a snapshot-enabled node: `SnapshotRepository` revalidated the recovered membership as fresh bootstrap and required self to remain a member. Directory recovery is independent of voting rights. The repository/build context now accept the authoritative recovered configuration without self; the node/core still recover the `Removed` role and never campaign. Both scenarios failed before this fix (`process-clusters-cleanup.trx`). This preserves the persisted-configuration and non-voter invariants; it changes no election/quorum rules.
