# Watch a Raft cluster

Open `Raft.HumanTest.sln`, select `Raft.Demo`, or run from `RaftNet8/`:

```sh
dotnet run --project HumanTest/Raft.Demo -c Release
```

Three voters start with real loopback TCP and separate DBreeze log/application/snapshot directories. Status prints roles, terms, frontiers, snapshot index, voters/learners and replication windows. Commands print applied values and election transitions. Ports are automatic; `--base-port 5000` reserves 5000–5003, including the spare `n4`.

```text
status
set greeting hello from Raft
get greeting
isolate n1
status
heal
stop n2
set greeting still available with a quorum
start n2
load 1000
snapshot
add n4
promote n4
transfer n4
remove n1
status
exit
```

Replace node IDs with those shown by `status`. Isolating/stopping the current leader causes failover; it cannot commit without quorum. `get` runs a fresh **ReadIndex** before querying. `add n4` starts the provisioned spare and commits it as learner; `promote` waits for leader-acknowledged catch-up. `transfer` requires an eligible voter. Configuration changes may report a gate/busy/leadership error during an election; wait for stable status and retry. Removed nodes remain Removed after restart.

The default `.data/human-test-v3/` is reused on restart and uses the prefixed single-table log. The demo recognizes its `demo-format` marker; existing `raft-demo-v2` directories remain readable with `--data` using v2 storage. It refuses to reinterpret an unrecognized non-empty directory. Use `--data <new-directory>` for a fresh cluster. It never deletes previous demo databases. Do not reuse a modified-membership scenario directory to request a brand-new initial scenario; choose a fresh one.

```sh
dotnet run --project HumanTest/Raft.Demo -c Release -- --scenario
```

Each scenario uses a new `.data/scenarios/` directory unless `--data` is supplied. It verifies quorum loss, orphan exclusion, failover/restart, concurrent proposals, snapshot catch-up beyond compaction, learner addition/promotion, transfer, removal/restart and ReadIndex. Success prints `SCENARIO PASS` and exits zero. `load` reports illustrative elapsed time; use [benchmarks](../benchmarks/README.md) for controlled measurements.

The interactive cluster runs in one process; its nodes still communicate over actual TCP. Automated `ProcessClusterTests` launch **one OS process per node**, use the demo's private JSON-lines `--worker` control mode, kill the leader without disposal, restart from disk, roll voters and add/promote/transfer/remove in 3/5-voter clusters. Other crash tests kill at storage/snapshot publication boundaries. The worker control channel is stdin/stdout, not an unauthenticated remote admin endpoint.
