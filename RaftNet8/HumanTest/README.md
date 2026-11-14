# Watch a Raft cluster

Open `Raft.HumanTest.sln`, select `Raft.Demo`, or run from `RaftNet8/`:

```sh
dotnet run --project HumanTest/Raft.Demo -c Release
```

Three voters start with real loopback TCP and separate DBreeze log/application/snapshot directories. New directories use wire3/v4. Status prints roles, terms, frontiers, snapshot index, membership operation ID/stage and replication windows. Ports are automatic; `--base-port 5000` selects5000–5005 for n1..n6. V4 saves endpoints in demo-endpoints.json and reuses them after full restart.

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

Replace IDs with those shown by `status`. Isolating/stopping the leader causes failover; it cannot commit without quorum. `get` uses fresh ReadIndex. In v4, `add n4` starts a new node in the demo process and executes registration→learner→catch-up→voter without restarting old nodes; `add n5` reaches five voters. `remove <leader-id>` transfers automatically and continues the same journaled ID on the successor. Results print MEMBERSHIP ID/stage. Use `operation <guid>` or explicit `cancel <guid>`. Wait timeout does not cancel an accepted operation. Standalone transfer requires an eligible voter and an unoccupied membership lane. Removed IDs remain retired across restart.

The default `.data/human-test-v4/` is reused on restart. V2/v3 marker directories remain explicit compatibility paths using the old learner/promote workflow. No old directory is implicitly converted; an unrecognized non-empty directory is rejected. Use `--data <new-directory>` for a fresh group; previous data is preserved. Do not reuse a modified-membership scenario directory for a brand-new initial scenario. See [hot membership](../docs/HOT-MEMBERSHIP.md) for API semantics, snapshots and limits.

```sh
dotnet run --project HumanTest/Raft.Demo -c Release -- --scenario
```

Each scenario uses a new `.data/scenarios/` directory unless `--data` is supplied. It verifies quorum loss, orphan exclusion, failover/restart, concurrent proposals, snapshot catch-up beyond compaction, learner addition/promotion, transfer, removal/restart and ReadIndex. Success prints `SCENARIO PASS` and exits zero. `load` reports illustrative elapsed time; use [benchmarks](../benchmarks/README.md) for controlled measurements.

The interactive cluster runs in one process; its nodes still communicate over actual TCP. Automated `ProcessClusterTests` launch **one OS process per node**, use the demo's private JSON-lines `--worker` control mode, kill the leader without disposal, restart from disk, roll voters and add/promote/transfer/remove in 3/5-voter clusters. Other crash tests kill at storage/snapshot publication boundaries. The worker control channel is stdin/stdout, not an unauthenticated remote admin endpoint.
