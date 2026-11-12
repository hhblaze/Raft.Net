# Human test: watch a real three-node Raft cluster

Open `Raft.HumanTest.sln` and run `Raft.Demo`, or run from `RaftNet8/`:

```sh
dotnet run --project HumanTest/Raft.Demo -c Release
```

The console starts three nodes, each with a separate DBreeze engine, state machine, and loopback TCP endpoint. It prints elections and applied commands. The initial screen shows the leader, term, commit index, application index, and log tail. Ports are assigned automatically; `--base-port 5000` uses ports 5000–5002.

Try these commands, replacing node IDs with the ones shown by `status`:

```text
status
set greeting hello from Raft
get greeting
isolate n1
status
heal
stop n2
set greeting still available with two nodes
start n2
load 1000
status
exit
```

To watch failover, isolate or stop the current leader. Two connected nodes elect another leader; the isolated node eventually steps down because it cannot reach a quorum. Heal/start it and watch its commit/application indices catch up. With only one reachable node in this three-member configuration, writes cannot commit. `get` performs a quorum-backed read barrier before reading the application state.

The default database folders are `.data/human-test/n1`, `n2`, and `n3`, relative to the working directory. Restarting the demo reopens them, including the application checkpoint and request deduplication records. Use `--data <new-directory>` for a fresh cluster; no command deletes existing databases.

For an automated, observable end-to-end check:

```sh
dotnet run --project HumanTest/Raft.Demo -c Release -- --scenario
```

The scenario writes values, isolates a leader and checks that its orphan command cannot commit, writes through the replacement, heals, stops/restarts a leader with its original disk state, submits concurrent commands, and verifies all nodes' application state. It prints `SCENARIO PASS` and exits zero on success. Each default scenario run gets a new directory under `.data/scenarios/`; `--data` selects a specific directory.

`load` reports elapsed demo time, which includes application persistence and logging behavior. Use the [benchmarks](../benchmarks/README.md) for controlled measurements. All three nodes run in one process but communicate using real TCP; stop/start simulates service availability, while process-kill recovery is covered separately by the automated tests.
