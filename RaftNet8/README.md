# Raft for .NET 8

`Raft.Net8.sln` contains the supported library, tests, benchmarks, human demo, and a process-level crash test helper. Every project targets `net8.0`. The SDK selection allows installed .NET 8 feature bands and does not silently switch to .NET 10.

DBreeze is pinned to the exact NuGet release **1.142.2026.1016-beta**, including its `net8.0` assembly. Package lock files are checked in. The beta suffix is part of the upstream release name. The library package/assembly is named `Raft.Net8`; its namespace is `Raft`.

## Build and verify

Install a .NET 8 SDK, then run from this directory:

```sh
dotnet restore Raft.Net8.sln --locked-mode
dotnet build Raft.Net8.sln -c Release --no-restore
dotnet test Raft.Net8.sln -c Release --no-build
dotnet run --project HumanTest/Raft.Demo -c Release --no-build -- --scenario
```

PowerShell users can run `./scripts/verify.ps1` for the same checks. The [human-test solution](HumanTest/Raft.HumanTest.sln) can also be opened directly in Visual Studio; set `Raft.Demo` as the startup project. No external service, container, or administrator privileges are required. The demo binds loopback TCP ports.

The test suite covers persistent voting, term-only updates, split elections, stale messages, Figure 8 commitment, committed-prefix protection, partial batches, replication correlation IDs, partitions, catch-up, bounded admission/caches, cancellation, application/storage failures, ordered application, concurrent clients, binary framing, actual TCP failover/restart, and abrupt process death. Seeded simulations combine message loss, duplication, reordering, partitions, and volatile-state resets, checking invariants after delivery. These tests provide regression evidence; they are not a formal proof of correctness.

## Supported scope

Implemented:

- Fixed membership with majority quorum, randomized elections, durable `currentTerm` and `votedFor`, and candidate log-freshness checks.
- AppendEntries consistency and conflict hints; atomic suffix replacement; persistent commits; an election no-op and current-term quorum commitment.
- Ordered application outside the consensus loop; optional batched application; completion after quorum commit **and local application**; restart replay from the application's checkpoint.
- Bounded proposal admission, event queues, replication batches, binary frames, and a log-tail cache; per-peer replication progress and correlated responses.
- Leader check-quorum, known-leader hints in `NotLeaderException`, and quorum-backed `ReadBarrierAsync`.
- Disk or explicitly volatile DBreeze storage and persistent per-peer TCP connections.

Not implemented: snapshots/InstallSnapshot, log compaction, dynamic membership/joint consensus, pre-vote, learner nodes, automatic client retries/deduplication, TLS/authentication, automatic legacy migration, or an HTTP/client service. The TCP implementation is for trusted networks; checking a member ID does not authenticate it. It exposes no untrusted-network security boundary.

The full log remains on disk. Cache memory is bounded, but disk use grows with retained commands. Monitor space and plan capacity accordingly. Do not delete or truncate live log tables to reclaim space. The in-memory storage factory is useful for simulations and ephemeral deployments; it does not provide crash durability.

## Small runnable example

Reference `src/Raft/Raft.csproj` from a .NET 8 console application. This single-node example exercises the same API without requiring other processes; a single member provides no fault tolerance.

```csharp
using System.Net;
using System.Text;
using Raft;
using Raft.Storage;
using Raft.Transport;

var options = new RaftOptions { NodeId = "n1", Members = new[] { "n1" } };
var endpoints = new Dictionary<string, IPEndPoint>
{
    ["n1"] = new(IPAddress.Loopback, 5001)
};
var machine = new CounterMachine();
await using var transport = new TcpRaftTransport(options, endpoints);
await using var node = new RaftNode(options,
    new DBreezeRaftStorage("./data/n1", options), transport, machine);
await node.StartAsync();
transport.Start(node.HandleAsync);

using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(10));
while (node.Status.Role != RaftRole.Leader)
    await Task.Delay(25, deadline.Token);
var result = await node.ProposeAsync(Encoding.UTF8.GetBytes("increment"), deadline.Token);
Console.WriteLine($"Applied index {result.Index}; count={machine.Count}");
await transport.DisposeAsync(); // stop inbound I/O before automatic node disposal

sealed class CounterMachine : IRaftStateMachine
{
    private long _lastApplied;
    private long _count;
    public long LastApplied => Interlocked.Read(ref _lastApplied);
    public long Count => Interlocked.Read(ref _count);
    public ValueTask ApplyAsync(LogEntry entry, CancellationToken token)
    {
        if (!entry.IsNoOp) Interlocked.Increment(ref _count);
        Interlocked.Exchange(ref _lastApplied, entry.Index);
        return ValueTask.CompletedTask;
    }
}
```

The in-memory application above reports checkpoint zero when recreated and rebuilds by replaying the retained committed log. For a three-node application, all nodes use the same `GroupId`, member IDs, endpoints, and compatible limits, each with its own `NodeId`, data folder, state machine, and transport. Submit to the elected leader. The [complete demo](HumanTest/Raft.Demo/DemoCluster.cs) shows lifecycle handling and an actual replicated application.

## Application contract

`IRaftStateMachine.ApplyAsync` receives every committed entry, including election and read-barrier no-ops. Advance `LastApplied` for no-ops too. A newly constructed application exposes the checkpoint that agrees with its reconstructed/durable state. The library rejects checkpoints beyond the durable commit index.

For external side effects, persist effects and the application checkpoint atomically or make replay idempotent. A crash after effects but before checkpointing can otherwise repeat the effects. The [demo state machine](HumanTest/Raft.Demo/KeyValueMachine.cs) atomically persists values, request IDs, and checkpoints using a shared DBreeze engine. `IRaftBatchStateMachine` lets an application handle one ordered committed batch per call, reducing transaction overhead. The ordinary per-entry interface remains supported. Application I/O must honor cancellation for prompt shutdown.

`ProposeAsync` returns after durable quorum replication and the leader's application of that entry. Followers can still be applying it. A canceled or failed request may have committed; a caller's timeout cannot undo a distributed command. Include stable request IDs and deduplicate inside the replicated state machine when retrying. The demo implements this explicitly. Keep supplied `ReadOnlyMemory<byte>` unchanged until the asynchronous call finishes; the host makes its owned copy after admission.

`ReadBarrierAsync` commits and applies a current-term no-op, then allows a read of that node's state machine. Serialize/read application data safely when querying concurrently with application. Reading `Status` or an application cache without a barrier can return stale data, including on an isolated former leader. Barriers add a log entry; this release does not implement ReadIndex or lease reads.

The application worker is independent of consensus. Slow application consumes bounded outstanding-proposal capacity and delays client results without blocking heartbeats. Application or storage failures fault the node and its `Completion` task; observe that task in a service and surface the error. Restart with a healthy state machine and the same durable identity. `RaftNode` owns/disposes its `IRaftStorage`; it does not own the transport. A shared `DBreezeEngine` remains caller-owned unless the storage constructor's `ownsEngine` flag is true. Dispose transport, node, then a caller-owned engine.

## Storage, bounds, and tuning

DBreeze tables `raft8/meta` and `raft8/log` hold a versioned identity/hard state and an index-keyed log. One engine/folder supports one Raft node/group and may also hold application tables. Reopening a folder with a different group, node, or member set is rejected. Reordering the same members is allowed. No transactions cross `await`; the engine is reused, and log inserts/deletes are ordered. Metadata and the last log index/term are cached; unchanged heartbeats do not write to the database. Startup reads metadata and the tail, rather than loading the entire log. Corruption such as a missing committed entry fails closed.

| Setting | Default | Meaning |
| --- | ---: | --- |
| `TickInterval` | 25 ms | Logical timer cadence |
| `HeartbeatTicks` | 2 | 50 ms heartbeat cadence |
| `ElectionMinTicks` / `ElectionMaxTicks` | 12 / 24 | Random election deadline in `[300, 600)` ms |
| `RpcTimeout` | 200 ms | Per-RPC deadline, shorter than minimum election timeout |
| `MaxAppendEntries` | 128 | Entries per replication/application batch |
| `MaxAppendBytes` | 256 KiB | Encoded entry bytes per batch |
| `MaxCommandBytes` | 64 KiB | Largest individual command |
| `MaxPendingProposals` | 1,024 | Admitted proposals until applied, rejected, or faulted |
| `LogCacheEntries` / `LogCacheBytes` | 1,024 / 8 MiB | Both limits apply; count zero disables caching |

The cache byte budget counts payload plus encoded entry overhead; object/dictionary overhead is additionally bounded by the entry count. Caller-owned tasks waiting for admission are outside the node's retained command budget. TCP accepts a bounded number of inbound connections, caps frame length, and times out idle/partial frames. There is at most one current AppendEntries exchange per peer; election exchanges are also bounded by membership and deadlines. Custom `IRaftTransport` implementations must enforce peer identity and promptly honor cancellation.

Tune election/RPC deadlines for measured network and storage tail latency, including pauses and durable writes. Keep the maximum command plus its 21-byte entry header within `MaxAppendBytes`. Keep resource and protocol limits compatible across members and do not lower command limits below existing stored commands. Large membership (maximum 1,024 IDs) is accepted, but the intended deployment is a small odd-sized quorum, commonly three or five nodes.

## Layout and migration

```text
src/Raft/                    supported library
tests/Raft.Tests/            deterministic, persistence, async, TCP, crash, and load tests
HumanTest/                   human-test .sln and console application
benchmarks/Raft.Benchmarks/  BenchmarkDotNet workloads
tools/Raft.CrashHarness/     isolated child process used by crash tests
legacy-source/              copied historical sources, never compiled by this solution
```

The legacy `RaftNode`, multi-entity TCP API, Biser messages, delayed persistence, and old database format are not reused. The [audit](../_docs/llm/okf/consensus-audit.md) records why election persistence and heartbeat/replication needed replacement. Export application data from an old deployment through its existing API and bootstrap a **new** cluster under an explicit migration procedure; pointing this implementation at historical Raft tables does not migrate them. New storage detects only its own versioned schema.

No NuGet package has been published by this work. Use a project reference or package the library deliberately after reviewing its API and your deployment requirements. The repository's BSD 2-Clause license applies.
