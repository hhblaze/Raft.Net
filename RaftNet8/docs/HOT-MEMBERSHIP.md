# Journaled hot membership (wire3 / raft8-v4)

Use this mode for **fresh .NET 8 clusters**. Select both `WireVersion = 3` and `DBreezeLogLayout.UnifiedTableV4` explicitly. DBreeze remains pinned to `1.142.2026.1016-beta`. Default options/constructors retain wire2/v2 for compatibility. There is no v2/v3→v4 converter; opening old data as v4 and mixed-wire deployments fail closed. Preserve old data and fixtures. The old v2→v3 migration is a separate legacy procedure.

## Bootstrap and a joining process

All initial voters use identical original `Members`, `Learners` and `InitialPeers`, a common group/application format, unique local IDs/endpoints and private directories. Every bootstrap voter/learner needs a live descriptor; this is checked when creating a fresh store. Recovery uses the persisted directory and configuration. For example, on n1:

~~~csharp
PeerDescriptor[] initial =
[
    new("n1", "10.0.0.1", 7001),
    new("n2", "10.0.0.2", 7001),
    new("n3", "10.0.0.3", 7001)
];
var options = new RaftOptions
{
    NodeId = "n1", GroupId = "orders", Members = ["n1", "n2", "n3"],
    InitialPeers = initial, WireVersion = 3,
    SnapshotDirectory = Path.GetFullPath("data/n1/images")
};
await using var log = new RaftStorageAdapter(new DBreezeLogStorage(
    "data/n1/log", options, DBreezeLogLayout.UnifiedTableV4));
// security: caller-owned RaftTransportSecurity?; null means trusted-network TCP.
// app: caller-owned deterministic snapshot-capable application.
await using var tcp = new TcpRaftTransport(options, initial[0], security);
await using var node = await RaftNode.CreateAsync(options, log, tcp, app);
await node.StartAsync();
tcp.Start(node.HandleAsync);
~~~

To start n4, keep those **same original** Members/Learners/InitialPeers/group, set `NodeId="n4", JoiningNode=true`, and pass `new PeerDescriptor("n4","10.0.0.4",7001)` as its local transport descriptor. Do not add self to the original Learners: that would create a different bootstrap configuration and contaminate snapshots of the prefix before the logged addition. `JoiningNode` only permits an ID outside the original configuration; it grants no membership or vote. Until the learner entry arrives, local Role is `Removed` (outside the configuration). Retirement is indicated separately by the replicated directory's `IsRetired` tombstone. The flag can stay set on restart; recovered/logged membership remains authoritative.

The host/operator launches the process; the library does not launch OS processes. Existing processes need neither a preprovisioned n4 ID nor restart. The local listener requires literal IP; remote routes support ASCII DNS/punycode or unbracketed IP, resolved asynchronously.

The successful node owns its log store. The host owns transport/application/security/certificates. Stop transport, then node/store, then application.

## Operations, retry and cancellation

~~~csharp
// Persist IDs and complete request bodies at the caller.
var peer = new PeerDescriptor("n4", "10.0.0.4", 7001);
var result = await leader.AddNodeAsync(peer, savedAddId, token);
if (result.Stage != MembershipOperationStage.Completed)
    throw new InvalidOperationException($"{result.Stage}: {result.Error}");

var status = await leader.GetMembershipOperationAsync(savedAddId, token);
// Explicit administrative cancellation; cancelling a token only stops waiting:
var cancelled = await leader.CancelMembershipOperationAsync(savedAddId, token);
var removed = await leader.RemoveNodeAsync("n2", savedRemoveId, token);
~~~

Add/Remove wait for a terminal `Completed`, `Cancelled` or `Failed` result. Inspect `Stage` and `Error`. `NodeId`, `Kind`, `Peer`, `SuccessorId`, `Index` and `Term` identify the target and durable status frontier. `Status.Membership` / `PeerDirectory` publish an immutable local diagnostic view. `GetMembershipOperationAsync` uses a fresh quorum fence on the leader and an indexed disk lookup; null means no committed intent/result for that ID.

The same non-empty Guid/content joins the original operation, including its terminal result. Different content with that ID throws `MembershipOperationConflictException`. Another ID while busy throws `MembershipChangeInProgressException`, whose `OperationId` names the owner. A local reservation covers an uncommitted start too.

Caller cancellation/timeout does **not** cancel an admitted operation. After `NotLeaderException`, resolve the successor through the host/client API and retry the same ID/content, or query it there. Losing a caller connection is not proof of failure. There is no built-in HTTP/gRPC administration service, client forwarding or leader routing.

Only one logical operation owns the lane across entries, elections and restarts. Its committed intent precedes actions. Add registers the descriptor, commits learner membership, waits for leader-acknowledged catch-up within `MaxLearnerPromotionLag`, commits promotion, then records completion. Ordinary commands and ReadIndex continue during catch-up. An unavailable/slow learner waits until success or explicit cancel.

Explicit cancellation is accepted before the final voter configuration is appended. It commits cancellation, removes the learner created by this operation, retires its registration, then records `Cancelled` and releases the lane. Committed entries are never erased. Once the voter configuration is appended, cancellation throws `MembershipCancellationTooLateException` with its current `Status`; query again when needed. Repeating a completed cancellation returns Cancelled. Other terminal operations return the too-late exception with their terminal Status. Remove and standalone Promote are not cancellable Add operations.

For removal of the current leader, intent commits first. Select a reachable voter by greatest acknowledged MatchIndex, then ordinal ID; transfer; the successor commits a current-term entry and continues the saved removal. The original caller normally follows NotLeaderException. A confirmed transfer failure records Failed and retains the original leader; no forced self-removal occurs.

Every configuration uses the latest logged voters, including uncommitted entries, and rolls back with its replaced suffix. The previous configuration and a current-term entry must commit before the next single-server/role change ([2015 correction](https://groups.google.com/g/raft-dev/c/t4xj6dJTP6E/m/d2D9LrWRza8J)). A wire3 learner may durably answer Vote/PreVote but cannot campaign or count itself. A candidate counts only its own latest logged voters. This avoids the promotion-not-yet-received liveness problem described in [etcd's correction](https://github.com/etcd-io/etcd/pull/10998).

Legacy membership APIs share this coordinator in wire3. `AddLearnerAsync` needs a descriptor already in the directory; `AddNodeAsync` is the full new-ID workflow. Standalone `TransferLeadershipAsync` cannot bypass a pending/active operation. Wire2 retains its separate preprovisioned-ID/manual-promotion workflow.

## Persistence and limits

One primary DBreeze table `raft8/v4` has ordered numeric ToIndex namespaces:

| Prefix | Data |
| --- | --- |
| 1 | Identity, RF4M hard-state/frontier, directory/active operation, snapshot/install/history-generation metadata |
| 2 | Checked business/no-op/configuration/membership log |
| 3 | Configuration index |
| 4 | Sparse service index for projections/recovery/suffix rollback |
| 5 | Terminal results keyed by generation / operation Guid |

Publish only after DBreeze Commit; no await occurs inside a transaction. Ordinary writes do not scan management history or rewrite an unchanged directory. Heartbeats do not write unchanged hard state. Results have no TTL and consume disk, with indexed lookup and at most128 rows per transaction/page. Runtime memory holds one bounded directory/active view.

Custom applications skip business effects for `entry.IsNoOp` while advancing checkpoint index/term through service records; DBreeze helpers do this automatically. V4 snapshots wrap the application, directory, active operation and streamed terminal history at the **same applied index/term fence**. Incoming history stages by image ID; a durable install intent allows recovery across application activation. History generation switches atomically with the log snapshot/frontier. Superseded/abandoned staging is collected in bounded128-row steps. Replay and wrapping use additional temp disk/I/O.

| Bound | Contract |
| --- | --- |
| Historical IDs | 1024 total, including removed/cancelled tombstones; stable IDs are not reused |
| ID / address | ID1..128 ASCII letters/digits/-/_; host1..253 ASCII DNS/IP characters; port1..65535 |
| Active operations | One per cluster, independent of caller deadlines/leadership |
| Service record | ≤4096 bytes; error text ≤1024 characters |
| V4 directory/metadata | ≤900000 bytes |
| Configuration payload | Independent of business MaxCommandBytes in wire3; fits min(270000, MaxAppendBytes−21, ReplicationByteLimit−21). Reject an oversized Add before intent. |
| Wire3 budgets | Application/replication budgets must fit4096+21 bytes |
| Frame body | max(replicationBytes, SnapshotChunkBytes+900000)+1024; default1052288 B, TCP prefix4 B extra |
| Routes / inbound sockets | ≤1024 historical routes; max(4,2×routes) ≤2048 sockets; retired routes permit final ACKs; idle incoming sockets expire after about30s |
| Other memory | Existing proposal/read, in-flight message/byte and cache bounds apply |
| Memory mode | CreateInMemory(options, UnifiedTableV4) forgets management state/results and term/vote/log; disposable lab only |

A reachable quorum and `MinimumVoters` remain mandatory. Two voters require both; Add/Remove cannot repair a lost quorum. Arbitrary producer tasks awaiting admission and host application objects are outside the node's memory budget. Log/snapshot retention does not delete authoritative terminal-result history.

## Directory repair and TLS

New connections/revisions exchange bounded routing metadata before ordinary RPCs. A trusted peer can introduce a new leader ID to a stale replica after its original contacts retire. Hints change only transport routes/admission, never Core voters, term, operation state, checkpoint or commit. Log/snapshot replication supplies authoritative state.

mTLS authenticates chain/name and certificate-to-ID binding before accepting an unknown sender's directory exchange. Host policy must authorize future IDs/certificates. Trust roots, private keys and authorization rules are not replicated. Keep SANs compatible with ID-based TLS TargetHost. Plain TCP assumes a trusted network.

## Observe and verify

New HumanTest directories default to v4; old v2/v3 markers remain compatibility paths. A saved endpoint file keeps v4 routes stable across a full demo restart. `add n4` executes the whole workflow; `operation <guid>` / `cancel <guid>` inspect/control it; `remove <leader-id>` transfers automatically. Existing data is never implicitly converted.

The [offline books](human/tutorial/index.html) contain the runnable3→5 exercise and compiled C# examples. [Process tests](../tests/Raft.Tests/ProcessClusterTests.cs) preserve PIDs and kill the leader during Add. Reducer regressions cover Add/Remove boundaries, competing former leaders, cancellation/failed transfer. [Snapshot tests](../tests/Raft.Tests/MembershipSnapshotTests.cs) transfer a150-result history across page boundaries and an earlier snapshot fence.
