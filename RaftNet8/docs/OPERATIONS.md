# Operations

The optional external message service has a separate [client/ledger runbook](CLUSTER-CLIENTS.md). Keep its HTTP(S) origins separate from peer TCP addresses. All nodes in a message group must run the same MessageLedger application; use fresh application directories rather than mixing arbitrary old state machines. Hosts require the ASP.NET Core8 shared framework; external clients require only.NET8. The subscriber owns the durable cursor locally, not on the cluster. Standard history and request-result metadata grow without TTL. Cache's latest-only application policy does not remove old technical log entries until snapshot/compaction; the default snapshot interval is0/manual. For large histories raise image/unpack limits and reserve replay/install space before adding an empty node.

Use distinct absolute log/application/snapshot directories per stable node ID. Never open two engines/nodes over the same directory. Provision compatible command/batch/image limits, group ID, identities, endpoints and certificates on every peer. Persisted membership overrides bootstrap lists on restart.

## Transport identities and TLS

For fresh groups, [wire3 / raft8-v4 hot membership](HOT-MEMBERSHIP.md) replicates PeerDescriptor(ID,host,port), updates existing routes without restart and repairs stale routes through bounded trusted directory exchange. KnownPeers/InitialPeers are bootstrap inputs; inspect node.PeerDirectory for the current immutable view. Directory hints never change voters. Roots, private keys and certificate authorization remain host-owned.

In legacy wire1/2 modes, `KnownPeers` authorizes provisioned IDs, never votes. Provision future IDs/routes on every existing host; `UpdatePeerAsync` changes only an already provisioned route. The old restart requirement for a new ID belongs to that compatibility mode.

Plain TCP belongs on a trusted network. For mTLS, supply a caller-owned certificate with private key and an authorization function:

```csharp
var security = new TlsRaftTransportSecurity(localCertificate,
    (peerId, remoteCertificate) => allowedCertificates.TryGetValue(peerId, out var expected) &&
        StringComparer.OrdinalIgnoreCase.Equals(expected, remoteCertificate.Thumbprint));
var tcp = new TcpRaftTransport(options, endpoints, security);
```

This example assumes a protected, operator-provisioned certificate map and platform-trusted chains. Default outbound name validation uses the **Raft peer ID** as TargetHost; provision matching SANs. A private-PKI validation callback must implement the intended chain/name/revocation policy; accepting every certificate defeats authentication. Certificate-to-ID binding remains mandatory. Stop connections/rotate trust and certificates coherently; the library does not distribute trust configuration. Windows SChannel needs a usable private-key provider; the test fixture reimports temporary PFX keys rather than relying on unsupported ephemeral key handles.

## Hot Add/Remove on wire3

Use explicit UnifiedTableV4, WireVersion3 and a canonical initial descriptor list. Start a new process with the same original voters/directory/group and self as a local learner. On the leader call `AddNodeAsync(peer, operationId, token)`; this owns registration, learner, catch-up and promotion. No existing process restarts. `RemoveNodeAsync(id, operationId, token)` transfers first if removing the leader; its successor resumes the persisted operation after current-term readiness.

Persist a non-empty operation Guid and exact request at the caller. Same ID/content joins the old operation; different content conflicts. A different ID while busy gets MembershipChangeInProgressException with the active ID. Only one logical operation owns the lane across elections and restarts. Inspect terminal Stage/Error. Wait cancellation/timeout does not cancel an accepted operation. Resolve NotLeader and retry the same ID/content, or GetMembershipOperationAsync on its successor. This is an indexed fresh quorum lookup.

Use CancelMembershipOperationAsync for an explicit Add cancellation. Before final voter-configuration append it commits cancellation, removes the created learner, retires registration and records Cancelled before freeing the lane. Afterwards it throws MembershipCancellationTooLateException; query the result. Unavailable learners otherwise wait indefinitely. A confirmed transfer failure produces Failed without forced leader removal. Legacy membership/standalone transfer APIs cannot bypass the lane in wire3.

There is no v2/v3→v4 converter; use fresh directories for a new group and preserve old data. Do not start mixed wire versions. The1024 historical-ID limit includes removed/cancelled tombstones, which are never reused. Results are indexed on disk without TTL, and included in streamed v4 snapshots at the application fence. See [the full contract and bounds](HOT-MEMBERSHIP.md) and the [RU/English books](human/tutorial/index.html).

## Legacy wire2 add, promote, remove

1. Provision a new unique ID and authenticated route everywhere. Start its empty data directories using the current voters plus itself in bootstrap `Learners`; use compatible snapshot application/format ID. It cannot elect itself or count toward quorum.
2. On the current leader, establish readiness with `await leader.ReadIndexAsync(token)`, then `await leader.AddLearnerAsync(newId, token)`. A leadership transition/current-term gate can require retrying the control request.
3. Wait for the **leader's acknowledged** `Status.Peers` match index to be within `MaxLearnerPromotionLag` of its tail. The learner's local tail alone is not proof of acknowledgment.
4. Call `PromoteToVoterAsync(newId, token)`. Await its committed/applied result before the next server/role change. Under sustained writes, temporarily reduce writes to meet the promotion lag gate.
5. Call `RemoveNodeAsync(oldId, token)` on the leader. Removing a voter must leave at least `MinimumVoters`. Removing the leader is supported: it steps down after the new configuration commits under the new quorum. Prefer transferring first for planned maintenance.

Only one server/role change may be uncommitted. Configuration takes effect on append, and rollback restores the previous one. The corrected single-server rule also requires a current-term committed entry before changes. Do not implement membership by editing log tables or replacing the configured voter array. During transition, require a majority of the active configuration, not a count of running processes.

Removed nodes persist/recover Removed and do not campaign, including snapshot-enabled nodes. Wait for retirement acknowledgment before decommissioning files. Keep IDs unique; do not run an old and replacement process with the same identity simultaneously.

## Transfer and rolling restart

`await leader.TransferLeadershipAsync(targetVoterId, token)` stops new proposals/reads/config changes on the old leader, catches up the target and sends TimeoutNow to bypass pre-vote. It completes when the requested target's leadership is observed, or aborts within the election timeout. Cancellation is a wait cancellation and does not retract a sent TimeoutNow.

For each planned restart, transfer if that node is leader, require a fresh ReadIndex on the new leader, stop transport/node/app, restart from the same directories, and wait for acknowledged catch-up before stopping another voter. Do this only within a mutually compatible wire/storage/application version. This is tested in actual 3/5-process clusters. A canceled/timed-out proposal may have committed; retry only with application deduplication.

## Replace a failed node

With a healthy quorum, add a **new ID** as learner using empty directories, let log/snapshot catch-up complete, promote it, then remove the failed ID (one change at a time). Size the temporary voter configuration so its required majority is available; promoting a replacement changes that majority. If necessary remove the failed voter first only while the remaining configuration can commit and remains above the configured minimum.

Do not copy another live node's log directory and change its NodeId: identity binding deliberately rejects that. Loss of quorum cannot be repaired with Add/Remove APIs; they require a quorum. The library exposes no unsafe force-new-cluster or arbitrary commit-index rewrite.

## Snapshots and restore

Set an absolute `SnapshotDirectory`, use checked v2 or v3 log storage and `ISnapshotStateMachine` (normally `DBreezeGenerationStateMachine`). `CreateSnapshotAsync` publishes a durable image and compacts to the configured retained tail. `SnapshotIntervalEntries` automates this; zero means manual. Cancellation stops waiting, not an already running build. Images and generation state use private owner locks; never edit their pointers/descriptors or delete pinned images.

Snapshot creation replays deterministic application transactions in another generation; estimate enough disk for live, replay, prepared generation and retained images. Throttle outbound `SnapshotBytesPerSecond`, watch offsets/replication lag, and keep latest images/backups. If reception/retention is busy, manual creation can return a retryable busy error. Local/storage execution failure fail-stops instead.

For a single replica with a healthy group, prefer a new learner/replacement. For a stopped replica, restore a consistent backup of its **log + application root + snapshot repository** with the same identity/application format, then start through `RaftNode.CreateAsync`. It finishes pending install intent before elections, verifies image SHA-256, validates checkpoint index/term and restores a missing/behind application from the published image. A bare image is not a standalone new-cluster bootstrap API.

For disaster recovery without quorum, restore enough original identities from coherent backups whose retained histories include the required committed prefix. Backup age limits recoverable acknowledged writes. Preserve original failed data for investigation; the library cannot infer missing committed history or safely manufacture a quorum from one stale snapshot. Test the selected backup procedure against the application's consistency requirements.

## Upgrade raft8-v1 to v2

Mixed wire versions are rejected, so this transition is **offline for the whole group**. A rolling transition from wire 1 to 2 is not supported.

1. Stop clients and all old nodes/application engines; make backups. Record original node/group/voters and committed/applied frontiers.
2. Migrate each node into a distinct non-existent destination outside its source. Preflight requires an existing raft8-v1 identity and hard state; it never initializes a missing source identity:

   ```sh
   dotnet run --project tools/Raft.Migrate -c Release -- old/n1 new/n1 mygroup n1 n1,n2,n3
   ```

3. Inspect `raft-migration.json`, copied file checksums, frontiers and business checkpoints before activation. Sources are retained. Targets with `.raft-migration-incomplete` refuse startup; discard only that failed new target and repeat from the preserved offline source. Tables outside the source/reparse trees are rejected; use a suitable coordinated filesystem backup/migration for those layouts.
4. Adapt the application checkpoint explicitly. The built-in legacy demo's index-only checkpoint gets its corresponding term automatically. Other applications can call `RaftStorageMigration.CompleteIndexOnlyCheckpoint(engine, v2Log, table, indexKey, termKey)` offline after proving their effects match that index; it writes the checked `(index,term)` checkpoint. Physical business tables are copied unchanged; their application schema is not guessed.
5. Continue a shared-engine application with `DBreezeStateMachine`, or rebuild a separate generation application from the complete migrated committed log with the same deterministic handler and validate its state. The demo's new generation layout is not an automatic import of arbitrary old business data. Enable compaction only after verifying the new snapshot-capable app and image.
6. Deploy all nodes on v2 storage/wire 2 with preserved identities, start a quorum, run ReadIndex/write/read and catch-up checks, then reopen clients. Old binaries are sealed out of migrated targets. Once v2 has accepted new writes, starting old source copies is not a safe rollback.

The original pre-.NET-8 projects use different schemas and contracts. This migration accepts **raft8-v1**, not their historical databases. Export validated committed application state/history and implement an application-specific migration rather than treating those files as v1.

## Upgrade raft8-v2 to v3

V3 keeps wire2 and checked record bodies, but uses ordered `ToIndex` namespaces in one physical DBreeze table. Choose `new DBreezeLogStorage(path, options, DBreezeLogLayout.UnifiedTableV3)` for new or explicitly migrated data. Existing constructors continue to select v2; selecting a different layout never migrates a database automatically. Old v1/v2 binaries refuse v3 targets. V2/v3 describe local disk layouts, not incompatible wire versions.

1. Stop clients and the whole group, dispose transports/nodes/applications/engines, and back up each stable node's log, application root and snapshot repository together. Use a fresh destination outside each source. Do not keep a second process with the same node identity running.
2. For each stopped **DBreeze database folder**, run from RaftNet8/:

~~~sh
dotnet run --project tools/Raft.Migrate -c Release -- --v2-to-v3 <source-db> <new-db> <group> <node> <initial-voter-list>
~~~

The API equivalent is `RaftStorageMigration.CopyV2ToV3(source, destination, options)`. Persisted bootstrap/current membership and known peers are copied, rather than replaced from the supplied bootstrap options. Group/node must match. A completed v1-to-v2 migrated database can be copied again to v3.

3. The tool copies files/tables **inside the DB folder**, verifies checked source records on the copy, converts in bounded ascending batches, verifies the complete v3 log/frontier/configuration and flushes `raft-v3-migration.json`. It does not copy sibling/outside application or snapshot directories: copy those separately while stopped, preserve generation/image descriptors, and point the new node to their copied absolute paths. A shared-engine application's physical business tables/checkpoint remain intact. No handler or application schema conversion is inferred.
4. Do not start a target carrying `.raft-migration-incomplete`; interrupted targets remain for inspection. The original source is retained. Keep all tables physically inside the source for this copy method; reparse points and alternative table files outside it are refused. Reserve enough staging space for a complete physical copy plus conversion.
5. Switch the stopped group to the new directories and explicit v3 constructors, then verify ReadIndex, write/read, replica catch-up, snapshots and another clean restart before reopening clients. The application/image format and wire2 configuration stay compatible. Rolling conversion is not the validated migration workflow here. After any new committed writes, the old copies are stale and cannot be used as a safe rollback.

## Diagnose availability/fail-stop

Watch term churn, current voter quorum, peer lag/flight bytes, pending/admission counts, uncommitted bytes, apply lag and durable Commit tail latency. An isolated former leader cannot complete fresh ReadIndex/writes. A slow app delays client results and consumes bounded admission while consensus can still run. A slow log store can delay the owner and requires suitable deadlines.

Always observe `Completion`. Storage/application errors retain the original exception and halt the node; do not swallow them and keep participating. Stop/dispose, preserve data and recover/restore before rejoining. Status alone is stale diagnostic information. Diagnostic callback failures are counted separately and do not replace a storage error.

## Wire2 frame-limit upgrade

MaxReplicationBytes now separates wire2 packet bytes (automatic1MiB+2688B) from MaxAppendBytes application bytes (default256KiB). Encoding and storage formats are unchanged, but older/smaller receivers reject oversized frames. Before a rolling upgrade with older wire2 nodes, explicitly set MaxReplicationBytes=262144 on every new node, matching the older cluster limit. Keep that value throughout the binary upgrade. To increase it afterward, ensure every node has compatible bounds; use a coordinated stop/configure/restart rather than assuming a mixed send/receive-limit rolling transition. Existing data requires no conversion. Wire1 remains on its old MaxAppendBytes limit and rejects an explicit modern override.

CoalesceCommitNotifications defaults true for wire2 with modern storage. Ordinary idle commit frontiers piggyback on the next data packet or scheduled heartbeat; follower apply can lag by that logical heartbeat plus I/O/network/owner delay. Term/vote/log/commit persistence and quorum-plus-leader-application client completion are unchanged. Read contexts, new configuration, transfer, snapshot progress and retirement retain immediate/control send paths. False restores the prior attempt to send an extra idle commit RPC; a peer with an existing flight can still await later traffic. Wire1/legacy storage retain prior behavior. See specification027 and the majority-only leader-death regressions.
