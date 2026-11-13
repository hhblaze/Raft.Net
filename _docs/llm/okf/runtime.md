---
type: Architecture
title: Consensus runtime and protocol contracts
description: Pure reducer, single owner, ReadIndex, corrected membership, bounded replication and secure transport.
status: stable
sources:
  - resource: ../../../RaftNet8/src/Raft/Core/RaftCore.cs
  - resource: ../../../RaftNet8/src/Raft/RaftNode.cs
  - resource: ../../../RaftNet8/docs/ARCHITECTURE.md
  - resource: ../../../RaftNet8/docs/CONFIGURATION.md
---

# State and effects

The immutable reducer has no storage/service interfaces, I/O/task/clock/random/lock or executable I/O continuation. Time, timeout samples and correlated read/write/snapshot completions arrive as events. The runtime driver executes bounded queries/durable mutations and only then releases dependent sends. Bounded separate proposal/control channels share one consensus owner; coalesced notifications avoid fragmenting queued proposal batches. Application and network/snapshot staging run independently. Storage and application have separate background threads for synchronous DBreeze work; genuine async application callbacks remain supported without a custom synchronization context. Slow log I/O still delays owner events; tune deadlines from actual tails.

Pre-vote requests do not persist receiver term/vote or reset its timer. Recent valid leader contact suppresses disruptive pre-votes. Check-quorum steps a leader down on missing voter contact. Learners/Removed never campaign or grant votes. Current-term-only commit and election no-op remain mandatory.

ReadIndex batches fresh contexts, gates on current-term commit, captures commit, confirms eligible voter quorum and waits local apply. Ordinary application/status reads remain stale-capable. Legacy ReadBarrier appends a no-op. Transfer stops new proposals/reads/config changes, catches up an eligible voter, sends TimeoutNow to bypass pre-vote, observes requested new leadership or aborts within the election timeout. A stale negative TimeoutNow reply cannot reject a transfer after the target has advanced its term; its regression failed before the fix.

# Membership and flow

Latest configuration activates on append, recovers by index/term and rolls back with the log. Only one server/role change may be pending; both prior configuration and a current-term entry must commit before another (2015 correction). Add learner, await leader-acknowledged match, promote, remove; respect MinimumVoters/promotion lag. A removed leader sends final commit and retires. Recovered membership overrides bootstrap options. Provision KnownPeers/routes/TLS trust everywhere before addition; provisioned IDs never count as voters.

Probe/Replicate/Snapshot peers maintain bounded count/byte flights; cumulative valid acknowledgments reclaim older flights. Stale IDs are ignored and rejection/unreach collapses to Probe. Uncommitted byte/proposal/read budgets provide backpressure. Snapshot streaming keeps independent contact heartbeats. TCP v2 multiplexes correlated exchanges; v1 remains serialized. Bounded frames/handlers, owned decoded command bytes and cancellation-safe pooled writes preserve memory ownership. Partial canceled writes break a connection rather than corrupting the next frame.

MaxProposalBatchBytes=null permits a local durable group min(4×MaxAppendBytes +21×MaxAppendEntries,uncommitted budget,64MiB), normally1MiB+2688B. MaxAppendEntries still caps count; wire/application remain at MaxAppendBytes (default256KiB). No command is acknowledged before its actual quorum plus apply. Existing stores/layouts retain their durability rules. A bounded contiguous run of successful current-term append completions can be delivered longest-prefix-first; all core correlation checks still run. Read/control/failure/term/configuration/transfer boundaries stop reordering. Deterministic real-completion/duplicate/read-barrier regression proves fewer intermediate commits without crossing ReadIndex.

The older R2 owner-waiting/full-wire early-flush policy is superseded by [spec020](../sdd/020-proposal-collection.md): deadlines run outside the owner; small2ms/bulk16ms/single1ms windows use immutable published status and shorten when committed application work drains.

# Client and lifetime

Successful proposals require quorum durability and leader apply, not all followers. Cancellation/timeouts stop waiting and may leave committed data: stable application request IDs and transactional deduplication are required for safe retries. Memory stays unchanged until proposal completion. Fail-stop execution errors of every type preserve the original exception through callers and Completion. Malformed input is validated before admission; never catch ArgumentException around execution and classify it as client input.

Node owns the successfully constructed log store, never transport/application/caller engine/logger/certificate. Stop transport then node/store then application/caller engines. The default trusted-network TCP overload is plaintext; TLS/mTLS hooks plus TlsRaftTransportSecurity require chain/name validation and certificate-to-peer binding. Meter/ActivitySource `Raft.Net8` and ILogger expose bounded-ID diagnostics without payloads. Broken diagnostic callbacks are counted and isolated from original storage/app failure.


The bounded proposal channel now has a separate asynchronous collection deadline task. The consensus owner continues processing control/RPC events while commands gather; only that owner drains commands and assigns log indices. The change signal is bounded and carries no command payloads. Leadership/transfer/memory checks still run on the owner before persistence.

BulkProposalBatchDelay is nullable: auto min(8×ProposalBatchDelay, one heartbeat,1second), normally16ms for byte-limited groups while committed application work remains. Count-limited groups keep ProposalBatchDelay (default2ms); single-voter windows cap at1ms. When the committed application pipeline empties, the collector uses the small window to avoid idle waiting. Set BulkProposalBatchDelay=TimeSpan.Zero to disable the intentional bulk wait. Explicit bulk values must fit one heartbeat/one second. This trades bounded intentional collection latency for group amortization; wire/application/count/admission/durability limits remain unchanged.

Current R18 candidate implements specification027 commit-only notification coalescing (wire2/modern storage), with CoalesceCommitNotifications=false opt-out. Data/read/config/transfer/snapshot/retirement paths remain active; periodic heartbeat still runs. Followers can learn/apply a frontier one logical heartbeat later, while leader client completion still follows durable quorum and local application.342tests and a fresh1400-run fault matrix passed. Current bulk default8ms(spec025/026), wire bytes1051264 vs application262144(spec024/026); earlier16/25ms notes are historical.
