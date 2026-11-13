# Consensus and persistence contract

Historical initial contract. Extensions and updated guarantees are specified by [006](006-autonomous-enhancement.md) through [012](012-process-and-human-tests.md); current public behavior is in the solution README and operations/configuration references. Earlier fixed-membership exclusions below are superseded.

Status: implemented for the fixed-membership API. Authority: [Raft extended paper, Figure 2](https://raft.github.io/raft.pdf).

## Safety rules and observable consequences

| Rule | Required behavior | Regression evidence |
| --- | --- | --- |
| Persistent election state | Save term and candidate ID before a vote reply or election RPC. Repeated requests from the same candidate can receive the same grant; another candidate cannot receive it in that term. | `Disk_restart_preserves_term_and_vote_without_any_log_entry`, `Observing_a_higher_term_is_durable_even_when_vote_is_denied`, `Repeated_vote_for_same_candidate_is_granted_but_other_candidate_is_rejected` |
| Candidate eligibility | Compare final log term, then final index; count self and each configured peer once. | `Voting_compares_last_term_before_log_length`, `Duplicate_votes_cannot_form_a_quorum` |
| Stale terms | Old heartbeats cannot reset election timers or change the known leader. Higher terms from configured members force durable term adoption and follower state. | `Old_heartbeats_do_not_suppress_elections_or_replace_known_leader`, `Stale_vote_and_append_responses_do_not_elect_a_candidate` |
| Prefix matching | Check previous index/term even on empty heartbeats. Delete only the conflicting suffix, never committed entries or an otherwise matching extra suffix. | `Matching_shorter_batch_keeps_existing_suffix`, `Conflict_replaces_only_the_uncommitted_suffix`, `Committed_entries_cannot_be_replaced` |
| Commit boundary | Follower commit is capped by the prefix matched in that specific request, not its unrelated local tail. A leader counts replicas only to commit a current-term entry. | `Heartbeat_commit_is_bounded_by_the_prefix_matched_in_that_rpc`, `Figure8_prior_term_majority_does_not_commit_without_a_current_term_entry` |
| Leader establishment | Append a no-op on election; committing it safely commits inherited prefix entries. | `Elects_one_leader_and_commits_its_noop`, Figure 8 test |
| Correlated replication | One current AppendEntries RPC per peer. Match progress cannot regress on a delayed duplicate/rejection. Connection failure frees the slot for a later tick, avoiding a busy failure loop. | `Delayed_duplicate_append_response_cannot_regress_peer_progress`, lagging-follower and partition tests |
| Check quorum | A leader periodically requires responses from a majority and relinquishes leadership if unavailable. Quorum loss never commits minority proposals. | `Isolated_leader_steps_down_without_a_quorum`, `Minority_proposal_fails_when_check_quorum_removes_leadership` |
| Ordered effects | Apply durable commits in index order; include no-ops in checkpoints. Acknowledgments follow application. Storage/application failures stop the node. | runtime, failure-injection, batch, and demo-state-machine tests |

## Durable schema

One DBreeze engine/folder hosts one Raft node/group and may host application tables too. `raft8/meta` has `identity` and `state` string keys. Identity encodes schema `raft8-v1`, group, node, and a sorted membership list. Hard state encodes little-endian term (8 bytes), commit (8), vote-byte length (4), and ASCII-compatible UTF-8 candidate bytes. `raft8/log` has ascending positive `long` keys and values consisting of term (8 little-endian bytes), no-op flag (1), and command bytes. Index keys use DBreeze's order-preserving numeric encoding, independent of value byte order.

Terms, votes, and commits cannot regress. Changing a non-null vote in the same term is rejected. Log entries are contiguous with nondecreasing terms no greater than durable current term. A suffix mutation reserves the log table and deletes/inserts inside one committed transaction. In-memory metadata/cache mutation follows successful database commit. Application state is not stored in these tables.

Startup reads identity, hard state, tail, and existence of the committed entry, without a full log load. Missing entries encountered later fail closed. Term-only consistency/conflict queries read just the fixed value header through `Row.GetValuePart(0, 9)` while the transaction is open; payload ranges use owned values and a bounded cache. No-op/unchanged heartbeats do not mutate durable metadata.

The schema has no migration path from historical `RaftTbl_StateLogEntry` data. Do not point an old deployment at the new API and assume compatibility. Membership changes, compaction, and snapshots need a new specification and tests before implementation.

## Ownership and concurrency

Phase 1 update: consensus storage and timeout sampling are now explicit effects executed by RaftDriver outside a pure immutable reducer. A bounded RaftStorageAdapter channel worker serves both consensus and application log requests. Legacy contracts use default interface bridges. The original invariants and wire/database schema above remain; [005](005-phase1-review.md) contains current evidence and limitations.

`RaftCore` has one owner. `RaftNode` serializes ticks, proposals, and RPC transitions through a bounded channel. DBreeze storage serializes its synchronous calls under a storage lock; it can be called from consensus and application workers without moving a live transaction between threads. No transaction contains `await` or application callbacks.

The application worker receives only committed, owned entries. Ordinary applications apply one at a time; optional batch applications checkpoint through the range's last entry. Both must honor cancellation. Admission is released only after actual completion/rejection, even when a caller no longer waits. A canceled proposal can still commit. Request deduplication is an application responsibility.

The host completes queued incoming RPCs with the actual failure. Its lifetime token cancels queue admission and work, but does not race an already-queued request's completion wait and hide a storage error as cancellation. This distinction was found and fixed by a storage failure-injection test.
