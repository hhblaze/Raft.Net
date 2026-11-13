# Bounded local proposal group commit and application scheduling

Active goal016 optimization; final measurements are pending. Preserve checked persistence and client/apply/quorum semantics.

## Independent limits

MaxAppendEntries still bounds proposal, wire and application entry counts. MaxAppendBytes still bounds wire and application bytes. New nullable MaxProposalBatchBytes controls only the atomic local proposal-log group. Null derives min(4 x MaxAppendBytes +21 x MaxAppendEntries, MaxUncommittedBytes,64MiB), so defaults allow1MiB+2688B locally while network/application batches remain256KiB. Explicit limits must fit one maximum command including21B accounting, be <=64MiB and <=uncommitted budget. Count/admission/cache/uncommitted bounds still apply.

The proposal collector and pure reducer validate the same local bound before effects. A larger durable local group is split into existing bounded replication frames and bounded application batches. Each command remains one entry and waits for actual quorum commitment plus local apply. No disk flush or application write is skipped. MaxAppendEntries=1 still disables batching. The local bound is independent of pipelining off/on. Explicit MaxProposalBatchBytes=MaxAppendBytes provides the prior local-byte limit as a measured control.

Iteration after the complete CFA candidate long matrix: default ProposalBatchDelay is now2ms. Always drain already-queued proposals up to the same count/local-byte limits. When the queue empties, finish without waiting if another entry of the smallest observed size would not fit in one replication byte budget. This avoids waiting behind an already-full packet while giving small groups a bounded window to absorb asynchronous caller feedback. No fixed benchmark-concurrency target is introduced. The current benchmark explicitly uses2ms in batched modes; DotNext retains1ms and the baseline drains queued work. New full measurements are required for this code; do not reuse CFA timings as if they came from it.

Commit-only prefix accounting can sum already-published immutable cached entries when the current transaction has not appended/replaced/compacted the log. Any miss uses the existing checked DB range. Transactions with log changes never trust stale cache sizes. This avoids rereading durable headers merely to count a committed prefix; durable frontier/CRC encoding remains unchanged.

## Worker ownership

Synchronous application transactions can block the shared ThreadPool needed by transport/consensus and DBreeze itself. Replace the outer application Task.Run loop with one dedicated background application thread, retaining its bounded coalesced signal and apply/snapshot semaphore. It starts each callback on that thread, consumes completed ValueTasks directly, and waits for incomplete callbacks through AsTask on the dedicated thread. No synchronization context is installed; genuine asynchronous callbacks continue normally. One ordered callback is active at a time, and no transaction is moved across threads by the runtime.

The node supervises the application worker completion exactly like the owner/ticker. Start failure fail-stops and drains admitted work. Exceptions/cancellation preserve previous semantics and original errors. The storage adapter keeps its separate dedicated thread and bounded2-operation channel. Account for two background workers per started node using this adapter; never change global ThreadPool minimums to improve a benchmark.

## Already-queued cumulative responses

The owner may process a bounded contiguous run of successful current-term AppendResponses by descending MatchIndex. Every reply still enters the unchanged core correlation/quorum/term checks; cumulative higher successes make older ones stale and can avoid intermediate durable commit writes. Do not wait to gather replies, merge RPCs, manufacture match indices or skip a durable completion. Stop at every other work type, failed/stale/higher-term response, active/queued ReadIndex, transfer, uncommitted configuration, nonmember or nonzero read context. Bound the temporary list to256 and configured peer windows. This is ordinary legal response reordering, not a new commit rule.

Internal ReportRpcCompletionAsync is the bounded transport-completion seam. Tests use real follower-persisted replies, hold a leader commit, inject exact ascending response order, then deliver the original replies again as harmless duplicates. Before ordering, the no-read scenario deterministically persisted five commits; afterward it persists two/three. A ReadIndex inserted between completions retains all five in original order and completes correctly. Red and green TRX files are retained under .artifacts/winner/tests/response-before.trx and response-batch.trx.

## Validation and measurement contract

Hold a real first log write, queue64 commands, and prove the local group changes from3 to13 entries when the wire/apply bound stays512B; every application batch must remain <=512B. Exercise multi-frame local groups over actual TCP with unchanged receiver validation. Reject invalid local limits before admission. Existing fault/property/membership/snapshot/compatibility tests must remain green. Dedicated-worker tests use genuinely asynchronous single/batch callbacks and prove ordered completion plus a stable non-ThreadPool invocation thread.

Fresh reports must state that current local durable groups default to1MiB+2688B, while baseline/DotNext local collectors retain256KiB and all common application limits remain256KiB. The change is a production optimization, not identical internal grouping across libraries. Preserve control/trial data separately and do not describe a group-size capability as a measured throughput improvement when the observed workload still releases only15 commands at a time.

## Default metadata headroom (R4)

Automatic encoded byte bound is min(4×MaxAppendBytes +21×MaxAppendEntries, MaxUncommittedBytes,64MiB), default1051264B. The small headroom covers entry accounting so power-of-two payload groups do not need a singleton local transaction just because of their21B entries. Explicit MaxProposalBatchBytes remains an exact encoded-byte cap; count, uncommitted memory, wire and application bounds are unchanged. Red/green blocked-storage regression queues64×16KiB after a held first proposal: previous[1,63,1], automatic fixed[1,64]; explicit1MiB still[1,63,1]. Application chunks never exceed256KiB. This capability does not itself prove a throughput gain.

Single-voter collection caps the configured maximum window at1ms while retaining any smaller/zero user setting. It drains all queued work first; multi-voter collection remains up to2ms by default, with full-wire-batch early flush. Quorum durability, application ordering and all byte/count bounds are unchanged.

Runtime timing/collection paragraphs above are historical iterations. Current off-owner adaptive collection is specified in020-proposal-collection.md; the local byte/count limits here remain current.
