# Proposal collection outside the consensus owner

Status: implemented candidate R11;304 full tests, locked restore/zero-warning Release build, native adapter, human scenario and BDN Dry passed. Final complete winner matrices are not yet run for this candidate.

## Purpose and ownership

The former collection timer occupied the consensus owner. While it waited, it could not process ACKs that advance commit/application and produce the next client feedback wave. A longer timer alone did not establish a material whole-stack gain. Instrumented probes showed local groups still averaging about15 large commands despite a much larger byte allowance.

Keep admitted owned command bytes in the existing bounded single-reader proposal channel. A separate asynchronous task observes a bounded coalesced change signal and short-lock metadata (count, bytes, first arrival/entry size). It only schedules a coalesced ready marker. It never reads the reducer, removes a command, assigns an index, performs storage or owns a second payload buffer. It reads immutable published node status for timing hints.

The consensus owner alone drains the command channel at readiness, preserving FIFO, count and local encoded-byte limits. It rechecks leadership/transfer/uncommitted limits before proposing. It performs no timer wait during collection. Existing admission covers queued plus in-flight commands, so TryWrite under the short metadata lock cannot overflow the equally bounded command channel. Lifetime completion still owns and rejects every queued/pending command. Caller cancellation after transfer only cancels waiting.

## Timing and resource bounds

ProposalBatchDelay remains2ms by default for count-limited commands. New nullable BulkProposalBatchDelay controls byte-limited groups. Automatic value is min(8×ProposalBatchDelay, heartbeat interval,1second), normally16ms; zero disables the intentional bulk wait. An explicit value must be nonnegative and at most one heartbeat/one second. Zero small delay derives zero automatic bulk delay. Single-voter waits remain capped at1ms.

Byte-limited means the first queued entry times MaxAppendEntries cannot fit MaxAppendBytes, or the queued bytes are already at a full-frame boundary. A longer window is used only while already committed application work remains. When that pipeline empties, fall back to the small window. Commit/role/configuration changes and application catching up wake the collector. Time is measured from original queue admission, not reset by every arrival.

Intentional collection trades per-command delay for fewer local durable writes. It is not a free latency improvement or an SLO promise. Wire/application bytes remain256KiB by default, count128, local encoded group1051264B with existing uncommitted/64MiB caps, and all per-command quorum/application requirements remain unchanged. No TCP frame, log format, CRC or business transaction is weakened.

## Evidence and limits

The higher-term control regression first failed on the restored owner-waiting implementation (r8-owner-before.trx). With off-owner scheduling, a pending command remains unassigned while a queued higher-term vote is durably processed; that command is rejected after stepdown. This is an availability/host scheduling improvement, not a claim that the earlier implementation violated Raft safety.

Existing deterministic blocked-storage tests still prove unfragmented bounded batches across interleaved controls, strict explicit byte caps, original failure identity, cancellation/admission release and actual multi-frame TCP commits. Full304 tests include recovery, snapshots, membership, crash children, compatibility and the new scheduling regression. R10 diagnostics proved a group-size change (~25 versus15 commands,161 versus273 leader appends/4096 commands), but instrumented timing is excluded from final capacity reports. R11 adds the work-conserving condition; a three-round long3n16KiB trial measured Pipeline1191/s versus baseline1079/native781 on this noisy disk. Fresh complete multi-point/five-round comparisons are still required.

No follower-RPC coalescing or special CoreEvent validation bypass was implemented. Core remains the same pure reducer. Preserve R4/R5 complete reports and all exploratory raw/source/runtime evidence; never combine different candidate binaries in the final matrix.
