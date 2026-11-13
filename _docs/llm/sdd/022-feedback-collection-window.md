# Preserve a selected feedback collection window

Status: rejected and reverted after full320-test/integration validation. Diagnostic172leader append writes versusR12 166,274–276followers versus273 showed no useful I/O reduction. All R13 source/runtime/tests/raw are archived. This is a historical experiment, not current runtime behavior.

## Observed mechanism and bounded change

R11/R12 reevaluates the bulk-delay eligibility after every application signal. Commands arriving while older committed work remains can initially receive a16ms window, then lose it as soon as that work finishes. This can split successive application feedback waves into short local groups. Measure whether preserving the initially selected deadline produces larger actual durable groups and less log I/O.

Select backlog eligibility once for the current queue head, using only immutable published status and the applied frontier. A generation counter under the existing short proposal gate identifies a changed head without retaining command memory or relying on unique timer ticks. Keep the oldest enqueue timestamp as the deadline origin; later admissions never restart it. A new head observed without committed backlog uses the small delay. Completing older application work does not shorten an already open bulk window.

The collector remains outside the consensus owner and owns no command bytes. Only the owner drains/assigns/persists. Role loss, count/byte limits and zero delay still release readiness immediately; single-voter cap remains1ms. Bulk delay remains bounded by one heartbeat/one second, normally16ms. No wire/storage/Core changes or additional payload queue. Intentional collection latency is explicit; do not describe it as unconditionally work-conserving.

## Regression and validation

Apply the new two-layout scheduling test to unchanged R12 first. Hold a committed application command; queue a byte-limited proposal with a long bulk window; finish the old application wave after the short window has elapsed. The queued proposal must retain its open bulk deadline. A higher-term vote must still be processed before it receives an index, and its caller must receive NotLeader without leaked admission. Use controlled clocks for elections and generous separation between short/bulk timers.

Then apply the guarded Node draft, require focused/full safety/integration checks and actual diagnostic group counts. Run fresh rotated reference trials before considering a new full five-round comparison. Preserve complete R4/R5/R11 reports and all R12 raw/source/runtime evidence; no outlier deletion, mode selection or identical-code rerun to obtain a favorable result.
