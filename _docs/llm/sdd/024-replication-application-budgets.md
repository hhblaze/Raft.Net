# Separate replication and application byte budgets

Status: implemented R15 candidate;327/327full tests and all integration/smoke checks passed. New behavior reproduced3fail/6pass before byte-path changes and43focused checks pass afterward. Diagnostic follower append calls216–217 versus273 show partial reduction; no capacity speedup yet. R13 did not reduce actual write counts and was reverted. R14 larger kernel send-buffer gave similar diagnostic throughput/write counts and was reverted; preserve all evidence and OS receive/global settings. Base is the validated R12 runtime.

## Contract

Keep MaxAppendBytes as the application/query byte bound and the legacy wire1 bound. Add nullable MaxReplicationBytes for modern wire2 AppendEntries. Automatic modern budget is min(4×MaxAppendBytes, MaxInFlightBytes−64,64MiB), normally1MiB; explicit values must fit command+21B,64MiB and the per-peer byte window. Wire1 retains MaxAppendBytes and rejects an explicit modern override. Local durable proposal grouping/count/admission/uncommitted limits stay unchanged.

Use the replication budget for normal incoming Append validation, wire decoding, outgoing range reads and frame sizing. Count/command/index/term/correlation limits still apply. Keep application and snapshot replay reads bounded by MaxAppendBytes. No new wire fields/version, compression, storage bytes or consensus rule. Default16KiB commands fit63 entries per wire packet instead of15; application transactions remain at most15 entries, with exactly the same business/checkpoint work. The shared ComparisonCluster.cs, including ComparisonMachine, must remain byte-identical to R12.

The larger frame budget changes bounded memory reservations and may reduce handlers per connection under the existing byte cap. Report this explicitly; do not compare it as equal packet sizing. Preserve the existing per-peer byte window. Do not increase kernel buffers or global runtime settings.

## Compatibility and upgrade

Wire1 golden bytes/default size behavior stay unchanged. Wire2 encoding is unchanged, but older binaries or peers configured for smaller frames will reject oversized packets before admission. For mixed older wire2 deployments explicitly configure MaxReplicationBytes to the old peer MaxAppendBytes on every new node (normally256KiB). Keep that setting throughout rolling binary upgrades. Raising the cluster limit afterward requires compatible bounds on every node; use a coordinated stop/configure/restart when send/receive limits cannot be changed separately. No data conversion is required. This is an operational frame-limit change, not a rolling mixed-limit guarantee.

## Regression and measurement

First add the option/validation and behavioral tests while leaving the old byte paths in place. A decoded incoming packet larger than the application budget but within an explicit replication budget must be accepted durably, then applied in smaller ordered transactions; an oversized packet must still be rejected before queue/storage. Run this for v2/v3 storage. Run an actual three-node TCP workload with independent limits, invalid-budget and wire1 compatibility checks. Existing tests that intentionally force a packet split must explicitly configure that wire bound.

After red/green, full safety/integration and diagnostics must show actual lower follower/leader write counts. Fresh rotated three/five-node capacity trials precede any full five-round comparison. All earlier complete reports, slow rounds and failed experiments remain intact. No winner claim from smaller exploratory samples.
