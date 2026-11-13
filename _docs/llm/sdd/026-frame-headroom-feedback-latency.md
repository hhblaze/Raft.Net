# Preserve frame headroom and shorten feedback collection

Status: implemented R17 candidate after both R16 target series completed. New frame regression1fail before,47focused green after; full330-test suite and integration passed, accepted in final342-test R18. No performance claim.

R16 reduces follower appends to about138 per4096 commands (R12~273,R15~216), but its25ms bulk ceiling can offset saved I/O with intentional waiting. Try automatic min(4×small window,one heartbeat,1s), normally8ms, retaining the in-flight feedback condition, oldest-command origin, explicit overrides, owner separation and single-voter1ms cap. Measure actual group sizes/speed rather than assuming a shorter timer helps.

The R15/R16 default wire budget4×MaxAppendBytes accounts for payload but can fragment the default local group solely because entry metadata falls outside that multiple. Include MaxAppendEntries×21B metadata headroom in the automatic replication budget, still capped by MaxInFlightBytes−64 and64MiB. Default becomes1051264B, matching the local bound, and a1MiB data group with64×16KiB commands fits one frame. Count/command/application256KiB/local/uncommitted bounds remain unchanged. Wire1 and explicit overrides are unaffected. The maximum frame and pool-bucket overhead are slightly larger and must be explicit in documentation/reporting.

First add only a regression encoding/decoding that full automatic payload group on unchanged R16; it must fail at the old frame limit. After guarded source application require full safety/integration and diagnostics/fresh reference trials. Existing heartbeat/zero/explicit bulk-limit checks remain; update the heartbeat-cap example so its heartbeat is below the new8ms automatic ceiling. Preserve all R16 source/runtime/target raw data and earlier complete reports.

Final R18 evidence:342tests/integration and fresh1400-run fault matrix passed; complete five-round report meets fixed Pipeline12/12median throughput points over both references,8/8distributed. See winner-r18-2026-11-13.html and OKF benchmark record. Memory/p99 losses and operational follower-lag/frame-limit trade-offs remain explicit.
