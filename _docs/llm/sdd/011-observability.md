# Diagnostics without consensus dependencies

Status: implemented; focused diagnostic/failure isolation and the full 221-test native-coverage suite passed. Current instrument names/units and API overloads are documented in RaftNet8/docs/CONFIGURATION.md.

Use Microsoft.Extensions.Logging.Abstractions 8.0.3 (net8 asset), source-generated structured events, System.Diagnostics.Metrics and ActivitySource, all outside Raft.Core. Keep the original constructor/CreateAsync signatures; an added constructor takes ILoggerFactory and the extended CreateAsync overload adds it after the existing seven explicit arguments, avoiding ambiguity for old null clock arguments. The caller owns the factory.

Meter/source name: `Raft.Net8`. Gauges expose term, numeric role, durable commit/applied indices, pending proposals/reads, uncommitted bytes, leader-reported replication lag, snapshot boundary/installation/offsets. Counters expose real/pre-vote elections, fail-stop/RPC failures and snapshot payload attempts/receipts. Histograms (seconds) expose proposal caller latency, ILogStore completion latency, DBreeze Commit duration (fsync-inclusive on disk, not a raw syscall timer) and durable application batch completion. Tag by bounded group/node/peer IDs and outcome; never command contents, request IDs or image IDs in metric dimensions.

Proposal activities include command byte count, committed/applied result index/term and caller outcome. Cancellation ends the caller span, not the command's possible future commitment. With no listeners, timer reads/histogram work are skipped. Observable node registration uses weak references and is explicitly removed on disposal. Metrics read immutable published status, not the core or storage.

Diagnostic callback exceptions (logger, meter listener, activity sampler/stop hook) are isolated and counted in CallbackFailures. They cannot replace the original persistence/application exception or prevent fail-stop. Focused tests exercise structured logs/gauges/latencies/spans and deliberate broken callbacks with an ArgumentException application failure. Keep original errors for both caller and Completion.

Authority: [Microsoft library logging guidance](https://learn.microsoft.com/en-us/dotnet/core/extensions/logging/library-guidance), [metrics instrumentation](https://learn.microsoft.com/en-us/dotnet/core/diagnostics/metrics-instrumentation). Core IL dependency checks must stay clean. No clock/logger/listener is placed in immutable core state or options.
