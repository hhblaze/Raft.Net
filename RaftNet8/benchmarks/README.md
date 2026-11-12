# Raft .NET 8 benchmarks

Run from `RaftNet8/` in Release mode. BenchmarkDotNet records the runtime, GC, timing distributions, and managed allocations; JSON/Markdown reports are written under the ignored `BenchmarkDotNet.Artifacts/` folder.

```sh
# List workloads
dotnet run --project benchmarks/Raft.Benchmarks -c Release -- --list flat

# Quick execution check; cold-start samples are not steady-state throughput measurements
dotnet run --project benchmarks/Raft.Benchmarks -c Release -- --filter '*' --job Dry

# Warmed codec measurements
dotnet run --project benchmarks/Raft.Benchmarks -c Release -- --filter '*WireBenchmarks*' --job Short --exporters json markdown

# Durable append batching, cache/range reads, deterministic replication, or real TCP load
dotnet run --project benchmarks/Raft.Benchmarks -c Release -- --filter '*AppendBenchmarks*' --job Short
dotnet run --project benchmarks/Raft.Benchmarks -c Release -- --filter '*ReadBenchmarks*' --job Short
dotnet run --project benchmarks/Raft.Benchmarks -c Release -- --filter '*CoreReplicationBenchmarks*' --job Short
dotnet run --project benchmarks/Raft.Benchmarks -c Release -- --filter '*TcpLoadBenchmarks*' --job Short
```

| Workload | What it measures | What it excludes |
| --- | --- | --- |
| `AppendBenchmarks.AppendBatch` | 1/32/128-entry ascending batches, 256-byte payloads; disk and memory DBreeze; mean per transaction | Node construction and command preparation |
| `ReadBenchmarks` | Tail and advancing catch-up ranges over 10,000 entries, disk/memory, cache disabled/enabled | Replication and application I/O |
| `CoreReplicationBenchmarks` | Three-node consensus and memory persistence, 1/32/128 commands per batch, deterministic delivery | Sockets and application callbacks |
| `WireBenchmarks` | Pooled encoding and owned decoding; heartbeat and 32 × 256-byte AppendEntries | Socket I/O |
| `TcpLoadBenchmarks.Replicate256` | 256 concurrent 256-byte proposals through actual three-node loopback TCP; disk/memory; completes after quorum persistence and leader application | Durable application-side effects (the state machine is a counter) |

Write/replication/TCP workloads reset per iteration and use fixed work counts, so calibration cannot grow the log indefinitely. Setup/cleanup is excluded. `OperationsPerInvoke` normalizes means to a batch transaction for append/core tests and to a command for TCP load. It is an amortized throughput measure, **not individual request latency**. Tail/catch-up reads return owned bounded batches, so their allocations include those materialized objects. Decode copies command bytes because receive buffers are returned to a pool.

Single-invocation iteration setup and short disk runs can trigger minimum-iteration-time warnings; increase work/iteration count for serious comparisons. Use the default job or longer configured jobs for stable distributions, run one workload at a time on an otherwise idle host, and preserve raw reports. Measure realistic payloads, slow followers, disk durability costs, application callbacks, GC pauses, and network tail latency before selecting timeouts or estimating capacity. Loopback throughput does not predict a cross-host cluster.

See [recorded validation](results/2026-11-12.md) for what was actually run. Dry-job numbers include cold JIT/initialization and must not be used as performance claims.
