# Raft.NET

The supported implementation targets **.NET 8**, with **DBreeze 1.142.2026.1016-beta**, a pure consensus core, checked durable storage, ReadIndex, learners/membership changes, leadership transfer, bounded replication pipelines, resumable snapshots and optional mutual TLS.

- [Build, API, behavior, and operational limits](RaftNet8/README.md)
- [Run the interactive demo](RaftNet8/HumanTest/README.md) — real TCP/disk, partitions, restart, snapshot catch-up, learner promotion and transfer
- [Operations and upgrade/restore runbooks](RaftNet8/docs/OPERATIONS.md)
- [Benchmarks and measurement guidance](RaftNet8/benchmarks/README.md)
- [LLM usage skill](_docs/raft_skill.md)
- [Enhancement specification](_docs/llm/sdd/006-autonomous-enhancement.md) and [project memory](_docs/llm/okf/index.md)

```sh
cd RaftNet8
dotnet restore Raft.Net8.sln --locked-mode
dotnet test Raft.Net8.sln -c Release
dotnet run --project HumanTest/Raft.Demo -c Release
```

`Raft/`, `RaftStandard/`, `Deployment/`, and `Docu/` are historical artifacts. Their original code was copied to `RaftNet8/legacy-source/` for reference and excluded from the supported solution. Those pre-.NET-8 applications/data need an explicit migration. The old NuGet packages are not the new implementation. The earlier .NET 8 public API is retained through compatible overloads/adapters.

The recommended format is raft8-v2/wire 2. Existing .NET 8 v1 data has an offline copy migration; unsafe mixed-version deployments are rejected. See [compatibility and boundaries](RaftNet8/README.md#compatibility-and-boundaries) and the executed [validation](RaftNet8/README.md#validation-and-measurements).

Algorithm: [Raft](https://raft.github.io/) · [BSD 2-Clause license](LICENSE) · Alexey Solovyov / tiesky.com
