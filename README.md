# Raft.NET

The supported implementation is now **.NET 8**, with **DBreeze 1.142.2026.1016-beta**, durable election state, bounded replication and application pipelines, and a reusable TCP transport.

- [Build, API, behavior, and operational limits](RaftNet8/README.md)
- [Run the interactive three-node demo](RaftNet8/HumanTest/README.md) — real TCP, disk persistence, partitions, stop/restart, and load commands
- [Benchmarks and measurement guidance](RaftNet8/benchmarks/README.md)
- [LLM usage skill](_docs/raft_skill.md)
- [Engineering specification](_docs/llm/sdd/001-resurrection.md) and [project memory](_docs/llm/okf/index.md)

```sh
cd RaftNet8
dotnet restore Raft.Net8.sln --locked-mode
dotnet test Raft.Net8.sln -c Release
dotnet run --project HumanTest/Raft.Demo -c Release
```

`Raft/`, `RaftStandard/`, `Deployment/`, and `Docu/` are historical artifacts. Their original code was copied to `RaftNet8/legacy-source/` for reference and is excluded from the supported solution. The .NET 8 API, wire protocol, and database schema are new; existing applications/data need an explicit migration. The old NuGet packages are not the new implementation.

This release implements fixed membership and retains the full disk log. Snapshot installation, log compaction, and live membership changes are not implemented. See the [supported scope](RaftNet8/README.md#supported-scope) before deployment.

Algorithm: [Raft](https://raft.github.io/) · [BSD 2-Clause license](LICENSE) · Alexey Solovyov / tiesky.com
