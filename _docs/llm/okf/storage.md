---
type: Storage Contract
title: DBreeze ownership and durable Raft state
description: Versioned tables, transaction discipline, caches, and application checkpoint ownership.
status: stable
generated: { by: codex/gpt-6, at: 2026-11-12T12:02:56Z }
verified: { by: process:codex-local-validation, at: 2026-11-12T12:02:56Z }
sources:
  - id: implementation
    resource: ../../../RaftNet8/src/Raft/Storage/DBreezeRaftStorage.cs
  - id: upstream
    resource: ../skill/DBreeze_skill.md
  - id: tests
    resource: ../../../RaftNet8/tests/Raft.Tests/StorageTests.cs
---

# Persistent identity and schema

`DBreezeRaftStorage` uses fixed `raft8/meta` and `raft8/log` tables. A node/group/member identity is persisted and checked on reopen; the member order is normalized. One engine/folder hosts one Raft identity, plus optional application tables. The new schema does not read or migrate historical log tables.[^implementation]

Hard state holds current term, candidate voted for, and commit index independently of the log tail. Suffix replacement is transactional; committed prefixes cannot be replaced. The storage caches immutable hard state, final index/term, and a log LRU bounded by entry count and encoded bytes. Startup loads metadata/tail rather than the log. Log keys are ascending `long`; values use a fixed term/no-op header and command bytes. Header-only term/conflict queries avoid loading command payloads.[^implementation]

# Engine and transaction ownership

The path constructor owns its engine. The existing-engine constructor leaves it caller-owned by default. `RaftNode` owns the supplied storage and waits for its workers before disposal. The caller owns transports and any shared engine. Dispose transport, node, then caller-owned engine. DBreeze transactions remain short and synchronous on their creating thread; reserve complete write sets before dependent reads and load lazy values inside the transaction.[^upstream]

The [demo application](../../../RaftNet8/HumanTest/Raft.Demo/KeyValueMachine.cs) shares an engine with Raft storage and uses separate application tables. Batched values, request IDs, and `LastApplied` are committed together. Application effects/checkpoint ownership is deliberately separate from consensus metadata.

[^implementation]: Supported storage implementation
[^upstream]: Downloaded DBreeze skill, sections 2–5 and 12
