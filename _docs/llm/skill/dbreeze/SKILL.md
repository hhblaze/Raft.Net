---
name: dbreeze
description: Implement DBreeze transactions, ordered key-value access, persistence, and application checkpoints in this repository.
---

Read the relevant sections of the owner's requested upstream [DBreeze reference](../DBreeze_skill.md), downloaded in full without edits. Its snapshot title says v1.139; this repository compiles and tests the needed transaction APIs against DBreeze 1.142.2026.1016-beta's net8.0 assembly. Prioritize engine lifetime, transaction thread affinity, complete table reservation, lazy-value lifetimes, ordered bulk mutations, and bounded key ranges. Specialized text/vector sections apply only when those APIs are used.
