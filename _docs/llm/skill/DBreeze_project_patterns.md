# Project DBreeze preferences

User instruction, 2026-11-13: prefer one physical DBreeze table for different information types when appropriate. Partition its ordered key space using distinct numeric type/index prefixes and composite keys through DBreeze.Utils.ToIndex, for example:

~~~csharp
byte[] firstType = 1.ToIndex(entityId);
byte[] anotherType = 2.ToIndex(createdUtc, entityId);
tx.Insert<byte[], byte[]>("data", firstType, firstValue);
tx.Insert<byte[], byte[]>("data", anotherType, secondValue);
~~~

Treat this as a schema-design preference, not a request to rewrite deployed databases. Choose stable, non-overlapping prefixes; document component widths/encoding. Use DBreeze's ordered numeric encodings, prefix/range traversals, and numeric keys in ascending byte order. Range iteration and deletion must stop at the namespace boundary. Never scan/remove unrelated record types in the shared table.

A single physical table can avoid multi-table commit overhead while preserving atomic changes across logical types. Compare flat prefixed keys with nested roots when useful. Nested tables remain an option for hierarchical isolation; do not choose them by habit for a flat schema. Keep real workload measurements separate from isolated transaction probes.

All upstream transaction/durability/ownership rules still apply: long-lived engine; synchronous short thread-affine transactions; reserve the physical write table before dependent reads; no await inside; publish caches/checkpoints only after Commit. A changed persistent key layout needs an explicit format/migration and incompatible-reader rejection.

Read this supplement together with the unchanged [upstream skill](DBreeze_skill.md), especially sections7 and8.5. The downloaded upstream file is retained byte-for-byte for provenance.
