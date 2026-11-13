# Cached-read experiment (reverted)

Status: evaluated and reverted; not a supported feature or remaining implementation task. Retain ordinary I/O-worker scheduling. The shipped term-range cache/LRU/full-tail accounting optimizations remain independently validated.

Hypothesis: reads entirely in the bounded immutable cache could bypass a worker hop. An internal nonblocking TryEnter-only capability returned published immutable metadata/ranges without a transaction; misses/contention used the existing worker. Three dedicated tests and the full221-test native-coverage suite passed, proving no staged publication and no waiting for a busy I/O lock. The reducer was unchanged.

Unprofiled long runs did not establish consistent benefit. For4096 measured16-KiB requests after2048 warmup, pipeline3 changed631→652/s, while pipeline5 changed474→373/s and p99 increased156→224ms. These single exploratory samples do not establish a universal causal estimate, but do not justify the extra capability/complexity. Worker scheduling also influences application/log I/O and batching. The implementation and experiment-only tests were removed; raw `cache-trial-3n/5n` and `cached-final-coverage` artifacts remain historical evidence.

CPU EventPipe traces are retained, but async rundown/ConditionalWeakTable sampling overhead is not an unprofiled production timing result. Do not revive this experiment without representative repeated evidence. New wire-v1 ReadIndex/transfer guards are an independent API correction; membership already had its v2 gate. Removing three experiment tests and adding the v1 guard regression yielded219; two migration preflight regressions bring the delivered suite to221.
