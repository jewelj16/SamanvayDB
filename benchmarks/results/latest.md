# SamanvayDB benchmark results

Generated 2026-11-09 05:44 UTC by `make bench` (TestBenchmarkMatrix).

Environment: linux/amd64, 8 logical CPUs, go1.27.3. Whole cluster runs in ONE process on ONE machine over localhost gRPC, so absolute numbers are bounded by that machine (and by its fsync latency); compare cells relative to each other.

Each cell: median of 3 runs. Reads use LEADER consistency (ReadIndex).

- write-heavy: 32 clients, 10% GET / 90% PUT, 10000 keys, 128B values, 1s warm-up + 3s measured
- read-heavy: 32 clients, 90% GET / 10% PUT, 10000 keys, 128B values, 1s warm-up + 3s measured

## Replica-count scaling (1 shard, fsync on)

| Topology | Workload | Throughput (ops/s) | p50 | p95 | p99 | max | errors |
|---|---|---:|---:|---:|---:|---:|---:|
| 1 node, RF=1 | write-heavy | 2766 | 10.75ms | 21.35ms | 32.73ms | 80.9ms | 0 |
| 1 node, RF=1 | read-heavy | 13560 | 910µs | 10.28ms | 25.93ms | 79.8ms | 0 |
| 3 nodes, RF=3 | write-heavy | 1927 | 15.73ms | 26.18ms | 37.69ms | 48.9ms | 0 |
| 3 nodes, RF=3 | read-heavy | 3598 | 8.77ms | 13.86ms | 17.28ms | 27.6ms | 0 |
| 6 nodes, RF=6 | write-heavy | 798 | 45.99ms | 66.12ms | 78.9ms | 88ms | 0 |
| 6 nodes, RF=6 | read-heavy | 2184 | 11.79ms | 46.48ms | 53.63ms | 57.5ms | 0 |
| 9 nodes, RF=9 | write-heavy | 911 | 28.77ms | 60.13ms | 78.5ms | 87.2ms | 0 |
| 9 nodes, RF=9 | read-heavy | 3093 | 9.68ms | 18.13ms | 28.31ms | 276.4ms | 0 |

## Shard-count scaling (6 nodes, RF=3, fsync on)

| Topology | Workload | Throughput (ops/s) | p50 | p95 | p99 | max | errors |
|---|---|---:|---:|---:|---:|---:|---:|
| 1 shard | write-heavy | 2030 | 14.93ms | 25.55ms | 32.57ms | 49.9ms | 0 |
| 1 shard | read-heavy | 3757 | 8.18ms | 13.98ms | 17.08ms | 33.7ms | 0 |
| 3 shards | write-heavy | 1541 | 18.77ms | 37.27ms | 51.41ms | 96.2ms | 0 |
| 3 shards | read-heavy | 4433 | 6.75ms | 16.73ms | 22.52ms | 38.1ms | 0 |
| 6 shards | write-heavy | 2524 | 12.15ms | 20.3ms | 25.61ms | 39.6ms | 0 |
| 6 shards | read-heavy | 6392 | 3.77ms | 13.4ms | 19.45ms | 39.3ms | 0 |
