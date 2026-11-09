# Benchmarks

All numbers below come from real runs on the development machine. None are
estimated or rounded placeholders. Re-run them yourself with `make bench`
(the matrices) and `go test -bench . -run TestRecoveryTime -v ./benchmarks/`
(the micro-benchmarks).

**Environment (micro-benchmarks, reads, recovery):** Windows 11, amd64,
8 logical CPUs, Go 1.27.2, a laptop SSD. The throughput matrices were run
later in a Linux container on the same laptop; see the note in that section.
**The whole cluster runs in one process on one machine over localhost gRPC.**
So the absolute numbers are bounded by this machine, and above all by its
fsync latency of about 22 ms. Compare the cells against each other rather
than against other systems. A Linux server with a write-caching or NVMe disk
typically fsyncs in under 1 ms, which would raise the write numbers by an
order of magnitude.

## Methodology

- Every run uses a fixed workload definition (op mix, clients, key space,
  value size), printed with the results.
- 1 s warm-up, and its samples are discarded.
- Throughput plus p50/p95/p99/max latency. Never just an average.
- Each cell runs 3 times, and the **median** run is reported.
- The topology and config are recorded with each table.
- fsync is **on** (production durability) unless stated otherwise.

## Write latency: fsync vs. replication

*(Micro-benchmarks below were measured before group commit and the
out-of-lock fsync. A single sequential client is unaffected by group
commit, but the 3-replica figure may drop with the follower change.)*

| Measurement | Result |
|---|---:|
| Raft log append, fsync on (`BenchmarkRaftLog_Append/fsync=true`) | 22.3 ms/op |
| Raft log append, fsync off (`fsync=false`) | 0.018 ms/op |
| End-to-end PUT, 1 replica, single client, sequential (`BenchmarkPut_EndToEnd/replicas=1`) | 22.2 ms/op |
| End-to-end PUT, 3 replicas, single client, sequential (`replicas=3`) | 63.0 ms/op |

**Reading:** with one replica, a PUT costs exactly one fsync. With three
replicas it costs the leader's fsync, then a follower's fsync, plus the
round trip and the commit notification. That is about 2.8 fsyncs, because
the follower fsync and the leader's next heartbeat-driven commit propagation
run one after the other. The network part is small next to the disk part
(0.5–1 ms on localhost). **Group commit** removes this per-write cost under
concurrency: concurrent proposals share one fsync (see the matrices below).

## Reads

| Measurement | Result |
|---|---:|
| GET, `LEADER` consistency (ReadIndex heartbeat round), single client | 1.03 ms/op |
| GET, `FOLLOWER_OK` (local read, may be stale), single client | 0.34 ms/op |

A linearizable read costs one heartbeat round to a majority. It costs no
fsync, because ReadIndex writes nothing to the log.

## Storage engine (the map)

| Measurement | Result |
|---|---:|
| `Engine.Put` (100k keys, 128 B values) | 230 ns/op, 3 allocs |
| `Engine.Get` | 293 ns/op |
| `Command.Marshal` | 53 ns/op |

## Recovery time vs. Raft log tail

A replica restarts by restoring its last snapshot and replaying the log
tail after it. Measured with no snapshot, so the whole log is replayed
(`TestRecoveryTime`):

| Log entries to replay | Time until all applied |
|---:|---:|
| 1,000 | 83 ms |
| 10,000 | 94 ms |
| 50,000 | 131 ms |

Restart time is dominated by a fixed cost (process start plus one election,
about 80 ms). Replay itself runs at more than 380k entries/s. With the
default `snapshot_threshold` of 10,000, the tail never exceeds about 10k
entries.

## Throughput matrices

Produced by `TestBenchmarkMatrix` (`make bench`). There are 32 concurrent
clients, a 10,000-key space, and 128-byte values. Each cell is the median of
3 runs of 3 s each, after a 1 s warm-up. Reads use LEADER consistency.

> **Environment for this table (differs from the sections above):** run on
> Nov 9, 2026 against the final code (group commit, out-of-lock fsync on
> leader and followers, exactly-once sessions). The run used a `golang:1.27`
> Linux container (go1.27.3, 8 vCPUs) under Docker Desktop's WSL2 VM on the
> same laptop. Data directories were on the container's filesystem inside the
> VM, whose fsync is much cheaper than native Windows NTFS. **So these
> numbers cannot be compared with the Windows micro-benchmarks above.** The
> whole cluster still runs in one process over localhost gRPC.
> Raw output: `benchmarks/results/latest.md`.

### Replica-count scaling (1 shard, fsync on)

| Topology | Workload | Throughput (ops/s) | p50 | p95 | p99 | max |
|---|---|---:|---:|---:|---:|---:|
| 1 node, RF=1 | write-heavy (90% PUT) | 2,766 | 10.75 ms | 21.35 ms | 32.73 ms | 80.9 ms |
| 1 node, RF=1 | read-heavy (90% GET) | 13,560 | 0.91 ms | 10.28 ms | 25.93 ms | 79.8 ms |
| 3 nodes, RF=3 | write-heavy | 1,927 | 15.73 ms | 26.18 ms | 37.69 ms | 48.9 ms |
| 3 nodes, RF=3 | read-heavy | 3,598 | 8.77 ms | 13.86 ms | 17.28 ms | 27.6 ms |
| 6 nodes, RF=6 | write-heavy | 798 | 45.99 ms | 66.12 ms | 78.90 ms | 88.0 ms |
| 6 nodes, RF=6 | read-heavy | 2,184 | 11.79 ms | 46.48 ms | 53.63 ms | 57.5 ms |
| 9 nodes, RF=9 | write-heavy | 911 | 28.77 ms | 60.13 ms | 78.50 ms | 87.2 ms |
| 9 nodes, RF=9 | read-heavy | 3,093 | 9.68 ms | 18.13 ms | 28.31 ms | 276.4 ms |

### Shard-count scaling (6 nodes, RF=3, fsync on)

| Topology | Workload | Throughput (ops/s) | p50 | p95 | p99 | max |
|---|---|---:|---:|---:|---:|---:|
| 1 shard | write-heavy | 2,030 | 14.93 ms | 25.55 ms | 32.57 ms | 49.9 ms |
| 1 shard | read-heavy | 3,757 | 8.18 ms | 13.98 ms | 17.08 ms | 33.7 ms |
| 3 shards | write-heavy | 1,541 | 18.77 ms | 37.27 ms | 51.41 ms | 96.2 ms |
| 3 shards | read-heavy | 4,433 | 6.75 ms | 16.73 ms | 22.52 ms | 38.1 ms |
| 6 shards | write-heavy | 2,524 | 12.15 ms | 20.30 ms | 25.61 ms | 39.6 ms |
| 6 shards | read-heavy | 6,392 | 3.77 ms | 13.40 ms | 19.45 ms | 39.3 ms |

All cells had 0 errors. The 6-shard cell, which failed in an earlier run on
older code, completed this time.

### How to read these numbers

- **Replication costs throughput.** Going from 1 to 3 replicas adds a network
  hop and a follower fsync to every commit. Write throughput drops by about
  30%, and linearizable reads drop much more, because a single-node read
  needs no heartbeat round while a replicated one does.
- **More replicas than a majority needs cost extra.** 6 and 9 replicas are
  slower than 3. The leader sends every entry to every follower, and here all
  replicas share one process, 8 CPUs and one disk. Results with 6 and 9
  replicas are close to each other, and the run-to-run noise is about as
  large as the gap between them.
- **Shard count: reads scale, writes are mixed.** Read-heavy throughput rises
  from 3,757 to 6,392 ops/s between 1 and 6 shards, because each shard has
  its own leader. Writes dip at 3 shards and rise at 6. With every shard on
  one machine and one disk, the shards compete for the same fsync. On separate
  machines each shard would have its own disk. This run does not demonstrate
  that case.
- **Earlier Windows run.** An earlier native-Windows run, on older code,
  measured 312 writes/s and 428 reads/s for 3 nodes at RF=3. Both the code
  and the environment changed since, so the gap cannot be attributed to
  either alone.

## Bugs found by benchmarking

1. **Windows atomic rename.** Replacing the Raft `state` file failed with
   "Access is denied" while another process (antivirus or indexer) had it
   open, and the node crashed. Fixed: the rename is retried
   (`renameRetry` in `internal/raft/persistence.go`).
2. **fsync under the Raft lock.** Fixed with group commit plus an
   out-of-lock fsync on both leader and followers. The leader counts itself
   toward a commit majority only for entries it has fsynced (`durableIndex`).
   A follower acknowledges only after its fsync, and only if it is still in
   the leader's term.
