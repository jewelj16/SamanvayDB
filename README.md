# SamanvayDB — a fault-tolerant, sharded key-value store

SamanvayDB is a distributed key-value store written from scratch in Go. Keys are
hash-partitioned into shards. Each shard is replicated by its own **Raft** group
(Multi-Raft), with leader election, log replication, durable persistence,
snapshots, membership changes and linearizable reads all implemented in this
repository. It also has shard migration, a fault injector, Prometheus metrics,
an admin API, a client library, and `kvctl`.

> **Storage engine:** by design each replica stores its data in an **in-memory
> map**. No LSM tree. Durability comes from the Raft log, which is fsynced before
> every acknowledgment, plus periodic Raft snapshots. See
> [docs/architecture.md §3](docs/architecture.md#3-storage-a-map-made-durable-by-raft).

## Architecture in one picture

```
Client ─▶ Router ─▶ Shard ─▶ Raft group ─▶ Storage engine
(pkg/client) (hash(key) mod N) (one per shard) (elect, replicate, (map, rebuilt from
 follows redirects                 fsync, snapshot)   snapshot + log)
```

Each node hosts several independent Raft groups, all sharing one pooled gRPC
connection per peer. A small **metadata Raft group** stores the versioned shard
map. Full details, diagrams and a glossary are in
[docs/architecture.md](docs/architecture.md).

## Quickstart

Prerequisites: Go 1.26+. Docker is needed only for the compose cluster.

```bash
make build                                  # bin/samanvaydb-server, bin/kvctl

# A 9-node, 3-shard, RF=3 cluster as local processes (state in .samanvay-local/)
bin/kvctl cluster start --nodes 9 --shards 3 --replication-factor 3

bin/kvctl put user:42 alice                 # OK (shard=1 leader=node-4)
bin/kvctl get user:42                       # alice
bin/kvctl get user:42 --consistency follower-ok
bin/kvctl cluster status                    # leaders, terms, commit index, lag
bin/kvctl node kill node-4                  # SIGKILL / TerminateProcess
bin/kvctl get user:42                       # still served: a new leader was elected
bin/kvctl node restart node-4               # rejoins and catches up
bin/kvctl cluster stop --purge
```

The scripted, narrated demo runs every capability end to end:
`bash scripts/demo.sh`. For Docker, run `bash scripts/cluster-up.sh`
(9 containers) and then `bash scripts/cluster-down.sh`.

A single node with no cluster section works too:
`bin/samanvaydb-server -config configs/single-node.yaml`.

## kvctl

| Area | Commands |
|---|---|
| Data | `put <k> <v> [--ttl 30s] [--direct]`, `get <k> [--consistency leader\|follower-ok]`, `delete <k>`, `cas <k> <expected> <new>`, `lock acquire\|renew\|release\|wait <name>` |
| Introspection | `cluster status`, `shard status --shard N`, `node status --node ID`, `cluster metrics` |
| Lifecycle | `cluster start --nodes N --shards S --replication-factor R`, `cluster stop`, `node add --node ID`, `node remove --node ID`, `node replace --failed A --node B`, `shard migrate --shard N --target a,b,c`, `rebalance` |
| Faults | `node kill <id>`, `node restart <id>`, `node slow <id> --latency 200ms`, `partition create --side-a ... [--side-b ...]`, `partition heal`, `net delay\|drop --from A --to B` |

Every command accepts `--output json`. Exit codes are meaningful:
`0` ok, `2` usage, `3` not found, `4` unavailable (retry), `5` invalid request
(don't retry), `6` precondition failed (CAS or lock). kvctl does not cache
topology. It reads the cluster layout from the admin API on every run.

## Guarantees, stated honestly

- **Durability:** a write is acknowledged only after a majority of its shard's
  replicas have fsynced it to their Raft logs. This was tested with real
  `kill -9` of server processes.
- **Writes are linearizable** per key: one Raft log order per shard.
- **Retries are exactly-once:** each client write carries a session ID and a
  sequence number. A retry whose first attempt already committed (for
  example, the leader crashed before replying) gets the original result. It
  is never applied a second time.
- **Reads:** the default `LEADER` consistency is linearizable. The leader
  confirms it is still leader with a ReadIndex heartbeat round before it
  reads. `FOLLOWER_OK` reads any replica's local state, so they may be stale
  by the replication lag; per replica they are sequentially consistent. A
  linearizability checker verifies recorded histories taken under injected
  leader kills and partitions (`tests/integration/correctness_test.go`).
- **Determinism:** `Apply` is a pure function of (state, command). Time-based
  decisions (TTL, lock expiry) use a timestamp that the leader writes into the
  command. Replicas never consult their own clocks.
- **Availability:** a shard accepts writes while a majority of its replicas is
  up and connected. A failure in one shard does not affect the others.

## Deliberately out of scope

- **Distributed transactions** (multi-key or cross-shard). These need a
  separate 2PC/MVCC layer on top of this design, not inside it.
- **Multi-key atomic operations**, even within one shard.
- **SQL, a query language, range scans, secondary indexes.** These conflict
  with hash sharding.
- **Geo-replication.** WAN latency and multi-region failure modes are a
  different, larger problem.
- **A dedicated read cache.** The engine is already in memory.
- **Authentication, authorization, TLS.** These are orthogonal to the core
  thesis. TLS is mostly a gRPC configuration change. Auth would need a new
  identity check in every RPC handler.
- **Consistency models beyond** linearizable leader reads and stale follower
  reads (no causal or session guarantees).
- **Datasets larger than memory.** The storage engine is a map by design.
- **Joint consensus** (changing several servers at once) and **consistent
  hashing** (changing the shard count) are documented future work.

## Testing

```bash
make test          # everything: unit, simulator, in-process clusters, real processes
make test-short    # skip tests that build and run real server processes
make test-race     # the same tests under the race detector
make stress        # full-size stress tests (SAMANVAY_STRESS_DURATION=1h for a soak)
make bench         # replica- and shard-count benchmark matrices
```

| Tier | Where |
|---|---|
| Unit (table-driven) | next to the code (`internal/**/_test.go`) |
| Raft failure scenarios over a message-level network simulator | `internal/raft/scenarios_test.go`, `internal/raft/raftsim` |
| Integration (in-process multi-node clusters, real gRPC, real disk) | `tests/integration` |
| Real-process fault injection (SIGKILL, partitions) | `tests/failure`, `internal/faultinjector` |
| Correctness (linearizability checker) | `internal/linearizability`, `tests/integration/correctness_test.go` |
| Stress / soak | `tests/stress` |
| Benchmarks | `benchmarks/`; results in [docs/benchmarks.md](docs/benchmarks.md) |

## Repository layout

```
cmd/samanvaydb-server   node entrypoint           cmd/kvctl            CLI
internal/raft           consensus (+ raftsim)     internal/shard       Raft group + FSM + manager
internal/storage        map engine                internal/router      hashing, shard map, routing
internal/transport      gRPC Raft transport       internal/node        node assembly, admin API
internal/membership     add/remove/replace        internal/migration   migration + rebalancing
internal/faultinjector  kill/partition/delay      internal/metrics     Prometheus
internal/linearizability history checker          pkg/client           Go client library
proto/                  kv, raft, admin APIs      tools/protogen       protoc-free code generation
docs/                   architecture, setup, benchmarks, self-audit
```

More: [docs/SETUP.md](docs/SETUP.md) (developer setup),
[docs/conventions.md](docs/conventions.md),
[docs/benchmarks.md](docs/benchmarks.md),
[docs/self-audit.md](docs/self-audit.md).
