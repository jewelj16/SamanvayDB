# SamanvayDB architecture

This document describes what is actually built: every component named here
exists in the repository and has tests. Section links point at the code.

## 1. The five-layer request path

```
                 ┌──────────────────────────────────────────────────────────┐
  Client ───────▶│ 1. Client      pkg/client: routing, redirects, retries    │
  (kvctl, app)   └───────────────┬──────────────────────────────────────────┘
                                 │ gRPC  (proto/kv.proto)
                 ┌───────────────▼──────────────────────────────────────────┐
                 │ 2. Router      internal/router: shardId = fnv1a64(key)     │
                 │                mod NUM_SHARDS; shard map → replica set    │
                 └───────────────┬──────────────────────────────────────────┘
                 ┌───────────────▼──────────────────────────────────────────┐
                 │ 3. Shard       internal/shard: one Raft group + one        │
                 │                storage engine; CAS, TTL, locks            │
                 └───────────────┬──────────────────────────────────────────┘
                 ┌───────────────▼──────────────────────────────────────────┐
                 │ 4. Raft group  internal/raft: election, replication,       │
                 │                persistence, snapshots, membership,        │
                 │                ReadIndex                                  │
                 └───────────────┬──────────────────────────────────────────┘
                 ┌───────────────▼──────────────────────────────────────────┐
                 │ 5. Storage     internal/storage: in-memory map engine      │
                 │    engine      (durability = Raft log + snapshots)        │
                 └──────────────────────────────────────────────────────────┘
```

A client computes the key's shard, sends the request to that shard's
presumed leader, and follows `NOT_LEADER` redirects. The leader proposes the
command to its Raft group; once a majority has it durably in their logs,
every replica applies it to its map, and the leader answers the client.

## 2. Inside one node

```
 samanvaydb-server (internal/node)
 ┌───────────────────────────────────────────────────────────────────────────┐
 │  gRPC :listen_addr                          HTTP :http_addr               │
 │  ┌──────────┐ ┌───────────┐ ┌───────────┐   /healthz  /metrics  /status   │
 │  │ KV svc   │ │ Admin svc │ │ Raft svc  │                                 │
 │  │kvserver  │ │ status,   │ │ transport │◀── RPCs from peers, dispatched  │
 │  │          │ │ membership│ │ dispatch  │    by shard_id                  │
 │  └────┬─────┘ │ migration │ └─────┬─────┘                                 │
 │       │       └─────┬─────┘       │                                       │
 │       ▼             ▼             ▼                                       │
 │  ┌─────────────────────────────────────────────────────────────────────┐  │
 │  │ shard.Manager  (one independent Raft group per hosted shard)        │  │
 │  │                                                                     │  │
 │  │  shard -1 (meta)     shard 0             shard 2          ...       │  │
 │  │  ┌─────────────┐     ┌─────────────┐     ┌─────────────┐            │  │
 │  │  │ raft.Node   │     │ raft.Node   │     │ raft.Node   │            │  │
 │  │  │  log+state  │     │  log+state  │     │  log+state  │            │  │
 │  │  │ FSM ─▶ map  │     │ FSM ─▶ map  │     │ FSM ─▶ map  │            │  │
 │  │  └─────┬───────┘     └─────┬───────┘     └─────┬───────┘            │  │
 │  └────────┼───────────────────┼───────────────────┼────────────────────┘  │
 │           ▼                   ▼                   ▼                       │
 │   data_dir/shard-meta/raft  shard-0/raft        shard-2/raft              │
 │   {state, log, snapshot}    (per-shard isolation)                         │
 │                                                                           │
 │  transport.Client ──▶ one pooled gRPC connection per peer node, shared    │
 │  (+ faultinjector.Rules on send AND receive)  by every co-hosted group     │
 └───────────────────────────────────────────────────────────────────────────┘
```

## 3. Storage: a map, made durable by Raft

The original plan called for an LSM tree. **SamanvayDB deliberately uses an
in-memory hash map instead** (`internal/storage/memkv`, behind the
`storage.Engine` interface). The guarantees an LSM tree's WAL and SSTables
would have provided come from one layer down:

| LSM component (not built) | What provides it in SamanvayDB |
|---|---|
| WAL append + fsync before ack | Raft log append + fsync before ack (`internal/raft/persistence.go`) |
| MemTable | The map itself (`storage.MemEngine`) |
| Flush to SSTables, manifest | Raft snapshot of the map, written atomically (tmp file + fsync + rename) |
| Compaction | Raft log compaction after each snapshot (`SnapshotThreshold`) |
| Crash recovery | Load snapshot → `Restore` the map → replay the log tail |

Consequences, stated plainly:

- The data set must fit in memory on each replica.
- Restart time grows with the log tail since the last snapshot
  (measured: ~50,000 entries replay in ~130 ms — see `docs/benchmarks.md`).
- Every acknowledged write is durable: it was fsynced to a majority's Raft
  logs before the client got `OK` (`TestShard_Phase4_LeaderKillMidWrite`,
  `TestLocalCluster_KillIsRealAndRestartRecovers` with a real `kill -9`).

### On-disk formats

Raft log record (`log`):

```
+-----------+-----------+------------------------------------------+
| CRC32 (4B)| Len (4B)  | Index(8) Term(8) Type(1) Command(Len-17)  |
+-----------+-----------+------------------------------------------+
```

Replay stops at the first record that is short or fails its CRC (a torn
write that was never acknowledged) and truncates the file there.
`state` (term + vote) and `snapshot` are rewritten atomically
(write temp → fsync → rename). Map snapshots are sorted by key so identical
states produce byte-identical snapshots on every replica.

Command encoding (`internal/command`): `Type(1) KeyLen(4) ValLen(4) Key Value
[extension]`, where the extension carries CAS/TTL/lock fields and the
**leader-assigned timestamp** — the only clock `Apply` is allowed to read.

## 4. Raft (`internal/raft`)

| Feature | Where | Notes |
|---|---|---|
| Terms, roles, universal higher-term rule | `node.go` | term/vote fsynced before replying |
| Leader election, randomized timeouts (150–300 ms), heartbeats (50 ms) | `election.go` | |
| **Pre-Vote** | `election.go` | a partitioned node cannot inflate terms and disrupt a healthy leader on return |
| **Check-quorum** | `node.go` | a leader cut off from a majority steps down within an election timeout |
| Log replication, fast conflict backtracking | `replication.go` | one replicator goroutine per peer, ≤1 RPC in flight |
| Current-term commit rule (Figure 8) | `replication.go` | dedicated adversarial test |
| No-op entry on election | `node.go` | lets a new leader commit earlier-term entries |
| **Group commit** | `replication.go` (`runProposer`) | concurrent proposals share one fsync; each client still waits for that fsync and for commit |
| Persistence + torn-write repair | `persistence.go` | `FileStorage`, `MemoryStorage` (tests) |
| Snapshots + InstallSnapshot | `snapshot.go` | log prefix dropped only after the snapshot is durable |
| Single-server membership changes, learners | `membership.go` | one change in flight at a time; learners never count toward quorum |
| ReadIndex linearizable reads | `membership.go` | leadership confirmed by a heartbeat round acknowledged after the read arrived |

Invariant violations panic (commit index moving backwards, truncating a
committed entry, two leaders in one term); everything that can happen in a
healthy-but-unlucky deployment is an error value (see `docs/conventions.md`).

## 5. Shards, Multi-Raft, and the metadata group

- **Hash sharding** (`internal/router/hash.go`): `fnv1a64(key) mod
  NUM_SHARDS`. Even load, no hot ranges, no split/merge logic — at the cost
  of range scans, which the API deliberately does not offer. `NUM_SHARDS` is
  fixed at cluster creation.
- **Multi-Raft** (`internal/shard/manager.go`): every hosted shard runs a
  fully independent Raft group (own term, log, timers, leader). Groups share
  only the process and the gRPC connections; RPCs carry `shard_id` and are
  dispatched to the right group (`internal/transport/dispatch.go`).
- **Per-shard isolation**: each replica's files live in
  `data_dir/shard-<id>/raft/`. Destroying one shard's directory does not
  affect another (`TestManager_PerShardStorageIsolation`).
- **Metadata group** (shard `-1`): a small Raft group (first three nodes by
  default) storing the versioned **shard map** — node addresses, each
  shard's replica set and migration state. It is updated only by
  compare-and-swap on the version, so concurrent orchestrators cannot
  clobber each other. Version 1 is derived deterministically from the
  bootstrap config on every node.
- **Node manifest** (`data_dir/node-manifest.json`): which replicas this
  node hosts. Written once on first boot, afterwards changed only by replica
  create/destroy — never re-derived from the config file (that would let a
  removed node resurrect itself with a stale peer list).

## 6. Data model and command semantics (`internal/shard/fsm.go`)

| Operation | Semantics |
|---|---|
| `PUT k v [ttl]` | ttl → leader computes an absolute deadline once and embeds it |
| `GET k` | `LEADER` (default): ReadIndex, linearizable. `FOLLOWER_OK`: local, possibly stale |
| `DELETE k` | idempotent |
| `CAS k expected new` | precondition evaluated at **apply** time, in log order |
| TTL expiry | leader's ticker proposes `EXPIRE` through Raft; a key is invisible only once that commits — never by local clock judgment |
| Locks | `__lock__:<name>` records; acquire/renew/release re-validated at apply time; fencing token = Raft index of the acquire; expiry proposed by the leader's ticker; `WAIT` long-poll is leader-local (dropped on failover, re-issued by the client) |

### Exactly-once client sessions (`internal/shard/session.go`)

Retries are unavoidable: a leader can commit a write and then crash before
replying. A plain retry commits a second copy, and even an "idempotent" PUT
or DELETE then breaks linearizability: `put(X)`, another client's
`put(Y)`, then the retried `put(X)` lands after `Y`. The correctness suite's
linearizability checker caught exactly this with a retried DELETE that
straddled a leader kill.

So every client write carries `(client_id, seq)`, and all retries of one
operation reuse its `seq` (Raft dissertation §6.3). The FSM keeps a per-client
result table and answers a duplicate from it instead of applying it again.
Leader-side pre-checks (CAS, locks) consult the table first, so a retry gets
its original outcome. `min_seq` (the client's lowest outstanding seq) prunes
old results, so one client may have many concurrent requests. The table is
stored in the engine under `__session__:`, which means it is replicated,
snapshotted and recovered like data. Idle sessions expire after an hour.
The expiry check runs at fixed log indexes using the leader's timestamp, so
it is deterministic on every replica.

## 7. Routing, redirects, and errors (`pkg/client`, `internal/router/router.go`)

1. compute the shard; 2. try the cached leader hint, then the replica set;
3. on `NOT_LEADER` follow `redirect_leader` and cache it; 4. on
`UNAVAILABLE` or a transport error try the next replica, back off, refresh
the shard map. `INVALID_REQUEST` is returned immediately and never retried.
Each request's overall timeout (5 s) is larger than the election timeout, so
clients ride through a failover. Because writes carry session sequence
numbers, these retries are exactly-once.

## 8. Shard migration (`internal/migration`)

State machine: `PENDING → SNAPSHOTTING → TRANSFERRING → CATCHING_UP → CUTOVER
→ DONE`, with `ABORTED` reachable from every state before `CUTOVER`.

**Design choice:** instead of bootstrapping a separate target Raft group and
replaying the source's tail into it ("freeze-and-flip"), the target nodes
join the *same* group as **learners**, receive the source snapshot through
`InstallSnapshot`, tail the log, and cutover is a sequence of single-server
changes (promote each target; remove each source, current leader last). This
gives zero gaps by construction, keeps fencing tokens monotonic (same log),
and needs **no write freeze** — the only unavailability is one election when
the source leader removes itself. Dual-write migration is not implemented.

Rebalancing (`rebalance.go`): greedy, count-based — move one replica from
the most- to the least-loaded node until the spread is ≤ 1. Load-aware
(CPU/disk/QPS) rebalancing is future work.

## 9. Membership (`internal/membership`)

- **Add**: start an empty replica (it cannot campaign: no config) → add as
  learner → leader streams snapshot + log → promote when within 32 entries.
- **Remove**: config-change entry; if the removed node was the leader it
  steps down once the change commits.
- **Replace**: `Remove(failed)` then `Add(new)`, per hosted shard.
- **Decommission node**: every replica it hosts — including its
  metadata-group replica — is moved before the node leaves the map.

Joint consensus (multi-server changes) is documented future work.

## 10. Fault injection (`internal/faultinjector`)

| Fault | Mechanism |
|---|---|
| Kill / restart | real OS processes (`LocalCluster`): `SIGKILL` on Unix, `TerminateProcess` on Windows — never a graceful shutdown |
| Partition | per-node `Rules` table enforced on send **and** receive in the transport; set via Admin `SetFaults` on every node |
| Delay / drop | same `Rules` (per link, `*` wildcards); drop is probabilistic |
| Slow node | artificial sleep in the node's Raft request path |
| Leader-targeted | `KillLeader` / `IsolateLeader` look up the leader via `ShardStatus` |
| Reproducibility | `Scenario` (seeded RNG, seed logged) + `Recorder` (exact fault sequence) |

The in-process simulator `internal/raft/raftsim` is the separate, fast
unit-test tier (drop/delay/duplicate/reorder/partition at message level).

## 11. Observability (`internal/metrics`, Admin API)

- `/metrics` (Prometheus): request counts/latency per op and code; per-shard
  term, is-leader, elections, leader changes, commit/applied index, log tail,
  replication lag per follower, apply lag, snapshots, membership changes,
  keys; peers reachable.
- `/status` and `kvctl cluster status`: per shard — leader, term, committed
  index, each replica's role and lag; per node — up/down and hosted shards.
- Structured logs (`log/slog`) with `node_id`, `shard_id`, `term` on every
  state transition: election started, became leader, stepped down, higher
  term observed, snapshot created/installed, membership change committed,
  shard map updated, network fault injected.

## 12. Glossary

| Term | Meaning here |
|---|---|
| **Client** | anything using `pkg/client` (kvctl, tests, apps) |
| **Router** | key → shard → replica resolution plus redirect handling |
| **Shard** | a hash bucket of the key space, served by one Raft group |
| **Raft group** | the replicas of one shard (or of the metadata group) |
| **Raft node** | one replica: a `raft.Node` inside a server process |
| **Leader / follower / learner** | Raft roles; learners replicate but never vote or count toward quorum |
| **Storage engine** | the per-replica in-memory map behind `storage.Engine` |
| **WAL** | here: the Raft log — every committed write is fsynced there first |
| **MemTable** | here: the map itself |
| **SSTable / compaction** | not built (no LSM). Their role is played by Raft snapshots + log compaction |
| **Snapshot** | a deterministic serialization of a replica's map at an applied index |
| **Shard map** | versioned topology stored in the metadata group |
| **Fencing token** | Raft index of a lock's acquire entry; strictly increasing per lock |
