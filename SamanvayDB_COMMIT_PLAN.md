# SamanvayDB — Commit-by-Commit Build Plan

> Source: *SamanvayDB Implementation Guide* (97 pp.) — a fault-tolerant, sharded,
> multi-Raft key-value store with an LSM-tree storage engine, built from scratch
> in Go as a 10-phase portfolio/interview-prep project.
>
> This document turns that guide into a **linear sequence of commits**. Each
> commit is small enough to implement, test, and review in isolation, but the
> sequence as a whole is sufficient for an autonomous coding agent to build the
> entire system end-to-end with no other input. Follow the commits **in order**
> — later commits assume everything in earlier commits compiles and passes its
> tests. Do not skip a commit's tests to "move faster"; the guide's central
> warning is that skipping validation compounds bugs across layers that become
> exponentially harder to debug once Raft and the storage engine are wired
> together (Phase 4).

## How to use this plan

- Each commit has: **Goal**, **Do this** (concrete file-level tasks), **Key
  implementation details** (algorithms/formats to follow exactly), **Tests**,
  and a suggested **Commit message**.
- "Checkpoint" commits (marked ✅) are hard gates — the guide is explicit that
  you should not start the next phase until a checkpoint's tests pass and you
  can explain *why* the result is correct.
- Language: the guide assumes **Go** (goroutines/channels model concurrent RPC
  handling naturally; gRPC + protobuf are idiomatic; gRPC gives TLS for free
  later). This plan assumes Go + gRPC + Protocol Buffers. It is not required —
  Rust/C++/Java all work equally well — but all package paths and snippets
  below use Go conventions (`internal/`, `pkg/`, `cmd/`).
- **Build-vs-reuse rule** (applies to every commit): implement Raft and the
  LSM storage engine yourself, from scratch — that is the entire point of the
  project. Reuse libraries for anything that is merely infrastructure: gRPC
  for RPC framing, Protocol Buffers for serialization, a hashing library
  (FNV/xxHash/murmur3) for sharding/bloom filters, a structured logging
  library (zap/slog), Cobra for the CLI, and the Prometheus client library for
  metrics.

## Global repository layout (final state)

```
samanvaydb/
├── cmd/
│   ├── samanvaydb-server/     entrypoint for a cluster node
│   └── kvctl/                 CLI entrypoint
├── internal/
│   ├── storage/                Phase 2: WAL, MemTable, SSTable, compaction, manifest
│   │   ├── wal/
│   │   ├── memtable/
│   │   ├── sstable/
│   │   └── compaction/
│   ├── raft/                   Phase 3: consensus (election.go, replication.go,
│   │                           persistence.go, snapshot.go)
│   ├── shard/                  Phase 4-6: shard <-> Raft-group <-> storage wiring
│   ├── router/                 Phase 5: key -> shard resolution, forwarding
│   ├── migration/               Phase 7: shard migration state machine
│   ├── membership/              Phase 8: node add/remove/replace
│   ├── faultinjector/           Phase 9: kill/partition/delay/drop primitives
│   ├── metrics/                 Phase 10: Prometheus exporters
│   └── transport/               gRPC transport, shared by Raft + client RPCs
├── pkg/client/                  Client library used by kvctl, tests, benchmarks
├── configs/                     Example single-node and multi-node cluster configs
├── scripts/                     cluster-up.sh, cluster-down.sh, demo.sh
├── tests/
│   ├── unit/                    Mirrors internal/ package structure
│   ├── integration/             Multi-node, real-process test scenarios
│   └── failure/                  Fault-injection scenarios
├── benchmarks/                  Benchmark harnesses and recorded results
├── docs/                         Design docs, architecture diagrams
├── docker/                       Dockerfile + docker-compose for local clusters
├── Makefile
└── README.md
```

`internal/storage` and `internal/raft` must compile and have their full test
suites pass **independently of each other** — `raft` depends only on an
abstract `StateMachine` interface, never on the concrete LSM engine. This
separation is deliberately over-engineered for and is verified explicitly in
a later checkpoint commit.

## Non-negotiable invariants (apply to every relevant commit below)

1. **Never apply a write to the state machine before Raft has committed it.**
   The single most damaging shortcut in the whole project (Phase 4).
2. **Every fsync that provides a durability guarantee must actually be
   called before the corresponding RPC/client response is sent** — WAL
   append, Raft log append, `currentTerm`/`votedFor` persistence, manifest
   updates.
3. **apply() must be a pure, deterministic function of (state, command).**
   No wall-clock reads, no randomness, no non-deterministic map iteration
   inside it.
4. **SSTables are immutable once written.** Never mutate a file in place.
5. Keys and values are **opaque bytes**, never `string`, starting in Phase 1.
6. Every concurrent-code commit's tests must be run with the race detector
   (`go test -race`) as part of its acceptance criteria.

---
## Phase 0 — Project Setup

### Commit 1 — Repository scaffolding
**Goal:** Lay down the skeleton so every later commit has a home.
**Do this:**
- `go mod init samanvaydb` (or your module path).
- Create every directory in the Global repository layout above (empty
  `.gitkeep` files where nothing exists yet).
- Add a `Makefile` with targets: `build`, `test`, `test-race` (runs `go test
  -race ./...` — non-negotiable for concurrent code), `cluster-up`,
  `cluster-down`.
- Add `.gitignore` (binaries, `*.log`, local data dirs, `.env`).
- Add a stub `README.md` with the project name and one-paragraph description.
**Tests:** `make build` succeeds on an empty `main.go` in
`cmd/samanvaydb-server`.
**Commit message:** `chore: initial repository scaffolding`

### Commit 2 — Configuration system
**Goal:** One declarative, file-based config format from day one.
**Do this:**
- Choose YAML. Define a `Config` struct in `internal/config` (or similar)
  with, at minimum: `node_id`, `listen_addr`, `data_dir`. Leave room to add
  `raft_peers` in Phase 3 without breaking the format.
- Implement `LoadConfig(path string) (*Config, error)`.
- Add an example config file under `configs/single-node.yaml`.
**Key implementation details:** Keep configuration declarative and file-based
rather than a growing pile of CLI flags — the project will need to spin up
9-node clusters by Phase 6 and hand-typed flags do not scale.
**Tests:** Config parsing round-trip test (write struct → YAML → parse →
equal). Missing/malformed file returns an error, not a panic.
**Commit message:** `feat(config): YAML-based node configuration loader`

### Commit 3 — Structured logging
**Goal:** Logging conventions that survive to Phase 10 without retrofitting.
**Do this:**
- Pick a structured logging library (zap, slog, or zerolog).
- Wrap it in a small `internal/logging` package that enforces every log line
  carries `node_id` from the start (and, once Raft exists, `term` and
  `role`).
- Wire it into the server entrypoint, initialized from `Config.node_id`.
**Key implementation details:** Unstructured `fmt.Println` debugging does not
scale past three nodes — you will spend a large fraction of this project
reading interleaved logs from multiple nodes.
**Tests:** A log line emitted during a test contains the expected structured
fields (assert on the log sink/buffer).
**Commit message:** `feat(logging): structured logging with node_id from startup`

### Commit 4 — Error-handling and testing conventions ✅ (Phase 0 checkpoint)
**Goal:** Lock in conventions before any distributed-systems logic exists.
**Do this:**
- Document (in `docs/conventions.md`) and enforce in code review going
  forward: errors as values for anything that can occur during normal
  operation (disk full, peer unreachable, stale term); panics/asserts
  reserved for invariant violations that indicate a bug (e.g. commit index
  moving backwards).
- Add a table-driven test template/example under `tests/unit/`.
- Add a `/healthz` HTTP (or gRPC) endpoint to `samanvaydb-server` that
  returns OK once config is loaded and the logger is initialized.
**Tests:** Health-check integration test: start the binary, hit `/healthz`,
expect 200/OK.
**Checkpoint (end of Phase 0):** A single empty binary reads a config file,
starts a structured logger, and exposes a health-check endpoint. Nothing
distributed-systems-specific exists yet — that is correct at this stage.
**Commit message:** `feat: health-check endpoint; document error/testing conventions (Phase 0 checkpoint)`

---
## Phase 1 — Basic Key-Value Interface

Scope for this whole phase: **no Raft, no sharding, no disk persistence**
beyond, optionally, a trivial file dump. A single process holds a
`map[string][]byte` guarded by a mutex, exposed over the network. The goal is
to nail the API contract before storage or consensus complexity exists.

### Commit 5 — Command type
**Goal:** Define the internal representation that every later phase (Raft
log entries, LSM writes) will reuse unchanged.
**Do this:** In `internal/storage` (or a shared `internal/command` package),
add:
```go
type CommandType uint8
const ( CmdPut CommandType = iota; CmdDelete )

type Command struct {
    Type  CommandType
    Key   []byte
    Value []byte // empty/ignored for CmdDelete
}
```
**Key implementation details:** This is not premature abstraction — from
Phase 3 onward, `Command` is exactly the payload serialized into Raft log
entries. Designing it now means Phase 4's integration is a rewiring exercise,
not a redesign. Decide now, and document, whether empty keys are legal —
enforce that decision consistently through every later phase.
**Tests:** Struct round-trips through your chosen serialization (protobuf/
gob) without loss.
**Commit message:** `feat(command): define internal Command/CommandType shared by KV, WAL, and Raft`

### Commit 6 — In-memory KV core
**Goal:** A mutex-guarded map implementing PUT/GET/DELETE semantics.
**Do this:** `internal/storage/memkv` (temporary, Phase-1-only package — will
be superseded by the real engine in Phase 2): `Put(k,v)`, `Get(k) (v, found)`,
`Delete(k)`, all safe for concurrent use.
**Tests (write all of these now — Section 5.4):**
- PUT then GET returns the value just written.
- GET on a missing key returns "not found," **not an error**.
- DELETE then GET returns "not found."
- PUT overwrites an existing key's value.
- Concurrent PUTs to different keys do not corrupt the map — run with
  `-race`.
- Empty key / empty value edge cases handled per your documented decision.
**Common mistake to avoid:** Do not build a rich query API (range scans,
prefix queries, batched multi-key ops) "while you're at it." Every extra
operation must later be threaded through Raft's command type, the LSM read
path, and the sharding router. Keep the surface to exactly PUT/GET/DELETE.
**Commit message:** `feat(kv): mutex-guarded in-memory KV core with PUT/GET/DELETE`

### Commit 7 — gRPC API definitions
**Goal:** Define the wire contract once, reused (extended, never replaced)
through every later phase.
**Do this:** `proto/kv.proto`:
```protobuf
service KV {
  rpc Put(PutRequest) returns (PutResponse);
  rpc Get(GetRequest) returns (GetResponse);
  rpc Delete(DeleteRequest) returns (DeleteResponse);
}
message PutRequest    { bytes key = 1; bytes value = 2; }
message PutResponse   { bool ok = 1; string error = 2; }
message GetRequest    { bytes key = 1; }
message GetResponse   { bool found = 1; bytes value = 2; string error = 3; }
message DeleteRequest { bytes key = 1; }
message DeleteResponse{ bool ok = 1; string error = 2; }
```
Generate Go stubs (`protoc` + `protoc-gen-go` + `protoc-gen-go-grpc`).
**Key implementation details:** Use `bytes`, never `string`, for keys and
values — the storage engine must treat them as opaque binary data (values may
contain arbitrary bytes; tombstones need a byte-level sentinel that must not
collide with legitimate values).
**Tests:** Generated stubs compile; a trivial client/server round-trip over
an in-process bufconn works.
**Commit message:** `feat(api): protobuf KV service definition (Put/Get/Delete)`

### Commit 8 — gRPC server + kvctl basic client
**Goal:** A single running node accepts PUT/GET/DELETE over the network.
**Do this:**
- Implement the `KV` gRPC server in `cmd/samanvaydb-server`, backed by
  Commit 6's in-memory core, translating gRPC requests to/from `Command`.
- Start `cmd/kvctl` using Cobra (or similar) with three subcommands: `kvctl
  put <key> <value>`, `kvctl get <key>`, `kvctl delete <key>`, talking to a
  node address from config/flag.
**Tests:** End-to-end test: start server in-process (or via `os/exec` against
the built binary), run each `kvctl` subcommand, assert expected output.
**Commit message:** `feat: gRPC KV server + basic kvctl put/get/delete commands`

### Commit 9 — Phase 1 checkpoint ✅
**Goal:** Confirm the API contract is solid before storage complexity begins.
**Do this:** Consolidate and expand Commit 6's test list into a full
`tests/unit/kv` suite; add a concurrency stress test (many goroutines hitting
PUT/GET/DELETE on overlapping and disjoint keys, run under `-race`).
**Checkpoint (end of Phase 1):** A single running node accepts PUT/GET/DELETE
over the network from `kvctl` or a test client, with correct semantics and
**zero persistence** — all data is lost on restart. That is expected and
correct at this stage; Phase 2 fixes it.
**Commit message:** `test: Phase 1 checkpoint — full KV CRUD + concurrency suite`

---
## Phase 2 — LSM-Tree Storage Engine

**The single most important phase in the guide.** Build a complete,
crash-safe, single-node log-structured-merge storage engine before touching
Raft. It must survive process crashes at any point — mid-write, mid-flush,
mid-compaction — with zero data loss for anything acknowledged, and zero
corruption. Raft (Phase 4) leans entirely on this guarantee; if local storage
can silently lose or corrupt data, replication just replicates "correct"-
looking corruption.

All work in this phase happens under `internal/storage/` and must compile
and test **independently of `internal/raft/`**.

### Commit 10 — Skip list
**Goal:** The in-memory sorted structure backing the MemTable.
**Do this:** `internal/storage/memtable/skiplist.go`. Fixed maximum height
(16 or 32 levels), randomized level assignment on insert (each node gets
level *k* with probability *p^k*, typically *p = 1/4*). Support: `Insert(key,
value)` (overwrites in place if key exists), `Get(key) (value, found)`,
and an in-order iterator (bottom-level linked-list walk) for sorted range
scan — needed by the flush step.
**Key implementation details:** Almost every production LSM engine
(LevelDB, RocksDB, Cassandra) uses a skip list over a balanced BST because:
concurrent reads are much simpler to support (no rebalancing/rotation
touching unrelated nodes); range iteration is a simple linked-list walk;
expected O(log n) insert/search with far less implementation complexity.
**Tests:** Insert/overwrite/lookup correctness; sorted iteration order;
concurrent reads during writes under `-race`.
**Commit message:** `feat(storage): skip list for the MemTable`

### Commit 11 — MemTable wrapper
**Goal:** Wrap the skip list with the semantics the engine needs: size
tracking, freeze/swap, delete-as-tombstone-insert.
**Do this:** `internal/storage/memtable/memtable.go`:
- `Put`/`Delete` both insert into the skip list (delete inserts a
  tombstone value — same code path, different payload tag; do not implement
  delete as removal).
- Track approximate byte size (sum of key+value lengths + per-entry
  overhead estimate) as entries are inserted.
- `Freeze()`: atomically swap in a fresh, empty MemTable while returning the
  frozen one for the flush pipeline (Commit 18). Use an atomic pointer swap
  plus an "immutable MemTable list" that reads also consult until each is
  fully flushed.
**Key implementation details:** A common and *acceptable first-pass*
simplification is a synchronous flush that blocks writes — but if you take
this shortcut, **document it explicitly** as a known limitation rather than
believing your write path is non-blocking (Common Mistake in the guide).
Track when you plan to fix it (Commit 18 gives the non-blocking version).
**Tests:** Size threshold triggers freeze at the right point; freeze produces
an atomically-immutable snapshot readers can still safely read from
concurrently with new writes to the fresh MemTable.
**Commit message:** `feat(storage): MemTable with size tracking and freeze/swap`

### Commit 12 — WAL writer (append + fsync)
**Goal:** Durability for every write before it is acknowledged.
**Do this:** `internal/storage/wal/writer.go`. Record format (self-
describing, includes a checksum):
```
+-----------+----------+----------+-----------+-------------+
| CRC32 (4B)| Type (1B)| KeyLen(4)| ValLen(4) | Key | Value |
+-----------+----------+----------+-----------+-------------+
```
CRC32 covers everything after itself. `Append(cmd Command) error` serializes
the record, writes it, and calls `fsync`/`fdatasync` on the file descriptor
— **only after this call returns does the write "exist."** This is the
single most important line of code in the entire storage engine.
**Key implementation details:**
- The WAL is append-only, never modified in place, until rotated.
- If you implement group commit (batching multiple writes into one fsync for
  throughput), **every client in the batch must wait for the shared fsync**
  before any of them are acknowledged — acknowledging early defeats the
  entire purpose (Common Mistake).
**Tests:** Append then read back produces byte-identical records; fsync is
actually invoked (assert via a fake/wrapped file or `strace` in CI, if
feasible) — this is worth a dedicated test even though it's awkward, because
silently missing it is invisible until a real crash.
**Commit message:** `feat(storage): WAL writer with checksummed records and fsync-before-ack`

### Commit 13 — WAL reader / replay
**Goal:** Reconstruct a MemTable from a WAL file on startup.
**Do this:** `internal/storage/wal/reader.go`. `Replay(path) (*Memtable,
error)`: read records sequentially from the start; compute each record's
CRC32 as read; **the first record whose stored CRC doesn't match its
computed CRC marks the true end of valid data** — everything from that point
(including further bytes) is a torn write and is discarded, not treated as
an error that aborts recovery.
**Tests:** Round-trip (writer → reader) reconstructs exact state; a
deliberately truncated/corrupted file at the tail is handled by stopping
replay exactly at the corruption point, not by crashing or silently
including garbage.
**Commit message:** `feat(storage): WAL replay with CRC-based torn-write detection`

### Commit 14 — SSTable writer
**Goal:** Produce the immutable, sorted, on-disk file format.
**Do this:** `internal/storage/sstable/writer.go`. Format:
```
+-----------------------------------------------------+
| Data Block 1 (sorted key/value records)              |
| Data Block 2 ...                                      |
| Data Block N                                          |
+-----------------------------------------------------+
| Bloom Filter Block                                     |
+-----------------------------------------------------+
| Index Block (last key of each data block -> offset)    |
+-----------------------------------------------------+
| Footer (offsets of bloom filter block, index block,      |
|         magic number, format version, smallest/largest  |
|         key, sequence number/generation)                 |
+-----------------------------------------------------+
```
Writer takes a **sorted iterator** of key/value records (from a frozen
MemTable, or a compaction merge) and streams data blocks of a few hundred KB
each. Consider prefix-compressing consecutive keys within a block. Store
smallest/largest key and a generation/sequence number in the footer.
`fsync()` the completed file before returning.
**Key implementation details:** Write this and the reader (Commit 15) as
two independent, heavily-unit-tested pieces per the guide's own
recommendation.
**Tests:** Writer produces files matching the documented layout; round-trip
against hand-crafted edge cases planned for Commit 15 (empty SSTable,
single-entry, all-tombstone).
**Commit message:** `feat(storage): SSTable writer (data blocks, sparse index, footer)`

### Commit 15 — SSTable reader
**Goal:** Answer point lookups against an SSTable file.
**Do this:** `internal/storage/sstable/reader.go`. `Lookup(key) (value,
found, isTombstone)`:
```
if key < smallestKey or key > largestKey: return NotFound   # footer short-circuit
if bloomFilter.mightContain(key) == false: return NotFound  # Commit 16/17
block = index.binarySearch(key)              # in-memory
record = block.binarySearchOrScan(key)       # one disk read
return record.value or Tombstone
```
Load footer + index block into memory on open; index binary search is O(log
number of blocks); the one disk read is the single data block.
**Tests:** Reader against files the Commit-14 writer produces; hand-crafted
edge-case files: empty SSTable, single-entry SSTable, all-tombstone SSTable.
Smallest/largest-key short-circuit verified with keys outside the range.
**Commit message:** `feat(storage): SSTable reader with index binary search`

### Commit 16 — Bloom filter
**Goal:** A compact, probabilistic set-membership structure to skip
unnecessary disk reads.
**Do this:** `internal/storage/sstable/bloom.go`. Bit array of size *m* plus
*k* independent hash functions.
```
insert(key): for i in 0..k: bitarray[hash_i(key) mod m] = 1
mightContain(key) -> bool:
    for i in 0..k:
        if bitarray[hash_i(key) mod m] == 0: return false   # definitely NOT present
    return true                                              # PROBABLY present
```
Use a well-distributed hash library (FNV/xxHash/murmur3) rather than
implementing your own hash function — reuse infrastructure, build the
interesting part (the bit-array structure and query logic).
**Key implementation details:** Target roughly 10 bits per key with *k ≈ 7*
for a ~1% false-positive rate. **False positives are possible; false
negatives are structurally impossible** — inserting a key always sets its
bits, and bits are never cleared. Be ready to derive this asymmetry from the
algorithm itself; it's a frequently-asked interview question.
**Tests:** No false negatives across a large randomized key set; measured
false-positive rate matches the theoretical prediction within tolerance.
**Commit message:** `feat(storage): bloom filter (bit array + k hash functions)`

### Commit 17 — Wire the bloom filter into SSTable writer/reader
**Goal:** Actually use Commit 16 in the read path built in Commit 15.
**Do this:** Writer populates one bloom filter per SSTable with every key in
the file and writes it into the Bloom Filter Block. Reader loads it into
memory on open and checks it **before** doing the data-block disk read.
**Tests:** A key known absent from the file returns NotFound via the bloom
filter short-circuit without a block read (assert via a call counter/mock on
the block-read path). A present key still round-trips correctly through the
full path (bloom hit → index → block).
**Commit message:** `feat(storage): integrate bloom filter into SSTable read path`

### Commit 18 — MemTable flush pipeline
**Goal:** Turn a frozen MemTable into a durable, registered SSTable, without
blocking new writes.
**Do this:** `internal/storage/engine/flush.go`:
```
MemTable (frozen, immutable, in memory)
  -> iterate keys in sorted order (skip list gives this for free)
  -> write Data Blocks + Bloom Filter + Index + Footer (Commit 14)
  -> fsync() the new SSTable file
  -> atomically register it in the manifest (Commit 19)
  -> delete/rotate the now-redundant WAL segment
  -> discard the frozen MemTable
```
Wire this to run in the background (a goroutine), so writes continue into
the fresh MemTable (Commit 11's freeze/swap) while the frozen one flushes —
fixing the documented Commit-11 limitation if you took the synchronous
shortcut there.
**Key implementation details:** Never delete a WAL file until its
corresponding SSTable flush has itself been durably completed (fsynced,
manifest-registered) — a crash between WAL deletion and SSTable durability
loses data permanently.
**Tests:** Flush under concurrent writes to the new MemTable (no blocking,
no lost writes, no corruption — run under `-race`); crash-simulated
mid-flush leaves the manifest pointing at pre-flush state (paired with
Commit 24).
**Commit message:** `feat(storage): non-blocking MemTable flush pipeline`

### Commit 19 — Manifest
**Goal:** Single source of truth for which SSTable files currently
constitute the engine's on-disk state.
**Do this:** `internal/storage/engine/manifest.go`. An append-only log or a
"current" pointer file listing current SSTables + their levels/generations.
Updates are: write new version → fsync → atomically rename/replace the
pointer. This ordering is what guarantees a crash mid-update leaves either
the fully-consistent old state or the fully-consistent new state — never a
half-registered SSTable.
**Tests:** Concurrent readers of the manifest never observe a half-written
version; simulated crash between "write new version" and "atomic rename"
leaves the old version intact and valid.
**Commit message:** `feat(storage): atomic manifest for SSTable registration`

### Commit 20 — Full read path
**Goal:** Wire MemTable + immutable MemTables + all SSTables into one
correct `GET`.
**Do this:** `internal/storage/engine/engine.go`, `Get(key)`:
```
1. Check active MemTable -> if found, return (value, or NotFound if tombstone)
2. Check immutable MemTables (pending flush), same rule
3. For each on-disk SSTable, NEWEST to OLDEST (by manifest generation):
     skip if key outside [smallestKey, largestKey]
     skip if bloom filter says "definitely not present"
     binary search index -> read block -> search block
     if found: return (value, or NotFound if tombstone) and STOP
4. Return NotFound
```
**Key implementation details:** "Newest to oldest" is the crux of LSM
correctness — the same key can exist in several SSTables; the first match in
newest-first order is definitionally the most recent value. Stop at the
first match.
**Tests:** A key overwritten across MemTable + multiple SSTable generations
returns the newest value at every stage of its lifecycle (in MemTable only;
flushed once; flushed twice with an older stale copy present).
**Commit message:** `feat(storage): full read path across MemTable and SSTables, newest-first`

### Commit 21 — Deletes and tombstones, end-to-end
**Goal:** Correct DELETE semantics through the whole pipeline.
**Do this:** Confirm/extend the `ValueRecord` shape used throughout storage:
```go
type ValueRecord struct {
    Key         []byte
    Value       []byte
    IsTombstone bool   // true means: ignore Value, key is deleted
    SeqNum      uint64 // monotonically increasing, breaks ties across files
}
```
DELETE goes through the exact same write path as PUT (WAL append → MemTable
insert), writing a tombstone instead of a value. The read path (Commit 20)
must honor a tombstone at **every layer it checks**, not just report "not
found" at the first layer that lacks an entry.
**Key implementation details (Common Mistake):** Treating "not found in the
MemTable" as license to skip straight to "not found overall" is wrong — a
tombstone can live in the MemTable, correctly shadowing an older on-disk
value.
**Tests:** PUT k v → DELETE k → GET k == NotFound, verified at every storage
layer independently (tombstone in MemTable shadowing on-disk value;
tombstone flushed to a newer SSTable shadowing an older SSTable's real
value).
**Commit message:** `feat(storage): tombstone-correct DELETE across all read-path layers`

### Commit 22 — Compaction (size-tiered)
**Goal:** Bound the number of SSTable files a read must check; reclaim
space; drop obsolete versions and safe-to-drop tombstones.
**Do this:** `internal/storage/compaction/compactor.go`. Implement
**size-tiered** compaction first (per the guide's explicit recommendation —
dramatically simpler than leveled; leveled is an optional, clearly-labeled
stretch goal). K-way merge of N similarly-sized input SSTables:
```
merge(inputs: []SSTableIterator) -> newSSTable:
    heap = minheap of (key, seqnum, iterator) for each input's current record
    while heap not empty:
        (key, seqnum, it) = heap.popMin()
        winningRecord = highestSeqnumRecordFor(key, heap, it)   # newest wins
        if winningRecord.isTombstone and safeToDropTombstone(key):
            # drop entirely, nothing written
        else:
            output.append(winningRecord)
        advance all iterators positioned at 'key'
```
Trigger compaction when enough similarly-sized SSTables accumulate at a size
tier.
**Key implementation details:** A tombstone is only safe to drop if no
older SSTable being merged — nor any SSTable outside the merge — still
holds a pre-delete value for that key. Dropping too early makes a deleted
key "come back to life"; this is a serious correctness bug, not an
optimization miss.
**Tests:** K-way merge produces correctly sorted, deduplicated output;
tombstones dropped only when provably safe (and *not* dropped when an older,
un-merged SSTable still holds the shadowed value); overlapping key ranges
merge correctly.
**Commit message:** `feat(storage): size-tiered compaction with k-way merge`

### Commit 23 — Compaction concurrency safety
**Goal:** Never delete a file an in-flight reader still holds.
**Do this:** Add reference counting (or an equivalent generation/epoch
mechanism) to SSTable file handles. A compaction that supersedes files only
unlinks them from disk once no reader can possibly still be using them; the
manifest update that "activates" the new merged file happens atomically
(Commit 19's pattern), and old files are only physically deleted after that
swap **and** after their refcount drops to zero.
**Key implementation details (Common Mistake):** Deleting a superseded
SSTable's file the instant compaction completes, without checking for an
in-flight reader holding a handle to it, is a classic bug here.
**Tests:** A read holding a handle to an SSTable mid-compaction completes
correctly even as that file is logically superseded and later reclaimed;
run under `-race` with concurrent reads + a background compactor.
**Commit message:** `feat(storage): reference-counted SSTable lifecycle for safe compaction`

### Commit 24 — Crash recovery
**Goal:** Tie Commits 10–23 together into the chapter's core guarantee:
after any crash, at any point in write/flush/compaction, the engine recovers
to exactly the writes that were acknowledged — no more, no less.
**Do this:** `internal/storage/engine/recovery.go`, run on startup before
serving any request:
```
Read the manifest -> determine currently-valid SSTable files
  (ignore partially-written files from an in-flight flush/compaction
   that never finished its atomic swap)
Open the WAL file(s) referenced as "not yet flushed"
Replay WAL records in order (Commit 13's CRC-stop-on-torn-write logic)
  into a fresh MemTable
Resume normal operation.
```
**Key implementation details:** No special-case recovery logic is needed
beyond "trust the manifest, ignore anything it doesn't reference" — this is
a direct consequence of Commits 18/19/23's atomic-swap discipline, not new
logic.
**Tests:** Write thousands of keys, `kill -9` at a random (scripted,
repeatable) point, restart, verify every acknowledged write present and
correct — repeat with a compaction deliberately running in the background at
kill time.
**Commit message:** `feat(storage): crash recovery via manifest + WAL replay`

### Commit 25 — Phase 2 checkpoint ✅
**Goal:** Prove the storage engine is solid before Raft depends on it.
**Do this:** Build a scripted, repeatable kill-and-restart test harness
(reusable later by Phase 9's fault injector). Demonstrate and automate as
tests:
1. Start a node, write thousands of keys, `kill -9` at a random scripted
   point, restart, verify every acknowledged write present and correct.
2. Same, while a compaction is deliberately running in the background.
3. Delete a large number of keys, run compaction, verify disk space is
   reclaimed.
Also add the full Section-15.1-style unit test matrix across MemTable, WAL,
SSTable, bloom filter, and compaction if any cases from Commits 10–24 are
still missing.
**Checkpoint (end of Phase 2):** If you cannot yet demonstrate all three
scenarios above under an automated test, **do not proceed to Phase 3** —
Raft will assume this layer is correct and will not help you find bugs in
it.
**Commit message:** `test: Phase 2 checkpoint — automated crash/compaction recovery suite`

---
## Phase 3 — Implementing Raft

Build consensus as a **standalone library**, tested against an in-process
network simulator, completely independent of the storage engine (`internal/
raft` must not import `internal/storage`). Only in Phase 4 does it get wired
to the LSM tree. Raft replicates an ordered log of opaque commands and
guarantees every node applies the same commands in the same order (state
machine replication) — Raft itself does not know what a "PUT" means.

### Commit 26 — Node states, terms, persistent state skeleton
**Goal:** The three-state model and the term rule that underlies everything
else.
**Do this:** `internal/raft/node.go`. States: `Follower`, `Candidate`,
`Leader`. Persistent fields: `currentTerm uint64`, `votedFor *NodeID`, `log
[]LogEntry`. Implement the universal term rule as a function every RPC
handler calls first:
```
Node receives any RPC or RPC response with term T:
    if T > currentTerm:
        currentTerm = T
        votedFor = null
        state = Follower   # even if I thought I was the Leader
```
**Key implementation details:** Terms are Raft's logical clock, **not** wall-
clock time — they only establish a strict ordering of "election epochs."
**Tests:** Table-driven tests over the term-update rule: higher term seen in
a request vs. a response; from each starting state (Follower/Candidate/
Leader).
**Commit message:** `feat(raft): node states, terms, and the universal higher-term rule`

### Commit 27 — RequestVote RPC + vote-granting rule
**Goal:** The mechanism a candidate uses to win an election.
**Do this:** `internal/raft/election.go`. `RequestVote{term, candidateId,
lastLogIndex, lastLogTerm}`. A voter grants its vote only if **all** hold:
- candidate's term ≥ voter's current term
- voter hasn't already voted for a different candidate this term
  (`votedFor` empty or equals candidate's id)
- candidate's log is **at least as up-to-date** as the voter's own: compare
  by last log entry's term first (higher wins), then by log length if terms
  tie (longer wins)
**Key implementation details:** The log-up-to-date check is a core safety
mechanism, not an optimization — it prevents a node missing recent committed
entries from ever becoming leader and overwriting them. `votedFor` must be
**persisted (fsynced) before responding** to the vote request.
**Tests:** Table-driven over term/votedFor/log-up-to-date combinations (this
is explicitly called out as a "run over many combinations" test in the
guide).
**Commit message:** `feat(raft): RequestVote RPC and vote-granting rule`

### Commit 28 — Leader election, randomized timeouts, heartbeats
**Goal:** The full election state machine plus the mechanism that keeps a
healthy leader from being challenged.
**Do this:** `internal/raft/election.go` (cont.):
```
Follower -> (election timeout, no heartbeat/AppendEntries received) -> Candidate
    currentTerm += 1; votedFor = self
    send RequestVote to all peers
    -> majority votes (incl. self) -> Leader
    -> AppendEntries from term >= currentTerm -> Follower
    -> timeout again, no majority (split vote) -> new election, term += 1
```
Randomize each node's election timeout on every reset, drawn from a range
(e.g. 150–300ms). Leader sends `AppendEntries` (empty = heartbeat) on a
fixed interval well under the minimum election timeout (e.g. every 50ms —
**at least a 3–5x safety margin** below the minimum election timeout, or
ordinary network jitter causes spurious elections with a healthy leader —
Common Mistake).
**Tests:** Simulated cluster converges to exactly one leader per term;
split-vote scenario (two followers time out simultaneously) resolves via
retry with new randomized timeouts, no deadlock; heartbeats correctly reset
follower election timers.
**Commit message:** `feat(raft): leader election with randomized timeouts and heartbeats`

### Commit 29 — AppendEntries + log replication + consistency check
**Goal:** Replicate the log and detect/correct divergent follower logs.
**Do this:** `internal/raft/replication.go`.
```
Leader: append {term, index, command} to local log (uncommitted)
Leader sends AppendEntries{prevLogIndex, prevLogTerm, entries, leaderCommit}
  to every follower, in parallel
Follower:
  reject if it doesn't have an entry at prevLogIndex with prevLogTerm
  otherwise: append new entries (truncating conflicting existing entries
    at those indexes), persist, ACK
Leader: on majority ACK (incl. self) -> entry COMMITTED
```
On rejection, the leader decrements the index it tries next and retries,
walking backward until logs agree, then overwrites the follower's log after
that point.
**Key implementation details (critical safety rule):** A leader must never
consider an entry from a **previous** term committed purely by counting
replicas — it must wait until it has replicated at least one entry of its
own current term, then commit everything up to and including that point
together. Getting this wrong is one of the most common subtle Raft bugs
(Common Mistake, Section 7.3).
**Tests:** Consistency check accepts/rejects correctly; a follower with a
divergent log is corrected via backtracking; leader never commits a
prior-term entry by count alone (a dedicated adversarial test for this
specific rule).
**Commit message:** `feat(raft): AppendEntries log replication with consistency-check backtracking`

### Commit 30 — Commit index, apply() interface
**Goal:** Propagate commit knowledge and define the boundary to the state
machine.
**Do this:** `internal/raft/raft.go`. Leader tracks `commitIndex`, includes
it in every `AppendEntries` (including heartbeats). Followers update
`commitIndex = min(leaderCommit, index of last new entry appended)` and,
whenever it advances, apply newly-committed entries **in order**. Define:
```go
type StateMachine interface {
    Apply(cmd Command) error
}
```
Raft holds a `StateMachine` and calls `Apply` only for entries it has
determined are committed — this is the seam Phase 4 will satisfy with the
real LSM engine, and it's why `internal/raft` has zero dependency on
`internal/storage`.
**Key implementation details:** Determinism requirement — every node applies
committed entries in the same order, deterministically, with no external
input (no wall-clock reads, no randomness, no non-deterministic iteration)
inside `Apply`.
**Tests:** Commit index only advances on true majority; every node (leader
and followers) applies entries in identical order given the same log.
**Commit message:** `feat(raft): commit index propagation and StateMachine apply interface`

### Commit 31 — Raft persistent state
**Goal:** Make `currentTerm`, `votedFor`, and the log itself durable.
**Do this:** `internal/raft/persistence.go`. A dedicated, append-friendly
file (or a small embedded WAL of your own — separate from the LSM engine's
WAL; different layers, different lifetimes). Every state change to
`currentTerm`/`votedFor`, and every log append, is fsynced **before** the
corresponding RPC response is sent.
**Key implementation details:** Why each piece matters:
- `currentTerm`: prevents a restarted node voting twice in the same term or
  a restarted leader being unaware it's been superseded.
- `votedFor`: prevents granting a second conflicting vote in a term already
  voted in — the basis of "at most one leader per term" surviving restarts.
- log entries: a restarted node must be able to prove to the cluster what it
  already replicated; losing acknowledged entries is a silent commit
  violation.
**Tests:** Kill the process immediately after each of these fsync points
(scripted), restart, verify the persisted value survives exactly.
**Commit message:** `feat(raft): durable persistence of currentTerm, votedFor, and log entries`

### Commit 32 — Snapshots
**Goal:** Bound log growth and bound catch-up time for lagging/new nodes.
**Do this:** `internal/raft/snapshot.go`. Trigger on a configurable log size
(entry count or bytes). On trigger: take a consistent point-in-time snapshot
of the already-applied state machine state, record `lastIncludedIndex` /
`lastIncludedTerm`, write it durably, **only then** truncate the log up to
that point. Implement `InstallSnapshot` RPC for followers too far behind to
catch up via `AppendEntries` — follower discards its state machine and log,
loads the snapshot, resumes normal replication from
`lastIncludedIndex + 1`. On restart, load the most recent snapshot first,
then replay only the log entries after it.
**Key implementation details:** In Phase 4 this becomes "a consistent view
of the LSM engine's data as of a given applied index" — the LSM engine's own
manifest plus a point-in-time flush gives you this essentially for free
(don't build a second, parallel snapshot mechanism for the storage layer).
**Tests:** Snapshot created at the configured threshold; log correctly
truncated; a far-behind follower catches up via `InstallSnapshot` rather
than a huge `AppendEntries` backlog; restart-from-snapshot replays only the
post-snapshot tail.
**Commit message:** `feat(raft): log-compacting snapshots and InstallSnapshot RPC`

### Commit 33 — In-process network simulator
**Goal:** A deterministic, repeatable harness for every failure scenario —
this becomes the primary Raft test tool, and is reused unchanged by Phase 9.
**Do this:** `tests/unit/raft/simulator.go` (or `internal/raft/testutil`).
Run several Raft nodes as goroutines within one test process, routing all
RPCs through an in-memory layer that can drop, delay, duplicate, or reorder
specific messages on command.
```go
type NetworkSimulator interface {
    DropMessages(from, to NodeID, rate float64)
    DelayMessages(from, to NodeID, d time.Duration)
    Partition(sideA, sideB []NodeID)
    HealPartition()
}
```
**Tests:** The simulator itself: a dropped message never arrives; a delayed
message arrives after the delay; a partition prevents cross-side delivery
until healed.
**Commit message:** `test(raft): in-process network simulator for deterministic fault tests`

### Commit 34 — Raft failure scenario test suite ✅ (Phase 3 checkpoint)
**Goal:** Turn every scenario in the guide's failure table into a
deterministic, repeatable unit test.
**Do this:** Using Commit 33's simulator, implement one test per scenario:

| Scenario | Assertion |
|---|---|
| Leader crashes | New leader elected within a few timeouts; no two leaders in same term; committed entries survive |
| Follower crashes | Writes still succeed via remaining majority; crashed follower catches up fully on restart |
| Multiple follower crashes (≥ majority alive) | Availability boundary matches ⌈(N+1)/2⌉ exactly |
| Delayed messages | Final state converges correctly regardless of arrival order after delay |
| Duplicate messages | Re-applying an idempotent AppendEntries with same index/term is a no-op |
| Out-of-order messages | Logs never contain gaps; rejected RPCs trigger correct backtracking |
| Network partition (minority isolated) | No writes succeed on minority side; majority side's leader serves normally |
| Old leader returns after partition heals | Discovers higher term, steps down, truncates conflicting uncommitted entries |

Also add the full network-partition/split-brain walkthrough as its own
dedicated test (5-node cluster, {N1,N2} vs {N3,N4,N5}, verify N1 never
commits on the minority side and correctly steps down on healing).
**Checkpoint (end of Phase 3):** All scenarios pass reliably and repeatably
in the simulator, entirely independent of any storage engine code.
**Commit message:** `test: Phase 3 checkpoint — full Raft failure-scenario suite`

---
## Phase 4 — Integrating Raft with the Storage Engine

Wire the two independently-tested systems from Phases 2 and 3 into a single
replicated, durable KV store. This is an **integration step, not a debugging
opportunity for either half** — both must already be independently solid.

### Commit 35 — `shard` package: one Raft group + one storage engine
**Goal:** The integration glue package.
**Do this:** Create `internal/shard/shard.go`. A `Shard` owns exactly one
`raft.Node` instance and one `storage.Engine` instance. Implement
`storage.Engine` (or a thin adapter) as the `raft.StateMachine` from Commit
30: `Apply(cmd Command) error` calls the engine's WAL-append-then-MemTable-
insert path (Commits 12+11).
**Tests:** Wiring compiles; a trivial single-node shard applies a command
end-to-end (Raft log → Apply → engine state) with a 1-node "cluster" (no
real fault tolerance yet, just the plumbing).
**Commit message:** `feat(shard): wire Raft StateMachine interface to the LSM storage engine`

### Commit 36 — Correct write path: apply only after commit
**Goal:** Enforce the project's single most important correctness rule.
**Do this:** In `Shard`, the write path is exactly:
```
Client -> PUT k v -> Leader
Leader appends {PUT,k,v} as a new Raft log entry (uncommitted)
Replication to followers (Commit 29)
MAJORITY ack -> entry COMMITTED
Leader calls apply(command): WAL append, MemTable insert (Commit 35)
Leader responds OK to client
Followers, on learning the new commitIndex (next heartbeat), independently
  call apply(command) on their OWN state machines
```
**Key implementation details (do not shortcut this):** Never have the
leader write directly to its local LSM engine before commit "for speed,"
even with a planned rollback on replication failure — rollback of a MemTable
insert isn't free, tombstone-based rollback has its own correctness
subtleties, and a concurrent read on the leader in that window would observe
an uncommitted, potentially-doomed value. There is no safe shortcut here.
**Tests:** A write that fails to reach majority (simulated via the network
simulator) is never visible via GET on the leader, even transiently, before
either committing or being abandoned.
**Commit message:** `feat(shard): enforce apply-only-after-commit write path`

### Commit 37 — Deterministic commands; leader/follower apply timing
**Goal:** Document and enforce the asymmetry between leader and follower
apply timing, and guard command determinism.
**Do this:** Add explicit code comments/assertions that `Apply` (and
anything feeding it) must be a pure function of (state, command) — flag any
future temptation to add "PUT with a server-generated timestamp" as needing
the timestamp decided once by the leader and embedded in the command before
replication, never independently generated per replica.
Instrument (for later use by Phase 10's replication-lag metric) the gap
between leader-apply-time and each follower's apply-time.
**Tests:** Followers lag behind the leader's applied state by a small,
bounded amount under load — measure and assert an upper bound in a test.
**Commit message:** `feat(shard): enforce command determinism; measure leader/follower apply lag`

### Commit 38 — Phase 4 checkpoint ✅
**Goal:** Prove the first genuinely fault-tolerant (if not yet sharded)
database.
**Do this:** Integration test: start a 3- or 5-node single-shard cluster.
PUT a large number of keys through the leader. Kill the leader mid-stream
(scripted, repeatable, reusing Phase 2's kill harness). Assert:
1. A new leader is elected.
2. Every previously-acknowledged write is present on the new leader.
3. Writes resume successfully against the new leader.
4. The killed node, once restarted, catches up to an identical state.
**Checkpoint (end of Phase 4):** This is the first point in the project with
a genuinely fault-tolerant database, even though it is not yet sharded.
**Commit message:** `test: Phase 4 checkpoint — leader-kill-mid-write end-to-end suite`

---

## Phase 5 — Sharding

A single Raft group scales only as fast as its leader. Sharding splits the
keyspace into independent partitions, each with its own Raft group/leader,
so writes to different shards proceed fully in parallel.

### Commit 39 — Hash-based key-to-shard mapping
**Goal:** Deterministic, evenly-distributed shard assignment.
**Do this:** `internal/router/hash.go`. `shardId(key) = fnv_hash(key) mod
NUM_SHARDS` (reuse a hashing library — FNV/xxHash/murmur3 — do not implement
your own hash function). Document, as a deliberate documented trade-off, why
hash-based (not range-based) sharding was chosen: even load distribution and
implementation simplicity, at the cost of scattered range scans — which
SamanvayDB's API doesn't need anyway since it has none.
**Tests:** Same key always maps to the same shard id; a large random key
sample distributes roughly evenly across `NUM_SHARDS`.
**Commit message:** `feat(router): deterministic hash-based key-to-shard mapping`

### Commit 40 — Shard map metadata
**Goal:** Cluster-wide knowledge of which shard owns which key-hash-bucket
and which nodes form that shard's Raft group.
**Do this:** `internal/router/shardmap.go`. A small, versioned config
structure:
```go
type ShardMeta struct {
    ShardID  int32
    Replicas []NodeID
    // Leader hint updated opportunistically from redirects (Commit 41)
}
```
Distribute it via a lightweight mechanism for now — a small dedicated
metadata Raft group, or a well-known bootstrap node reachable by every
router/client. **Do not conflate this "metadata about shards" with the
actual sharded data itself** — it's a separate, much smaller piece of state.
**Tests:** Shard map loads correctly from config; a version bump is
observable to consumers.
**Commit message:** `feat(router): versioned shard map metadata`

### Commit 41 — Router / forwarding + redirect handling
**Goal:** Route a request to the correct shard's presumed leader, and
recover transparently from stale leader hints.
**Do this:** `internal/router/router.go`:
```
1. Compute shardId(key)
2. Look up shardId in the shard map -> {leader hint, replica set}
3. Forward the request to the presumed leader
4. If the presumed leader responds "not leader, try X" (or times out):
     update the local shard-map hint, retry against the correct node
     (or against any replica, which itself redirects)
```
Wire the `redirect_leader` field from the gRPC responses (extend Commit 7's
proto messages with `redirect_leader` on Put/Get/Delete responses now, since
Section 18's full API design specifies it here).
**Tests:** A request sent to a follower is transparently redirected and
succeeds; the shard-map hint is updated so the *next* request goes straight
to the correct leader.
**Commit message:** `feat(router): leader-redirect-aware request forwarding`

### Commit 42 — Phase 5 checkpoint ✅
**Goal:** Confirm deterministic multi-shard routing before adding real
per-shard replication.
**Do this:** Stand up a cluster with at least 2 shards — even if, for now,
each shard is a single-node "Raft group" with no real replication (that
composition arrives in Phase 6).
**Checkpoint:** PUTs to different keys route to different shards
deterministically; a GET for a given key always resolves to the same shard
it was written to.
**Commit message:** `test: Phase 5 checkpoint — multi-shard deterministic routing`

---

## Phase 6 — Multi-Raft

Run one fully independent Raft group per shard, all sharing the same
physical nodes, so failures and elections in one shard never affect another.

### Commit 43 — Multi-Raft group manager
**Goal:** Host S independent Raft state machines in one process.
**Do this:** `internal/shard/manager.go`. `ShardManager` holds a map from
`shardId -> *Shard` (Commit 35's type), where each `Shard` runs its own,
fully independent instance of the Commit 26–34 Raft state machine —
own `currentTerm`, own log, own election timers, own leader — parameterized
only by which shard and which peer set it belongs to. **No shared mutable
Raft state between groups** — only shared infrastructure (transport, the
node process itself).
**Tests:** Two `Shard` instances in the same process run independent
elections with no cross-talk (assert term/leader independence directly).
**Commit message:** `feat(shard): ShardManager hosting one independent Raft group per shard`

### Commit 44 — Per-shard storage isolation
**Goal:** A bug or corruption in one shard's storage cannot cross-contaminate
another's.
**Do this:** Each `Shard`'s Raft log/state file and LSM engine
(WAL/SSTables/manifest) live under a per-shard subdirectory of `data_dir`
(e.g. `data_dir/shard-<id>/{raft,storage}/`).
**Tests:** Deleting/corrupting one shard's data directory (in a test) has no
observable effect on another shard's state or tests.
**Commit message:** `feat(storage): per-shard directory isolation`

### Commit 45 — Shard-aware transport dispatch
**Goal:** Route an incoming RPC to the correct in-process Raft group.
**Do this:** `internal/transport/dispatch.go`. Every Raft/KV RPC carries a
`shard_id` field (extend proto messages). A single TCP connection between
two nodes typically carries traffic for many shards — dispatch by the
message's shard id, never by assuming one connection == one shard. As an
optimization note (not required for correctness), batch/coalesce heartbeats
for co-located shard groups between the same node pair into fewer RPCs —
implement the simple per-group version first and treat coalescing as a
later optimization.
**Tests:** RPCs for shard A and shard B arriving on the same connection are
dispatched to the correct `Shard` instance.
**Commit message:** `feat(transport): shard-id-aware RPC dispatch`

### Commit 46 — Phase 6 checkpoint ✅
**Goal:** Prove failure isolation across shards at realistic scale.
**Do this:** Run a 9-node cluster with 3 shards, replication factor 3
(matching the guide's own demo topology from Section 1.2). Confirm leaders
are distributed across different nodes (not all on Node 1). Kill one shard's
leader; confirm only that shard runs an election and the other two shards
serve writes with **zero interruption** throughout.
**Checkpoint:** As above.
**Commit message:** `test: Phase 6 checkpoint — 9-node/3-shard failure isolation`

---
## Phase 7 — Shard Migration & Rebalancing

Move a shard's replica set from one set of nodes to another, with no
downtime and no lost/duplicated writes.

### Commit 47 — Migration state machine
**Goal:** A well-defined, abortable-at-every-stage state progression.
**Do this:** `internal/migration/migration.go`:
```go
type ShardMeta struct {
    ShardID          int32
    State            string // STABLE | MIGRATING | CUTOVER_PENDING
    SourceGroup      []NodeID
    TargetGroup      []NodeID // empty unless State != STABLE
    SourceLastIndex  uint64   // index up to which target is caught up
}
```
States: `PENDING -> SNAPSHOTTING -> TRANSFERRING -> CATCHING_UP -> CUTOVER ->
DONE`, with an `ABORTED` branch from any pre-CUTOVER state. Every stage
before `CUTOVER` must be trivially abortable/retryable: the source group
stays fully authoritative and serving traffic normally until the shard map
is flipped, so a failure during snapshotting/transferring is just "throw
away the half-built target group and try again," zero risk to live data.
**Tests:** State transitions follow the legal graph only; abort from every
pre-CUTOVER state cleanly reverts to `STABLE` pointing at the (untouched)
source.
**Commit message:** `feat(migration): abortable shard migration state machine`

### Commit 48 — Snapshot export/transfer to target group
**Goal:** Bootstrap a brand-new target Raft group from the source shard's
current state.
**Do this:** `internal/migration/transfer.go`. Provision a new Raft group on
target nodes, initially empty. Reuse the Raft snapshot mechanism (Commit 32)
— or a dedicated bulk-export of the LSM engine's SSTables — to snapshot/
export the source shard's state, transfer it to the target group, and
install it as the target's starting state (like `InstallSnapshot`, but
bootstrapping a brand-new group rather than catching up an existing one).
**Tests:** Target group, after installation, contains exactly the source's
state as of the snapshot index.
**Commit message:** `feat(migration): snapshot-based bootstrap of the target shard group`

### Commit 49 — Freeze-and-flip cutover
**Goal:** Implement the catch-up window and atomic cutover, choosing the
guide's recommended (simpler) strategy over dual-write.
**Do this:** `internal/migration/cutover.go`. During `CATCHING_UP`: source
continues accepting writes; target tails/replicates from the snapshot index
onward until lag is small. On `CUTOVER`: briefly pause new writes to the
shard (source stops accepting writes but keeps serving reads), wait for the
last entries to replicate to target, **durably update the shard map**, then
resume writes against the target. Only after this atomic update does the
source stop serving that shard's traffic. Document freeze-and-flip's small,
bounded write-unavailability window as a deliberate trade-off (and note
dual-write-with-idempotent-replay as a labeled, more complex stretch goal —
do not implement it unless freeze-and-flip is solid and tested first).
**Tests:** Continuous background writes to the shard during migration; after
cutover, the client's write count matches exactly the number of entries in
the target group, with **zero gaps**.
**Commit message:** `feat(migration): freeze-and-flip cutover with zero write loss`

### Commit 50 — Rebalancing heuristic
**Goal:** A simple, defensible policy for redistributing load when nodes are
added.
**Do this:** `internal/migration/rebalance.go`. Track each node's current
shard-replica count; when a new node joins, identify the most heavily-loaded
existing nodes and migrate one replica of one of their shards to the new
node (via Commits 47–49), repeating until load is roughly even. This greedy,
count-based heuristic is a legitimate first version — note load-aware
(CPU/disk/QPS-based) rebalancing as a documented future improvement, not a
requirement.
**Tests:** Adding a node to an unbalanced cluster reduces the max-min
replica-count spread across nodes.
**Commit message:** `feat(migration): greedy count-based rebalancing on node add`

### Commit 51 — Phase 7 checkpoint ✅
**Goal:** Prove zero-gap migration under load.
**Do this:** Migrate one shard from a 3-node group to a different, disjoint
3-node group while a client continuously writes to that shard in the
background.
**Checkpoint:** The client's write count matches the number of entries
present in the target group after cutover, with zero gaps.
**Commit message:** `test: Phase 7 checkpoint — zero-gap shard migration under load`

---

## Phase 8 — Node Membership Changes

Support adding, removing, and replacing nodes in a running cluster without
ever risking two disjoint majorities existing at once.

### Commit 52 — Add node (learner → voting promotion)
**Goal:** Safely bring a new, empty node into a shard's Raft group.
**Do this:** `internal/membership/add.go`.
```
1. New node starts empty, joins as a non-voting LEARNER
     (replicates log/snapshot, does NOT count toward quorum yet)
2. Leader streams a snapshot + subsequent log entries to the new node
3. Once caught up (lag below a small threshold), leader proposes a
     config-change entry promoting the node to a full voting member
4. Config-change entry commits via NORMAL Raft replication (it's just
     another log entry)
5. New node is now a full voting participant
```
**Key implementation details:** Use the guide's recommended simpler
approach — **single-node-at-a-time** membership changes (never two
membership changes in flight simultaneously for the same group) — rather
than implementing full joint consensus. With this restriction, old and new
majorities are provably guaranteed to overlap by at least one node, which is
sufficient to prevent the split-majority hazard. Document joint consensus as
the general solution you'd reach for at greater scale, without implementing
it. The non-voting-learner stage exists specifically so a freshly-joined,
empty node cannot affect quorum math before it has any data.
**Tests:** Add a node under load; confirm it becomes a correct voting member
with no availability gap; confirm quorum math is correct at every stage
(learner never counted, voter counted immediately after config-change
commits).
**Commit message:** `feat(membership): add-node via non-voting learner + single-node-at-a-time config change`

### Commit 53 — Remove node
**Goal:** Symmetric removal, safe under the same single-node-at-a-time rule.
**Do this:** `internal/membership/remove.go`. Leader proposes a config-
change entry removing the target node from the voting set. Once committed,
remaining nodes recompute majority against the smaller peer set; the removed
node can then be safely stopped. If the removed node was the leader, let it
step down (or simply be killed, letting normal election proceed) once
removal is decided.
**Tests:** Remove a node; confirm quorum math updates correctly (majority
now computed against N-1); the removed node can be killed without
disrupting the remaining group.
**Commit message:** `feat(membership): remove-node via config-change log entry`

### Commit 54 — Replace node
**Goal:** Compose Commits 52–53 rather than writing new, separately-risky
logic.
**Do this:** `internal/membership/replace.go`. `ReplaceNode(failed, new
NodeID)` = `Remove(failed)` followed by `Add(new)`, reusing Commit 52's
snapshot-streaming catch-up path to populate the new node with fresh data.
**Tests:** A failed node is replaced under load with no data loss and
correct final quorum.
**Commit message:** `feat(membership): replace-node as remove+add composition`

### Commit 55 — Phase 8 checkpoint ✅
**Goal:** Confirm all three membership primitives work against a live,
loaded shard, and prove why out-of-band config changes are unsafe.
**Do this:** Integration tests for add/remove/replace, each run against a
shard under continuous write load. Add a regression test / doc note
explicitly explaining (for the interview-prep record) why updating a peer
list in a config file + restart is unsafe: it gives no ordering guarantee
about when each node adopts the change, unlike routing it through the
replicated log.
**Checkpoint:** Add, remove, and replace all work correctly against a live,
loaded shard.
**Commit message:** `test: Phase 8 checkpoint — membership changes under load`

---

## Phase 9 — Fault Injection

The operational-level fault injector — for demos and real end-to-end tests,
distinct from Phase 3's in-process simulator (Commit 33), which stays as the
fast, unit-test-tier tool.

### Commit 56 — FaultInjector interface + process-level kill/restart
**Goal:** The interface every fault primitive implements, plus the first two
primitives.
**Do this:** `internal/faultinjector/faultinjector.go`:
```go
type FaultInjector interface {
    KillNode(nodeID string) error
    RestartNode(nodeID string) error
    CreatePartition(sideA, sideB []string) error
    HealPartition() error
    DelayMessages(from, to string, duration time.Duration) error
    DropMessages(from, to string, rate float64) error
}
```
Implement `KillNode`/`RestartNode` for a real-process backend: `SIGKILL`
(not `SIGTERM` — simulate a true crash with no graceful shutdown).
**Tests:** Kill a running node process; confirm it's actually gone (not just
unresponsive); restart brings it back and it rejoins per Commit 24's/Phase
4's recovery path.
**Commit message:** `feat(faultinjector): FaultInjector interface + process kill/restart`

### Commit 57 — Network partition
**Goal:** Real network-level partitioning of a running multi-node cluster.
**Do this:** Implement `CreatePartition`/`HealPartition` via iptables rules
(containers/VMs) or a proxy layer between nodes that drops packets between
designated groups.
**Tests:** With a partition active, nodes in different groups genuinely
cannot exchange RPCs (assert via a failed/timed-out RPC, not just log
inspection); healing restores connectivity.
**Commit message:** `feat(faultinjector): real network partition via iptables/proxy`

### Commit 58 — Message delay and drop
**Goal:** Fine-grained network fault primitives beyond full partition.
**Do this:** Implement `DelayMessages` (proxy layer holds messages for a
configurable duration before forwarding) and `DropMessages` (proxy silently
discards a configurable percentage of messages). Also implement the "slow
node" fault type from the guide's table: cgroup CPU/IO throttling, or an
artificial sleep injected into the node's own request-handling path behind a
debug flag. And a convenience "leader-specific failure" wrapper: look up the
current leader via cluster status, then kill/partition exactly that node.
**Tests:** Delayed messages arrive after (at least) the configured delay;
dropped messages never arrive at the configured rate (statistically, over
many messages).
**Commit message:** `feat(faultinjector): message delay/drop and slow-node fault types`

### Commit 59 — Reproducibility: scripted scenarios + seeded randomness
**Goal:** Make every failure reproducible, never "it happened once."
**Do this:** `scripts/` — every fault-injection sequence used for testing or
demos is a checked-in script (calling `kvctl`, see Phase-11-below /
Commit 65), not manually typed commands. Anywhere a test/scenario randomly
picks a target (which node to kill, when), seed the RNG explicitly and log
the seed so a failing run can be replayed exactly.
**Tests:** Running the same seeded scenario twice produces identical
sequences of injected faults.
**Commit message:** `feat(faultinjector): checked-in scripted scenarios with seeded randomness`

### Commit 60 — Phase 9 checkpoint ✅
**Goal:** One script that exercises the whole fault-injection surface and
asserts correctness automatically — this becomes (almost verbatim) the final
demo (Phase 11's Commit ~69).
**Do this:** A single script: start a 9-node cluster, write a known set of
keys, kill the leader of a random shard, wait for a new leader, write more
keys, do **not** heal (permanent kill), assert all previously-acknowledged
writes are still readable and correct. Wire the core scenarios into the
normal CI test run — per the guide's explicit Common Mistake warning,
fault-injection tests are only valuable if automated, not a one-time manual
demo watched by a human once.
**Checkpoint:** As above.
**Commit message:** `test: Phase 9 checkpoint — automated end-to-end fault-injection scenario`

---

## Phase 10 — Observability

Production-style instrumentation: metrics and status views so an operator
(or you, in a demo) can see what the cluster is doing without reading raw
logs.

### Commit 61 — Metrics package + Prometheus endpoint
**Goal:** Instrument exactly the vocabulary already built throughout the
project.
**Do this:** `internal/metrics/metrics.go`, using the Prometheus client
library, exposed via a `/metrics` HTTP endpoint on every node. Cover:

| Category | Metrics |
|---|---|
| Request-level | requests/sec (per op type), p50/p95/p99 latency, error rate |
| Raft | current term per shard, elections triggered (counter), leader changes (counter), time since last heartbeat, replication lag per follower, committed entries/sec |
| Storage engine | WAL size, active MemTable size, immutable MemTable count, SSTable count per level, compaction runs (counter), compaction duration, bytes compacted, bloom filter false-positive rate |
| Cluster | nodes up/down, shard count, replicas per shard, page-cache hit rate (if applicable) |

**Tests:** Each metric increments/updates correctly around the code path it
instruments (e.g. election-count increments exactly once per election
started).
**Commit message:** `feat(metrics): Prometheus-compatible /metrics endpoint`

### Commit 62 — Cluster status view
**Goal:** A human-readable and machine-readable narration of live cluster
state.
**Do this:** `internal/transport` (Admin service) + `kvctl cluster status`.
Format:
```
Shard 0
  Leader:   node-2   (term 3, committed index 1,204)
  Replicas: node-1 (follower, lag 0), node-2 (leader), node-3 (follower, lag 0)
```
Expose both as an HTTP/JSON endpoint (for tooling) and as `kvctl cluster
status`'s human-readable table.
**Tests:** Status view correctly reflects a live cluster's leader/term/
committed-index/replica-lag, verified against the underlying Raft/shard
state directly.
**Commit message:** `feat(observability): human- and machine-readable cluster status view`

### Commit 63 — Structured logs for state transitions
**Goal:** The third observability pillar — reconstructable event history.
**Do this:** Ensure every state transition worth knowing about emits one
structured log line with consistent fields (`node_id`, `shard_id`, `term`):
election started, became leader, became follower, applied snapshot,
compaction started/finished, membership change committed.
**Tests:** Each transition type emits exactly one correctly-fielded log line
in a test harness that exercises it.
**Commit message:** `feat(observability): structured log lines for every major state transition`

### Commit 64 — Phase 10 checkpoint ✅
**Goal:** Prove the full triad (metrics + status + logs) tells a coherent
live story.
**Do this:** Point a metrics scraper (or just `curl /metrics`) at a running
9-node cluster, kill a leader, and confirm election-count and leader-change
metrics increment in real time while request latency shows a brief spike
then recovers.
**Note on scope:** Distributed tracing is explicitly **not required** for
this phase to be complete — Prometheus metrics + structured logs + the
cluster-status view are sufficient; do not over-invest here.
**Checkpoint:** As above — this is the capstone artifact for the interview-
prep and final-demo sections later in this plan.
**Commit message:** `test: Phase 10 checkpoint — live fault-injection demo via metrics dashboard`

---
## Phase 11 — CLI, Admin API, and Client Library Completion

The core ten phases are done. These commits finish the operator-facing
surface the demo and later tests depend on.

### Commit 65 — Full kvctl command set
**Goal:** `kvctl` as the single tool for everything — data ops, cluster
introspection, lifecycle, and fault simulation.
**Do this:** Extend `cmd/kvctl` (Cobra) with the full command set:
```
# Data
kvctl put <key> <value>
kvctl get <key> [--consistency=follower-ok]
kvctl delete <key>

# Introspection
kvctl cluster status [--output json]
kvctl shard status --shard <n>
kvctl node status --node <id>
kvctl cluster metrics

# Lifecycle
kvctl cluster start --nodes N --shards S --replication-factor R
kvctl cluster stop
kvctl node add --node <id>
kvctl node remove --node <id>
kvctl shard migrate --shard <n> --target <node,node,node>

# Fault simulation (wraps Phase 9)
kvctl node kill <id>
kvctl node restart <id>
kvctl partition create --side-a <...> --side-b <...>
kvctl partition heal
kvctl node slow <id> --latency <dur>
```
**Design principles to enforce:** every command supports `--output json`
alongside human-readable tables; exit codes are meaningful (a script can
branch on them); `kvctl` is stateless — it discovers topology fresh from the
admin API every invocation, never from a local cache that could go stale.
**Tests:** Each subcommand exercised end-to-end against a running local
cluster; JSON output round-trips through a JSON parser.
**Commit message:** `feat(cli): complete kvctl command set (data, introspection, lifecycle, fault simulation)`

### Commit 66 — Admin API
**Goal:** Formalize the administrative gRPC surface `kvctl` and dashboards
consume.
**Do this:** `proto/admin.proto`:
```protobuf
service Admin {
  rpc ClusterStatus(Empty) returns (ClusterStatusResponse);
  rpc NodeStatus(NodeStatusRequest) returns (NodeStatusResponse);
  rpc ShardStatus(ShardStatusRequest) returns (ShardStatusResponse);
  rpc Metrics(Empty) returns (stream MetricSample); // or plain HTTP /metrics
}
message ClusterStatusResponse { repeated ShardInfo shards; repeated NodeInfo nodes; }
message ShardInfo { int32 shard_id; string leader; repeated string replicas;
                    uint64 committed_index; string state; } // STABLE|MIGRATING
message NodeInfo  { string node_id; bool up; repeated int32 hosted_shards; }
```
**Key implementation details:** `ShardInfo` deliberately exposes exactly the
vocabulary developed throughout the project — leader, replicas, committed
index, migration state. A well-designed admin API isn't a separate design
exercise; it's the architecture's internal concepts made queryable.
**Tests:** Each RPC returns state matching the underlying cluster directly.
**Commit message:** `feat(api): Admin gRPC service (ClusterStatus/NodeStatus/ShardStatus)`

### Commit 67 — Client library and request conventions
**Goal:** The only package external users (or tests/benchmarks) should
import to talk to a cluster.
**Do this:** `pkg/client/client.go`. Wraps the router's redirect-following
behavior transparently for callers. Also finalize error taxonomy across the
whole API: distinguish `NOT_LEADER` (retry elsewhere), `UNAVAILABLE` (no
majority currently reachable; retry with backoff), `INVALID_REQUEST` (do not
retry) — conflating these causes either needless retry storms or clients
giving up on transient conditions. Every RPC gets a client-side timeout
**larger than** the Raft election timeout, so a client waiting through a
leader failover doesn't give up before a new leader has a chance to be
elected. Note PUT/DELETE idempotency explicitly in docs — safe to retry
after a client-side timeout.
**Tests:** Client transparently follows redirects; retries correctly
differentiate the three error classes; a GET's `consistency` field (`LEADER`
vs `FOLLOWER_OK`) is honored and documented as the explicit, per-request
choice between the two consistency levels the system actually supports.
**Commit message:** `feat(client): pkg/client library with redirect-following and error taxonomy`

---

## Phase 12 — Testing Strategy Completion

The unit/integration/failure tiers already have tests woven into each
phase's commits above. These commits add the remaining tiers: correctness
checking and stress testing.

### Commit 68 — Integration test suite consolidation
**Goal:** One well-organized `tests/integration` suite covering the
cross-cutting scenarios, run against a real (multi-process or multi-
container) cluster rather than mocks.
**Do this:** Consolidate/add:
```
PUT -> replicate -> commit -> GET
PUT k v1 -> PUT k v2 -> GET k == v2                (last write wins)
PUT k v -> DELETE k -> GET k == NotFound
PUT across many keys -> kill follower -> restart -> GET all keys correct
PUT -> shard split/migrate -> GET still resolves correctly post-migration
```
**Commit message:** `test: consolidated cross-component integration suite`

### Commit 69 — Correctness tests (including a linearizability checker)
**Goal:** Formal-ish verification that the system's actual guarantees hold.
**Do this:** `tests/integration/correctness_test.go`:
- No committed data is lost after any injected fault sequence.
- No conflicting committed values — for any key, all replicas that have
  applied up to the same index agree on its value (direct test of Commit
  30/37's determinism requirement).
- Linearizable writes where supported — verify with a hand-rolled
  linearizability checker, or integrate an existing tool (e.g. Jepsen's
  Knossos/Elle) against a recorded history of client operations during a
  fault-injection run.
- Correct recovery — a node's post-restart state must be bit-for-bit
  equivalent (at the key/value level) to what it would have been without the
  crash, for every acknowledged write.
**Key implementation details:** Only claim linearizability for **reads** if
a read-index or lease-read mechanism was actually implemented (not part of
the core 10 phases — see the "Leader leases" extension below). If GET is
simply served from local state, document it explicitly as **sequential
consistency per-replica** or **eventually consistent follower reads**, and
reserve "linearizable" only for a mechanism that actually earns the name.
**Tests:** As listed above.
**Commit message:** `test: correctness suite (no lost commits, determinism, honest consistency labeling)`

### Commit 70 — Stress tests
**Goal:** Push every dimension the guide calls out, watching for bounded
behavior.
**Do this:** `tests/stress/`:

| Dimension | What to push |
|---|---|
| Key count | Millions of keys; memory and compaction behavior stay bounded |
| Value size | Small (bytes) through large (multi-MB); no pathological SSTable block behavior |
| Write rate | Sustained high-throughput PUT; MemTable-flush backpressure correctness |
| Read rate | High concurrent GET, including cache-unfriendly random-key patterns |
| Concurrent clients | Many simultaneous connections, overlapping keys, stress locking |
| Long-running cluster | Multi-hour soak: continuous writes, periodic injected faults, periodic compaction — watch for leaks (memory, fds, disk space from uncollected old SSTables) |

**Commit message:** `test: stress-test suite across key count, value size, throughput, and soak duration`

### Commit 71 — Benchmark harness and comparative matrices
**Goal:** Turn the finished system into measured, reproducible numbers.
**Do this:** `benchmarks/`. Implement benchmarks for: PUT/GET/DELETE
throughput; read latency split by MemTable-hot vs. single-SSTable vs.
multi-SSTable; write latency split into WAL-fsync vs. Raft-round-trip
contributions; compaction overhead (latency impact while compaction runs vs.
idle); recovery time as a function of WAL size/entries-since-snapshot; Raft
replication latency under varying injected network latency (via Phase 9's
proxy); sharding scalability (aggregate throughput vs. shard count, per-shard
load held constant). Run two comparative matrices — replica-count scaling
(1/3/6/9 nodes) and shard-count scaling (1/3/6 shards, fixed total cluster
size) — and record throughput + p99 latency for each cell. For every run,
record: the fixed workload definition; throughput and p50/p95/p99/max
latency (never just an average); resource utilization (CPU/disk I/O/
network); and the exact topology/config alongside the result. Use a
dedicated/quiesced environment, discard an initial warm-up window, run each
configuration multiple times, report median or a confidence interval.
**Commit message:** `feat(benchmarks): throughput/latency harness with replica- and shard-count scaling matrices`

---
## Phase 13 — Optional Extensions (High Interview Signal)

These are explicitly optional in the source guide but are the highest
signal-to-effort additions once the core ten phases are solid — each reuses
infrastructure already built. Implement in this order; skip any and the
system is still complete per the Phase 0–10 checklist.

### Commit 72 — Compare-And-Swap (CAS)
**Goal:** The primitive everything else in this section (locks, counters)
builds on.
**Do this:** Add `CAS(key, expectedValue, newValue)`. The leader reads its
own current state (exactly as GET does), checks the condition, and **only
if it holds** proposes the write as a normal Raft command:
```
CAS(key, expected, new):
    currentValue = localRead(key)         # leader's own applied state
    if currentValue != expected: return CAS_FAILED   # no Raft entry proposed
    propose Command{Type: CAS_WRITE, Key: key, Value: new} to Raft log
    # replicates and commits exactly like a normal PUT from here
```
Followers never re-evaluate the condition — they apply whatever the leader
committed.
**Tests:** CAS succeeds only when the expectation matches at propose time;
concurrent CAS attempts on the same key from different clients — exactly one
should observably win per the check-then-append ordering.
**Commit message:** `feat(kv): Compare-And-Swap command`

### Commit 73 — TTL / expiring keys
**Goal:** Correct, deterministic expiry — never trusting a single replica's
clock to decide state.
**Do this:** Attach an optional `expires_at` timestamp to a value. Implement
the guide's **recommended** approach: leader-proposed expiry commands — a
small background ticker on the leader scans for expired keys and proposes
tombstone commands through Raft (not lazy-on-read local judgment, which
would let clock skew between replicas make "the same" read return different
answers on different replicas, breaking Commit 30/37's determinism
guarantee). Reuse the existing tombstone lifecycle (Commit 21/22) rather than
inventing a parallel deletion path — expiry is exactly a leader-proposed
delete.
**Tests:** A key with an expired TTL is invisible via GET on every replica
consistently, only once the leader's expiry command has actually committed
(not merely once wall-clock time has passed on some node).
**Commit message:** `feat(kv): leader-proposed TTL expiry, agreement-based not clock-trust-based`

### Commit 74 — Distributed locks: data model and command types
**Goal:** Lay down the on-disk encoding and command surface for the lock
subsystem — the highest-value extension in the guide (this is the canonical
real-world use case for this class of system: Chubby, ZooKeeper, etcd).
**Do this:**
```go
// Key: "__lock__:" + lockName  (reserved prefix; reject any client
// PUT/GET/DELETE targeting a __lock__: key directly at the API boundary)
type LockRecord struct {
    OwnerID       []byte
    ExpiresAtMs   uint64 // leader-assigned deadline
    FencingToken  uint64 // == the Raft log index that created this record
    AcquiredAtMs  uint64 // informational only
}

type CommandType uint8
const (
    CmdPut CommandType = iota
    CmdDelete
    CmdLockAcquire
    CmdLockRenew
    CmdLockRelease
    CmdLockExpire // proposed only by the leader's expiry ticker
)
```
**Key implementation details:** A lock is not a new storage primitive — it's
a regular key/value pair in the same LSM engine, so WAL durability,
MemTable/SSTable storage, compaction, and snapshotting apply automatically
with **zero new storage-layer code**. Every one of these command types still
goes through the exact write path from Commit 36 (leader proposes →
replicates → commits → apply()). Nothing about the Raft layer needs to know
these are "lock" commands.
**Tests:** `LockRecord` encode/decode round-trips; reserved-prefix rejection
enforced at the API boundary.
**Commit message:** `feat(locks): lock data model, reserved key prefix, and command types`

### Commit 75 — Leader-side Acquire/Renew/Release handlers (naive, pre-fix)
**Goal:** Implement the check-then-propose shape first, *without* apply-time
re-validation — the guide explicitly recommends doing this deliberately so
the next commit's race and its fix are observed as a real exercise, not
assumed away.
**Do this:**
```
handleLockAcquire(lockName, ownerID, ttlMs):
    current, found = localRead(key)
    if found AND current.expires_at_ms > nowMs(): return ALREADY_HELD
    newRecord = LockRecord{owner_id: ownerID, expires_at_ms: nowMs()+ttlMs,
                            fencing_token: PLACEHOLDER, acquired_at_ms: nowMs()}
    entry = proposeToRaftLog(CmdLockAcquire, key, encode(newRecord))
    wait for entry to commit
    return {ok: true, fencing_token: entry.index}

handleLockRenew(lockName, ownerID, ttlMs):
    current, found = localRead(key)
    if not found or current.owner_id != ownerID: return NOT_OWNER
    if current.expires_at_ms <= nowMs(): return EXPIRED
    propose(CmdLockRenew, key, ownerID, newExpiresAt=nowMs()+ttlMs)
    return ok   # fencing_token carried over unchanged -- renewal mints no new token

handleLockRelease(lockName, ownerID):
    current, found = localRead(key)
    if not found: return ok   # already unlocked; idempotent
    if current.owner_id != ownerID: return NOT_OWNER
    propose(CmdLockRelease, key, ownerID)   # applied as a tombstone delete
    return ok
```
And the corresponding `apply()` cases: `CmdLockAcquire` writes the record
(WAL+MemTable, exactly like Commit 11/12); `CmdLockRenew` re-reads and
updates `expires_at_ms` if owner matches; `CmdLockRelease`/`CmdLockExpire`
write a tombstone and wake waiters (Commit 79).
**Tests:** Single-client acquire/renew/release round-trip works correctly.
**Deliberately do NOT yet test:** concurrent acquire races — that's next.
**Commit message:** `feat(locks): leader-side acquire/renew/release handlers (pre-race-fix)`

### Commit 76 — Fix the leader-side race with apply-time re-validation
**Goal:** Close the concurrency hole in Commit 75: two concurrent acquire
requests can both pass the leader-side `localRead` check before either
commits, since Raft's log is a total order but the check happens before
proposing.
**Do this:**
```
apply(CmdLockAcquire, cmd):
    existing = localRead(cmd.Key)
    if existing found AND existing.expires_at_ms > applyTimeMs():
        # someone else's acquire committed first for this exact key;
        # this entry loses the race -- do NOT overwrite
        recordOutcome(cmd.requestID, ALREADY_HELD)
        return
    record = decode(cmd.Value)
    record.fencing_token = cmd.raftIndex
    walAppend(...); memtableInsert(...)
    recordOutcome(cmd.requestID, OK, record.fencing_token)
```
**Key implementation details:** Correctness must come from `apply()`
re-validating the precondition against state **as of that exact point in
the applied log**, not from the earlier leader-side check — the earlier
check only guarantees a request is rejected without proposing when the
check is evaluated; it does nothing to prevent two checks passing
concurrently before either commits. Because every replica applies entries
in identical committed order, every replica resolves the race identically.
**Tests:** N concurrent acquire attempts for the same lock — exactly one
succeeds, verified under `-race` and repeated with randomized
scheduling/seeds.
**Commit message:** `fix(locks): apply-time precondition re-validation closes concurrent-acquire race`

### Commit 77 — Expiry ticker
**Goal:** Leader-driven, Raft-ordered lock expiry — never a per-node
judgment call.
**Do this:**
```
expiryTicker (runs only while this node believes it is the leader):
    every TICK_INTERVAL (e.g. 500ms):
        for each lock record in localState where expires_at_ms <= nowMs():
            propose(CmdLockExpire, key, ownerID=record.owner_id)
            # apply() treats this exactly like a Release; idempotent-safe --
            # if already released/re-acquired before this commits, apply()
            # re-validates and no-ops if stale
```
**Key implementation details:** Because the ticker only runs on the leader
and re-derives its list from Raft-committed state every tick, a leader
failover means the new leader's own ticker just picks up the same work — no
per-node expiry state needs to be handed off during failover.
**Tests:** A lock with a lapsed TTL and no renewal is expired via a
committed `CmdLockExpire` (not merely "looks expired" locally); after leader
failover mid-expiry-window, the new leader's ticker correctly continues
expiring it.
**Commit message:** `feat(locks): leader-only expiry ticker proposing Raft-ordered expirations`

### Commit 78 — Fencing token assignment at apply time
**Goal:** Confirm/lock in that fencing tokens are minted correctly and are
non-optional.
**Do this:** Ensure (per Commit 76's `apply()`) the fencing token is
assigned using `cmd.raftIndex` **at apply time**, never at propose time
(the leader doesn't know in advance exactly which index a proposed entry
will land at, since other entries for other shards/keys may be concurrently
appended ahead of it). Document the fencing-token safety argument
explicitly: a lease-based lock alone is unsafe against a client that stalls
(GC pause, slow disk, descheduled VM) past its TTL without knowing it — the
lease expires, a second client acquires and starts work assuming
exclusivity, and the first client can wake up and act concurrently,
corrupting whatever the lock protected. A monotonically increasing fencing
token — trivially free here, since the Raft log index of the successful
`LOCK_ACQUIRE` entry already is one — closes this gap: any protected
resource **rejects** a request whose token is ≤ the highest token it has
already seen, regardless of what the caller believes about holding the
lock.
**Tests:** Token values are strictly increasing across successive
successful acquires of the same lock name; a mock protected resource
correctly rejects a stale (lower) token presented after a newer one has been
seen.
**Commit message:** `feat(locks): fencing-token assignment at apply time + safety documentation`

### Commit 79 — Waiter registry / WAIT
**Goal:** Let clients block on lock release instead of polling, as a
best-effort convenience feature (deliberately **not** replicated through
Raft — it's a liveness/UX feature, not a durability-requiring one).
**Do this:**
```go
type WaiterRegistry struct {
    mu      sync.Mutex
    waiters map[string][]chan struct{} // lockKey -> channels to close
}
// WAIT(key): register a channel, block until closed or context cancelled
// wakeWaiters(key): called from apply(), on Release/Expire — close all
//   registered channels for that key
```
**Key implementation details:** Because this state is leader-local and not
replicated, a leader failover silently drops all pending waiters on that
node — an acceptable, explicitly documented trade-off. Clients issuing WAIT
must treat it as best-effort long-poll and be prepared to reissue it against
the new leader.
**Tests:** A `WAIT` call unblocks promptly when the lock is released/
expires; a `WAIT` call against a leader that then fails over times out/
errors and can be safely reissued (tested at the client-library layer, next
commit).
**Commit message:** `feat(locks): leader-local waiter registry for WAIT`

### Commit 80 — Client library: LockHandle with auto-renewal
**Goal:** Prevent the most common real-world lock-usage bugs (forgetting to
renew, forgetting to release, racing renewal against release) by
construction, in `pkg/client`.
**Do this:**
```go
type LockHandle struct {
    lockName, ownerID string
    fencingToken      uint64
    stopRenewal       chan struct{}
}
// AcquireLock(client, lockName, ttl): Lock() call, spins up autoRenewLoop
// autoRenewLoop: renews at ttl/3 intervals; on NOT_OWNER/EXPIRED -> markLost();
//   on repeated transient failure past a safety margin (e.g. 2 missed
//   intervals) -> markLost() proactively, do not assume the lock is still held
// Release(): stop renewal goroutine, call Unlock()
// FencingToken() uint64
```
**Key implementation details:** `markLost()` is the client-side half of the
fencing-token safety story — application code using this handle must check
a "still valid" flag before any critical action, and must **always** pass
`FencingToken()` to whatever resource it's protecting, so that even if
`markLost()` is somehow missed (the exact paused-goroutine failure mode
fencing tokens exist for), the protected resource's own token check is the
final backstop.
**Tests:** Auto-renewal keeps a long-held lock alive across many TTL
periods; a client that stops renewing (simulated stall) correctly
`markLost()`s within the safety margin; `WAIT`'s leader-failover
reissue behavior (from Commit 79) is handled transparently by this layer.
**Commit message:** `feat(client): LockHandle with background auto-renewal and markLost safety`

### Commit 81 — Lock subsystem checkpoint ✅
**Goal:** Prove the full lock subsystem — including its interaction with
leader failover and fencing — under real faults.
**Do this:** Full test matrix:

| Tier | Cases |
|---|---|
| Unit | Acquire on unheld lock succeeds with a monotonically increasing token; acquire on held-unexpired fails without proposing; renew/release by non-owner fails; release is idempotent |
| Concurrency | N concurrent acquire attempts for the same lock — exactly one succeeds, race-detector + randomized-seed repeats |
| Integration | Acquire → kill leader → new leader serves same lock state + fencing token unchanged; acquire → let TTL lapse with no renewal → expiry commits → a second client can then acquire |
| Fault injection (reuse Phase 9) | Partition the lock holder away → expiry still occurs via the majority-side leader's ticker; partition the leader into the minority → no expiry/acquire commands can commit until partition heals or a majority-side leader takes over |
| Fencing-token end-to-end | A mock protected resource tracking the highest token seen; simulate a stalled client presenting an old token after a second client has acquired and used a newer one; assert the stale request is rejected |

Run the guide's capstone scenario as an automated test: two independent
client processes racing to hold the same named lock in a loop while
incrementing a shared counter in a mock protected resource guarded by
fencing-token checks; inject a leader kill and a simulated GC-pause (a
client that stops renewing but keeps issuing "protected" requests with its
stale token) mid-run. Assert the counter's final value is exactly consistent
with one holder's requests being accepted at a time, and the mock
resource's rejection log shows the stale-token client's late requests
correctly refused.
**Checkpoint:** As above.
**Commit message:** `test: distributed-locks checkpoint — fencing-token safety under failover and GC-pause`

---
## Phase 14 — Packaging, Demo, and Documentation

### Commit 82 — Docker/docker-compose for local multi-node clusters
**Goal:** Make it trivial to stand up the demo topology (9 nodes, 3 shards,
RF=3) locally.
**Do this:** `docker/Dockerfile` (builds `samanvaydb-server` + `kvctl`),
`docker/docker-compose.yml` defining 9 node services on a shared network,
plus `scripts/cluster-up.sh` / `scripts/cluster-down.sh` wrapping
`docker-compose up/down` and an initial `kvctl cluster start` call.
**Tests:** `scripts/cluster-up.sh` produces a healthy 9-node/3-shard/RF=3
cluster from a clean checkout, verified by `kvctl cluster status`.
**Commit message:** `feat(docker): docker-compose local multi-node cluster + up/down scripts`

### Commit 83 — Final demo script
**Goal:** A single, narratable sequence exercising every major capability
across all phases, suitable for an interview screen-share or portfolio
video — and, not coincidentally, already largely written as Commit 60's
fault-injection scenario.
**Do this:** `scripts/demo.sh`, sequencing exactly:
1. `kvctl cluster start --nodes 9 --shards 3 --replication-factor 3` →
   show 3 independent leaders on different nodes (Multi-Raft, Phase 6).
2. PUTs to several keys landing on different shards → show each response's
   `shard=` field (Phase 5 routing, in parallel).
3. `kvctl node kill <shard-1-leader>` → show election completing in
   ~300ms, `GET` still correct via the new leader, **and** the other two
   shards' status completely unaffected (failure isolation, Phase 6).
4. `kvctl node restart <killed-node>` → show it rejoin as a follower with
   lag 0 (Phase 4 recovery).
5. `kvctl partition create` isolating a shard's leader from its own
   majority specifically → show a write against that shard failing
   `UNAVAILABLE`, a new majority-side leader elected, then `kvctl partition
   heal` → show reconciliation (Phase 3's partition walkthrough, made
   live).
6. (If Phase 7 implemented) `kvctl shard migrate` with a background write
   loop running throughout → show the migration state machine progressing
   and reads/writes unaffected pre/post cutover.
7. Throughout: keep the cluster-status view and a live metrics dashboard
   visible as the narrative backbone (Phase 10), and call out client
   operations continuing to succeed except during the deliberately
   demonstrated brief minority-partition unavailability window.
**Tests:** `scripts/demo.sh` runs end-to-end without manual intervention in
CI (as a slow/e2e test tier) and exits 0.
**Commit message:** `docs+test: final demo script exercising all ten phases live`

### Commit 84 — Documentation and final deliverables checklist ✅
**Goal:** Close out the project with documentation reflecting exactly what
was built (no more, no less), and a final self-audit against every
capability the guide defines.
**Do this:**
- `README.md`: architecture summary (the five-layer request path: Client →
  Router → Shard → Raft Group → Storage Engine), quickstart (`docker/`
  compose up + `kvctl` walkthrough), and an explicit, honest list of what is
  **deliberately out of scope** — do not silently omit this section:
  - Distributed transactions (multi-key, cross-shard) — needs a separate
    2PC/MVCC layer on top of this project, not inside it.
  - Multi-key atomic operations, even single-shard.
  - SQL / a query language, or a query engine (range scans, secondary
    indexes) — interacts badly with hash-based sharding as designed.
  - Geo-replication (multi-datacenter) — WAN latency and multi-region
    failure modes are a different, larger problem.
  - A dedicated read-cache layer beyond the MemTable/OS page cache.
  - Authentication, authorization, TLS (Section 20) — orthogonal to the
    project's core thesis; note that enabling TLS later is mostly a gRPC
    configuration change, while auth/authz would need a new cross-cutting
    identity check on every RPC handler.
  - Consistency models beyond the two-tier linearizable-leader-read /
    stale-follower-read distinction actually implemented (do not claim full
    linearizability unless Commit-set for read-index/lease-reads, an
    optional stretch item, was actually built).
- `docs/`: architecture diagrams (the five-layer view and the node-internal
  pipeline view), a glossary (Client, Router, Shard, Raft Group, Raft Node,
  Storage Engine, WAL, MemTable, SSTable, Compaction, Snapshot), and the
  measured-metrics table from benchmarking (Commit 71) with every number
  filled in from real, reproducible runs — never invented or rounded
  placeholders.
- Final self-audit — go through every line below and confirm it is checked
  off **and has a corresponding passing test**, not just working code you
  eyeballed once:

```
☐ KV API                         ☐ Multi-Raft
☐ MemTable                       ☐ Leader failover
☐ WAL                            ☐ Node recovery
☐ SSTables                       ☐ Network partition simulation
☐ Bloom filters                  ☐ Shard migration
☐ Compaction                     ☐ Membership changes
☐ Crash recovery                 ☐ Metrics
☐ Raft                           ☐ Benchmarks
☐ Raft persistence               ☐ CLI
☐ Snapshots                      ☐ Documentation
☐ Raft + LSM integration         ☐ Final demo
☐ Sharding
```

**Commit message:** `docs: final README, architecture docs, and deliverables self-audit`

---

## Appendix A — Scope guard: do not add these mid-project

Every item below is a legitimate, well-known extension to this class of
system — and every one of them would meaningfully expand the project for
comparatively little additional learning about its two core subjects
(Raft consensus and LSM storage). If a compelling use case for one of these
arises while building any commit above, **write it down as a documented
future-work item and keep going** — do not insert it mid-sequence.

- Distributed transactions (multi-key, cross-shard)
- Multi-key atomic operations, even within a single shard
- A SQL/query language, or a query planner/execution engine
- Range scans / secondary indexes (actively conflicts with the hash-based
  sharding decision made in Commit 39)
- Geo-replication / multi-datacenter deployment
- A dedicated read-cache layer beyond the MemTable + OS page cache
- Authentication, authorization, TLS (defer per Commit 84's README section
  — note it as a decision, not an oversight)
- Consistency models beyond linearizable-leader-reads / stale-follower-reads
  (causal consistency, session guarantees) unless read-index/lease reads
  (Appendix B) are separately, explicitly built and tested

## Appendix B — Further stretch goals (build only after everything above is solid)

If time remains after Commit 84, in roughly descending signal-to-effort
order:
1. **Deterministic simulation testing (DST):** run the entire cluster
   (network, clock, disk) inside one seeded, single-threaded simulated
   environment — enables millions of simulated cluster-days and byte-for-
   byte reproducible failures. The single highest-signal addition to the
   project's testing story if pursued.
2. **A formal TLA+/Alloy spec:** model-check the Raft implementation's core
   safety properties (election safety, leader completeness, state machine
   safety) rather than relying on test coverage alone.
3. **Leader leases / read-index reads:** closes the linearizable-reads gap
   documented throughout this plan, turning a stated limitation into an
   implemented guarantee — required before ever claiming full
   linearizability.
4. **Consistent hashing** for sharding (replacing Commit 39's static
   `hash mod N`), layered on top of the already-working migration mechanics
   from Phase 7, so that adding/removing a shard reassigns only a
   proportional slice of keys instead of the majority of the keyspace.
5. **Leveled compaction** (replacing/supplementing Commit 22's size-tiered
   scheme) for better read/space amplification at the cost of higher write
   amplification.
6. **Joint consensus** for membership changes (replacing Commit 52's
   single-node-at-a-time restriction) — the general solution for multi-node
   membership changes at larger scale.
7. **RPC heartbeat coalescing** across co-located shard groups between the
   same node pair (an optimization noted, not required, in Commit 45).
