# SamanvayDB — Setup and Developer Guide

From a fresh machine to a running cluster, a passing test suite, and a map of
the code.

## 1. Prerequisites

| Tool | Version | Required? | Why |
|------|---------|-----------|-----|
| **Go** | 1.26+ (`go.mod`) | Yes | Everything is Go. |
| **Git** | any | Yes | |
| **GNU Make** | any | Optional | Shortcuts; every target is a plain `go` command. |
| **C compiler (gcc)** | any | For `-race` | The race detector needs cgo. |
| **bash** | any | For `scripts/` | Git Bash works on Windows. |
| **Docker** | any | Optional | Only for the docker-compose cluster. |

Windows notes: run `make` and `scripts/*.sh` from Git Bash. Build with `.exe`
names (the Makefile does this automatically when `OS=Windows_NT`).

## 2. Build

```bash
go mod download
make build        # bin/samanvaydb-server, bin/kvctl (.exe on Windows)
```

## 3. Run

### A single node

```bash
bin/samanvaydb-server -config configs/single-node.yaml
curl http://127.0.0.1:8080/healthz          # {"status":"ok","node_id":"node-1"}
bin/kvctl --server 127.0.0.1:7000 put hello world
```

### A local multi-process cluster (recommended)

```bash
bin/kvctl cluster start --nodes 9 --shards 3 --replication-factor 3
bin/kvctl cluster status
bin/kvctl cluster stop --purge
```

`kvctl cluster start` writes node configs and process state under
`.samanvay-local/` (`--local-dir`). Other `kvctl` commands use it as the
default seed list, and `--server`/`$SAMANVAY_SERVERS` override it.

### Hand-started nodes

`configs/cluster-3/node-{1,2,3}.yaml` describes a 3-node cluster. Start each
node in its own terminal:
`bin/samanvaydb-server -config configs/cluster-3/node-1.yaml`.

### Docker

```bash
bash scripts/cluster-up.sh       # 9 containers, waits for every shard leader
docker compose -f docker/docker-compose.yml exec node-1 kvctl --server node-1:7000 cluster status
bash scripts/cluster-down.sh
```

### Server flags

| Flag | Default | Meaning |
|------|---------|---------|
| `-config` | `configs/single-node.yaml` | YAML config |
| `-health-addr` | `http_addr`, else `127.0.0.1:8080` | HTTP endpoints (`/healthz`, `/metrics`, `/status`) |
| `-log-json` | false | JSON log lines |
| `-log-level` | info | debug, info, warn, error |

### Config reference

```yaml
node_id: node-1
listen_addr: 127.0.0.1:7001      # gRPC: KV + Admin + Raft
http_addr: 127.0.0.1:8001        # /healthz /metrics /status
data_dir: ./data/node-1          # Raft logs/snapshots, node manifest
join: false                      # true = node added at runtime (hosts nothing until added)
cluster:                         # identical on every node; bootstrap topology (shard map v1)
  num_shards: 3
  replication_factor: 3
  meta_replicas: [node-1, node-2, node-3]   # default: first three nodes
  nodes:
    - {id: node-1, addr: 127.0.0.1:7001, http_addr: 127.0.0.1:8001}
  placement: {0: [node-1, node-2, node-3]}  # optional explicit placement
raft:
  election_timeout_min_ms: 150
  election_timeout_max_ms: 300
  heartbeat_interval_ms: 50
  snapshot_threshold: 10000
  no_sync: false                 # benchmarks only: disables fsync, voids durability
```

After bootstrap, the authoritative topology lives in the metadata Raft group.
Change it only through the admin API (`kvctl node add/remove/replace`,
`shard migrate`, `rebalance`), never by editing configs and restarting.
See `internal/membership` for the reason.

## 4. Tests

```bash
make test           # go test ./...
make test-short     # skips tests that build and run real server processes
make test-race      # go test -race ./...
go test -v ./tests/integration -run TestPhase7     # one scenario
SAMANVAY_TEST_LOG=1 go test -v ./tests/integration -run TestSmoke   # with node logs
```

Long-running tiers: `make stress` (set `SAMANVAY_STRESS_DURATION=1h` for a
soak) and `make bench` (writes `benchmarks/results/latest.md`). Replay a
failure scenario with the seed printed in the test log, for example
`CHAOS_SEED=31337 go test ./tests/failure`.

To run the suite in a Linux container (for example from Git Bash on
Windows, with Docker Desktop running):

```bash
MSYS_NO_PATHCONV=1 docker run --rm -v "$(pwd -W)":/src \
  -v samanvay-gomod:/go/pkg/mod -v samanvay-gocache:/root/.cache/go-build \
  -w /src golang:1.27 go test ./...        # or: go test -race ./...
```

## 5. Regenerating protobuf code

```bash
make proto-gen
```

This needs no `protoc` binary. `tools/protogen` (its own Go module) compiles
the `.proto` files with the pure-Go `bufbuild/protocompile` compiler, then
runs the standard `protoc-gen-go` and `protoc-gen-go-grpc` plugins.

## 6. Where things are

| Package | Responsibility |
|---|---|
| `internal/raft` | consensus: election (Pre-Vote, check-quorum), replication, group commit, persistence, snapshots, membership, ReadIndex |
| `internal/raft/raftsim` | in-process network simulator + cluster harness |
| `internal/storage` | `Engine` interface, map engine with deterministic snapshots |
| `internal/command` | the replicated command format |
| `internal/shard` | Raft group + FSM (KV, CAS, TTL, locks), shard manager |
| `internal/router` | hashing, versioned shard map, redirect-following router |
| `internal/transport` | gRPC Raft transport with shard-ID dispatch |
| `internal/node` | node assembly: KV/Admin/Raft services, metadata group, status |
| `internal/membership`, `internal/migration` | orchestration: add/remove/replace, migration, rebalance |
| `internal/faultinjector` | process kill/restart, partitions, delay/drop, slow node, seeded scenarios |
| `internal/metrics` | Prometheus collectors |
| `internal/linearizability` | linearizability checker for recorded histories |
| `internal/testcluster` | in-process multi-node cluster for tests |
| `pkg/client` | public Go client (redirects, error taxonomy, `LockHandle`) |
| `cmd/kvctl`, `cmd/samanvaydb-server` | binaries |

Read `docs/conventions.md` before changing code. The rules are: errors as
values, panics only for invariant violations, table-driven tests, and
`-race` for concurrent code.
