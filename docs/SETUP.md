# SamanvayDB — Setup and Developer Guide

This guide takes you from a fresh machine to a running SamanvayDB node, a
passing test suite, and a clear picture of where to start contributing.

> **Project status:** SamanvayDB is in its foundation stage (Phase 0
> checkpoint done). What exists today: YAML config loading, structured logging,
> a `/healthz` HTTP endpoint, and the shared `Command` type that the KV layer,
> WAL and Raft will use. The storage engine, Raft, sharding, the gRPC API and
> the `kvctl` client are **not implemented yet**. Their directories are empty
> placeholders (`.gitkeep`). A running node today starts up, reports healthy,
> and waits for shutdown. It does not store keys yet.

---

## 1. Prerequisites

| Tool | Version | Required? | Why |
|------|---------|-----------|-----|
| **Go** | **1.23.1 or newer** | Yes | The whole project is Go (`go.mod` declares `go 1.23.1`). |
| **Git** | any recent | Yes | Source control. |
| **GNU Make** | any | Optional | Shortcuts in the `Makefile`. Every target is a single `go` command you can run by hand. |
| **C compiler (gcc)** | any | For `-race` | The Go race detector needs cgo, so it needs a C toolchain. |
| **Docker** | any | Not yet | `docker/` is reserved for multi-node clusters in later phases. |
| **curl** | any | Optional | To hit the health endpoint. |

### 1.1 Install Go

**Windows**
1. Download the Windows `.msi` installer (go1.23.x or newer) from
   <https://go.dev/dl/> and run it. It installs to `C:\Program Files\Go` and
   adds `C:\Program Files\Go\bin` to your `PATH`.
2. **Close and reopen** every terminal and VS Code window so they pick up the
   new `PATH`.
3. Alternatively, with winget: `winget install GoLang.Go`

**macOS:** `brew install go`, or use the `.pkg` from go.dev/dl.

**Linux:** download the tarball from go.dev/dl, then:
```bash
sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf go1.23.*.linux-amd64.tar.gz
echo 'export PATH=$PATH:/usr/local/go/bin:$HOME/go/bin' >> ~/.bashrc
```

Verify:
```bash
go version        # should print go1.23.1 or newer
go env GOPATH     # where downloaded modules and tools live
```

### 1.2 Install Make (optional)

- **macOS / Linux:** usually preinstalled. If not: `xcode-select --install` or `sudo apt install make`.
- **Windows:** pick one:
  - MSYS2 (if installed): `pacman -S make` in an MSYS2 shell, or use the bundled `mingw32-make` from `C:\msys64\ucrt64\bin`.
  - `winget install GnuWin32.Make`, or `choco install make`.
  - Or skip Make and run the equivalent `go` commands shown in each section below.

> On Windows, run `make` from **Git Bash**, not PowerShell. `make clean` uses
> `rm -rf`, which PowerShell and cmd do not have.

### 1.3 Install a C compiler for the race detector

The conventions in [conventions.md](conventions.md) require
`go test -race ./...` to pass for any concurrent code. `-race` needs cgo.

- **Linux:** `sudo apt install build-essential`
- **macOS:** `xcode-select --install`
- **Windows:** install MSYS2 (<https://www.msys2.org/>), then in the UCRT64 shell run
  `pacman -S mingw-w64-ucrt-x86_64-gcc`, and add `C:\msys64\ucrt64\bin` to your `PATH`.
  Check with `gcc --version`.

### 1.4 Editor setup (recommended)

In VS Code, install the official **Go** extension (`golang.go`). When it
prompts you, accept installing the Go tools (`gopls`, `dlv`, `staticcheck`).
You can also run **"Go: Install/Update Tools"** from the command palette.

---

## 2. Get the code

```bash
git clone <repository-url> SamanvayDB
cd SamanvayDB
```

The Go module name is `samanvaydb`, so internal imports look like
`samanvaydb/internal/config`. You do **not** need to clone into `GOPATH`.
Modules work from any directory.

---

## 3. Download dependencies

The only external dependency right now is `gopkg.in/yaml.v3`.

```bash
go mod download
go mod tidy
```

`go mod tidy` also fixes a small inconsistency: `go.mod` currently marks
`gopkg.in/yaml.v3` as `// indirect`, but `internal/config` imports it directly.
After running tidy, commit the updated `go.mod` and `go.sum`.

---

## 4. Build

```bash
make build
# equivalent:
go build -o bin/samanvaydb-server ./cmd/samanvaydb-server
```

**Windows note:** `make build` produces a file named `samanvaydb-server` with
no `.exe` extension. It runs from Git Bash, but PowerShell won't execute it.
In PowerShell, build with an extension instead:
```powershell
go build -o bin\samanvaydb-server.exe .\cmd\samanvaydb-server
```

`bin/` is gitignored.

---

## 5. Run a node

Run from the **repository root**. The default config path
`configs/single-node.yaml` is relative to it.

```bash
# Option A: run without building
go run ./cmd/samanvaydb-server

# Option B: run the built binary
./bin/samanvaydb-server                       # Linux / macOS / Git Bash
.\bin\samanvaydb-server.exe                   # PowerShell
```

### Command-line flags

| Flag | Default | Meaning |
|------|---------|---------|
| `-config` | `configs/single-node.yaml` | Path to the node's YAML config |
| `-health-addr` | `127.0.0.1:8080` | Address for the `/healthz` HTTP endpoint |

Example: `go run ./cmd/samanvaydb-server -config configs/single-node.yaml -health-addr 127.0.0.1:9090`

### Expected output

```
time=... level=INFO msg="SamanvayDB server starting" node_id=node-1 listen_addr=127.0.0.1:7000 data_dir=./data/node-1
time=... level=INFO msg="health server listening" node_id=node-1 addr=127.0.0.1:8080
time=... level=INFO msg="health server ready" node_id=node-1 addr=127.0.0.1:8080
```

### Check that it's healthy

In a second terminal:
```bash
curl http://127.0.0.1:8080/healthz        # Linux / macOS / Git Bash
curl.exe http://127.0.0.1:8080/healthz    # PowerShell (plain `curl` is an alias for Invoke-WebRequest there)
```
Response:
```json
{"status":"ok","node_id":"node-1"}
```

Stop the node with **Ctrl+C**. It logs `shutting down` and exits cleanly.

### What the node does *not* do yet
- `listen_addr` (`127.0.0.1:7000`) is read and logged but **nothing listens on
  it yet**. The gRPC/KV server comes in a later phase.
- `data_dir` is not created or written to yet.
- `raft_peers` is parsed but unused until Raft exists (Phase 3+).

### Troubleshooting

| Symptom | Cause / fix |
|---------|-------------|
| `go: command not found` / `'go' is not recognized` | Go isn't on `PATH`. Reinstall it, or reopen the terminal and VS Code after installing. |
| `failed to load config: ... failed to read file "configs/single-node.yaml"` | You aren't in the repo root. `cd` there, or pass `-config` with a full path. |
| `config: node_id is required` (or `listen_addr` / `data_dir`) | The YAML is missing a required field. |
| `failed to start health server ... bind: address already in use` / `Only one usage of each socket address` | Port 8080 is taken. Use `-health-addr 127.0.0.1:<other-port>`. |
| `-race requires cgo` / `cgo: C compiler "gcc" not found` | Install gcc (section 1.3). On Windows you may also need `set CGO_ENABLED=1` (cmd) or `$env:CGO_ENABLED=1` (PowerShell). |

---

## 6. Run the tests

```bash
make test          # go test ./...
make test-race     # go test -race ./...   (required before committing concurrent code)
```

Useful variants:
```bash
go test -v ./internal/config/...                 # one package, verbose
go test -run TestLoadConfig ./internal/config    # one test by name (regex)
go test -cover ./...                             # coverage summary
go test -coverprofile=coverage.out ./... && go tool cover -html=coverage.out
go vet ./...                                     # static checks
gofmt -l .                                       # list files that need formatting (should print nothing)
```

Current test files:
- `internal/config/config_test.go`: config loading and validation
- `internal/logging/logger_test.go`: structured logging fields
- `internal/server/health_test.go`: `/healthz` readiness behavior
- `internal/command/command_test.go`: Command validation and binary encoding round-trip
- `tests/unit/example_table_test.go`: the **template** for table-driven tests

---

## 7. Project layout

```
cmd/
  samanvaydb-server/   main.go: loads config, starts logger + health server     [implemented]
  kvctl/               command-line client                                      [placeholder]
configs/               node YAML configs (single-node.yaml)
internal/
  config/              YAML config loader + validation                          [implemented]
  logging/             slog wrapper; node_id on every line, later term/role     [implemented]
  server/              /healthz endpoint; later the gRPC service                [partial]
  command/             Command{Type, Key, Value} + binary wire format           [implemented]
  storage/
    wal/               write-ahead log                                          [placeholder]
    memtable/          in-memory sorted table                                   [placeholder]
    sstable/           on-disk sorted string tables                             [placeholder]
    compaction/        LSM compaction                                           [placeholder]
  raft/                Raft consensus                                           [placeholder]
  transport/           inter-node RPC                                           [placeholder]
  shard/ router/       key -> shard -> Raft group routing                       [placeholder]
  membership/ migration/  cluster membership, shard moves                       [placeholder]
  metrics/             metrics                                                  [placeholder]
  faultinjector/       fault injection for failure tests                        [placeholder]
pkg/client/            public Go client library                                 [placeholder]
tests/unit|integration|failure/                                                 [unit template only]
benchmarks/ docker/ scripts/                                                    [placeholder]
docs/conventions.md    error-handling and testing rules: read before coding
```

### How a write will flow (target design)

```
client -> server (gRPC) -> router/shard -> Raft propose(Command bytes)
       -> replicated & committed -> apply -> WAL append -> MemTable
       -> flush to SSTable -> compaction
```

`internal/command` is the shared currency across all of these. Its wire format
is `Type(1B) | KeyLen(4B) | ValLen(4B) | Key | Value`, big-endian.

---

## 8. Start working on the project

### 8.1 Read these first
1. [conventions.md](conventions.md): errors as values with wrapped context,
   panics only for invariant violations, table-driven tests, and the race
   detector requirement.
2. `internal/command/command.go`: **empty keys are illegal everywhere**. Every
   new write path (WAL writer, Raft propose, gRPC handler) must call
   `Validate()`.
3. `tests/unit/example_table_test.go`: copy this pattern for new tests.

### 8.2 Day-to-day workflow
```bash
git checkout -b feat/wal-append          # branch off master
# ... write code + *_test.go alongside it ...
gofmt -w . && go vet ./...
make test-race
git commit -m "feat(wal): append and fsync Command records"
```
Commit messages follow Conventional Commits with a scope, matching history:
`feat(config): ...`, `feat(logging): ...`, `chore: ...`.

### 8.3 Logging rules
Never use `fmt.Println` for diagnostics. Use the injected `*logging.Logger`:
```go
log := logger.WithComponent("wal")
log.Info("segment rotated", "segment", id, "bytes", n)
```
Once Raft exists, wrap it with `logger.WithRaftContext(term, role)`.

### 8.4 Suggested next steps (what's unimplemented, in dependency order)
1. **Storage engine:** `storage/wal`, then `memtable`, then `sstable`,
   then `compaction`, using `command.Command` as the record type.
2. **KV server + `kvctl`:** bind `listen_addr`, expose Put/Get/Delete, and
   create `data_dir` on startup.
3. **Raft (Phase 3):** `raft/` + `transport/`, using `raft_peers` from the config.
4. **Raft ↔ storage integration (Phase 4):** apply committed `Command`s to the
   storage engine.
5. **Sharding / multi-Raft (by Phase 6, 9+ nodes):** `shard/`, `router/`, `membership/`,
   `migration/`. Then multi-node configs in `configs/`, `docker/`, and the
   `cluster-up` / `cluster-down` Make targets (currently they only print "not
   yet implemented").
6. **Fault injection and failure tests (Phase 9+):** `faultinjector/`, `tests/failure/`.

---

## 9. Quick-start checklist

- [ ] `go version` shows 1.23.1 or newer
- [ ] `gcc --version` works (needed for `-race`)
- [ ] `go mod tidy` run from the repo root
- [ ] `make test` (or `go test ./...`) passes
- [ ] `make test-race` passes
- [ ] `go run ./cmd/samanvaydb-server` starts and logs `health server ready`
- [ ] `curl http://127.0.0.1:8080/healthz` returns `{"status":"ok","node_id":"node-1"}`
- [ ] Read `docs/conventions.md`
