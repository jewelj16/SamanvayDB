.PHONY: build test test-short test-race test-integration bench stress proto-gen cluster-up cluster-down demo docker-up docker-down clean

GO        ?= go
BIN       ?= bin
EXE       := $(if $(filter Windows_NT,$(OS)),.exe,)
NODES     ?= 9
SHARDS    ?= 3
RF        ?= 3

# Build the server and the CLI
build:
	$(GO) build -o $(BIN)/samanvaydb-server$(EXE) ./cmd/samanvaydb-server
	$(GO) build -o $(BIN)/kvctl$(EXE) ./cmd/kvctl

# Run all tests (unit + integration + failure scenarios)
test:
	$(GO) test ./...

# Fast tier: skips tests that build and run real server processes
test-short:
	$(GO) test -short ./...

# All tests under the race detector — non-negotiable for concurrent code
test-race:
	$(GO) test -race ./...

test-integration:
	$(GO) test -v ./tests/integration/...

# Benchmark matrices (replica-count and shard-count scaling); writes
# benchmarks/results/latest.md
bench:
	SAMANVAY_BENCH=1 $(GO) test -run TestBenchmarkMatrix -timeout 60m -v ./benchmarks/

# Stress tests (long-running; SAMANVAY_STRESS_DURATION=10m for a soak)
stress:
	SAMANVAY_STRESS=1 $(GO) test -timeout 120m -v ./tests/stress/

# Regenerate Go stubs from proto/*.proto. Uses a pure-Go compiler
# (bufbuild/protocompile) to drive protoc-gen-go and protoc-gen-go-grpc,
# so the protoc C++ binary is not required.
proto-gen:
	$(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
	$(GO) install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
	cd tools/protogen && $(GO) run . -root ../.. -module samanvaydb proto/kv.proto proto/raft.proto proto/admin.proto

# Local multi-process cluster (no Docker needed)
cluster-up: build
	$(BIN)/kvctl$(EXE) cluster start --nodes $(NODES) --shards $(SHARDS) --replication-factor $(RF)

cluster-down:
	$(BIN)/kvctl$(EXE) cluster stop --purge

# Scripted end-to-end demo against a local cluster
demo: build
	bash scripts/demo.sh

# Docker Compose 9-node / 3-shard / RF=3 cluster
docker-up:
	bash scripts/cluster-up.sh

docker-down:
	bash scripts/cluster-down.sh

clean:
	rm -rf $(BIN)/ .samanvay-local/ data/
