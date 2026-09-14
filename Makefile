.PHONY: build test test-race proto-gen cluster-up cluster-down clean

# Build all binaries
build:
	go build -o bin/samanvaydb-server ./cmd/samanvaydb-server
	go build -o bin/kvctl ./cmd/kvctl

# Run all tests
test:
	go test ./...

# Run all tests with the race detector — non-negotiable for concurrent code
test-race:
	go test -race ./...

# Regenerate Go stubs from proto/kv.proto (requires protoc + plugins)
proto-gen:
	protoc --go_out=. --go_opt=paths=source_relative \
	       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
	       proto/kv.proto

# Start a local cluster (placeholder — wired up in later phases)
cluster-up:
	@echo "cluster-up: not yet implemented"

# Stop a local cluster (placeholder — wired up in later phases)
cluster-down:
	@echo "cluster-down: not yet implemented"

# Remove build artifacts
clean:
	rm -rf bin/
