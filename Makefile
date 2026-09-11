.PHONY: build test test-race cluster-up cluster-down clean

# Build the server binary
build:
	go build -o bin/samanvaydb-server ./cmd/samanvaydb-server

# Run all tests
test:
	go test ./...

# Run all tests with the race detector — non-negotiable for concurrent code
test-race:
	go test -race ./...

# Start a local cluster (placeholder — wired up in later phases)
cluster-up:
	@echo "cluster-up: not yet implemented"

# Stop a local cluster (placeholder — wired up in later phases)
cluster-down:
	@echo "cluster-down: not yet implemented"

# Remove build artifacts
clean:
	rm -rf bin/
