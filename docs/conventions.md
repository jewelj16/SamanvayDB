# SamanvayDB — Error-Handling and Testing Conventions

## Error Handling

### Errors as Values
Use Go's standard `error` return for anything that can occur during **normal
operation**:
- Disk full / I/O failure
- Peer unreachable / RPC timeout
- Stale Raft term
- Configuration parse failure
- Key not found (not an error — return `(nil, false)`, never an error value)

Always wrap errors with context using `fmt.Errorf("component: action: %w", err)`
so that callers can inspect the chain with `errors.Is` / `errors.As`.

### Panics / Asserts
Reserved **exclusively** for invariant violations that indicate a bug in the
code itself — conditions that should be structurally impossible if the program
is correct:
- Commit index moving backwards
- A leader receiving an AppendEntries from its own term
- An SSTable file referenced by the manifest not existing on disk (after
  recovery has completed)
- Negative sequence numbers

If you find yourself writing `panic()`, ask: "Could a well-behaved but
unlucky deployment hit this?" If yes, it must be an error return, not a panic.

### Error Wrapping Pattern
```go
// Good: wrapped with context, caller can unwrap
if err := wal.Append(record); err != nil {
    return fmt.Errorf("storage: WAL append for key %x: %w", record.Key, err)
}

// Bad: context lost
if err := wal.Append(record); err != nil {
    return err
}

// Bad: panic on a recoverable condition
if err := wal.Append(record); err != nil {
    panic(err)
}
```

## Testing Conventions

### Table-Driven Tests
All unit tests should use Go's table-driven test pattern. This keeps test
cases declarative, easy to extend, and produces clear output on failure.

See `tests/unit/example_table_test.go` for a canonical template.

### Test Naming
- Test functions: `TestComponentName_Behavior` (e.g. `TestMemTable_FreezeSwap`)
- Subtests: descriptive lowercase with underscores (e.g. `missing_node_id`)

### Race Detector
Every commit that touches concurrent code **must** pass `go test -race ./...`
(the `make test-race` target). This is non-negotiable.

### Test Organization
- **Unit tests**: live alongside the code they test (`*_test.go` in the same
  package), or under `tests/unit/` for cross-package tests.
- **Integration tests**: `tests/integration/` — multi-node, real-process
  scenarios.
- **Failure tests**: `tests/failure/` — fault-injection scenarios (Phase 9+).

### Coverage
Aim for high coverage on correctness-critical paths (WAL, MemTable, Raft state
transitions). Don't chase 100% coverage on trivial getters/setters.
