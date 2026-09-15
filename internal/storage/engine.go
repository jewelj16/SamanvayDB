// Package storage defines the StorageEngine interface used by all higher layers
// (KV server, Raft state machine, shard manager). No layer above this package
// may import a concrete engine type — only this interface.
//
// Current implementation: MemEngine (map-backed, in-memory).
// Future drop-in: LSMEngine (WAL + MemTable + SSTable) — same interface,
// one constructor call change, zero impact on callers.
package storage

// Engine is the storage abstraction for SamanvayDB.
// All implementations must be safe for concurrent use.
type Engine interface {
	// Put inserts or overwrites the value for key.
	// Empty keys must be rejected with an error.
	Put(key, value []byte) error

	// Get returns the value for key and whether it was found.
	// A missing key is NOT an error — callers check the found bool.
	// Empty keys must be rejected with an error.
	Get(key []byte) (value []byte, found bool, err error)

	// Delete removes key from the store. Deleting a missing key is a no-op.
	// Empty keys must be rejected with an error.
	Delete(key []byte) error

	// Close releases any resources held by the engine (file handles, background
	// goroutines, etc.). Must be called before process exit.
	Close() error
}
