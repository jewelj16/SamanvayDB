// Package storage defines the storage Engine interface used by all higher
// layers (KV server, shard state machine, shard manager). No layer above this
// package may import a concrete engine type — only this interface.
//
// Architectural decision (supersedes the original Phase 2 LSM-tree plan):
// SamanvayDB's storage engine is an in-memory, mutex-guarded hash map
// (MemEngine). There is no LSM tree, WAL, MemTable, SSTable or compaction in
// the storage layer. Durability is provided one layer down the stack by Raft
// itself:
//
//   - every committed write lives in the fsynced Raft log
//     (internal/raft/persistence.go) before it is applied;
//   - the Raft log is bounded by periodic snapshots of the engine
//     (Engine.Snapshot), written atomically to disk;
//   - on restart a shard restores the latest snapshot and replays the Raft
//     log tail after it, reconstructing the map exactly.
//
// The Raft log therefore plays the role the LSM WAL would have played, and
// the snapshot plays the role of the flushed SSTables. The interface is kept
// narrow so a persistent engine could still be dropped in later.
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

	// Len returns the number of live keys.
	Len() int

	// ForEach calls fn for every key/value pair in ascending key order until
	// fn returns false. It is for internal use (expiry scans, debugging) —
	// it is deliberately NOT exposed through the client API (no range scans).
	// fn must not call back into the engine.
	ForEach(fn func(key, value []byte) bool)

	// Snapshot returns a deterministic, point-in-time serialization of the
	// whole engine (keys in sorted order, so identical states produce
	// byte-identical snapshots on every replica).
	Snapshot() ([]byte, error)

	// Restore atomically replaces the engine's entire contents with a
	// snapshot previously produced by Snapshot.
	Restore(snapshot []byte) error

	// Close releases any resources held by the engine. Must be called before
	// process exit.
	Close() error
}
