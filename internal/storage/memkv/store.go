// Package memkv provides a thread-safe, in-memory key-value store for Phase 1
// of SamanvayDB. This is a temporary package that will be superseded by the
// real LSM-tree engine in Phase 2. It exists to nail the API contract
// (PUT/GET/DELETE semantics) before storage or consensus complexity is added.
//
// Surface is intentionally minimal — exactly PUT/GET/DELETE, no range scans,
// no prefix queries, no batched multi-key ops. Every extra operation would need
// to be threaded through Raft's command type and the sharding router later.
//
// Design decision (consistent with internal/command): empty keys are illegal.
package memkv

import (
	"fmt"
	"sync"
)

// Store is a mutex-guarded in-memory key-value store.
// All methods are safe for concurrent use.
type Store struct {
	mu   sync.RWMutex
	data map[string][]byte
}

// New creates and returns an empty Store.
func New() *Store {
	return &Store{
		data: make(map[string][]byte),
	}
}

// Put inserts or overwrites the value for key.
// Returns an error if key is empty (consistent with Command.Validate).
func (s *Store) Put(key, value []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("memkv: empty key is not allowed")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[string(key)] = cloneBytes(value)
	return nil
}

// Get returns the value associated with key and whether the key was found.
// A missing key is NOT an error — callers must check the found bool.
// Returns an error only if the key is empty.
func (s *Store) Get(key []byte) (value []byte, found bool, err error) {
	if len(key) == 0 {
		return nil, false, fmt.Errorf("memkv: empty key is not allowed")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[string(key)]
	if !ok {
		return nil, false, nil
	}
	return cloneBytes(v), true, nil
}

// Delete removes key from the store.
// Deleting a non-existent key is a no-op (not an error).
// Returns an error if key is empty.
func (s *Store) Delete(key []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("memkv: empty key is not allowed")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, string(key))
	return nil
}

// Len returns the number of keys currently in the store.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

// cloneBytes returns a copy of b so that callers can't mutate internal state.
func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
