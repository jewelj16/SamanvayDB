// Package memkv provides the thread-safe, in-memory key-value map that backs
// SamanvayDB's storage engine (storage.MemEngine). The project deliberately
// uses a map instead of an LSM tree; durability comes from the Raft log and
// Raft snapshots (see internal/storage/engine.go).
//
// Surface is intentionally minimal — exactly PUT/GET/DELETE, no range scans,
// no prefix queries, no batched multi-key ops. Every extra operation would need
// to be threaded through Raft's command type and the sharding router later.
//
// Design decision (consistent with internal/command): empty keys are illegal.
package memkv

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"sort"
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

// ForEach calls fn for every pair in ascending key order until fn returns
// false. It iterates over a consistent copy taken under the read lock, so fn
// may run arbitrarily long without blocking writers.
func (s *Store) ForEach(fn func(key, value []byte) bool) {
	s.mu.RLock()
	keys := s.sortedKeysLocked()
	vals := make([][]byte, len(keys))
	for i, k := range keys {
		vals[i] = s.data[k]
	}
	s.mu.RUnlock()
	for i, k := range keys {
		if !fn([]byte(k), cloneBytes(vals[i])) {
			return
		}
	}
}

func (s *Store) sortedKeysLocked() []string {
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	sort.Strings(keys) // deterministic order — never rely on map iteration order
	return keys
}

// Snapshot format (big-endian):
//
//	magic(4) count(8) { keyLen(4) key valLen(4) val }* crc32(4)
//
// Keys are written in sorted order so that identical states always produce
// byte-identical snapshots on every replica.
const snapshotMagic = 0x534b5631 // "SKV1"

// Snapshot serializes the whole store.
func (s *Store) Snapshot() []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := s.sortedKeysLocked()
	size := 4 + 8 + 4
	for _, k := range keys {
		size += 8 + len(k) + len(s.data[k])
	}
	buf := make([]byte, 0, size)
	buf = binary.BigEndian.AppendUint32(buf, snapshotMagic)
	buf = binary.BigEndian.AppendUint64(buf, uint64(len(keys)))
	for _, k := range keys {
		v := s.data[k]
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(k)))
		buf = append(buf, k...)
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(v)))
		buf = append(buf, v...)
	}
	return binary.BigEndian.AppendUint32(buf, crc32.ChecksumIEEE(buf))
}

// Restore atomically replaces the store's contents with a snapshot. On any
// decoding error the existing contents are left untouched.
func (s *Store) Restore(snap []byte) error {
	if len(snap) < 16 {
		return fmt.Errorf("memkv: snapshot too short (%d bytes)", len(snap))
	}
	body, sum := snap[:len(snap)-4], binary.BigEndian.Uint32(snap[len(snap)-4:])
	if crc32.ChecksumIEEE(body) != sum {
		return fmt.Errorf("memkv: snapshot checksum mismatch")
	}
	if binary.BigEndian.Uint32(body) != snapshotMagic {
		return fmt.Errorf("memkv: bad snapshot magic")
	}
	count := binary.BigEndian.Uint64(body[4:])
	off := uint64(12)
	n := uint64(len(body))
	data := make(map[string][]byte, count)
	for i := uint64(0); i < count; i++ {
		if off+4 > n {
			return fmt.Errorf("memkv: truncated snapshot")
		}
		kl := uint64(binary.BigEndian.Uint32(body[off:]))
		off += 4
		if off+kl+4 > n {
			return fmt.Errorf("memkv: truncated snapshot")
		}
		k := string(body[off : off+kl])
		off += kl
		vl := uint64(binary.BigEndian.Uint32(body[off:]))
		off += 4
		if off+vl > n {
			return fmt.Errorf("memkv: truncated snapshot")
		}
		data[k] = cloneBytes(body[off : off+vl])
		off += vl
	}
	if off != n {
		return fmt.Errorf("memkv: trailing bytes in snapshot")
	}
	s.mu.Lock()
	s.data = data
	s.mu.Unlock()
	return nil
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
