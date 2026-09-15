package storage

import "samanvaydb/internal/storage/memkv"

// MemEngine wraps memkv.Store and satisfies the Engine interface.
// This is the current production implementation — replace with LSMEngine
// by changing NewMemEngine() to NewLSMEngine() at the call site.
type MemEngine struct {
	store *memkv.Store
}

// NewMemEngine creates an Engine backed by an in-memory map.
func NewMemEngine() Engine {
	return &MemEngine{store: memkv.New()}
}

func (e *MemEngine) Put(key, value []byte) error    { return e.store.Put(key, value) }
func (e *MemEngine) Get(key []byte) ([]byte, bool, error) { return e.store.Get(key) }
func (e *MemEngine) Delete(key []byte) error        { return e.store.Delete(key) }
func (e *MemEngine) Close() error                   { return nil } // no-op for in-memory
