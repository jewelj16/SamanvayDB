package storage

import "samanvaydb/internal/storage/memkv"

// MemEngine wraps memkv.Store (a mutex-guarded map) and satisfies Engine.
// It is the storage engine for every shard replica.
type MemEngine struct {
	store *memkv.Store
}

// NewMemEngine creates an Engine backed by an in-memory map.
func NewMemEngine() Engine {
	return &MemEngine{store: memkv.New()}
}

func (e *MemEngine) Put(key, value []byte) error             { return e.store.Put(key, value) }
func (e *MemEngine) Get(key []byte) ([]byte, bool, error)    { return e.store.Get(key) }
func (e *MemEngine) Delete(key []byte) error                 { return e.store.Delete(key) }
func (e *MemEngine) Len() int                                { return e.store.Len() }
func (e *MemEngine) ForEach(fn func(key, value []byte) bool) { e.store.ForEach(fn) }
func (e *MemEngine) Snapshot() ([]byte, error)               { return e.store.Snapshot(), nil }
func (e *MemEngine) Restore(snapshot []byte) error           { return e.store.Restore(snapshot) }
func (e *MemEngine) Close() error                            { return nil } // nothing to release
