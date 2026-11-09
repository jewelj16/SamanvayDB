package memkv

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
)

// TestStore_PutThenGet verifies PUT then GET returns the value just written.
func TestStore_PutThenGet(t *testing.T) {
	s := New()

	if err := s.Put([]byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("Put() error: %v", err)
	}

	val, found, err := s.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Get() error: %v", err)
	}
	if !found {
		t.Fatal("Get() found=false, want true")
	}
	if !bytes.Equal(val, []byte("value1")) {
		t.Errorf("Get() = %q, want %q", val, "value1")
	}
}

// TestStore_GetMissingKey verifies GET on a missing key returns not found, not an error.
func TestStore_GetMissingKey(t *testing.T) {
	s := New()

	val, found, err := s.Get([]byte("nonexistent"))
	if err != nil {
		t.Fatalf("Get() on missing key should not error, got: %v", err)
	}
	if found {
		t.Error("Get() found=true for missing key, want false")
	}
	if val != nil {
		t.Errorf("Get() value=%q for missing key, want nil", val)
	}
}

// TestStore_DeleteThenGet verifies DELETE then GET returns not found.
func TestStore_DeleteThenGet(t *testing.T) {
	s := New()

	s.Put([]byte("key"), []byte("value"))

	if err := s.Delete([]byte("key")); err != nil {
		t.Fatalf("Delete() error: %v", err)
	}

	_, found, err := s.Get([]byte("key"))
	if err != nil {
		t.Fatalf("Get() after Delete() error: %v", err)
	}
	if found {
		t.Error("Get() found=true after Delete(), want false")
	}
}

// TestStore_PutOverwrites verifies PUT overwrites an existing key's value.
func TestStore_PutOverwrites(t *testing.T) {
	s := New()

	s.Put([]byte("key"), []byte("original"))
	s.Put([]byte("key"), []byte("updated"))

	val, found, err := s.Get([]byte("key"))
	if err != nil {
		t.Fatalf("Get() error: %v", err)
	}
	if !found {
		t.Fatal("Get() found=false after overwrite")
	}
	if !bytes.Equal(val, []byte("updated")) {
		t.Errorf("Get() = %q, want %q", val, "updated")
	}
}

// TestStore_DeleteNonExistent verifies deleting a missing key is a no-op.
func TestStore_DeleteNonExistent(t *testing.T) {
	s := New()
	if err := s.Delete([]byte("nope")); err != nil {
		t.Errorf("Delete() on missing key should not error, got: %v", err)
	}
}

// TestStore_EmptyKey verifies empty keys are rejected by all operations.
func TestStore_EmptyKey(t *testing.T) {
	s := New()

	if err := s.Put([]byte{}, []byte("v")); err == nil {
		t.Error("Put() with empty key should error")
	}
	if err := s.Put(nil, []byte("v")); err == nil {
		t.Error("Put() with nil key should error")
	}

	_, _, err := s.Get([]byte{})
	if err == nil {
		t.Error("Get() with empty key should error")
	}

	if err := s.Delete([]byte{}); err == nil {
		t.Error("Delete() with empty key should error")
	}
}

// TestStore_EmptyValue verifies empty values (zero-length) are legal.
func TestStore_EmptyValue(t *testing.T) {
	s := New()

	if err := s.Put([]byte("key"), []byte{}); err != nil {
		t.Fatalf("Put() with empty value should succeed, got: %v", err)
	}

	val, found, err := s.Get([]byte("key"))
	if err != nil {
		t.Fatalf("Get() error: %v", err)
	}
	if !found {
		t.Fatal("Get() found=false for empty-value key")
	}
	if len(val) != 0 {
		t.Errorf("Get() = %q, want empty slice", val)
	}
}

// TestStore_BinaryKeysAndValues verifies opaque binary data round-trips correctly.
func TestStore_BinaryKeysAndValues(t *testing.T) {
	s := New()

	key := []byte{0x00, 0xFF, 0xAB, 0xCD}
	value := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0x00}

	if err := s.Put(key, value); err != nil {
		t.Fatalf("Put() binary key error: %v", err)
	}

	got, found, err := s.Get(key)
	if err != nil || !found {
		t.Fatalf("Get() binary key: found=%v, err=%v", found, err)
	}
	if !bytes.Equal(got, value) {
		t.Errorf("Get() = %x, want %x", got, value)
	}
}

// TestStore_ValueIsolation verifies that mutating the returned value doesn't
// corrupt the store's internal state.
func TestStore_ValueIsolation(t *testing.T) {
	s := New()
	s.Put([]byte("key"), []byte("original"))

	val, _, _ := s.Get([]byte("key"))
	val[0] = 'X' // mutate the returned slice

	val2, _, _ := s.Get([]byte("key"))
	if !bytes.Equal(val2, []byte("original")) {
		t.Errorf("store state corrupted by caller mutation: got %q", val2)
	}
}

// TestStore_ConcurrentPutsDifferentKeys verifies concurrent PUTs to different
// keys don't corrupt the map. Run with -race.
func TestStore_ConcurrentPutsDifferentKeys(t *testing.T) {
	s := New()
	const goroutines = 100
	const opsPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				key := []byte(fmt.Sprintf("goroutine-%d-key-%d", id, j))
				val := []byte(fmt.Sprintf("value-%d-%d", id, j))
				if err := s.Put(key, val); err != nil {
					t.Errorf("Put() error: %v", err)
				}
			}
		}(i)
	}
	wg.Wait()

	expectedLen := goroutines * opsPerGoroutine
	if s.Len() != expectedLen {
		t.Errorf("Len() = %d, want %d", s.Len(), expectedLen)
	}
}

// TestStore_ConcurrentMixed verifies concurrent PUT/GET/DELETE on overlapping
// and disjoint keys don't cause races or corruption. Run with -race.
func TestStore_ConcurrentMixed(t *testing.T) {
	s := New()

	// Pre-populate some shared keys.
	for i := 0; i < 10; i++ {
		s.Put([]byte(fmt.Sprintf("shared-%d", i)), []byte("initial"))
	}

	var wg sync.WaitGroup
	const workers = 50

	// Writers: overwrite shared keys and write unique keys.
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				sharedKey := []byte(fmt.Sprintf("shared-%d", j%10))
				s.Put(sharedKey, []byte(fmt.Sprintf("worker-%d-val-%d", id, j)))

				uniqueKey := []byte(fmt.Sprintf("unique-%d-%d", id, j))
				s.Put(uniqueKey, []byte("v"))
			}
		}(i)
	}

	// Readers: concurrently read shared keys.
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				key := []byte(fmt.Sprintf("shared-%d", j%10))
				s.Get(key) // just verify no race; value may be stale
			}
		}(i)
	}

	// Deleters: delete unique keys (may or may not exist — no-op is fine).
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				key := []byte(fmt.Sprintf("unique-%d-%d", id, j))
				s.Delete(key)
			}
		}(i)
	}

	wg.Wait()
}

// TestStore_SnapshotRestoreRoundTrip verifies Restore(Snapshot()) reproduces
// the exact state, including binary keys/values and empty values.
func TestStore_SnapshotRestoreRoundTrip(t *testing.T) {
	s := New()
	s.Put([]byte("b"), []byte("2"))
	s.Put([]byte("a"), []byte("1"))
	s.Put([]byte{0x00, 0xff}, []byte{0xde, 0xad})
	s.Put([]byte("empty"), []byte{})

	snap := s.Snapshot()
	r := New()
	r.Put([]byte("stale"), []byte("x"))
	if err := r.Restore(snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if r.Len() != 4 {
		t.Fatalf("Len = %d, want 4", r.Len())
	}
	if _, found, _ := r.Get([]byte("stale")); found {
		t.Error("Restore must replace, not merge")
	}
	if v, _, _ := r.Get([]byte{0x00, 0xff}); !bytes.Equal(v, []byte{0xde, 0xad}) {
		t.Errorf("binary value = %x", v)
	}
	if !bytes.Equal(r.Snapshot(), snap) {
		t.Error("snapshots of identical states must be byte-identical")
	}
}

// TestStore_SnapshotDeterministic verifies insertion order does not affect
// the snapshot bytes (no map-iteration nondeterminism).
func TestStore_SnapshotDeterministic(t *testing.T) {
	a, b := New(), New()
	for i := 0; i < 100; i++ {
		a.Put([]byte(fmt.Sprintf("k%03d", i)), []byte("v"))
		b.Put([]byte(fmt.Sprintf("k%03d", 99-i)), []byte("v"))
	}
	if !bytes.Equal(a.Snapshot(), b.Snapshot()) {
		t.Error("snapshot depends on insertion order")
	}
}

// TestStore_RestoreRejectsCorruption verifies a corrupted snapshot is
// rejected and leaves the store untouched.
func TestStore_RestoreRejectsCorruption(t *testing.T) {
	s := New()
	s.Put([]byte("k"), []byte("v"))
	snap := s.Snapshot()
	snap[14] ^= 0xff
	r := New()
	r.Put([]byte("keep"), []byte("me"))
	if err := r.Restore(snap); err == nil {
		t.Fatal("expected checksum error")
	}
	if _, found, _ := r.Get([]byte("keep")); !found {
		t.Error("failed Restore must not modify the store")
	}
}

// TestStore_ForEachSorted verifies ascending key iteration and early stop.
func TestStore_ForEachSorted(t *testing.T) {
	s := New()
	for _, k := range []string{"c", "a", "b"} {
		s.Put([]byte(k), []byte(k))
	}
	var got []string
	s.ForEach(func(k, _ []byte) bool { got = append(got, string(k)); return len(got) < 2 })
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Errorf("ForEach = %v, want [a b]", got)
	}
}
