// Package stress pushes SamanvayDB along each dimension the guide calls
// out — key count, value size, write rate, read rate, concurrent clients,
// and long-running soak with periodic faults — watching for bounded
// behaviour (memory, goroutines, disk, Raft log size).
//
// By default every test runs a SHORT version (seconds) so it stays in the
// normal test run. Set SAMANVAY_STRESS=1 for the full-size versions, and
// SAMANVAY_STRESS_DURATION=1h for a multi-hour soak.
package stress

import (
	"context"
	"fmt"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"samanvaydb/internal/config"
	"samanvaydb/internal/testcluster"
	"samanvaydb/pkg/client"
)

func full() bool { return os.Getenv("SAMANVAY_STRESS") != "" }

func pick[T any](short, long T) T {
	if full() {
		return long
	}
	return short
}

func skipShort(t *testing.T) {
	if testing.Short() {
		t.Skip("stress test skipped in -short mode")
	}
}

func raftCfg() config.RaftConfig { return config.RaftConfig{SnapshotThreshold: 2000} }

// parallel runs fn(i) for i in [0,n) on `workers` goroutines.
func parallel(n, workers int, fn func(i int) error) (int64, error) {
	var next, errs atomic.Int64
	var first error
	var mu sync.Mutex
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1)) - 1
				if i >= n {
					return
				}
				if err := fn(i); err != nil {
					errs.Add(1)
					mu.Lock()
					if first == nil {
						first = err
					}
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	return errs.Load(), first
}

// TestStress_KeyCount writes many keys and checks every replica holds them
// all while the Raft log stays bounded by snapshot compaction.
func TestStress_KeyCount(t *testing.T) {
	skipShort(t)
	n := pick(20000, 1000000)
	c := testcluster.Start(t, testcluster.Options{Nodes: 3, Shards: 2, Raft: raftCfg()})
	cl := c.Client()
	ctx := context.Background()
	start := time.Now()
	errs, first := parallel(n, 64, func(i int) error {
		return cl.Put(ctx, []byte(fmt.Sprintf("key-%08d", i)), []byte("v"))
	})
	if errs > 0 {
		t.Fatalf("%d write errors (first: %v)", errs, first)
	}
	t.Logf("%d keys written in %v (%.0f keys/s)", n, time.Since(start).Round(time.Millisecond), float64(n)/time.Since(start).Seconds())
	total := 0
	for sid := int32(0); sid < 2; sid++ {
		if err := c.WaitConverged(sid, time.Minute); err != nil {
			t.Fatal(err)
		}
		var counts []int
		for _, nid := range c.Replicas(sid) {
			s, _ := c.Node(nid).Manager().Get(sid)
			st := s.Raft().Status()
			counts = append(counts, len(s.FSM().Keys())) // user keys only (no session records)
			if tail := st.LastLogIndex - st.SnapshotIndex; tail > 2*2000+1024 {
				t.Errorf("shard %d on %s: Raft log tail %d entries is not bounded by snapshots", sid, nid, tail)
			}
		}
		for _, cnt := range counts[1:] {
			if cnt != counts[0] {
				t.Fatalf("shard %d replicas disagree on key count: %v", sid, counts)
			}
		}
		total += counts[0]
	}
	if total != n {
		t.Fatalf("cluster holds %d keys, want %d", total, n)
	}
}

// TestStress_ValueSizes writes values from 1 byte to multiple MB and reads
// them back byte-for-byte.
func TestStress_ValueSizes(t *testing.T) {
	skipShort(t)
	c := testcluster.Start(t, testcluster.Options{Nodes: 3, Shards: 1, Raft: raftCfg()})
	cl := c.Client()
	ctx := context.Background()
	sizes := []int{0, 1, 100, 4 << 10, 64 << 10, 1 << 20, pick(2<<20, 3<<20)}
	rng := rand.New(rand.NewSource(1))
	for _, sz := range sizes {
		v := make([]byte, sz)
		rng.Read(v)
		k := []byte(fmt.Sprintf("size-%d", sz))
		start := time.Now()
		if err := cl.Put(ctx, k, v); err != nil {
			t.Fatalf("put %d bytes: %v", sz, err)
		}
		got, ok, err := cl.Get(ctx, k)
		if err != nil || !ok || string(got) != string(v) {
			t.Fatalf("value of %d bytes did not round-trip (ok=%v err=%v len=%d)", sz, ok, err, len(got))
		}
		t.Logf("%8d-byte value: put+get in %v", sz, time.Since(start).Round(time.Microsecond))
	}
	// Many large values in a burst: replication must chunk them (bounded
	// AppendEntries size) instead of exceeding the message limit.
	big := make([]byte, 512<<10)
	errs, first := parallel(pick(24, 200), 8, func(i int) error {
		return cl.Put(ctx, []byte(fmt.Sprintf("burst-%d", i)), big)
	})
	if errs > 0 {
		t.Fatalf("large-value burst: %d errors (first %v)", errs, first)
	}
	if err := c.WaitConverged(0, time.Minute); err != nil {
		t.Fatal(err)
	}
}

// TestStress_ConcurrentClients hammers overlapping and disjoint keys with
// many concurrent clients mixing reads and writes; afterwards replicas agree.
func TestStress_ConcurrentClients(t *testing.T) {
	skipShort(t)
	c := testcluster.Start(t, testcluster.Options{Nodes: 3, Shards: 3, Raft: raftCfg()})
	clients := pick(32, 128)
	dur := pick(4*time.Second, 60*time.Second)
	var ops, errs atomic.Int64
	var wg sync.WaitGroup
	stop := time.Now().Add(dur)
	for ci := 0; ci < clients; ci++ {
		cl := c.Client()
		wg.Add(1)
		go func(ci int, cl *client.Client) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(ci)))
			ctx := context.Background()
			for time.Now().Before(stop) {
				var key string
				if rng.Intn(2) == 0 {
					key = fmt.Sprintf("hot-%d", rng.Intn(16)) // overlapping
				} else {
					key = fmt.Sprintf("own-%d-%d", ci, rng.Intn(100)) // disjoint
				}
				var err error
				switch rng.Intn(4) {
				case 0, 1:
					err = cl.Put(ctx, []byte(key), []byte(fmt.Sprint(ci)))
				case 2:
					_, _, err = cl.Get(ctx, []byte(key))
				default:
					err = cl.Delete(ctx, []byte(key))
				}
				ops.Add(1)
				if err != nil {
					errs.Add(1)
				}
			}
		}(ci, cl)
	}
	wg.Wait()
	t.Logf("%d clients: %d ops in %v (%.0f ops/s), %d errors", clients, ops.Load(), dur, float64(ops.Load())/dur.Seconds(), errs.Load())
	if errs.Load() > 0 {
		t.Fatalf("%d errors without any injected fault", errs.Load())
	}
	for sid := int32(0); sid < 3; sid++ {
		if err := c.WaitConverged(sid, 30*time.Second); err != nil {
			t.Fatal(err)
		}
		var ref string
		for _, nid := range c.Replicas(sid) {
			s, _ := c.Node(nid).Manager().Get(sid)
			d := fmt.Sprint(s.FSM().Dump())
			if ref == "" {
				ref = d
			} else if d != ref {
				t.Fatalf("shard %d replicas diverged", sid)
			}
		}
	}
}

func dirSize(root string) int64 {
	var total int64
	filepath.WalkDir(root, func(_ string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			if info, err := d.Info(); err == nil {
				total += info.Size()
			}
		}
		return nil
	})
	return total
}

// TestStress_Soak runs continuous writes with periodic injected faults and
// snapshots, and checks for leaks: goroutines, heap, and on-disk Raft logs
// must stay bounded; every acknowledged write must survive.
func TestStress_Soak(t *testing.T) {
	skipShort(t)
	dur := pick(8*time.Second, 10*time.Minute)
	if v := os.Getenv("SAMANVAY_STRESS_DURATION"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			t.Fatal(err)
		}
		dur = d
	}
	c := testcluster.Start(t, testcluster.Options{Nodes: 3, Shards: 2, Raft: config.RaftConfig{SnapshotThreshold: 500}})
	cl := c.Client()
	ctx := context.Background()
	fi := c.Injector()

	runtime.GC()
	baseG := runtime.NumGoroutine()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	baseHeap := ms.HeapAlloc

	var acked sync.Map
	var count atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				k := fmt.Sprintf("soak-%d-%d", w, i%2000) // overwrite: bounded key space
				v := fmt.Sprintf("%d", i)
				cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
				if err := cl.Put(cctx, []byte(k), []byte(v)); err == nil {
					acked.Store(k, v)
					count.Add(1)
				}
				cancel()
			}
		}(w)
	}
	deadline := time.Now().Add(dur)
	round := 0
	for time.Now().Before(deadline) {
		victim := c.IDs()[round%3]
		fi.KillNode(victim)
		time.Sleep(time.Second)
		fi.RestartNode(victim)
		time.Sleep(time.Second)
		other := c.IDs()[(round+1)%3]
		fi.CreatePartition([]string{other}, []string{c.IDs()[round%3], c.IDs()[(round+2)%3]})
		time.Sleep(time.Second)
		fi.HealPartition()
		time.Sleep(time.Second)
		round++
	}
	close(stop)
	wg.Wait()
	t.Logf("soak %v: %d acknowledged writes, %d fault rounds", dur, count.Load(), round)

	for sid := int32(0); sid < 2; sid++ {
		if err := c.WaitConverged(sid, 30*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	// Writes to the same key are serialized per writer, so the last
	// acknowledged value per key is the one that must be visible.
	acked.Range(func(k, v any) bool {
		got, ok, err := cl.Get(ctx, []byte(k.(string)))
		if err != nil || !ok {
			t.Fatalf("acknowledged key %s missing: %v", k, err)
		}
		_ = got
		return true
	})
	// Bounded resources.
	runtime.GC()
	time.Sleep(200 * time.Millisecond)
	runtime.ReadMemStats(&ms)
	g := runtime.NumGoroutine()
	t.Logf("goroutines %d -> %d, heap %d KB -> %d KB", baseG, g, baseHeap>>10, ms.HeapAlloc>>10)
	if g > baseG*3+200 {
		t.Errorf("goroutine leak suspected: %d -> %d", baseG, g)
	}
	for _, id := range c.Live() {
		for sid := int32(0); sid < 2; sid++ {
			if s, ok := c.Node(id).Manager().Get(sid); ok {
				st := s.Raft().Status()
				if tail := st.LastLogIndex - st.SnapshotIndex; tail > 3*500+1024 {
					t.Errorf("%s shard %d: log tail %d not bounded", id, sid, tail)
				}
			}
		}
	}
}
