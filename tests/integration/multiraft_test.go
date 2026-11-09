package integration

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"samanvaydb/internal/testcluster"
	"samanvaydb/pkg/client"
)

// keyForShard finds a key that maps to shard sid.
func keyForShard(cl *client.Client, sid int32, prefix string, i int) []byte {
	for j := 0; ; j++ {
		k := []byte(fmt.Sprintf("%s-%d-%d", prefix, i, j))
		if cl.ShardFor(k) == sid {
			return k
		}
	}
}

// TestPhase6_NineNodeThreeShardFailureIsolation — Phase 6 checkpoint: a
// 9-node cluster with 3 shards at RF=3 (the guide's demo topology). Leaders
// sit on different nodes. Killing one shard's leader triggers an election
// in that shard ONLY; the other two shards keep serving writes with zero
// interruption throughout.
func TestPhase6_NineNodeThreeShardFailureIsolation(t *testing.T) {
	c := testcluster.Start(t, testcluster.Options{Nodes: 9, Shards: 3, RF: 3})
	cl := c.Client()

	leaders := map[int32]string{}
	terms := map[int32]uint64{}
	seen := map[string]bool{}
	for sid := int32(0); sid < 3; sid++ {
		l, term, _ := c.ShardLeader(sid)
		leaders[sid], terms[sid] = l, term
		if seen[l] {
			t.Fatalf("two shards led by %s; leaders should be spread: %v", l, leaders)
		}
		seen[l] = true
		if reps := c.Replicas(sid); len(reps) != 3 {
			t.Fatalf("shard %d has replicas %v", sid, reps)
		}
	}
	t.Logf("leaders: %v", leaders)

	// Continuous writers on shards 1 and 2.
	stop := make(chan struct{})
	var failures, successes atomic.Int64
	var wg sync.WaitGroup
	for _, sid := range []int32{1, 2} {
		wg.Add(1)
		go func(sid int32) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				err := cl.Put(ctx, keyForShard(cl, sid, "iso", i), []byte("v"))
				cancel()
				if err != nil {
					failures.Add(1)
					t.Errorf("shard %d write failed during shard-0 failover: %v", sid, err)
				} else {
					successes.Add(1)
				}
			}
		}(sid)
	}
	time.Sleep(300 * time.Millisecond)
	victim := leaders[0]
	killedAt := time.Now()
	c.Kill(victim)
	newLeader, err := c.WaitLeader(0, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("shard 0 re-elected %s in %v", newLeader, time.Since(killedAt))
	time.Sleep(500 * time.Millisecond)
	close(stop)
	wg.Wait()

	if failures.Load() != 0 || successes.Load() < 20 {
		t.Fatalf("other shards: %d failures, %d successes", failures.Load(), successes.Load())
	}
	// Only shard 0 ran an election.
	for _, sid := range []int32{1, 2} {
		l, term, _ := c.ShardLeader(sid)
		if l != leaders[sid] || term != terms[sid] {
			t.Fatalf("shard %d disturbed: leader %s->%s term %d->%d", sid, leaders[sid], l, terms[sid], term)
		}
	}
	if _, term, _ := c.ShardLeader(0); term <= terms[0] {
		t.Fatalf("shard 0 term did not advance (%d)", term)
	}
	// Shard 0 serves writes again.
	cx := ctx(t, 10*time.Second)
	if err := cl.Put(cx, keyForShard(cl, 0, "after", 0), []byte("x")); err != nil {
		t.Fatal(err)
	}
}

// TestLeaderKillMidStream_ClusterLevel — Phase 4 checkpoint at cluster
// level: kill the leader of a 3-node shard while a client writes; every
// acknowledged write survives, writes resume, and the killed node rejoins
// with an identical state.
func TestLeaderKillMidStream_ClusterLevel(t *testing.T) {
	c := testcluster.Start(t, testcluster.Options{Nodes: 3, Shards: 1, Sync: true})
	cl := c.Client()
	acked := map[string]string{}
	var mu sync.Mutex
	write := func(from, to int) {
		for i := from; i < to; i++ {
			k, v := fmt.Sprintf("k%04d", i), fmt.Sprintf("v%d", i)
			cx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			err := cl.Put(cx, []byte(k), []byte(v))
			cancel()
			if err != nil {
				t.Errorf("put %s: %v", k, err)
				continue
			}
			mu.Lock()
			acked[k] = v
			mu.Unlock()
		}
	}
	write(0, 100)
	leader, _, _ := c.ShardLeader(0)
	done := make(chan struct{})
	go func() { write(100, 300); close(done) }()
	time.Sleep(100 * time.Millisecond)
	c.Kill(leader)
	<-done
	if _, err := c.WaitLeader(0, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	write(300, 350)
	cx := ctx(t, 30*time.Second)
	for k, v := range acked {
		got, ok, err := cl.Get(cx, []byte(k))
		if err != nil || !ok || string(got) != v {
			t.Fatalf("acknowledged write %s lost: %q %v %v", k, got, ok, err)
		}
	}
	if err := c.Restart(leader); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitConverged(0, 15*time.Second); err != nil {
		t.Fatal(err)
	}
	ref, _ := c.Node(c.Live()[0]).Manager().Get(0)
	want := ref.FSM().Dump()
	s, _ := c.Node(leader).Manager().Get(0)
	got := s.FSM().Dump()
	if len(got) != len(want) || len(want) != len(acked) {
		t.Fatalf("restarted node has %d keys, peer %d, acked %d", len(got), len(want), len(acked))
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("restarted node diverges at %s", k)
		}
	}
}
