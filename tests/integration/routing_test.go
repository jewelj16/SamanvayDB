package integration

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"samanvaydb/internal/testcluster"
	"samanvaydb/pkg/client"
)

func ctx(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	c, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return c
}

// TestSmoke_CRUD: PUT -> replicate -> commit -> GET, overwrite, delete.
func TestSmoke_CRUD(t *testing.T) {
	c := testcluster.Start(t, testcluster.Options{Nodes: 3, Shards: 2})
	cl := c.Client()
	cx := ctx(t, 20*time.Second)

	if err := cl.Put(cx, []byte("k"), []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if v, ok, err := cl.Get(cx, []byte("k")); err != nil || !ok || string(v) != "v1" {
		t.Fatalf("get = %q %v %v", v, ok, err)
	}
	if err := cl.Put(cx, []byte("k"), []byte("v2")); err != nil { // last write wins
		t.Fatal(err)
	}
	if v, _, _ := cl.Get(cx, []byte("k")); string(v) != "v2" {
		t.Fatalf("after overwrite: %q", v)
	}
	if err := cl.Delete(cx, []byte("k")); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := cl.Get(cx, []byte("k")); err != nil || ok {
		t.Fatalf("after delete: found=%v err=%v", ok, err)
	}
	// Empty key and reserved prefix: INVALID_REQUEST, never retried.
	if err := cl.Put(cx, nil, []byte("x")); !errors.Is(err, client.ErrInvalidRequest) {
		t.Fatalf("empty key: %v", err)
	}
	if err := cl.Put(cx, []byte("__lock__:x"), []byte("x")); !errors.Is(err, client.ErrInvalidRequest) {
		t.Fatalf("reserved key: %v", err)
	}
	// Follower read eventually observes the write.
	cl.Put(cx, []byte("f"), []byte("1"))
	deadline := time.Now().Add(3 * time.Second)
	for {
		if v, ok, err := cl.Get(cx, []byte("f"), client.WithFollowerOK()); err == nil && ok && string(v) == "1" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("follower read never observed the write")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestPhase5_DeterministicMultiShardRouting — Phase 5 checkpoint: PUTs to
// different keys route to different shards deterministically, and a GET for
// a key always resolves to the shard it was written to.
func TestPhase5_DeterministicMultiShardRouting(t *testing.T) {
	c := testcluster.Start(t, testcluster.Options{Nodes: 4, Shards: 2, RF: 2})
	cl := c.Client()
	cx := ctx(t, 30*time.Second)
	perShard := map[int32]int{}
	for i := 0; i < 200; i++ {
		k := []byte(fmt.Sprintf("user:%d", i))
		if err := cl.Put(cx, k, []byte(fmt.Sprint(i))); err != nil {
			t.Fatal(err)
		}
		perShard[cl.ShardFor(k)]++
	}
	if len(perShard) != 2 || perShard[0] < 50 || perShard[1] < 50 {
		t.Fatalf("keys not spread across shards: %v", perShard)
	}
	// Each key lives ONLY in its own shard's replicas.
	for _, sid := range []int32{0, 1} {
		if err := c.WaitConverged(sid, 5*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 200; i++ {
		k := []byte(fmt.Sprintf("user:%d", i))
		sid := cl.ShardFor(k)
		for _, nid := range c.Live() {
			for _, other := range []int32{0, 1} {
				s, ok := c.Node(nid).Manager().Get(other)
				if !ok {
					continue
				}
				_, has := s.FSM().Get(k)
				if has != (other == sid) {
					t.Fatalf("key %s (shard %d) present=%v in shard %d on %s", k, sid, has, other, nid)
				}
			}
		}
		v, ok, err := cl.Get(cx, k)
		if err != nil || !ok || string(v) != fmt.Sprint(i) {
			t.Fatalf("get %s: %q %v %v", k, v, ok, err)
		}
	}
}

// TestRedirect_FollowerRedirectsAndHintIsCached: a request sent to a node
// that is not the leader is redirected; the client caches the leader hint.
func TestRedirect_FollowerRedirectsAndHintIsCached(t *testing.T) {
	c := testcluster.Start(t, testcluster.Options{Nodes: 3, Shards: 1})
	cl := c.Client()
	cx := ctx(t, 20*time.Second)
	if err := cl.Put(cx, []byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	leader, _, _ := c.ShardLeader(0)
	if hint := cl.LeaderHint(0); hint != leader {
		t.Fatalf("cached hint %q, leader %q", hint, leader)
	}
}
