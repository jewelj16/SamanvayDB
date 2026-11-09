package integration

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"samanvaydb/internal/config"
	"samanvaydb/internal/faultinjector"
	"samanvaydb/internal/linearizability"
	"samanvaydb/internal/testcluster"
	"samanvaydb/pkg/client"
)

// testRaftWithSmallSnapshots makes shards snapshot often, so tests also
// exercise snapshot install and log compaction.
func testRaftWithSmallSnapshots() config.RaftConfig {
	return config.RaftConfig{SnapshotThreshold: 50}
}

// TestCorrectness_LinearizableUnderFaults records a history of concurrent
// PUT/GET/DELETE operations from several clients while leaders are killed
// and partitioned, then checks it with a linearizability checker.
//
// Consistency labelling (honest): writes are linearizable (Raft log order),
// and GETs at the default LEADER consistency are linearizable because the
// leader confirms its leadership with a ReadIndex round before reading.
// FOLLOWER_OK reads are only sequentially consistent per replica (possibly
// stale) and are therefore NOT part of this history.
func TestCorrectness_LinearizableUnderFaults(t *testing.T) {
	c := testcluster.Start(t, testcluster.Options{Nodes: 5, Shards: 2, RF: 3})
	sc := faultinjector.NewScenario(7)
	t.Logf("scenario seed %d", sc.Seed)

	start := time.Now()
	now := func() int64 { return int64(time.Since(start)) }
	keys := []string{"x", "y", "z", "w"}
	var mu sync.Mutex
	var history []linearizability.Op
	stop := make(chan struct{})
	var wg sync.WaitGroup
	const clients = 5
	for ci := 0; ci < clients; ci++ {
		cl := c.Client()
		wg.Add(1)
		go func(ci int, cl *client.Client) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(ci) + 100))
			for seq := 0; ; seq++ {
				select {
				case <-stop:
					return
				default:
				}
				key := keys[rng.Intn(len(keys))]
				op := linearizability.Op{Client: ci, Key: key, Call: now()}
				cctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				var err error
				switch r := rng.Intn(10); {
				case r < 4:
					op.Kind, op.Value = linearizability.Put, fmt.Sprintf("c%d-%d", ci, seq)
					err = cl.Put(cctx, []byte(key), []byte(op.Value))
				case r < 5:
					op.Kind = linearizability.Delete
					err = cl.Delete(cctx, []byte(key))
				default:
					op.Kind = linearizability.Get
					var v []byte
					v, op.Found, err = cl.Get(cctx, []byte(key))
					op.Value = string(v)
				}
				cancel()
				op.Return = now()
				op.Note = "@" + cl.LeaderHint(cl.ShardFor([]byte(key)))
				if err != nil {
					if errors.Is(err, client.ErrInvalidRequest) {
						t.Errorf("unexpected invalid request: %v", err)
					}
					op.Pending = true // outcome unknown: it may or may not have taken effect
				}
				mu.Lock()
				history = append(history, op)
				mu.Unlock()
			}
		}(ci, cl)
	}

	fi := c.Injector()
	pause := func() { time.Sleep(700 * time.Millisecond) }
	pause()
	shard := int32(sc.Intn(2))
	victim, err := faultinjector.KillLeader(fi, c.LeaderLookup(), shard)
	if err != nil {
		t.Fatal(err)
	}
	pause()
	if err := fi.RestartNode(victim); err != nil {
		t.Fatal(err)
	}
	pause()
	if _, err := faultinjector.IsolateLeader(fi, c.LeaderLookup(), 1-shard, c.IDs()); err != nil {
		t.Fatal(err)
	}
	pause()
	fi.HealPartition()
	pause()
	close(stop)
	wg.Wait()

	pending := 0
	for _, op := range history {
		if op.Pending {
			pending++
		}
	}
	res := linearizability.Check(history)
	t.Logf("%d operations (%d with unknown outcome), %d search states explored", len(history), pending, res.Explored)
	if len(history) < 200 {
		t.Fatalf("history too small to be meaningful: %d ops", len(history))
	}
	if !res.OK {
		for _, op := range res.History {
			t.Log(op)
		}
		t.Fatalf("history for key %q is NOT linearizable", res.Key)
	}

	// No conflicting committed values: once converged, every replica of a
	// shard holds byte-identical state (determinism of apply).
	for sid := int32(0); sid < 2; sid++ {
		if err := c.WaitConverged(sid, 15*time.Second); err != nil {
			t.Fatal(err)
		}
		var ref map[string]string
		var refNode string
		for _, nid := range c.Replicas(sid) {
			s, _ := c.Node(nid).Manager().Get(sid)
			d := s.FSM().Dump()
			if ref == nil {
				ref, refNode = d, nid
				continue
			}
			if fmt.Sprint(d) != fmt.Sprint(ref) {
				t.Fatalf("shard %d: %s and %s diverge:\n%v\n%v", sid, refNode, nid, ref, d)
			}
		}
	}
}

// TestCorrectness_RecoveryIsBitForBit: a follower killed mid-workload and
// restarted ends up with exactly the state it would have had without the
// crash (identical to its peers, key for key).
func TestCorrectness_RecoveryIsBitForBit(t *testing.T) {
	c := testcluster.Start(t, testcluster.Options{Nodes: 3, Shards: 1, Sync: true,
		Raft: testRaftWithSmallSnapshots()})
	cl := c.Client()
	cx := ctx(t, time.Minute)
	leader, _, _ := c.ShardLeader(0)
	var follower string
	for _, id := range c.IDs() {
		if id != leader {
			follower = id
			break
		}
	}
	for i := 0; i < 150; i++ {
		cl.Put(cx, []byte(fmt.Sprintf("k%d", i%40)), []byte(fmt.Sprintf("v%d", i)))
		if i == 60 {
			c.Kill(follower)
		}
		if i%7 == 0 {
			cl.Delete(cx, []byte(fmt.Sprintf("k%d", (i+3)%40)))
		}
	}
	if err := c.Restart(follower); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitConverged(0, 15*time.Second); err != nil {
		t.Fatal(err)
	}
	ls, _ := c.Node(leader).Manager().Get(0)
	fs, _ := c.Node(follower).Manager().Get(0)
	a, _ := ls.FSM().Snapshot()
	b, _ := fs.FSM().Snapshot()
	if string(a) != string(b) {
		t.Fatalf("recovered follower state differs from leader (%d vs %d bytes)", len(b), len(a))
	}
}
