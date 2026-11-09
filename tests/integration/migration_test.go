package integration

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"samanvaydb/internal/membership"
	"samanvaydb/internal/migration"
	"samanvaydb/internal/router"
	"samanvaydb/internal/testcluster"
	"samanvaydb/pkg/client"
	"samanvaydb/proto/adminpb"
)

// writer continuously writes sequentially numbered keys and records every
// acknowledged one.
type writer struct {
	cl    *client.Client
	stop  chan struct{}
	done  chan struct{}
	mu    sync.Mutex
	acked []string
	errs  int
}

func startWriter(cl *client.Client, prefix string) *writer {
	w := &writer{cl: cl, stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(w.done)
		// Key i+1 is only written after key i is acknowledged (retrying the
		// same idempotent PUT on errors), so the committed sequence is
		// gap-free unless the database itself loses a write.
		for i := 0; ; {
			select {
			case <-w.stop:
				return
			default:
			}
			k := fmt.Sprintf("%s-%06d", prefix, i)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			err := cl.Put(ctx, []byte(k), []byte(k))
			cancel()
			w.mu.Lock()
			if err == nil {
				w.acked = append(w.acked, k)
				i++
			} else {
				w.errs++
			}
			w.mu.Unlock()
			time.Sleep(2 * time.Millisecond)
		}
	}()
	return w
}

func (w *writer) finish() ([]string, int) {
	close(w.stop)
	<-w.done
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.acked...), w.errs
}

func adminClient(t *testing.T, cl *client.Client, addr string) adminpb.AdminClient {
	a, err := cl.AdminFor(addr)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

// TestPhase7_ZeroGapMigrationUnderLoad — Phase 7 checkpoint: migrate a
// shard from a 3-node group to a DISJOINT 3-node group while a client writes
// continuously. After cutover the target group holds exactly the client's
// acknowledged writes, with zero gaps.
func TestPhase7_ZeroGapMigrationUnderLoad(t *testing.T) {
	c := testcluster.Start(t, testcluster.Options{Nodes: 6, Shards: 1, RF: 3,
		MetaReplicas: []string{"node-4", "node-5", "node-6"}})
	cl := c.Client()
	source := c.Replicas(0)
	if strings.Join(source, ",") != "node-1,node-2,node-3" {
		t.Fatalf("unexpected source group %v", source)
	}
	targets := []string{"node-4", "node-5", "node-6"}

	w := startWriter(cl, "mig")
	time.Sleep(500 * time.Millisecond)
	cx := ctx(t, 2*time.Minute)
	resp, err := adminClient(t, cl, c.Addr("node-4")).MigrateShard(cx, &adminpb.MigrateShardRequest{ShardId: 0, Targets: targets})
	if err != nil || !resp.Ok {
		t.Fatalf("migrate: err=%v resp=%v", err, resp)
	}
	for _, l := range resp.Log {
		t.Log(l)
	}
	time.Sleep(500 * time.Millisecond) // keep writing after cutover
	acked, errs := w.finish()
	t.Logf("%d acknowledged writes, %d transient client errors", len(acked), errs)

	// The migration walked the full state machine.
	joined := strings.Join(resp.Log, "\n")
	for _, st := range []migration.State{migration.Snapshotting, migration.Transferring, migration.CatchingUp, migration.Cutover, migration.Done} {
		if !strings.Contains(joined, "-> "+string(st)) {
			t.Errorf("log missing transition to %s", st)
		}
	}
	// Topology: only the targets host the shard; the map agrees.
	deadline := time.Now().Add(10 * time.Second)
	for strings.Join(c.Replicas(0), ",") != strings.Join(targets, ",") {
		if time.Now().After(deadline) {
			t.Fatalf("replicas after cutover: %v", c.Replicas(0))
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err := cl.RefreshShardMap(cx); err != nil {
		t.Fatal(err)
	}
	meta, _ := cl.ShardMap().Shard(0)
	if meta.State != router.StateStable || strings.Join(meta.Replicas, ",") != strings.Join(targets, ",") {
		t.Fatalf("shard map after cutover: %+v", meta)
	}
	if err := c.WaitConverged(0, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	// Zero gaps: every acknowledged write is on every target replica, and
	// the target holds nothing the client did not write.
	for _, nid := range targets {
		s, ok := c.Node(nid).Manager().Get(0)
		if !ok {
			t.Fatalf("%s does not host shard 0", nid)
		}
		dump := s.FSM().Dump()
		for _, k := range acked {
			if dump[k] != k {
				t.Fatalf("%s is missing acknowledged write %s", nid, k)
			}
		}
		if len(dump) < len(acked) {
			t.Fatalf("%s has %d keys < %d acked", nid, len(dump), len(acked))
		}
		// Unacknowledged-but-committed writes (client timeout) may exist;
		// they must still be contiguous with the acked sequence.
		keys := make([]string, 0, len(dump))
		for k := range dump {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for i, k := range keys {
			if want := fmt.Sprintf("mig-%06d", i); k != want {
				t.Fatalf("%s: gap in key sequence at %d: have %s want %s", nid, i, k, want)
			}
		}
	}
	// Reads and writes work after cutover.
	if err := cl.Put(cx, []byte("post-migration"), []byte("ok")); err != nil {
		t.Fatal(err)
	}
	if v, ok, err := cl.Get(cx, []byte(acked[0])); err != nil || !ok || string(v) != acked[0] {
		t.Fatalf("read after migration: %q %v %v", v, ok, err)
	}
}

// TestMigration_AbortLeavesSourceAuthoritative: a migration to an
// unreachable target aborts and leaves the source serving, map STABLE.
func TestMigration_AbortLeavesSourceAuthoritative(t *testing.T) {
	c := testcluster.Start(t, testcluster.Options{Nodes: 4, Shards: 1, RF: 3})
	cl := c.Client()
	cx := ctx(t, time.Minute)
	cl.Put(cx, []byte("a"), []byte("1"))
	c.Kill("node-4") // target is down: TRANSFERRING cannot complete
	short, cancel := context.WithTimeout(cx, 8*time.Second)
	defer cancel()
	_, err := migration.Run(short, c.Node("node-1").Ops(), 0, []string{"node-2", "node-3", "node-4"}, t.Logf)
	if err == nil {
		t.Fatal("migration to a dead node succeeded")
	}
	m, _ := c.Node("node-1").Ops().ShardMap(cx)
	meta, _ := m.Shard(0)
	if meta.State != router.StateStable || strings.Join(meta.Replicas, ",") != "node-1,node-2,node-3" {
		t.Fatalf("after abort: %+v", meta)
	}
	if err := cl.Put(cx, []byte("b"), []byte("2")); err != nil {
		t.Fatalf("source not serving after abort: %v", err)
	}
}

// TestPhase8_MembershipChangesUnderLoad — Phase 8 checkpoint: add, remove
// and replace nodes on a live shard under continuous writes, checking
// quorum math at each step and that no acknowledged write is lost.
func TestPhase8_MembershipChangesUnderLoad(t *testing.T) {
	c := testcluster.Start(t, testcluster.Options{Nodes: 3, Shards: 1, RF: 3})
	cl := c.Client()
	ops := c.Node("node-1").Ops()
	cx := ctx(t, 3*time.Minute)
	voters := func() int {
		l, _ := c.WaitLeader(0, 5*time.Second)
		s, _ := c.Node(l).Manager().Get(0)
		return len(s.Raft().Membership().Voters())
	}
	w := startWriter(cl, "mem")
	time.Sleep(300 * time.Millisecond)

	// ADD: node-4 joins as a learner, catches up, becomes a voter.
	n4, err := c.AddNode("node-4")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := membership.RegisterNode(cx, ops, n4); err != nil {
		t.Fatal(err)
	}
	if err := membership.AddShardReplica(cx, ops, 0, "node-4", t.Logf); err != nil {
		t.Fatal(err)
	}
	if v := voters(); v != 4 {
		t.Fatalf("voters after add = %d, want 4", v)
	}

	// REMOVE: decommission node-1 (possibly a leader). RemoveNode moves or
	// removes EVERY replica it hosts — including its metadata-group replica,
	// which is relocated to node-4 — before it leaves the map.
	if err := membership.RemoveNode(cx, ops, "node-1", t.Logf); err != nil {
		t.Fatal(err)
	}
	if v := voters(); v != 3 {
		t.Fatalf("voters after remove = %d, want 3", v)
	}
	c.Kill("node-1") // safe: no longer a member

	// REPLACE: node-2 "fails"; node-5 replaces it via Remove + Add.
	c.Kill("node-2")
	n5, err := c.AddNode("node-5")
	if err != nil {
		t.Fatal(err)
	}
	ops = c.Node("node-3").Ops()
	if err := membership.ReplaceNode(cx, ops, "node-2", n5, t.Logf); err != nil {
		t.Fatal(err)
	}
	if v := voters(); v != 3 {
		t.Fatalf("voters after replace = %d, want 3", v)
	}
	time.Sleep(300 * time.Millisecond)
	acked, errs := w.finish()
	t.Logf("%d writes acknowledged during membership changes (%d transient errors)", len(acked), errs)
	if len(acked) < 50 {
		t.Fatalf("only %d writes succeeded under load", len(acked))
	}
	if reps := c.Replicas(0); strings.Join(reps, ",") != "node-3,node-4,node-5" {
		t.Fatalf("final replicas %v", reps)
	}
	if err := c.WaitConverged(0, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	for _, nid := range []string{"node-3", "node-4", "node-5"} {
		s, _ := c.Node(nid).Manager().Get(0)
		dump := s.FSM().Dump()
		for _, k := range acked {
			if dump[k] != k {
				t.Fatalf("%s lost acknowledged write %s", nid, k)
			}
		}
	}
}

// TestRebalance_AddNodeSpreadsReplicas: AddNode with rebalance moves
// replicas onto the new node until counts are even.
func TestRebalance_AddNodeSpreadsReplicas(t *testing.T) {
	c := testcluster.Start(t, testcluster.Options{Nodes: 3, Shards: 3, RF: 3})
	cl := c.Client()
	cx := ctx(t, 3*time.Minute)
	for i := 0; i < 30; i++ {
		cl.Put(cx, []byte(fmt.Sprintf("r%d", i)), []byte("v"))
	}
	n4, err := c.AddNode("node-4")
	if err != nil {
		t.Fatal(err)
	}
	resp, err := adminClient(t, cl, c.Addr("node-2")).AddNode(cx, &adminpb.AddNodeRequest{NodeId: n4.ID, Addr: n4.Addr, HttpAddr: n4.HTTPAddr, Rebalance: true})
	if err != nil || !resp.Ok {
		t.Fatalf("add node: %v %v", err, resp)
	}
	for _, l := range resp.Log {
		t.Log(l)
	}
	after, _ := c.Node("node-1").Ops().ShardMap(cx)
	counts := after.ReplicaCounts()
	total := 0
	for _, n := range counts {
		total += n
	}
	// Before: node-4 registered with 0 replicas vs 3 on every other node
	// (spread 3). After: spread <= 1, replication factor intact.
	if migration.Spread(after) > 1 || counts["node-4"] < 2 || total != 9 {
		t.Fatalf("after rebalance: counts=%v spread=%d", counts, migration.Spread(after))
	}
	if reps := c.Replicas(0); len(reps)+len(c.Replicas(1))+len(c.Replicas(2)) != 9 {
		t.Fatalf("live replica placement does not match RF=3")
	}
	for i := 0; i < 30; i++ {
		if _, ok, err := cl.Get(cx, []byte(fmt.Sprintf("r%d", i))); err != nil || !ok {
			t.Fatalf("key r%d lost after rebalance: %v", i, err)
		}
	}
}
