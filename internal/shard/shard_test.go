package shard

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"samanvaydb/internal/logging"
	"samanvaydb/internal/raft"
	"samanvaydb/internal/raft/raftsim"
)

// group is a test harness: one shard's replicas over a simulated network.
type group struct {
	t     *testing.T
	net   *raftsim.Network
	ids   []raft.NodeID
	dir   string // "" = in-memory raft storage
	mu    sync.Mutex
	reps  map[raft.NodeID]*Shard
	lagMu sync.Mutex
	lags  []time.Duration
	extra func(*Config)
}

func newGroup(t *testing.T, n int, onDisk bool, extra func(*Config)) *group {
	t.Helper()
	g := &group{t: t, net: raftsim.NewNetwork(int64(n) + 100), reps: map[raft.NodeID]*Shard{}, extra: extra}
	if onDisk {
		g.dir = t.TempDir()
	}
	for i := 1; i <= n; i++ {
		g.ids = append(g.ids, raft.NodeID(fmt.Sprintf("n%d", i)))
	}
	for _, id := range g.ids {
		g.start(id)
	}
	t.Cleanup(g.stopAll)
	return g
}

func quietLogger(id string) *logging.Logger {
	return logging.NewLogger(logging.Options{NodeID: id, Level: slog.LevelError + 4})
}

func (g *group) start(id raft.NodeID) *Shard {
	g.t.Helper()
	boot := raft.NewMembership(g.ids...)
	cfg := Config{
		ShardID:            7,
		NodeID:             id,
		Bootstrap:          &boot,
		Transport:          g.net.Transport(id),
		Logger:             quietLogger(string(id)),
		ElectionTimeoutMin: 100 * time.Millisecond,
		ElectionTimeoutMax: 200 * time.Millisecond,
		HeartbeatInterval:  20 * time.Millisecond,
		ExpiryInterval:     50 * time.Millisecond,
		OnApplyLag: func(d time.Duration) {
			g.lagMu.Lock()
			g.lags = append(g.lags, d)
			g.lagMu.Unlock()
		},
	}
	if g.dir != "" {
		cfg.RaftDir = filepath.Join(g.dir, string(id), "raft")
	}
	if g.extra != nil {
		g.extra(&cfg)
	}
	s, err := New(cfg)
	if err != nil {
		g.t.Fatal(err)
	}
	g.mu.Lock()
	g.reps[id] = s
	g.mu.Unlock()
	g.net.Register(id, s.Raft())
	s.Start()
	return s
}

func (g *group) kill(id raft.NodeID) {
	g.mu.Lock()
	s := g.reps[id]
	delete(g.reps, id)
	g.mu.Unlock()
	g.net.Unregister(id)
	if s != nil {
		s.Stop()
	}
}

func (g *group) stopAll() {
	for _, id := range g.ids {
		g.kill(id)
	}
}

func (g *group) rep(id raft.NodeID) *Shard {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.reps[id]
}

func (g *group) leader() *Shard {
	g.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		g.mu.Lock()
		for _, s := range g.reps {
			if s.Raft().IsLeader() {
				g.mu.Unlock()
				return s
			}
		}
		g.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	g.t.Fatal("no shard leader elected")
	return nil
}

// put retries against whichever replica is leader, like a real client.
func (g *group) put(key, val string) error {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := g.leader().Put(ctx, []byte(key), []byte(val), 0)
		cancel()
		if err == nil {
			return nil
		}
		var nl *NotLeaderError
		if errors.As(err, &nl) || errors.Is(err, ErrUnavailable) || errors.Is(err, ErrStopped) {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		return err
	}
	return fmt.Errorf("put %s timed out", key)
}

func ctxT(d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), d)
}

// Commit 35: single-node shard applies end-to-end (Raft log -> Apply -> engine).
func TestShard_SingleNodePlumbing(t *testing.T) {
	g := newGroup(t, 1, false, nil)
	l := g.leader()
	ctx, cancel := ctxT(time.Second)
	defer cancel()
	if err := l.Put(ctx, []byte("k"), []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	v, ok, err := l.Get(ctx, []byte("k"), ConsistencyLeader)
	if err != nil || !ok || string(v) != "v" {
		t.Fatalf("Get = %q %v %v", v, ok, err)
	}
	st := l.Raft().Status()
	if st.LastApplied < 2 { // no-op + put
		t.Fatalf("lastApplied=%d", st.LastApplied)
	}
}

// Commit 36: a write that cannot reach a majority is NEVER visible via GET
// on the leader, even transiently.
func TestShard_UncommittedWriteNeverVisible(t *testing.T) {
	g := newGroup(t, 3, false, nil)
	l := g.leader()
	if err := g.put("k", "committed"); err != nil {
		t.Fatal(err)
	}
	g.net.Isolate(l.Raft().ID(), g.ids)

	var sawDoomed atomic.Bool
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if v, _, _ := l.Get(context.Background(), []byte("k"), ConsistencyFollowerOK); string(v) == "doomed" {
				sawDoomed.Store(true)
			}
		}
	}()
	ctx, cancel := ctxT(600 * time.Millisecond)
	err := l.Put(ctx, []byte("k"), []byte("doomed"), 0)
	cancel()
	if err == nil {
		t.Fatal("isolated leader acknowledged a write")
	}
	g.net.HealPartition()
	if err := g.put("k2", "after"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
	if sawDoomed.Load() {
		t.Fatal("uncommitted value was observable on the leader")
	}
	for _, id := range g.ids {
		if v, _ := g.rep(id).FSM().Get([]byte("k")); string(v) == "doomed" {
			t.Fatalf("%s applied the abandoned write", id)
		}
	}
}

// Commit 37: follower apply lag under load stays small and bounded.
func TestShard_FollowerApplyLagBounded(t *testing.T) {
	g := newGroup(t, 3, false, nil)
	g.leader()
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				if err := g.put(fmt.Sprintf("w%d-%d", w, i), "v"); err != nil {
					t.Error(err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	time.Sleep(200 * time.Millisecond)
	g.lagMu.Lock()
	lags := append([]time.Duration(nil), g.lags...)
	g.lagMu.Unlock()
	sort.Slice(lags, func(i, j int) bool { return lags[i] < lags[j] })
	if len(lags) < 3*200 {
		t.Fatalf("only %d apply observations", len(lags))
	}
	p99 := lags[len(lags)*99/100]
	t.Logf("apply lag p50=%v p99=%v max=%v over %d applies", lags[len(lags)/2], p99, lags[len(lags)-1], len(lags))
	if p99 > 500*time.Millisecond {
		t.Fatalf("p99 apply lag %v exceeds 500ms bound", p99)
	}
}

// Commit 38 — Phase 4 checkpoint: a 3-node single-shard group on real disk.
// Kill the leader mid-stream; a new leader is elected; every acknowledged
// write is present; writes resume; the killed node restarts FROM DISK and
// converges to an identical state.
func TestShard_Phase4_LeaderKillMidWrite(t *testing.T) {
	g := newGroup(t, 3, true, func(c *Config) { c.SnapshotThreshold = 150 })
	acked := map[string]string{}
	var mu sync.Mutex
	write := func(from, to int) {
		for i := from; i < to; i++ {
			k, v := fmt.Sprintf("key-%04d", i), fmt.Sprintf("val-%d", i)
			if err := g.put(k, v); err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			acked[k] = v
			mu.Unlock()
		}
	}
	write(0, 200)
	old := g.leader().Raft().ID()
	// Kill the leader while a writer is mid-stream.
	done := make(chan struct{})
	go func() { write(200, 400); close(done) }()
	time.Sleep(50 * time.Millisecond)
	g.kill(old)
	<-done

	nl := g.leader()
	if nl.Raft().ID() == old {
		t.Fatal("killed node still leader")
	}
	for k, v := range acked {
		got, ok := nl.FSM().Get([]byte(k))
		if !ok || string(got) != v {
			t.Fatalf("new leader lost acknowledged write %s (got %q, %v)", k, got, ok)
		}
	}
	write(400, 450) // writes resume

	g.start(old) // restart from its on-disk Raft log + snapshot
	final := nl.Raft().Status().CommitIndex
	if !g.rep(old).Raft().WaitApplied(final, 10*time.Second) {
		t.Fatal("restarted node did not catch up")
	}
	want := nl.FSM().Dump()
	got := g.rep(old).FSM().Dump()
	if len(want) != 450 || len(got) != len(want) {
		t.Fatalf("restarted node has %d keys, leader %d (want 450)", len(got), len(want))
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("restarted node diverges at %s: %q vs %q", k, got[k], v)
		}
	}
}

// Commit 72: concurrent CAS from many clients — exactly one wins.
func TestShard_ConcurrentCASExactlyOneWins(t *testing.T) {
	g := newGroup(t, 3, false, nil)
	if err := g.put("ctr", "0"); err != nil {
		t.Fatal(err)
	}
	l := g.leader()
	var wins atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := ctxT(2 * time.Second)
			defer cancel()
			r, err := l.CAS(ctx, []byte("ctr"), []byte("0"), false, []byte(fmt.Sprintf("winner-%d", i)))
			if err == nil && r.Code == ResultOK {
				wins.Add(1)
			}
		}(i)
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatalf("%d CAS winners, want exactly 1", wins.Load())
	}
}

// Commit 76: N concurrent acquires of the same lock — exactly one succeeds,
// even though several pass the leader-side pre-check.
func TestShard_ConcurrentLockAcquireExactlyOne(t *testing.T) {
	for round := 0; round < 3; round++ {
		g := newGroup(t, 3, false, nil)
		l := g.leader()
		var wins atomic.Int32
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := 0; i < 12; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				ctx, cancel := ctxT(2 * time.Second)
				defer cancel()
				r, err := l.LockAcquire(ctx, "job", fmt.Sprintf("client-%d", i), 5*time.Second)
				if err == nil && r.Code == ResultOK {
					wins.Add(1)
				}
			}(i)
		}
		close(start)
		wg.Wait()
		if wins.Load() != 1 {
			t.Fatalf("round %d: %d lock winners, want exactly 1", round, wins.Load())
		}
	}
}

// Commits 77/78: TTL lapse -> leader-proposed expiry commits on every
// replica; the next acquire gets a strictly larger fencing token; the lock
// state (and token) survives leader failover.
func TestShard_LockExpiryTokensAndFailover(t *testing.T) {
	g := newGroup(t, 3, false, nil)
	l := g.leader()
	ctx, cancel := ctxT(2 * time.Second)
	defer cancel()
	r1, err := l.LockAcquire(ctx, "res", "a", 300*time.Millisecond)
	if err != nil || r1.Code != ResultOK {
		t.Fatalf("acquire: %+v %v", r1, err)
	}
	// Not expired yet: everyone sees it held.
	for _, id := range g.ids {
		applied := g.rep(id).Raft().Status().LastApplied // read BEFORE the FSM
		if _, held := g.rep(id).FSM().GetLock("res"); !held && applied >= r1.Token {
			t.Fatalf("%s applied the acquire but does not see the lock", id)
		}
	}
	// Let the TTL lapse; the leader's ticker must COMMIT the expiry.
	deadline := time.Now().Add(3 * time.Second)
	for {
		allFree := true
		for _, id := range g.ids {
			if _, held := g.rep(id).FSM().GetLock("res"); held {
				allFree = false
			}
		}
		if allFree {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("expired lock was never released via a committed expiry")
		}
		time.Sleep(20 * time.Millisecond)
	}
	r2, err := g.leader().LockAcquire(ctx, "res", "b", 10*time.Second)
	if err != nil || r2.Code != ResultOK || r2.Token <= r1.Token {
		t.Fatalf("re-acquire: %+v (prev token %d) %v", r2, r1.Token, err)
	}
	// Failover: the new leader serves the same lock state + token.
	old := g.leader().Raft().ID()
	g.kill(old)
	nl := g.leader()
	if !nl.Raft().WaitApplied(r2.Token, 3*time.Second) {
		t.Fatal("new leader missing the acquire")
	}
	rec, held := nl.FSM().GetLock("res")
	if !held || string(rec.OwnerID) != "b" || rec.FencingToken != r2.Token {
		t.Fatalf("lock state after failover: %+v held=%v", rec, held)
	}
	if r, _ := nl.LockAcquire(ctx, "res", "c", time.Second); r.Code != ResultAlreadyHeld {
		t.Fatalf("lock not held after failover: %v", r.Code)
	}
}

// Commit 73: TTL keys disappear on every replica only via the committed
// expiry, consistently.
func TestShard_TTLExpiryConsistentAcrossReplicas(t *testing.T) {
	g := newGroup(t, 3, false, nil)
	l := g.leader()
	ctx, cancel := ctxT(2 * time.Second)
	defer cancel()
	if err := l.Put(ctx, []byte("temp"), []byte("x"), 200*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := l.Put(ctx, []byte("perm"), []byte("y"), 0); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		gone := 0
		for _, id := range g.ids {
			if _, ok := g.rep(id).FSM().Get([]byte("temp")); !ok {
				gone++
			}
		}
		if gone == len(g.ids) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("TTL key not expired on all replicas")
		}
		time.Sleep(20 * time.Millisecond)
	}
	for _, id := range g.ids {
		if _, ok := g.rep(id).FSM().Get([]byte("perm")); !ok {
			t.Fatalf("%s lost a non-TTL key", id)
		}
	}
}

// Commit 79: WAIT unblocks promptly on release.
func TestShard_LockWaitUnblocksOnRelease(t *testing.T) {
	g := newGroup(t, 3, false, nil)
	l := g.leader()
	ctx, cancel := ctxT(5 * time.Second)
	defer cancel()
	if r, err := l.LockAcquire(ctx, "w", "a", 10*time.Second); err != nil || r.Code != ResultOK {
		t.Fatal(err)
	}
	res := make(chan bool, 1)
	go func() {
		ok, _ := l.LockWait(ctx, "w")
		res <- ok
	}()
	time.Sleep(100 * time.Millisecond)
	start := time.Now()
	if r, err := l.LockRelease(ctx, "w", "a"); err != nil || r.Code != ResultOK {
		t.Fatal(err)
	}
	select {
	case ok := <-res:
		if !ok {
			t.Fatal("wait returned without release")
		}
		t.Logf("waiter woke %v after release", time.Since(start))
	case <-time.After(2 * time.Second):
		t.Fatal("waiter not woken")
	}
	// Unheld lock: WAIT returns immediately.
	if ok, err := l.LockWait(ctx, "never-held"); !ok || err != nil {
		t.Fatalf("wait on free lock: %v %v", ok, err)
	}
}

// Commit 43: two shards in one process run independent elections.
func TestManager_IndependentRaftGroups(t *testing.T) {
	nets := map[int32]*raftsim.Network{1: raftsim.NewNetwork(1), 2: raftsim.NewNetwork(2)}
	ids := []raft.NodeID{"a", "b", "c"}
	managers := map[raft.NodeID]*Manager{}
	for _, id := range ids {
		id := id
		m := NewManager(ManagerConfig{NodeID: id, Logger: quietLogger(string(id)), Configure: func(c Config) Config {
			c.Transport = nets[c.ShardID].Transport(id)
			c.ElectionTimeoutMin = 100 * time.Millisecond
			c.ElectionTimeoutMax = 200 * time.Millisecond
			c.HeartbeatInterval = 20 * time.Millisecond
			return c
		}})
		managers[id] = m
		boot := raft.NewMembership(ids...)
		for sid := int32(1); sid <= 2; sid++ {
			s, err := m.Open(sid, &boot, false)
			if err != nil {
				t.Fatal(err)
			}
			nets[sid].Register(id, s.Raft())
		}
		t.Cleanup(m.Close)
	}
	leaderOf := func(sid int32) (raft.NodeID, uint64) {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			for _, id := range ids {
				if s, ok := managers[id].Get(sid); ok && s.Raft().IsLeader() {
					return id, s.Raft().CurrentTerm()
				}
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("shard %d: no leader", sid)
		return "", 0
	}
	l1, _ := leaderOf(1)
	l2, t2 := leaderOf(2)
	// Disrupt ONLY shard 1: isolate its leader on shard 1's network.
	nets[1].Isolate(l1, ids)
	deadline := time.Now().Add(5 * time.Second)
	for {
		nl, _ := leaderOf(1)
		if nl != l1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("shard 1 never re-elected")
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Shard 2 is untouched: same leader, same term.
	if nl2, nt2 := leaderOf(2); nl2 != l2 || nt2 != t2 {
		t.Fatalf("shard 2 was disturbed: leader %s->%s term %d->%d", l2, nl2, t2, nt2)
	}
}

// Commit 44: corrupting one shard's directory has no effect on another.
func TestManager_PerShardStorageIsolation(t *testing.T) {
	dir := t.TempDir()
	mk := func() *Manager {
		return NewManager(ManagerConfig{NodeID: "solo", DataDir: dir, Logger: quietLogger("solo")})
	}
	m := mk()
	boot := raft.NewMembership("solo")
	for sid := int32(0); sid < 2; sid++ {
		s, err := m.Open(sid, &boot, false)
		if err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(3 * time.Second)
		for !s.Raft().IsLeader() && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		ctx, cancel := ctxT(time.Second)
		if err := s.Put(ctx, []byte("k"), []byte(fmt.Sprintf("shard-%d", sid)), 0); err != nil {
			t.Fatal(err)
		}
		cancel()
	}
	m.Close()
	// Destroy shard 0's files entirely.
	if err := os.RemoveAll(filepath.Join(dir, "shard-0")); err != nil {
		t.Fatal(err)
	}
	m2 := mk()
	defer m2.Close()
	ids, ok, err := m2.LoadManifest()
	if err != nil || !ok || len(ids) != 2 {
		t.Fatalf("manifest: %v %v %v", ids, ok, err)
	}
	s1, err := m2.Open(1, &boot, false)
	if err != nil {
		t.Fatal(err)
	}
	if !s1.Raft().WaitApplied(2, 3*time.Second) {
		t.Fatal("shard 1 did not recover")
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		if v, ok := s1.FSM().Get([]byte("k")); ok && string(v) == "shard-1" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("shard 1 lost its data after shard 0 was destroyed")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
