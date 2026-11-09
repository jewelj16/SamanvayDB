package raft_test

// Phase 3 checkpoint — the Raft failure-scenario suite. Every scenario runs
// entirely in-process over the raftsim network simulator, independent of any
// storage engine, and ends by asserting the two core safety properties:
//   - Election Safety: at most one leader per term, over the whole run.
//   - State Machine Safety: no two nodes applied different commands at the
//     same index.

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"samanvaydb/internal/raft"
	"samanvaydb/internal/raft/raftsim"
)

const (
	leaderWait = 5 * time.Second
	commitWait = 5 * time.Second
)

func newCluster(t *testing.T, n int, opts raftsim.Options) *raftsim.Cluster {
	t.Helper()
	if opts.ElectionTimeoutMin == 0 {
		opts.ElectionTimeoutMin = 100 * time.Millisecond
		opts.ElectionTimeoutMax = 200 * time.Millisecond
		opts.HeartbeatInterval = 20 * time.Millisecond
	}
	c, err := raftsim.NewCluster(n, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Shutdown)
	return c
}

func mustLeader(t *testing.T, c *raftsim.Cluster, ids ...raft.NodeID) raft.NodeID {
	t.Helper()
	id, err := c.WaitForLeader(leaderWait, ids...)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func mustPropose(t *testing.T, c *raftsim.Cluster, cmd string, ids ...raft.NodeID) uint64 {
	t.Helper()
	idx, err := c.Propose([]byte(cmd), commitWait, ids...)
	if err != nil {
		t.Fatal(err)
	}
	return idx
}

func checkSafety(t *testing.T, c *raftsim.Cluster) {
	t.Helper()
	if err := c.CheckElectionSafety(); err != nil {
		t.Fatal(err)
	}
	if err := c.CheckLogsAgree(); err != nil {
		t.Fatal(err)
	}
}

func contains(cmds []string, want string) bool {
	for _, c := range cmds {
		if c == want {
			return true
		}
	}
	return false
}

// ---- Election ----

func TestElection_ConvergesToOneLeader(t *testing.T) {
	for _, n := range []int{1, 3, 5} {
		t.Run(fmt.Sprintf("%d_nodes", n), func(t *testing.T) {
			c := newCluster(t, n, raftsim.Options{Seed: int64(n)})
			mustLeader(t, c)
			leaders := 0
			term := uint64(0)
			for _, id := range c.Live() {
				st := c.Node(id).Status()
				if st.State == raft.Leader {
					leaders++
					term = st.Term
				}
			}
			if leaders != 1 {
				t.Fatalf("%d leaders, want exactly 1", leaders)
			}
			for _, id := range c.Live() {
				if c.Node(id).Status().Term > term {
					t.Errorf("%s is in a term beyond the leader's", id)
				}
			}
			checkSafety(t, c)
		})
	}
}

// TestElection_SplitVoteResolves forces repeated split votes (a 4-node
// cluster split 2/2 — each candidate can collect at most 2 of the 3 votes
// it needs) and verifies the cluster neither deadlocks nor elects two
// leaders, and converges once the split ends.
func TestElection_SplitVoteResolves(t *testing.T) {
	c := newCluster(t, 4, raftsim.Options{Seed: 11, DisablePreVote: true})
	c.Net.Partition([]raft.NodeID{"node-1", "node-2"}, []raft.NodeID{"node-3", "node-4"})
	time.Sleep(1200 * time.Millisecond)
	if id, _, ok := c.Leader(); ok {
		// A leader from before the split may linger briefly; it must not
		// be able to commit anything.
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		_, err := c.Node(id).ProposeAndWait(ctx, []byte("split"))
		cancel()
		if err == nil {
			t.Fatal("a write committed without a majority")
		}
	}
	total := 0
	for _, id := range c.IDs {
		total += c.Elections(id)
	}
	if total < 2 {
		t.Fatalf("expected repeated elections during the split, saw %d", total)
	}
	c.Net.HealPartition()
	mustLeader(t, c)
	mustPropose(t, c, "after-split")
	checkSafety(t, c)
}

func TestElection_HeartbeatsSuppressElections(t *testing.T) {
	c := newCluster(t, 3, raftsim.Options{Seed: 3})
	leader := mustLeader(t, c)
	term := c.Node(leader).CurrentTerm()
	before := map[raft.NodeID]int{}
	for _, id := range c.IDs {
		before[id] = c.Elections(id)
	}
	time.Sleep(1500 * time.Millisecond) // many election timeouts
	for _, id := range c.IDs {
		if c.Elections(id) != before[id] {
			t.Errorf("%s started an election despite a healthy leader", id)
		}
	}
	if c.Node(leader).CurrentTerm() != term || !c.Node(leader).IsLeader() {
		t.Error("healthy leader was disrupted")
	}
}

// ---- Replication ----

func TestReplication_AllNodesApplySameOrder(t *testing.T) {
	c := newCluster(t, 5, raftsim.Options{Seed: 5})
	mustLeader(t, c)
	var last uint64
	for i := 0; i < 100; i++ {
		last = mustPropose(t, c, fmt.Sprintf("cmd-%d", i))
	}
	if err := c.WaitApplied(last, commitWait); err != nil {
		t.Fatal(err)
	}
	want := c.SM("node-1").Commands()
	if len(want) != 100 {
		t.Fatalf("applied %d commands, want 100", len(want))
	}
	for _, id := range c.IDs {
		got := c.SM(id).Commands()
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("%s applied %q at position %d, node-1 applied %q", id, got[i], i, want[i])
			}
		}
	}
	checkSafety(t, c)
}

// ---- Failure table (Section 7 of the guide) ----

func TestFailure_LeaderCrash(t *testing.T) {
	c := newCluster(t, 5, raftsim.Options{Seed: 21})
	old := mustLeader(t, c)
	var committed []string
	for i := 0; i < 20; i++ {
		cmd := fmt.Sprintf("pre-%d", i)
		mustPropose(t, c, cmd)
		committed = append(committed, cmd)
	}
	c.Crash(old)
	start := time.Now()
	nl := mustLeader(t, c)
	if nl == old {
		t.Fatal("crashed node still leader")
	}
	t.Logf("new leader %s elected in %v", nl, time.Since(start))
	idx := mustPropose(t, c, "post")
	if err := c.WaitApplied(idx, commitWait); err != nil {
		t.Fatal(err)
	}
	for _, id := range c.Live() {
		got := c.SM(id).Commands()
		for _, cmd := range committed {
			if !contains(got, cmd) {
				t.Fatalf("%s lost committed entry %q after leader crash", id, cmd)
			}
		}
	}
	checkSafety(t, c)
}

func TestFailure_FollowerCrashAndCatchUp(t *testing.T) {
	c := newCluster(t, 3, raftsim.Options{Seed: 22})
	leader := mustLeader(t, c)
	var follower raft.NodeID
	for _, id := range c.IDs {
		if id != leader {
			follower = id
			break
		}
	}
	c.Crash(follower)
	var last uint64
	for i := 0; i < 50; i++ {
		last = mustPropose(t, c, fmt.Sprintf("w-%d", i)) // majority (2/3) still alive
	}
	if err := c.Restart(follower); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitApplied(last, commitWait, follower); err != nil {
		t.Fatal(err)
	}
	if got, want := c.SM(follower).Commands(), c.SM(leader).Commands(); len(got) < 50 || len(got) != len(want) {
		t.Fatalf("restarted follower applied %d commands, leader %d", len(got), len(want))
	}
	checkSafety(t, c)
}

// TestFailure_AvailabilityBoundary verifies writes succeed exactly while a
// majority ⌈(N+1)/2⌉ of the 5 nodes is alive.
func TestFailure_AvailabilityBoundary(t *testing.T) {
	c := newCluster(t, 5, raftsim.Options{Seed: 23})
	leader := mustLeader(t, c)
	var followers []raft.NodeID
	for _, id := range c.IDs {
		if id != leader {
			followers = append(followers, id)
		}
	}
	c.Crash(followers[0])
	c.Crash(followers[1])
	mustPropose(t, c, "with-3-of-5") // 3 alive = majority

	c.Crash(followers[2]) // 2 alive: below majority
	if _, err := c.Propose([]byte("with-2-of-5"), 1500*time.Millisecond); err == nil {
		t.Fatal("write committed with only 2 of 5 nodes alive")
	}
	if err := c.Restart(followers[2]); err != nil {
		t.Fatal(err)
	}
	mustPropose(t, c, "back-to-3-of-5")
	checkSafety(t, c)
}

func TestFailure_DelayedMessagesConverge(t *testing.T) {
	c := newCluster(t, 3, raftsim.Options{Seed: 24})
	mustLeader(t, c)
	for _, a := range c.IDs {
		for _, b := range c.IDs {
			if a != b {
				c.Net.DelayMessages(a, b, 15*time.Millisecond)
				c.Net.JitterMessages(a, b, 25*time.Millisecond)
			}
		}
	}
	var last uint64
	for i := 0; i < 30; i++ {
		last = mustPropose(t, c, fmt.Sprintf("d-%d", i))
	}
	c.Net.Reset()
	if err := c.WaitApplied(last, commitWait); err != nil {
		t.Fatal(err)
	}
	checkSafety(t, c)
}

func TestFailure_DuplicateMessagesAreNoOps(t *testing.T) {
	c := newCluster(t, 3, raftsim.Options{Seed: 25})
	mustLeader(t, c)
	for _, a := range c.IDs {
		for _, b := range c.IDs {
			if a != b {
				c.Net.DuplicateMessages(a, b, 0.5)
			}
		}
	}
	var last uint64
	for i := 0; i < 40; i++ {
		last = mustPropose(t, c, fmt.Sprintf("dup-%d", i))
	}
	if err := c.WaitApplied(last, commitWait); err != nil {
		t.Fatal(err)
	}
	for _, id := range c.IDs {
		seen := map[string]int{}
		for _, cmd := range c.SM(id).Commands() {
			seen[cmd]++
			if seen[cmd] > 1 {
				t.Fatalf("%s applied %q twice", id, cmd)
			}
		}
	}
	checkSafety(t, c)
}

// TestFailure_OutOfOrderMessages reorders traffic with heavy jitter plus
// drops and verifies logs never contain gaps (applied indexes contiguous)
// and backtracking repairs every rejection.
func TestFailure_OutOfOrderMessages(t *testing.T) {
	c := newCluster(t, 3, raftsim.Options{Seed: 26})
	mustLeader(t, c)
	for _, a := range c.IDs {
		for _, b := range c.IDs {
			if a != b {
				c.Net.JitterMessages(a, b, 40*time.Millisecond)
				c.Net.DropMessages(a, b, 0.1)
			}
		}
	}
	var last uint64
	for i := 0; i < 30; i++ {
		last = mustPropose(t, c, fmt.Sprintf("o-%d", i))
	}
	c.Net.Reset()
	if err := c.WaitApplied(last, commitWait); err != nil {
		t.Fatal(err)
	}
	for _, id := range c.IDs {
		st := c.Node(id).Status()
		if st.LastApplied < last {
			t.Fatalf("%s applied %d < %d", id, st.LastApplied, last)
		}
	}
	checkSafety(t, c)
}

// TestFailure_MinorityPartition: no writes succeed on the minority side; the
// majority side's leader serves normally.
func TestFailure_MinorityPartition(t *testing.T) {
	c := newCluster(t, 5, raftsim.Options{Seed: 27})
	leader := mustLeader(t, c)
	var minority, majority []raft.NodeID
	minority = append(minority, leader)
	for _, id := range c.IDs {
		if id == leader {
			continue
		}
		if len(minority) < 2 {
			minority = append(minority, id)
		} else {
			majority = append(majority, id)
		}
	}
	c.Net.Partition(minority, majority)

	ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
	_, err := c.Node(leader).ProposeAndWait(ctx, []byte("minority-write"))
	cancel()
	if err == nil {
		t.Fatal("minority-side write committed")
	}
	mustLeader(t, c, majority...)
	idx := mustPropose(t, c, "majority-write", majority...)
	if err := c.WaitApplied(idx, commitWait, majority...); err != nil {
		t.Fatal(err)
	}
	for _, id := range minority {
		if contains(c.SM(id).Commands(), "majority-write") {
			t.Fatalf("%s applied a write while partitioned away", id)
		}
	}
	checkSafety(t, c)
}

// TestFailure_SplitBrainWalkthrough is the guide's full network-partition
// walkthrough: 5 nodes, {N1,N2} vs {N3,N4,N5} with N1 the leader. N1 must
// never commit on the minority side, and must step down and discard its
// uncommitted entries once the partition heals.
func TestFailure_SplitBrainWalkthrough(t *testing.T) {
	c := newCluster(t, 5, raftsim.Options{Seed: 28})
	n1 := mustLeader(t, c)
	oldTerm := c.Node(n1).CurrentTerm()
	var n2 raft.NodeID
	var majority []raft.NodeID
	for _, id := range c.IDs {
		if id == n1 {
			continue
		}
		if n2 == "" {
			n2 = id
		} else {
			majority = append(majority, id)
		}
	}
	mustPropose(t, c, "before-partition")
	c.Net.Partition([]raft.NodeID{n1, n2}, majority)

	// N1 still accepts the proposal into its log but can never commit it.
	idx, _, err := c.Node(n1).Propose([]byte("doomed"))
	if err == nil {
		if c.Node(n1).WaitApplied(idx, 600*time.Millisecond) {
			t.Fatal("N1 committed a write on the minority side")
		}
	}
	n3 := mustLeader(t, c, majority...)
	if c.Node(n3).CurrentTerm() <= oldTerm {
		t.Fatalf("majority leader term %d not above old term %d", c.Node(n3).CurrentTerm(), oldTerm)
	}
	last := mustPropose(t, c, "majority-1", majority...)

	c.Net.HealPartition()
	last = mustPropose(t, c, "after-heal")
	if err := c.WaitApplied(last, commitWait); err != nil {
		t.Fatal(err)
	}
	if c.Node(n1).IsLeader() && c.Node(n1).CurrentTerm() <= oldTerm {
		t.Fatal("old leader did not step down after healing")
	}
	for _, id := range c.IDs {
		cmds := c.SM(id).Commands()
		if contains(cmds, "doomed") {
			t.Fatalf("%s applied the minority leader's uncommitted entry", id)
		}
		if !contains(cmds, "majority-1") || !contains(cmds, "before-partition") {
			t.Fatalf("%s is missing committed entries: %v", id, cmds)
		}
	}
	checkSafety(t, c)
}

// ---- Snapshots ----

func TestSnapshot_FarBehindFollowerUsesInstallSnapshot(t *testing.T) {
	c := newCluster(t, 3, raftsim.Options{Seed: 31, SnapshotThreshold: 20})
	leader := mustLeader(t, c)
	var lagger raft.NodeID
	for _, id := range c.IDs {
		if id != leader {
			lagger = id
			break
		}
	}
	c.Crash(lagger)
	var last uint64
	for i := 0; i < 120; i++ {
		last = mustPropose(t, c, fmt.Sprintf("s-%d", i))
	}
	lst := c.Node(leader).Status()
	if lst.SnapshotIndex == 0 {
		t.Fatal("leader never snapshotted")
	}
	if err := c.Restart(lagger); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitApplied(last, commitWait, lagger); err != nil {
		t.Fatal(err)
	}
	fst := c.Node(lagger).Status()
	if fst.SnapshotIndex == 0 {
		t.Fatal("lagging follower caught up without InstallSnapshot (expected snapshot transfer)")
	}
	got := c.SM(lagger).Commands()
	if len(got) != 120 || got[0] != "s-0" || got[119] != "s-119" {
		t.Fatalf("follower state after InstallSnapshot has %d commands", len(got))
	}
	checkSafety(t, c)
}

// TestSnapshot_RestartReplaysOnlyTail verifies a restarted node restores its
// snapshot first and then needs only the post-snapshot log tail.
func TestSnapshot_RestartReplaysOnlyTail(t *testing.T) {
	c := newCluster(t, 3, raftsim.Options{Seed: 32, SnapshotThreshold: 25})
	mustLeader(t, c)
	var last uint64
	for i := 0; i < 60; i++ {
		last = mustPropose(t, c, fmt.Sprintf("r-%d", i))
	}
	if err := c.WaitApplied(last, commitWait); err != nil {
		t.Fatal(err)
	}
	victim := c.IDs[2]
	snapIdx := c.Node(victim).Status().SnapshotIndex
	if snapIdx == 0 {
		t.Fatal("victim has no snapshot")
	}
	c.Crash(victim)
	c.Net.Isolate(victim, c.IDs) // keep it from catching up so we can inspect
	if err := c.Restart(victim); err != nil {
		t.Fatal(err)
	}
	st := c.Node(victim).Status()
	if st.LastApplied != snapIdx || st.SnapshotIndex != snapIdx {
		t.Fatalf("after restart: applied=%d snapshot=%d, want both %d", st.LastApplied, st.SnapshotIndex, snapIdx)
	}
	if st.LastLogIndex <= snapIdx {
		t.Fatalf("restart lost the log tail (last=%d snap=%d)", st.LastLogIndex, snapIdx)
	}
	restored := len(c.SM(victim).Commands())
	c.Net.HealPartition()
	if err := c.WaitApplied(last, commitWait, victim); err != nil {
		t.Fatal(err)
	}
	if got := len(c.SM(victim).Commands()); got != 60 || restored >= 60 {
		t.Fatalf("restored %d from snapshot, %d after tail replay", restored, got)
	}
	checkSafety(t, c)
}

// ---- Membership ----

func TestMembership_AddLearnerPromoteRemoveUnderLoad(t *testing.T) {
	c := newCluster(t, 3, raftsim.Options{Seed: 41, SnapshotThreshold: 30})
	mustLeader(t, c)
	stop := make(chan struct{})
	done := make(chan int)
	go func() {
		n := 0
		for {
			select {
			case <-stop:
				done <- n
				return
			default:
			}
			if _, err := c.Propose([]byte(fmt.Sprintf("load-%d", n)), 3*time.Second); err == nil {
				n++
			}
		}
	}()
	time.Sleep(300 * time.Millisecond)

	if err := c.AddNode("node-4"); err != nil {
		t.Fatal(err)
	}
	change := func(cc raft.ConfChange) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			id, _, ok := c.Leader()
			if !ok {
				time.Sleep(20 * time.Millisecond)
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_, err := c.Node(id).ProposeConfChange(ctx, cc)
			cancel()
			if err == nil {
				return
			}
			if errors.Is(err, raft.ErrInvalidConfChange) {
				t.Fatal(err)
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatalf("conf change %+v did not commit", cc)
	}
	change(raft.ConfChange{Type: raft.AddLearner, Node: "node-4"})

	// Learner: never counted toward quorum.
	lid, _, _ := c.Leader()
	if q := c.Node(lid).Membership().Quorum(); q != 2 {
		t.Fatalf("quorum with a learner = %d, want 2", q)
	}
	// Wait for it to catch up, then promote.
	deadline := time.Now().Add(5 * time.Second)
	for {
		lid, _, _ = c.Leader()
		st := c.Node(lid).Status()
		if p, ok := st.Progress["node-4"]; ok && st.LastLogIndex-p.Match < 10 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("learner never caught up")
		}
		time.Sleep(20 * time.Millisecond)
	}
	change(raft.ConfChange{Type: raft.PromoteLearner, Node: "node-4"})
	lid, _, _ = c.Leader()
	if q := c.Node(lid).Membership().Quorum(); q != 3 {
		t.Fatalf("quorum after promotion = %d, want 3 (of 4 voters)", q)
	}

	// Remove an original follower, then kill it: the group keeps working.
	var victim raft.NodeID
	for _, id := range []raft.NodeID{"node-1", "node-2", "node-3"} {
		if id != lid {
			victim = id
			break
		}
	}
	change(raft.ConfChange{Type: raft.RemoveMember, Node: victim})
	c.Crash(victim)
	lid, _, _ = c.Leader()
	if q := c.Node(lid).Membership().Quorum(); q != 2 {
		t.Fatalf("quorum after removal = %d, want 2 (of 3 voters)", q)
	}

	close(stop)
	writes := <-done
	idx := mustPropose(t, c, "final")
	if writes == 0 {
		t.Fatal("no writes succeeded during membership changes")
	}
	var live []raft.NodeID
	for _, id := range c.Live() {
		live = append(live, id)
	}
	if err := c.WaitApplied(idx, commitWait, live...); err != nil {
		t.Fatal(err)
	}
	if !contains(c.SM("node-4").Commands(), "final") {
		t.Fatal("new member did not apply later writes")
	}
	checkSafety(t, c)
}

// TestMembership_RemovingLeaderStepsDown verifies a leader that removes
// itself steps down once the change commits and the rest elect a new leader.
func TestMembership_RemovingLeaderStepsDown(t *testing.T) {
	c := newCluster(t, 3, raftsim.Options{Seed: 42})
	leader := mustLeader(t, c)
	mustPropose(t, c, "x")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := c.Node(leader).ProposeConfChange(ctx, raft.ConfChange{Type: raft.RemoveMember, Node: leader}); err != nil {
		t.Fatal(err)
	}
	var rest []raft.NodeID
	for _, id := range c.IDs {
		if id != leader {
			rest = append(rest, id)
		}
	}
	nl := mustLeader(t, c, rest...)
	if nl == leader {
		t.Fatal("removed leader still leading")
	}
	time.Sleep(300 * time.Millisecond)
	if c.Node(leader).IsLeader() {
		t.Fatal("removed leader did not step down")
	}
	mustPropose(t, c, "after-removal", rest...)
	checkSafety(t, c)
}

// ---- ReadIndex ----

func TestReadIndex_LeaderConfirmsLeadership(t *testing.T) {
	c := newCluster(t, 3, raftsim.Options{Seed: 51})
	leader := mustLeader(t, c)
	idx := mustPropose(t, c, "v1")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	ri, err := c.Node(leader).ReadIndex(ctx)
	cancel()
	if err != nil || ri < idx {
		t.Fatalf("ReadIndex = %d, %v; want >= %d", ri, err, idx)
	}

	// An isolated leader cannot confirm leadership, so it must not serve.
	c.Net.Isolate(leader, c.IDs)
	ctx, cancel = context.WithTimeout(context.Background(), 400*time.Millisecond)
	_, err = c.Node(leader).ReadIndex(ctx)
	cancel()
	if err == nil {
		t.Fatal("isolated leader served a linearizable read")
	}
	checkSafety(t, c)
}

// TestCheckQuorum_IsolatedLeaderStepsDown verifies a leader cut off from
// the majority stops claiming leadership within a few election timeouts.
func TestCheckQuorum_IsolatedLeaderStepsDown(t *testing.T) {
	c := newCluster(t, 3, raftsim.Options{Seed: 52})
	leader := mustLeader(t, c)
	c.Net.Isolate(leader, c.IDs)
	deadline := time.Now().Add(2 * time.Second)
	for c.Node(leader).IsLeader() {
		if time.Now().After(deadline) {
			t.Fatal("isolated leader never stepped down")
		}
		time.Sleep(20 * time.Millisecond)
	}
	checkSafety(t, c)
}
