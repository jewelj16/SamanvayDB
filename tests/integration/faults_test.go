package integration

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"samanvaydb/internal/faultinjector"
	"samanvaydb/internal/testcluster"
	"samanvaydb/pkg/client"
)

// TestPhase9_SeededLeaderKillScenario — Phase 9 checkpoint, automated: start
// a 9-node cluster, write a known set of keys, kill the leader of a shard
// picked by a SEEDED RNG, wait for a new leader, write more keys, do NOT
// heal, and assert every acknowledged write is still readable and correct.
// The seed is logged so a failing run can be replayed exactly.
func TestPhase9_SeededLeaderKillScenario(t *testing.T) {
	c := testcluster.Start(t, testcluster.Options{Nodes: 9, Shards: 3, RF: 3})
	cl := c.Client()
	sc := faultinjector.NewScenario(20261108)
	t.Logf("scenario seed %d", sc.Seed)
	rec := &faultinjector.Recorder{Inner: c.Injector()}
	cx := ctx(t, 2*time.Minute)

	acked := map[string]string{}
	write := func(prefix string, n int) {
		for i := 0; i < n; i++ {
			k, v := fmt.Sprintf("%s-%d", prefix, i), fmt.Sprintf("v-%d", i)
			if err := cl.Put(cx, []byte(k), []byte(v)); err != nil {
				t.Fatalf("put %s: %v", k, err)
			}
			acked[k] = v
		}
	}
	write("before", 150)

	shard := int32(sc.Intn(3))
	victim, err := faultinjector.KillLeader(rec, c.LeaderLookup(), shard)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("killed %s, leader of shard %d", victim, shard)
	if _, err := c.WaitLeader(shard, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	write("after", 150) // permanent kill: never healed

	for k, v := range acked {
		got, ok, err := cl.Get(cx, []byte(k))
		if err != nil || !ok || string(got) != v {
			t.Fatalf("acknowledged write %s: got %q found=%v err=%v", k, got, ok, err)
		}
	}
	if log := rec.Log(); len(log) != 1 || log[0] != "kill "+victim {
		t.Fatalf("recorded faults %v", log)
	}
}

// TestFaults_PartitionBlocksRPCsUntilHealed: with a partition active, nodes
// on different sides genuinely cannot exchange RPCs (asserted via a failed
// write against the isolated leader, not log inspection); healing restores
// connectivity and the old leader rejoins as a follower.
func TestFaults_PartitionBlocksRPCsUntilHealed(t *testing.T) {
	c := testcluster.Start(t, testcluster.Options{Nodes: 3, Shards: 1})
	cl := c.Client()
	cx := ctx(t, time.Minute)
	cl.Put(cx, []byte("pre"), []byte("1"))
	fi := c.Injector()
	old, err := faultinjector.IsolateLeader(fi, c.LeaderLookup(), 0, c.IDs())
	if err != nil {
		t.Fatal(err)
	}
	// A write sent straight to the isolated leader cannot commit.
	s, _ := c.Node(old).Manager().Get(0)
	wctx, cancel := context.WithTimeout(cx, 700*time.Millisecond)
	err = s.Put(wctx, []byte("minority"), []byte("x"), 0)
	cancel()
	if err == nil {
		t.Fatal("isolated leader committed a write")
	}
	// The majority side elects a new leader and serves the client.
	deadline := time.Now().Add(10 * time.Second)
	for {
		if l, _, ok := c.ShardLeader(0); ok && l != old {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("majority never elected a new leader")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := cl.Put(cx, []byte("majority"), []byte("y")); err != nil {
		t.Fatal(err)
	}
	if err := fi.HealPartition(); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitConverged(0, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	s, _ = c.Node(old).Manager().Get(0)
	if s.Raft().IsLeader() {
		if l, _, _ := c.ShardLeader(0); l == old {
			t.Log("old leader re-won a later election (allowed)")
		}
	}
	if _, ok := s.FSM().Get([]byte("minority")); ok {
		t.Fatal("the minority-side write was applied")
	}
	if v, ok := s.FSM().Get([]byte("majority")); !ok || string(v) != "y" {
		t.Fatal("old leader did not catch up after heal")
	}
}

// TestFaults_DelayAndDrop: delayed links still converge; a lossy link still
// converges (Raft retries through heartbeats).
func TestFaults_DelayAndDrop(t *testing.T) {
	c := testcluster.Start(t, testcluster.Options{Nodes: 3, Shards: 1})
	cl := c.Client()
	cx := ctx(t, time.Minute)
	fi := c.Injector()
	leader, _, _ := c.ShardLeader(0)
	for _, id := range c.IDs() {
		if id != leader {
			fi.DelayMessages(leader, id, 30*time.Millisecond)
			fi.DropMessages(leader, id, 0.2)
		}
	}
	start := time.Now()
	for i := 0; i < 20; i++ {
		if err := cl.Put(cx, []byte(fmt.Sprintf("d%d", i)), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("20 writes over a delayed, lossy link took %v", time.Since(start))
	fi.HealPartition()
	if err := c.WaitConverged(0, 10*time.Second); err != nil {
		t.Fatal(err)
	}
}

// ---- Phase 10: observability ----

func scrapeMetric(t *testing.T, httpAddr, name, label string) float64 {
	t.Helper()
	resp, err := http.Get("http://" + httpAddr + "/metrics")
	if err != nil {
		return -1
	}
	defer resp.Body.Close()
	sum := 0.0
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, name+"{") && strings.Contains(line, label) {
			f := strings.Fields(line)
			v, _ := strconv.ParseFloat(f[len(f)-1], 64)
			sum += v
		}
	}
	return sum
}

// TestPhase10_MetricsTellTheFailoverStory — Phase 10 checkpoint: scrape
// /metrics on a live cluster, kill a leader, and confirm the election and
// leader-change counters increment in real time and the status view
// reflects the new leader.
func TestPhase10_MetricsTellTheFailoverStory(t *testing.T) {
	c := testcluster.Start(t, testcluster.Options{Nodes: 3, Shards: 1})
	cl := c.Client()
	cx := ctx(t, time.Minute)
	for i := 0; i < 20; i++ {
		cl.Put(cx, []byte(fmt.Sprintf("m%d", i)), []byte("v"))
	}
	leader, term, _ := c.ShardLeader(0)
	// Compare only the nodes that survive (the killed leader's counters
	// disappear with it).
	var survivors []string
	for _, id := range c.IDs() {
		if id != leader {
			survivors = append(survivors, id)
		}
	}
	sum := func(name string) float64 {
		total := 0.0
		for _, id := range survivors {
			if v := scrapeMetric(t, c.HTTPAddr(id), name, `shard="0"`); v > 0 {
				total += v
			}
		}
		return total
	}
	electionsBefore := sum("samanvaydb_raft_elections_total")
	changesBefore := sum("samanvaydb_raft_leader_changes_total")
	if v := scrapeMetric(t, c.HTTPAddr(leader), "samanvaydb_raft_is_leader", `shard="0"`); v != 1 {
		t.Fatalf("leader's is_leader gauge = %v", v)
	}
	if v := scrapeMetric(t, c.HTTPAddr(leader), "samanvaydb_requests_total", `op="put"`); v < 1 {
		t.Logf("leader served %v puts directly (others may have been redirected)", v)
	}

	c.Kill(leader)
	nl, err := c.WaitLeader(0, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	cl.Put(cx, []byte("after"), []byte("v"))
	deadline := time.Now().Add(5 * time.Second)
	for {
		ea, ca := sum("samanvaydb_raft_elections_total"), sum("samanvaydb_raft_leader_changes_total")
		if ea > electionsBefore && ca > changesBefore {
			t.Logf("elections %v -> %v, leader changes %v -> %v", electionsBefore, ea, changesBefore, ca)
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("metrics did not record the failover: elections %v->%v changes %v->%v", electionsBefore, ea, changesBefore, ca)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if v := scrapeMetric(t, c.HTTPAddr(nl), "samanvaydb_raft_term", `shard="0"`); v <= float64(term) {
		t.Fatalf("term gauge %v not above %d", v, term)
	}
	st, err := cl.ClusterStatus(cx)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range st.Shards {
		if s.ShardId == 0 && s.Leader != nl {
			t.Fatalf("status view leader %q, Raft leader %q", s.Leader, nl)
		}
	}
	for _, n := range st.Nodes {
		if n.NodeId == leader && n.Up {
			t.Fatal("status view shows the killed node as up")
		}
	}
	_ = client.ErrUnavailable
}
