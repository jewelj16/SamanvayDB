package metrics

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"samanvaydb/internal/raft"
)

// TestElectionCounter_ExactlyOncePerElection drives a real single-node Raft
// group through one election and checks the counter moved by exactly one.
func TestElectionCounter_ExactlyOncePerElection(t *testing.T) {
	m := New("n1", nil, nil)
	node, err := raft.NewNode(raft.Config{ID: "n1", Hooks: m.RaftHooks(4),
		ElectionTimeoutMin: 20 * time.Millisecond, ElectionTimeoutMax: 40 * time.Millisecond, HeartbeatInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	node.Start()
	defer node.Stop()
	deadline := time.Now().Add(3 * time.Second)
	for !node.IsLeader() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond) // a healthy single-node leader never re-elects
	if got := testutil.ToFloat64(m.Elections.WithLabelValues("4")); got != 1 {
		t.Fatalf("elections_total = %v, want 1", got)
	}
	// The no-op entry committed.
	if got := testutil.ToFloat64(m.Committed.WithLabelValues("4")); got < 1 {
		t.Fatalf("committed_entries_total = %v, want >= 1", got)
	}
}

func TestLeaderChanges_CountsOnlyChanges(t *testing.T) {
	m := New("n1", nil, nil)
	h := m.RaftHooks(0)
	h.OnStateChange(1, raft.Follower, "a") // first leader seen: not a change
	h.OnStateChange(1, raft.Follower, "a")
	h.OnStateChange(2, raft.Follower, "")  // leader unknown: ignored
	h.OnStateChange(2, raft.Follower, "b") // a -> b
	h.OnStateChange(3, raft.Leader, "n1")  // b -> n1
	if got := testutil.ToFloat64(m.LeaderChanges.WithLabelValues("0")); got != 2 {
		t.Fatalf("leader_changes_total = %v, want 2", got)
	}
}

func TestSnapshotAndCommitCounters(t *testing.T) {
	m := New("n1", nil, nil)
	h := m.RaftHooks(-1)
	h.OnCommit(5)
	h.OnCommit(9)
	h.OnSnapshot(9, 2, false)
	h.OnSnapshot(20, 3, true)
	h.OnConfigChange(10, raft.NewMembership("a"), false)
	h.OnConfigChange(10, raft.NewMembership("a"), true)
	if got := testutil.ToFloat64(m.Committed.WithLabelValues("meta")); got != 9 {
		t.Errorf("committed = %v, want 9", got)
	}
	if got := testutil.ToFloat64(m.Snapshots.WithLabelValues("meta", "installed")); got != 1 {
		t.Errorf("installed snapshots = %v", got)
	}
	if got := testutil.ToFloat64(m.ConfigChanges.WithLabelValues("meta")); got != 1 {
		t.Errorf("membership changes = %v, want 1 (committed only)", got)
	}
}

func TestEndpoint_ExposesGaugesAndRequests(t *testing.T) {
	status := func() []ShardStatus {
		return []ShardStatus{{ShardID: 2, Keys: 42, Raft: raft.Status{State: raft.Leader, Term: 7, CommitIndex: 100,
			LastApplied: 100, LastLogIndex: 103, SnapshotIndex: 50, LastLeaderContact: time.Now(),
			Progress: map[raft.NodeID]raft.PeerProgress{"n2": {Match: 90}}}}}
	}
	m := New("n1", status, func() (int, int) { return 2, 3 })
	m.ObserveRequest("put", "OK", 3*time.Millisecond)
	srv := httptest.NewServer(m.Handler())
	defer srv.Close()
	resp, err := srv.Client().Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	text := string(body)
	for _, want := range []string{
		`samanvaydb_raft_term{node_id="n1",shard="2"} 7`,
		`samanvaydb_raft_is_leader{node_id="n1",shard="2"} 1`,
		`samanvaydb_storage_keys{node_id="n1",shard="2"} 42`,
		`samanvaydb_raft_log_entries{node_id="n1",shard="2"} 53`,
		`samanvaydb_raft_replication_lag_entries{node_id="n1",peer="n2",shard="2"} 13`,
		`samanvaydb_requests_total{code="OK",node_id="n1",op="put"} 1`,
		`samanvaydb_cluster_peers_reachable{node_id="n1"} 2`,
		`samanvaydb_request_duration_seconds_bucket{node_id="n1",op="put",le="0.005"} 1`,
	} {
		if !strings.Contains(text, want) {
			t.Errorf("missing %q", want)
		}
	}
}
