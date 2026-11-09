package raft

import (
	"bytes"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"samanvaydb/internal/logging"
)

// syncBuffer is a goroutine-safe log sink.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) lines(t *testing.T) []map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []map[string]any
	for _, l := range strings.Split(strings.TrimSpace(s.b.String()), "\n") {
		if l == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(l), &m); err != nil {
			t.Fatalf("log line is not JSON: %q", l)
		}
		out = append(out, m)
	}
	return out
}

// TestStructuredLogs_OneLinePerTransition — Commit 63: every major state
// transition emits exactly one structured line carrying node_id, shard_id
// and term.
func TestStructuredLogs_OneLinePerTransition(t *testing.T) {
	sink := &syncBuffer{}
	logger := logging.NewLogger(logging.Options{NodeID: "n1", JSON: true, Output: sink}).WithShard(3)
	applied := []LogEntry{}
	n, err := NewNode(Config{ID: "n1", Peers: []NodeID{"n2"}, StateMachine: &captureSM{applied: &applied},
		Transport: &noopTransport{}, Logger: logger})
	if err != nil {
		t.Fatal(err)
	}

	n.mu.Lock()
	n.becomeCandidate() // election started
	n.becomeLeader()    // became leader
	n.ps.Log = append(n.ps.Log, LogEntry{Index: 1, Term: n.ps.CurrentTerm, Type: EntryConfig,
		Command: NewMembership("n1", "n2", "n3").Encode()})
	n.durableIndex = 1
	n.ls.MatchIndex["n2"] = 1
	n.maybeAdvanceCommitIndex()         // membership change committed
	n.takeSnapshotLocked()              // snapshot created
	n.observeTerm(n.ps.CurrentTerm + 5) // higher term observed + stepped down
	n.mu.Unlock()

	f, _ := NewNode(Config{ID: "n2", StateMachine: &captureSM{applied: &applied}, Transport: &noopTransport{},
		Logger: logging.NewLogger(logging.Options{NodeID: "n2", JSON: true, Output: sink}).WithShard(3)})
	f.HandleInstallSnapshot(InstallSnapshotRequest{Term: 9, LeaderID: "n1", LastIncludedIndex: 1, LastIncludedTerm: 1,
		Config: NewMembership("n1", "n2")}) // snapshot installed

	// node -> transition -> count
	want := map[string]map[string]int{
		"n1": {"election started": 0, "became leader": 0, "membership change committed": 0,
			"snapshot created": 0, "higher term observed — reverting to Follower": 0, "stepped down from leader": 0},
		"n2": {"higher term observed — reverting to Follower": 0, "snapshot installed": 0},
	}
	for _, l := range sink.lines(t) {
		msg, _ := l["msg"].(string)
		node, _ := l["node_id"].(string)
		if _, ok := want[node][msg]; !ok {
			continue
		}
		want[node][msg]++
		for _, field := range []string{"node_id", "shard_id", "term"} {
			if _, ok := l[field]; !ok {
				t.Errorf("%q line lacks %s: %v", msg, field, l)
			}
		}
	}
	for node, msgs := range want {
		for msg, count := range msgs {
			if count != 1 {
				t.Errorf("%s: transition %q logged %d times, want exactly 1", node, msg, count)
			}
		}
	}
}
