package raft

import (
	"log/slog"
	"testing"

	"samanvaydb/internal/logging"
)

// noopSM is a StateMachine that does nothing (for node-state tests).
type noopSM struct{}

func (n *noopSM) Apply(_ LogEntry) error      { return nil }
func (n *noopSM) Snapshot() ([]byte, error)   { return nil, nil }
func (n *noopSM) Restore(_ []byte) error      { return nil }

// noopTransport discards all RPC calls (for node-state tests).
type noopTransport struct{}

func (t *noopTransport) SendRequestVote(_ NodeID, _ RequestVoteRequest) (RequestVoteResponse, error) {
	return RequestVoteResponse{}, nil
}
func (t *noopTransport) SendAppendEntries(_ NodeID, _ AppendEntriesRequest) (AppendEntriesResponse, error) {
	return AppendEntriesResponse{}, nil
}
func (t *noopTransport) SendInstallSnapshot(_ NodeID, _ InstallSnapshotRequest) (InstallSnapshotResponse, error) {
	return InstallSnapshotResponse{}, nil
}

func testLogger() *logging.Logger {
	return logging.NewLogger(logging.Options{NodeID: "test", Level: slog.LevelError})
}

func newTestNode(id NodeID, peers []NodeID) *Node {
	return NewNode(id, peers, &noopSM{}, &noopTransport{}, testLogger())
}

// ---- Term rule tests ----

// TestObserveTerm_HigherTermFromFollower verifies that a Follower seeing a
// higher term updates currentTerm and clears votedFor.
func TestObserveTerm_HigherTermFromFollower(t *testing.T) {
	n := newTestNode("node-1", nil)
	n.ps.CurrentTerm = 3

	n.mu.Lock()
	reverted := n.observeTerm(5)
	n.mu.Unlock()

	if !reverted {
		t.Error("expected observeTerm to return true for higher term")
	}
	if n.ps.CurrentTerm != 5 {
		t.Errorf("CurrentTerm = %d, want 5", n.ps.CurrentTerm)
	}
	if n.ps.VotedFor != nil {
		t.Errorf("VotedFor should be nil after term bump, got %v", n.ps.VotedFor)
	}
	if n.state != Follower {
		t.Errorf("state = %v, want Follower", n.state)
	}
}

// TestObserveTerm_HigherTermFromLeader verifies that even a Leader reverts to
// Follower when it sees a higher term — the critical safety property.
func TestObserveTerm_HigherTermFromLeader(t *testing.T) {
	n := newTestNode("node-1", []NodeID{"node-2", "node-3"})
	n.mu.Lock()
	n.becomeCandidate()
	n.becomeLeader()
	n.mu.Unlock()

	if n.CurrentState() != Leader {
		t.Fatal("expected Leader state before test")
	}

	n.mu.Lock()
	n.observeTerm(n.ps.CurrentTerm + 1)
	n.mu.Unlock()

	if n.CurrentState() != Follower {
		t.Errorf("Leader should revert to Follower on higher term, got %v", n.CurrentState())
	}
	if n.ls != nil {
		t.Error("leader volatile state should be cleared on revert")
	}
}

// TestObserveTerm_HigherTermFromCandidate verifies a Candidate reverts on higher term.
func TestObserveTerm_HigherTermFromCandidate(t *testing.T) {
	n := newTestNode("node-1", nil)
	n.mu.Lock()
	n.becomeCandidate() // term becomes 1
	n.mu.Unlock()

	n.mu.Lock()
	n.observeTerm(10)
	n.mu.Unlock()

	if n.CurrentState() != Follower {
		t.Errorf("Candidate should revert to Follower on higher term, got %v", n.CurrentState())
	}
	if n.ps.CurrentTerm != 10 {
		t.Errorf("CurrentTerm = %d, want 10", n.ps.CurrentTerm)
	}
}

// TestObserveTerm_SameTermNoChange verifies equal terms don't trigger a reversion.
func TestObserveTerm_SameTermNoChange(t *testing.T) {
	n := newTestNode("node-1", nil)
	n.mu.Lock()
	n.becomeCandidate() // term=1
	n.mu.Unlock()

	n.mu.Lock()
	reverted := n.observeTerm(1) // same term
	n.mu.Unlock()

	if reverted {
		t.Error("same-term observeTerm should not revert")
	}
	if n.CurrentState() != Candidate {
		t.Errorf("state should remain Candidate, got %v", n.CurrentState())
	}
}

// TestObserveTerm_LowerTermNoChange verifies lower terms are ignored.
func TestObserveTerm_LowerTermNoChange(t *testing.T) {
	n := newTestNode("node-1", nil)
	n.ps.CurrentTerm = 10

	n.mu.Lock()
	reverted := n.observeTerm(3)
	n.mu.Unlock()

	if reverted {
		t.Error("lower-term observeTerm should not revert")
	}
	if n.ps.CurrentTerm != 10 {
		t.Errorf("CurrentTerm changed unexpectedly to %d", n.ps.CurrentTerm)
	}
}

// TestStateString verifies human-readable state names.
func TestStateString(t *testing.T) {
	tests := []struct{ s State; want string }{
		{Follower, "Follower"},
		{Candidate, "Candidate"},
		{Leader, "Leader"},
	}
	for _, tt := range tests {
		if got := tt.s.String(); got != tt.want {
			t.Errorf("State(%d).String() = %q, want %q", tt.s, got, tt.want)
		}
	}
}

// TestBecomeLeader_InitializesPerFollowerState verifies NextIndex/MatchIndex setup.
func TestBecomeLeader_InitializesPerFollowerState(t *testing.T) {
	peers := []NodeID{"node-2", "node-3"}
	n := newTestNode("node-1", peers)

	n.mu.Lock()
	n.becomeCandidate()
	n.becomeLeader()
	lastIdx := n.lastLogIndex()
	n.mu.Unlock()

	if n.ls == nil {
		t.Fatal("leader volatile state is nil after becomeLeader")
	}
	for _, p := range peers {
		if n.ls.NextIndex[p] != lastIdx+1 {
			t.Errorf("NextIndex[%s] = %d, want %d", p, n.ls.NextIndex[p], lastIdx+1)
		}
		if n.ls.MatchIndex[p] != 0 {
			t.Errorf("MatchIndex[%s] = %d, want 0", p, n.ls.MatchIndex[p])
		}
	}
}
