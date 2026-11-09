package raft

import (
	"testing"
)

// ---- AppendEntries handler tests ----

// TestAppendEntries_Heartbeat verifies that an empty AppendEntries (heartbeat)
// from a valid leader is accepted and triggers a heartbeat notification.
func TestAppendEntries_Heartbeat(t *testing.T) {
	n := newTestNode("node-1", nil)
	n.ps.CurrentTerm = 1

	resp := n.HandleAppendEntries(AppendEntriesRequest{
		Term:         1,
		LeaderID:     "node-0",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      nil,
		LeaderCommit: 0,
	})

	if !resp.Success {
		t.Errorf("heartbeat should succeed, got success=false term=%d", resp.Term)
	}
}

// TestAppendEntries_StaleTerm rejects AppendEntries from a stale leader.
func TestAppendEntries_StaleTerm(t *testing.T) {
	n := newTestNode("node-1", nil)
	n.ps.CurrentTerm = 5

	resp := n.HandleAppendEntries(AppendEntriesRequest{
		Term:     3,
		LeaderID: "node-0",
	})

	if resp.Success {
		t.Error("should reject AppendEntries with stale term")
	}
	if resp.Term != 5 {
		t.Errorf("response Term = %d, want 5", resp.Term)
	}
}

// TestAppendEntries_HigherTermReverts verifies the universal term rule fires
// and the node reverts to Follower when a higher-term AppendEntries arrives.
func TestAppendEntries_HigherTermReverts(t *testing.T) {
	n := newTestNode("node-1", []NodeID{"node-2", "node-3"})
	n.mu.Lock()
	n.becomeCandidate() // term=1
	n.mu.Unlock()

	n.HandleAppendEntries(AppendEntriesRequest{
		Term:     5,
		LeaderID: "node-0",
	})

	if n.CurrentState() != Follower {
		t.Errorf("should revert to Follower on higher-term AppendEntries, got %v", n.CurrentState())
	}
	if n.CurrentTerm() != 5 {
		t.Errorf("CurrentTerm = %d, want 5", n.CurrentTerm())
	}
}

// TestAppendEntries_ConsistencyCheck_MissingPrevEntry rejects when follower
// doesn't have the entry at PrevLogIndex.
func TestAppendEntries_ConsistencyCheck_MissingPrevEntry(t *testing.T) {
	n := newTestNode("node-1", nil)
	n.ps.CurrentTerm = 1
	// Follower log: sentinel only (len=1, lastIndex=0)

	resp := n.HandleAppendEntries(AppendEntriesRequest{
		Term:         1,
		LeaderID:     "node-0",
		PrevLogIndex: 2, // follower doesn't have index 2
		PrevLogTerm:  1,
	})

	if resp.Success {
		t.Error("should reject: PrevLogIndex beyond follower's log length")
	}
	if resp.ConflictIndex != 1 {
		t.Errorf("ConflictIndex = %d, want 1 (len of log)", resp.ConflictIndex)
	}
}

// TestAppendEntries_ConsistencyCheck_TermMismatch rejects when PrevLogTerm
// doesn't match the entry at PrevLogIndex.
func TestAppendEntries_ConsistencyCheck_TermMismatch(t *testing.T) {
	n := newTestNode("node-1", nil)
	n.ps.CurrentTerm = 3
	n.ps.Log = append(n.ps.Log, LogEntry{Index: 1, Term: 1}) // follower has term=1 at idx 1

	resp := n.HandleAppendEntries(AppendEntriesRequest{
		Term:         3,
		LeaderID:     "node-0",
		PrevLogIndex: 1,
		PrevLogTerm:  2, // mismatch: leader says term=2, follower has term=1
	})

	if resp.Success {
		t.Error("should reject: PrevLogTerm mismatch")
	}
	if resp.ConflictTerm != 1 {
		t.Errorf("ConflictTerm = %d, want 1", resp.ConflictTerm)
	}
}

// TestAppendEntries_AppendsNewEntries verifies new entries are appended to log.
func TestAppendEntries_AppendsNewEntries(t *testing.T) {
	n := newTestNode("node-1", nil)
	n.ps.CurrentTerm = 1

	resp := n.HandleAppendEntries(AppendEntriesRequest{
		Term:         1,
		LeaderID:     "node-0",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []LogEntry{
			{Index: 1, Term: 1, Command: []byte("put:a=1")},
			{Index: 2, Term: 1, Command: []byte("put:b=2")},
		},
		LeaderCommit: 0,
	})

	if !resp.Success {
		t.Fatalf("expected success, got false")
	}
	if n.lastLogIndex() != 2 {
		t.Errorf("lastLogIndex = %d, want 2", n.lastLogIndex())
	}
}

// TestAppendEntries_TruncatesConflictingEntries verifies that conflicting entries
// are removed and replaced by the leader's entries.
func TestAppendEntries_TruncatesConflictingEntries(t *testing.T) {
	n := newTestNode("node-1", nil)
	n.ps.CurrentTerm = 3
	// Follower has a conflicting entry at index 1 (stale term).
	n.ps.Log = append(n.ps.Log, LogEntry{Index: 1, Term: 1})

	resp := n.HandleAppendEntries(AppendEntriesRequest{
		Term:         3,
		LeaderID:     "node-0",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []LogEntry{
			{Index: 1, Term: 3, Command: []byte("put:x=1")}, // leader's version
		},
		LeaderCommit: 0,
	})

	if !resp.Success {
		t.Fatal("should succeed after truncating conflict")
	}
	if n.ps.Log[1].Term != 3 {
		t.Errorf("log[1].Term = %d, want 3 (conflicting entry should be replaced)", n.ps.Log[1].Term)
	}
}

// TestAppendEntries_AdvancesCommitIndex verifies that commitIndex advances
// when LeaderCommit is higher than the current commitIndex.
func TestAppendEntries_AdvancesCommitIndex(t *testing.T) {
	applied := make([]LogEntry, 0)
	sm := &captureSM{applied: &applied}

	n := newTestNode("node-1", nil)
	n.sm = sm
	n.ps.CurrentTerm = 1

	n.HandleAppendEntries(AppendEntriesRequest{
		Term:         1,
		LeaderID:     "node-0",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []LogEntry{
			{Index: 1, Term: 1, Command: []byte("cmd1")},
			{Index: 2, Term: 1, Command: []byte("cmd2")},
		},
		LeaderCommit: 2, // leader says both are committed
	})

	n.mu.Lock()
	commitIdx := n.vs.CommitIndex
	lastApplied := n.vs.LastApplied
	n.mu.Unlock()

	if commitIdx != 2 {
		t.Errorf("commitIndex = %d, want 2", commitIdx)
	}
	if lastApplied != 2 {
		t.Errorf("lastApplied = %d, want 2", lastApplied)
	}
	if len(applied) != 2 {
		t.Errorf("applied %d entries, want 2", len(applied))
	}
}

// TestPropose_RejectsNonLeader verifies Propose returns ErrNotLeader on a Follower.
func TestPropose_RejectsNonLeader(t *testing.T) {
	n := newTestNode("node-1", nil)

	_, _, err := n.Propose([]byte("cmd"))
	if err != ErrNotLeader {
		t.Errorf("Propose on Follower should return ErrNotLeader, got %v", err)
	}
}

// TestPropose_AppendsToLeaderLog verifies a Leader's Propose appends to its log.
func TestPropose_AppendsToLeaderLog(t *testing.T) {
	n := newTestNode("node-1", []NodeID{})
	n.mu.Lock()
	n.becomeCandidate()
	n.becomeLeader()
	n.mu.Unlock()

	if _, _, err := n.Propose([]byte("put:k=v")); err != nil {
		t.Fatalf("Propose on Leader failed: %v", err)
	}
	if n.lastLogIndex() != 1 {
		t.Errorf("lastLogIndex = %d, want 1 after propose", n.lastLogIndex())
	}
}

// captureSM is a StateMachine that records applied entries.
type captureSM struct {
	applied *[]LogEntry
}

func (s *captureSM) Apply(e LogEntry) any {
	*s.applied = append(*s.applied, e)
	return nil
}
func (s *captureSM) Snapshot() ([]byte, error) { return nil, nil }
func (s *captureSM) Restore(_ []byte) error    { return nil }
