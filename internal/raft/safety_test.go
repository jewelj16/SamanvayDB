package raft

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestCommit_NeverCommitsPriorTermEntryByCount is the dedicated adversarial
// test for Raft §5.4.2 (Figure 8): a leader must not commit an entry from a
// previous term merely because a majority stores it.
func TestCommit_NeverCommitsPriorTermEntryByCount(t *testing.T) {
	n := newTestNode("s1", []NodeID{"s2", "s3", "s4", "s5"})
	// Log: idx1@t1, idx2@t2 (written by an earlier leader in term 2).
	n.ps.Log = append(n.ps.Log, LogEntry{Index: 1, Term: 1}, LogEntry{Index: 2, Term: 2})
	n.vs.CommitIndex, n.vs.LastApplied = 1, 1
	n.ps.CurrentTerm = 3 // we are leader of term 3 now
	n.durableIndex = 2   // both entries are fsynced locally
	n.mu.Lock()
	n.becomeLeader()
	// Index 2 (term 2) is now replicated on a majority: s1, s2, s3.
	n.ls.MatchIndex["s2"] = 2
	n.ls.MatchIndex["s3"] = 2
	n.maybeAdvanceCommitIndex()
	commit := n.vs.CommitIndex
	n.mu.Unlock()
	if commit != 1 {
		t.Fatalf("leader committed prior-term entry by count: commitIndex=%d, want 1", commit)
	}

	// Once an entry of the CURRENT term reaches a majority, it and every
	// earlier entry commit together.
	n.mu.Lock()
	n.ps.Log = append(n.ps.Log, LogEntry{Index: 3, Term: 3})
	n.durableIndex = 3
	n.ls.MatchIndex["s2"] = 3
	n.ls.MatchIndex["s3"] = 3
	n.maybeAdvanceCommitIndex()
	commit = n.vs.CommitIndex
	n.mu.Unlock()
	if commit != 3 {
		t.Fatalf("commitIndex=%d, want 3 after current-term entry reached majority", commit)
	}
}

// TestCommit_LearnersNeverCountTowardQuorum verifies a learner's match index
// is ignored by commit math.
func TestCommit_LearnersNeverCountTowardQuorum(t *testing.T) {
	n := newTestNode("s1", []NodeID{"s2", "s3"})
	n.mu.Lock()
	n.config.Members = append(n.config.Members, Member{ID: "l1", Learner: true}, Member{ID: "l2", Learner: true})
	n.becomeCandidate()
	n.becomeLeader()
	n.ps.Log = append(n.ps.Log, LogEntry{Index: 1, Term: n.ps.CurrentTerm})
	n.durableIndex = 1
	n.ls.MatchIndex["l1"] = 1
	n.ls.MatchIndex["l2"] = 1
	n.maybeAdvanceCommitIndex()
	if n.vs.CommitIndex != 0 {
		t.Fatalf("learners counted toward quorum: commitIndex=%d", n.vs.CommitIndex)
	}
	n.ls.MatchIndex["s2"] = 1
	n.maybeAdvanceCommitIndex()
	if n.vs.CommitIndex != 1 {
		t.Fatalf("commitIndex=%d, want 1 with voters s1+s2", n.vs.CommitIndex)
	}
	n.mu.Unlock()
}

// TestAppendEntries_CommitIndexNeverMovesBackwards verifies a stale
// heartbeat with a small prevLogIndex cannot lower commitIndex.
func TestAppendEntries_CommitIndexNeverMovesBackwards(t *testing.T) {
	n := newTestNode("f", nil)
	n.ps.CurrentTerm = 1
	n.HandleAppendEntries(AppendEntriesRequest{Term: 1, LeaderID: "l", Entries: []LogEntry{
		{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1}}, LeaderCommit: 3})
	n.HandleAppendEntries(AppendEntriesRequest{Term: 1, LeaderID: "l", PrevLogIndex: 1, PrevLogTerm: 1, LeaderCommit: 3})
	if n.vs.CommitIndex != 3 {
		t.Fatalf("commitIndex=%d, want 3", n.vs.CommitIndex)
	}
}

// TestAppendEntries_DuplicateIsIdempotent verifies re-delivering the same
// AppendEntries (same indexes and terms) is a no-op.
func TestAppendEntries_DuplicateIsIdempotent(t *testing.T) {
	applied := []LogEntry{}
	n := newTestNode("f", nil)
	n.sm = &captureSM{applied: &applied}
	n.ps.CurrentTerm = 1
	req := AppendEntriesRequest{Term: 1, LeaderID: "l", Entries: []LogEntry{{Index: 1, Term: 1, Command: []byte("x")}}, LeaderCommit: 1}
	n.HandleAppendEntries(req)
	n.HandleAppendEntries(req)
	if n.lastLogIndex() != 1 || len(applied) != 1 {
		t.Fatalf("duplicate not idempotent: last=%d applied=%d", n.lastLogIndex(), len(applied))
	}
}

// TestAppendEntries_TruncatingCommittedEntryPanics verifies the invariant
// guard: committed entries are never overwritten.
func TestAppendEntries_TruncatingCommittedEntryPanics(t *testing.T) {
	n := newTestNode("f", nil)
	n.ps.CurrentTerm = 2
	n.ps.Log = append(n.ps.Log, LogEntry{Index: 1, Term: 1})
	n.vs.CommitIndex, n.vs.LastApplied = 1, 1
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic when truncating a committed entry")
		}
	}()
	n.HandleAppendEntries(AppendEntriesRequest{Term: 2, LeaderID: "l", Entries: []LogEntry{{Index: 1, Term: 2}}})
}

// TestSnapshot_TriggeredAtThresholdAndLogTruncated verifies automatic
// snapshots and that the log prefix is only dropped after the snapshot.
func TestSnapshot_TriggeredAtThresholdAndLogTruncated(t *testing.T) {
	st := NewMemoryStorage()
	applied := []LogEntry{}
	n, err := NewNode(Config{ID: "a", StateMachine: &captureSM{applied: &applied}, Transport: &noopTransport{},
		Storage: st, Logger: testLogger(), SnapshotThreshold: 10})
	if err != nil {
		t.Fatal(err)
	}
	n.mu.Lock()
	n.becomeCandidate()
	n.startLeading()
	n.mu.Unlock()
	for i := 0; i < 25; i++ {
		if _, _, err := n.Propose([]byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	st0 := n.Status()
	if st0.SnapshotIndex < 20 {
		t.Fatalf("snapshot index = %d, want >= 20", st0.SnapshotIndex)
	}
	_, meta, _, es, _, _ := st.InitialState()
	if meta.Index != st0.SnapshotIndex {
		t.Errorf("stored snapshot index %d != node snapshot index %d", meta.Index, st0.SnapshotIndex)
	}
	if len(es) != int(st0.LastLogIndex-meta.Index) {
		t.Errorf("stored log has %d entries; want only the %d after the snapshot", len(es), st0.LastLogIndex-meta.Index)
	}
}

// TestConfChange_Validation verifies single-server change rules.
func TestConfChange_Validation(t *testing.T) {
	base := NewMembership("a", "b", "c")
	tests := []struct {
		name string
		cc   ConfChange
		ok   bool
	}{
		{"add_learner", ConfChange{Type: AddLearner, Node: "d"}, true},
		{"add_existing", ConfChange{Type: AddLearner, Node: "a"}, false},
		{"promote_voter", ConfChange{Type: PromoteLearner, Node: "a"}, false},
		{"remove_member", ConfChange{Type: RemoveMember, Node: "b"}, true},
		{"remove_unknown", ConfChange{Type: RemoveMember, Node: "z"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := applyConfChange(base, tt.cc)
			if (err == nil) != tt.ok {
				t.Errorf("err=%v, want ok=%v", err, tt.ok)
			}
		})
	}
	m, _ := applyConfChange(base, ConfChange{Type: AddLearner, Node: "d"})
	if m.IsVoter("d") || !m.Contains("d") || m.Quorum() != 2 {
		t.Errorf("learner must not change quorum: %v quorum=%d", m, m.Quorum())
	}
	m, _ = applyConfChange(m, ConfChange{Type: PromoteLearner, Node: "d"})
	if !m.IsVoter("d") || m.Quorum() != 3 {
		t.Errorf("promoted voter must count: %v quorum=%d", m, m.Quorum())
	}
	if _, err := applyConfChange(NewMembership("a"), ConfChange{Type: RemoveMember, Node: "a"}); err == nil {
		t.Error("removing the last voter must fail")
	}
}

// TestConfChange_OneAtATime verifies a second change is rejected while the
// first is uncommitted.
func TestConfChange_OneAtATime(t *testing.T) {
	n := newTestNode("a", []NodeID{"b", "c"})
	n.mu.Lock()
	n.becomeCandidate()
	n.startLeading()
	// Commit the no-op (pretend b acknowledged it).
	n.ls.MatchIndex["b"] = n.lastLogIndex()
	n.maybeAdvanceCommitIndex()
	n.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := n.ProposeConfChange(ctx, ConfChange{Type: AddLearner, Node: "d"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first change should be pending (uncommitted), got %v", err)
	}
	_, err = n.ProposeConfChange(context.Background(), ConfChange{Type: AddLearner, Node: "e"})
	if !errors.Is(err, ErrConfigChangeInProgress) {
		t.Fatalf("second concurrent change: got %v, want ErrConfigChangeInProgress", err)
	}
	// The pending config is already in effect on the leader (append-time).
	if !n.Membership().Contains("d") {
		t.Error("config must take effect when appended")
	}
}

// TestPreVote_DoesNotChangeReceiverState verifies a pre-vote request never
// bumps the receiver's term or records a vote.
func TestPreVote_DoesNotChangeReceiverState(t *testing.T) {
	n := newTestNode("v", nil)
	n.ps.CurrentTerm = 4
	resp := n.HandleRequestVote(RequestVoteRequest{Term: 9, CandidateID: "c", PreVote: true})
	if !resp.VoteGranted {
		t.Error("pre-vote should be granted to an up-to-date candidate when no leader is known")
	}
	if n.ps.CurrentTerm != 4 || n.ps.VotedFor != nil {
		t.Errorf("pre-vote changed state: term=%d votedFor=%v", n.ps.CurrentTerm, n.ps.VotedFor)
	}
	// With a live leader, pre-votes are refused (leader stickiness).
	n.mu.Lock()
	n.recordLeaderContactLocked("leader")
	n.mu.Unlock()
	if n.HandleRequestVote(RequestVoteRequest{Term: 9, CandidateID: "c", PreVote: true}).VoteGranted {
		t.Error("pre-vote must be refused while a leader is live")
	}
}

// TestCommit_LeaderCountsItselfOnlyOnceDurable: with group commit the leader
// replicates entries before its own fsync finishes; it must not count itself
// toward a majority for entries that are not yet durable locally.
func TestCommit_LeaderCountsItselfOnlyOnceDurable(t *testing.T) {
	n := newTestNode("s1", []NodeID{"s2", "s3"})
	n.mu.Lock()
	defer n.mu.Unlock()
	n.becomeCandidate()
	n.becomeLeader()
	n.ps.Log = append(n.ps.Log, LogEntry{Index: 1, Term: n.ps.CurrentTerm}) // written, not synced
	n.ls.MatchIndex["s2"] = 1
	n.maybeAdvanceCommitIndex()
	if n.vs.CommitIndex != 0 {
		t.Fatal("leader counted a non-durable entry toward the majority")
	}
	n.ls.MatchIndex["s3"] = 1 // two durable followers: a majority without the leader
	n.maybeAdvanceCommitIndex()
	if n.vs.CommitIndex != 1 {
		t.Fatalf("commitIndex=%d, want 1 via follower majority", n.vs.CommitIndex)
	}
}
