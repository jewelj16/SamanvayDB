package raft

import (
	"testing"
)

// voteID is a helper to make a *NodeID inline.
func voteID(id NodeID) *NodeID { return &id }

// TestRequestVote_GrantsVote_BasicCase verifies vote is granted when all
// conditions are met.
func TestRequestVote_GrantsVote_BasicCase(t *testing.T) {
	n := newTestNode("node-1", nil)

	resp := n.HandleRequestVote(RequestVoteRequest{
		Term:         1,
		CandidateID:  "node-2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if !resp.VoteGranted {
		t.Error("expected vote to be granted")
	}
	if n.ps.VotedFor == nil || *n.ps.VotedFor != "node-2" {
		t.Errorf("VotedFor = %v, want node-2", n.ps.VotedFor)
	}
}

// TestRequestVote_StaleTerm rejects a candidate with a lower term.
func TestRequestVote_StaleTerm(t *testing.T) {
	n := newTestNode("node-1", nil)
	n.ps.CurrentTerm = 5

	resp := n.HandleRequestVote(RequestVoteRequest{
		Term:        3,
		CandidateID: "node-2",
	})

	if resp.VoteGranted {
		t.Error("should reject vote for stale term")
	}
	if resp.Term != 5 {
		t.Errorf("response Term = %d, want 5", resp.Term)
	}
}

// TestRequestVote_AlreadyVotedForOther rejects when votedFor is set to a
// different candidate.
func TestRequestVote_AlreadyVotedForOther(t *testing.T) {
	n := newTestNode("node-1", nil)
	n.ps.CurrentTerm = 1
	other := NodeID("node-3")
	n.ps.VotedFor = &other

	resp := n.HandleRequestVote(RequestVoteRequest{
		Term:         1,
		CandidateID:  "node-2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if resp.VoteGranted {
		t.Error("should reject vote when already voted for another candidate")
	}
}

// TestRequestVote_SameCandidateRetryAllowed verifies voting for the same
// candidate again in the same term is allowed (idempotent retry).
func TestRequestVote_SameCandidateRetryAllowed(t *testing.T) {
	n := newTestNode("node-1", nil)
	n.ps.CurrentTerm = 1
	cid := NodeID("node-2")
	n.ps.VotedFor = &cid

	resp := n.HandleRequestVote(RequestVoteRequest{
		Term:         1,
		CandidateID:  "node-2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if !resp.VoteGranted {
		t.Error("same-candidate retry should be granted")
	}
}

// TestRequestVote_LogNotUpToDate_LowerTerm rejects when candidate's last log
// term is lower than voter's.
func TestRequestVote_LogNotUpToDate_LowerTerm(t *testing.T) {
	n := newTestNode("node-1", nil)
	// Give the voter a log entry in term 3.
	n.ps.Log = append(n.ps.Log, LogEntry{Index: 1, Term: 3})

	resp := n.HandleRequestVote(RequestVoteRequest{
		Term:         4,
		CandidateID:  "node-2",
		LastLogIndex: 1,
		LastLogTerm:  2, // candidate's last term is lower
	})

	if resp.VoteGranted {
		t.Error("should reject: candidate log term 2 < voter log term 3")
	}
}

// TestRequestVote_LogNotUpToDate_ShorterLog rejects when terms are equal but
// candidate's log is shorter.
func TestRequestVote_LogNotUpToDate_ShorterLog(t *testing.T) {
	n := newTestNode("node-1", nil)
	// Voter has 3 entries in term 2.
	n.ps.Log = append(n.ps.Log,
		LogEntry{Index: 1, Term: 2},
		LogEntry{Index: 2, Term: 2},
		LogEntry{Index: 3, Term: 2},
	)

	resp := n.HandleRequestVote(RequestVoteRequest{
		Term:         3,
		CandidateID:  "node-2",
		LastLogIndex: 2, // candidate only has 2 entries
		LastLogTerm:  2, // same term
	})

	if resp.VoteGranted {
		t.Error("should reject: candidate log shorter at same term")
	}
}

// TestRequestVote_LogUpToDate_HigherTerm grants when candidate's last log term
// is higher, regardless of length.
func TestRequestVote_LogUpToDate_HigherTerm(t *testing.T) {
	n := newTestNode("node-1", nil)
	n.ps.Log = append(n.ps.Log,
		LogEntry{Index: 1, Term: 1},
		LogEntry{Index: 2, Term: 1},
		LogEntry{Index: 3, Term: 1},
	)

	resp := n.HandleRequestVote(RequestVoteRequest{
		Term:         5,
		CandidateID:  "node-2",
		LastLogIndex: 1, // shorter log
		LastLogTerm:  5, // but higher term — wins
	})

	if !resp.VoteGranted {
		t.Error("should grant: candidate last log term 5 > voter last log term 1")
	}
}

// TestRequestVote_HigherTermUpdatesCurrentTerm verifies observeTerm fires when
// a higher-term RequestVote arrives.
func TestRequestVote_HigherTermUpdatesCurrentTerm(t *testing.T) {
	n := newTestNode("node-1", nil)
	n.ps.CurrentTerm = 3

	n.HandleRequestVote(RequestVoteRequest{
		Term:        10,
		CandidateID: "node-2",
	})

	if n.ps.CurrentTerm != 10 {
		t.Errorf("CurrentTerm = %d, want 10 after seeing higher-term request", n.ps.CurrentTerm)
	}
}
