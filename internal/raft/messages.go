package raft

// RPC message types used between Raft nodes.

// ---- RequestVote ----

// RequestVoteRequest is sent by a Candidate to gather votes.
type RequestVoteRequest struct {
	Term         uint64 // candidate's term (for a pre-vote: the term it WOULD use)
	CandidateID  NodeID // candidate requesting vote
	LastLogIndex uint64 // index of candidate's last log entry
	LastLogTerm  uint64 // term of candidate's last log entry
	// PreVote marks a Pre-Vote probe (Raft dissertation §9.6): the receiver
	// answers "would I vote for you?" without changing any of its own state.
	// A node only starts a real election (and bumps its term) after winning
	// a pre-vote majority, so a partitioned node rejoining the cluster cannot
	// disrupt a healthy leader with an inflated term.
	PreVote bool
}

// RequestVoteResponse is the voter's reply.
type RequestVoteResponse struct {
	Term        uint64 // responder's currentTerm (so candidate can update itself)
	VoteGranted bool
}

// ---- AppendEntries ----

// AppendEntriesRequest is sent by the Leader both for log replication and
// as a heartbeat (Entries empty).
type AppendEntriesRequest struct {
	Term         uint64     // leader's term
	LeaderID     NodeID     // so followers can redirect clients
	PrevLogIndex uint64     // index of log entry immediately preceding new ones
	PrevLogTerm  uint64     // term of PrevLogIndex entry
	Entries      []LogEntry // new entries to append (empty for heartbeat)
	LeaderCommit uint64     // leader's commitIndex
}

// AppendEntriesResponse is the follower's reply.
type AppendEntriesResponse struct {
	Term    uint64 // responder's currentTerm
	Success bool
	// ConflictIndex is the earliest index the leader should retry from when
	// Success=false, enabling faster log backtracking than decrementing by 1.
	ConflictIndex uint64
	// ConflictTerm is the term of the follower's conflicting entry (0 if the
	// follower's log is simply too short).
	ConflictTerm uint64
}

// ---- InstallSnapshot ----

// InstallSnapshotRequest is sent by the Leader when a follower is too far
// behind to catch up via normal AppendEntries (the entries it needs have
// already been compacted into a snapshot).
type InstallSnapshotRequest struct {
	Term              uint64 // leader's term
	LeaderID          NodeID
	LastIncludedIndex uint64     // snapshot covers log up to and including this index
	LastIncludedTerm  uint64     // term of LastIncludedIndex entry
	Config            Membership // configuration as of LastIncludedIndex
	Data              []byte     // raw snapshot bytes
}

// InstallSnapshotResponse is the follower's reply.
type InstallSnapshotResponse struct {
	Term uint64 // responder's currentTerm
}
