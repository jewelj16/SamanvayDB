// Package raft implements the Raft consensus algorithm as a standalone library.
// It is completely independent of internal/storage — Raft replicates an ordered
// log of opaque commands and guarantees every node applies them in the same order.
// The concrete storage engine is wired in Phase 4 via the StateMachine interface.
//
// Key invariants (enforced throughout):
//  1. Never apply a write to the state machine before Raft has committed it.
//  2. currentTerm, votedFor, and log entries are fsynced before responding to RPCs.
//  3. Apply() is a pure, deterministic function — no wall-clock reads, no randomness.
package raft

import "fmt"

// NodeID uniquely identifies a Raft peer within a cluster.
type NodeID string

// State represents the three Raft node roles.
type State uint8

const (
	Follower  State = iota // default state; votes for candidates, applies committed entries
	Candidate              // seeking election; has incremented term and voted for itself
	Leader                 // replicates log entries; sends heartbeats to suppress elections
)

func (s State) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return fmt.Sprintf("Unknown(%d)", s)
	}
}

// LogEntry is one entry in the replicated Raft log.
// Command is opaque bytes — Raft doesn't know what a "PUT" means.
type LogEntry struct {
	Index   uint64 // 1-based; 0 means "no entry"
	Term    uint64 // term when the leader created this entry
	Command []byte // serialized Command (from internal/command)
}

// PersistentState holds the fields that must be fsynced before responding to
// any RPC. Losing these across a restart violates Raft safety guarantees:
//   - currentTerm: prevents voting twice in the same term after a restart.
//   - votedFor:    prevents granting a conflicting vote in an already-voted term.
//   - log:         a restarted node must prove to the cluster what it replicated.
type PersistentState struct {
	CurrentTerm uint64  // latest term this node has seen (monotonically increasing)
	VotedFor    *NodeID // candidate voted for in CurrentTerm (nil if none)
	Log         []LogEntry
}

// VolatileState holds fields that are safely re-derived on restart.
type VolatileState struct {
	CommitIndex uint64 // index of highest log entry known to be committed
	LastApplied uint64 // index of highest log entry applied to the state machine
}

// LeaderVolatileState holds per-follower tracking that only the leader maintains.
// Reset to initial values on every new election.
type LeaderVolatileState struct {
	NextIndex  map[NodeID]uint64 // next log index to send to each follower
	MatchIndex map[NodeID]uint64 // highest log index known replicated on each follower
}
