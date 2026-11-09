// Package raft implements the Raft consensus algorithm as a standalone library.
// It is completely independent of internal/storage — Raft replicates an ordered
// log of opaque commands and guarantees every node applies them in the same
// order. The concrete state machine is wired in by internal/shard via the
// StateMachine interface.
//
// Implemented: leader election with randomized timeouts and Pre-Vote,
// heartbeats, log replication with fast conflict backtracking, the
// current-term commit rule, durable persistence (term, vote, log), snapshots
// and InstallSnapshot, single-server membership changes with non-voting
// learners, check-quorum leader step-down, and ReadIndex linearizable reads.
//
// Key invariants (enforced throughout):
//  1. Never apply a write to the state machine before Raft has committed it.
//  2. currentTerm, votedFor, and log entries are fsynced before responding to RPCs.
//  3. Apply() is a pure, deterministic function — no wall-clock reads, no randomness.
//  4. commitIndex and lastApplied never move backwards; committed entries are
//     never truncated (violations panic — they indicate a bug, not bad luck).
package raft

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

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

// EntryType distinguishes client commands from Raft-internal entries.
type EntryType uint8

const (
	// EntryNormal carries an opaque state-machine command.
	EntryNormal EntryType = iota
	// EntryConfig carries a full encoded Membership. A configuration takes
	// effect on each server as soon as it is appended to that server's log
	// (not when committed), per the Raft dissertation §4.1.
	EntryConfig
	// EntryNoop is appended by every new leader so it can commit entries
	// from previous terms (which may never be committed by counting alone).
	EntryNoop
)

func (t EntryType) String() string {
	switch t {
	case EntryNormal:
		return "normal"
	case EntryConfig:
		return "config"
	case EntryNoop:
		return "noop"
	default:
		return fmt.Sprintf("unknown(%d)", t)
	}
}

// LogEntry is one entry in the replicated Raft log.
// Command is opaque bytes — Raft doesn't know what a "PUT" means.
type LogEntry struct {
	Index   uint64 // 1-based; 0 means "no entry"
	Term    uint64 // term when the leader created this entry
	Type    EntryType
	Command []byte // serialized Command (from internal/command), or Membership for EntryConfig
}

// Member is one server in a Raft group's configuration.
type Member struct {
	ID NodeID `json:"id"`
	// Addr is an opaque transport address carried with the configuration so
	// every replica learns how to reach newly added members. Raft itself
	// never interprets it.
	Addr string `json:"addr,omitempty"`
	// Learner members receive the replicated log but never vote and are
	// never counted toward any quorum.
	Learner bool `json:"learner,omitempty"`
}

// Membership is a Raft group's configuration. Members are kept sorted by ID
// so that encodings are deterministic.
type Membership struct {
	Members []Member `json:"members"`
}

// NewMembership builds a voter-only membership from IDs.
func NewMembership(voters ...NodeID) Membership {
	m := Membership{}
	for _, v := range voters {
		m.Members = append(m.Members, Member{ID: v})
	}
	m.normalize()
	return m
}

func (m *Membership) normalize() {
	sort.Slice(m.Members, func(i, j int) bool { return m.Members[i].ID < m.Members[j].ID })
}

// Clone returns a deep copy.
func (m Membership) Clone() Membership {
	out := Membership{Members: make([]Member, len(m.Members))}
	copy(out.Members, m.Members)
	return out
}

// Get returns the member with the given ID.
func (m Membership) Get(id NodeID) (Member, bool) {
	for _, mem := range m.Members {
		if mem.ID == id {
			return mem, true
		}
	}
	return Member{}, false
}

// Contains reports whether id is a voter or learner.
func (m Membership) Contains(id NodeID) bool {
	_, ok := m.Get(id)
	return ok
}

// IsVoter reports whether id is a voting member.
func (m Membership) IsVoter(id NodeID) bool {
	mem, ok := m.Get(id)
	return ok && !mem.Learner
}

// Voters returns the voting member IDs in sorted order.
func (m Membership) Voters() []NodeID {
	var out []NodeID
	for _, mem := range m.Members {
		if !mem.Learner {
			out = append(out, mem.ID)
		}
	}
	return out
}

// Learners returns the learner IDs in sorted order.
func (m Membership) Learners() []NodeID {
	var out []NodeID
	for _, mem := range m.Members {
		if mem.Learner {
			out = append(out, mem.ID)
		}
	}
	return out
}

// Quorum returns the number of votes required for a majority of voters.
func (m Membership) Quorum() int {
	return len(m.Voters())/2 + 1
}

// Encode serializes the membership deterministically.
func (m Membership) Encode() []byte {
	c := m.Clone()
	c.normalize()
	b, err := json.Marshal(c)
	if err != nil {
		panic(fmt.Sprintf("raft: encode membership: %v", err)) // cannot happen for this type
	}
	return b
}

// DecodeMembership parses an encoded membership. Empty input yields an
// empty membership.
func DecodeMembership(b []byte) (Membership, error) {
	var m Membership
	if len(b) == 0 {
		return m, nil
	}
	if err := json.Unmarshal(b, &m); err != nil {
		return m, fmt.Errorf("raft: decode membership: %w", err)
	}
	m.normalize()
	return m, nil
}

func (m Membership) String() string {
	return fmt.Sprintf("voters=%v learners=%v", m.Voters(), m.Learners())
}

// PersistentState holds the fields that must be fsynced before responding to
// any RPC. Losing these across a restart violates Raft safety guarantees:
//   - currentTerm: prevents voting twice in the same term after a restart.
//   - votedFor:    prevents granting a conflicting vote in an already-voted term.
//   - log:         a restarted node must prove to the cluster what it replicated.
//
// Log[0] is a sentinel holding the index/term of the last entry covered by
// the most recent snapshot (0/0 if there is none), so prevLogIndex checks
// never need special cases at the log boundary.
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

	lastContact map[NodeID]int64  // unix nanos of last response in this term (check-quorum)
	ackSeq      map[NodeID]uint64 // highest ReadIndex round each follower has acknowledged
	readSeq     uint64            // current ReadIndex round
}

// PeerProgress is the leader's view of one follower, exposed via Status.
type PeerProgress struct {
	Match   uint64
	Next    uint64
	Learner bool
	// Recent is true if the peer responded within the last election timeout.
	Recent bool
}

// Status is a point-in-time snapshot of a node's Raft state, for metrics,
// admin APIs and tests.
type Status struct {
	ID            NodeID
	State         State
	Term          uint64
	Leader        NodeID
	CommitIndex   uint64
	LastApplied   uint64
	LastLogIndex  uint64
	SnapshotIndex uint64
	Config        Membership
	// LastLeaderContact is when this node last heard from a leader (now,
	// for the leader itself). Zero if never.
	LastLeaderContact time.Time
	// Progress is populated only on the leader.
	Progress map[NodeID]PeerProgress
}
