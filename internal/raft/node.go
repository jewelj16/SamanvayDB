package raft

import (
	"sync"

	"samanvaydb/internal/logging"
)

// Node is a single Raft participant. It holds all state (persistent + volatile)
// and exposes RPC handlers (RequestVote, AppendEntries, InstallSnapshot).
//
// All exported methods are safe for concurrent use; the internal mu protects
// all mutable state. RPC handlers run in goroutines dispatched by the transport.
type Node struct {
	mu sync.Mutex

	// Identity
	id    NodeID
	peers []NodeID // IDs of all other nodes in the cluster (excludes self)

	// Persistent state — must be saved to stable storage before responding to RPCs.
	ps PersistentState

	// Volatile state — safely re-derived on restart.
	vs VolatileState

	// Leader-only volatile state — re-initialized on each new election win.
	ls *LeaderVolatileState

	// Current role.
	state State

	// StateMachine to apply committed entries to (wired in Phase 4).
	sm StateMachine

	// Transport for sending RPCs to peers (injected; swapped for simulator in tests).
	transport Transport

	// Channels for the main Raft loop.
	heartbeatCh chan struct{} // receives a signal every time a valid heartbeat arrives
	electionCh  chan struct{} // closed when an election should start
	stopCh      chan struct{} // closed to stop all background goroutines

	logger *logging.Logger
}

// NewNode creates a new Raft node. The node starts as a Follower in term 0.
// Call Start() to begin the election-timer loop.
func NewNode(id NodeID, peers []NodeID, sm StateMachine, transport Transport, logger *logging.Logger) *Node {
	n := &Node{
		id:          id,
		peers:       peers,
		state:       Follower,
		sm:          sm,
		transport:   transport,
		heartbeatCh: make(chan struct{}, 1),
		electionCh:  make(chan struct{}, 1),
		stopCh:      make(chan struct{}),
		logger:      logger.WithComponent("raft").WithRaftContext(0, "Follower"),
	}
	// Initialize log with a sentinel entry at index 0 (term 0) so that
	// prevLogIndex=0 is always valid — simplifies boundary conditions.
	n.ps.Log = []LogEntry{{Index: 0, Term: 0}}
	return n
}

// ---- Universal term rule ----
// Every RPC handler calls this first. If we see a higher term, we immediately
// revert to Follower — even if we thought we were the Leader. This is the
// single most important safety rule in Raft.

// observeTerm checks if term > currentTerm and, if so, updates currentTerm,
// clears votedFor, and reverts to Follower. Returns true if a reversion occurred.
// Caller must hold n.mu.
func (n *Node) observeTerm(term uint64) bool {
	if term > n.ps.CurrentTerm {
		n.logger.Info("higher term observed — reverting to Follower",
			"old_term", n.ps.CurrentTerm, "new_term", term)
		n.ps.CurrentTerm = term
		n.ps.VotedFor = nil
		n.becomeFollower()
		return true
	}
	return false
}

// becomeFollower transitions node to Follower state.
// Caller must hold n.mu.
func (n *Node) becomeFollower() {
	n.state = Follower
	n.ls = nil
	n.logger = n.logger.WithRaftContext(n.ps.CurrentTerm, "Follower")
}

// becomeCandidate transitions node to Candidate, increments term, votes for self.
// Caller must hold n.mu.
func (n *Node) becomeCandidate() {
	n.ps.CurrentTerm++
	self := n.id
	n.ps.VotedFor = &self
	n.state = Candidate
	n.ls = nil
	n.logger = n.logger.WithRaftContext(n.ps.CurrentTerm, "Candidate")
	n.logger.Info("starting election")
}

// becomeLeader transitions node to Leader and initializes per-follower tracking.
// Caller must hold n.mu.
func (n *Node) becomeLeader() {
	n.state = Leader
	lastIdx := n.lastLogIndex()
	n.ls = &LeaderVolatileState{
		NextIndex:  make(map[NodeID]uint64),
		MatchIndex: make(map[NodeID]uint64),
	}
	for _, peer := range n.peers {
		n.ls.NextIndex[peer] = lastIdx + 1
		n.ls.MatchIndex[peer] = 0
	}
	n.logger = n.logger.WithRaftContext(n.ps.CurrentTerm, "Leader")
	n.logger.Info("became leader")
}

// ---- Log helpers ----
// Caller must hold n.mu.

func (n *Node) lastLogIndex() uint64 {
	return n.ps.Log[len(n.ps.Log)-1].Index
}

func (n *Node) lastLogTerm() uint64 {
	return n.ps.Log[len(n.ps.Log)-1].Term
}

// entryAt returns the log entry at the given 1-based index, or a zero entry
// if the index is out of range.
func (n *Node) entryAt(index uint64) (LogEntry, bool) {
	// Log[0] is sentinel (index=0,term=0); real entries start at Log[1].
	if index == 0 || index >= uint64(len(n.ps.Log)) {
		return LogEntry{}, false
	}
	e := n.ps.Log[index]
	if e.Index != index {
		// Invariant violation — log is corrupted or accessed incorrectly.
		panic("raft: log index mismatch — this is a bug")
	}
	return e, true
}

// Stop shuts down all background goroutines started by Start().
func (n *Node) Stop() {
	close(n.stopCh)
}

// CurrentTerm returns the node's current term (safe to call without the lock).
func (n *Node) CurrentTerm() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.ps.CurrentTerm
}

// CurrentState returns the node's current role.
func (n *Node) CurrentState() State {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state
}
