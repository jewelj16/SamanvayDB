package raft

// StateMachine is the seam between Raft and the storage layer.
// Raft calls Apply() only for entries it has determined are committed,
// and in strictly increasing index order, exactly once per entry per
// incarnation of the state machine. Apply must be:
//   - Deterministic: no wall-clock reads, no randomness, no non-deterministic
//     map iteration — every node must reach the same state given the same log.
//   - Pure with respect to Raft: it must not call back into Raft or modify
//     Raft state (it runs while Raft holds its internal lock).
//
// Apply receives every committed entry, including EntryNoop and EntryConfig
// entries (which a state machine normally ignores). Its return value is
// delivered to the ProposeAndWait caller that proposed the entry, if any.
//
// Satisfied in production by internal/shard's FSM (backed by
// storage.Engine); in tests by simple mocks.
type StateMachine interface {
	Apply(entry LogEntry) any
	// Snapshot returns a point-in-time snapshot of the state machine,
	// reflecting exactly the entries applied so far.
	Snapshot() ([]byte, error)
	// Restore replaces the state machine state with a snapshot previously
	// produced by Snapshot().
	Restore(snapshot []byte) error
}

// Transport abstracts the network layer used by Raft to send RPCs to peers.
// The real implementation uses gRPC (internal/transport); tests inject the
// in-process simulator (internal/raft/raftsim). Implementations must apply
// their own timeouts so a dead peer never blocks a sender indefinitely.
type Transport interface {
	SendRequestVote(to NodeID, req RequestVoteRequest) (RequestVoteResponse, error)
	SendAppendEntries(to NodeID, req AppendEntriesRequest) (AppendEntriesResponse, error)
	SendInstallSnapshot(to NodeID, req InstallSnapshotRequest) (InstallSnapshotResponse, error)
}

// RPCHandler is the receive side of the Transport: what a transport calls
// when a message for this node arrives. *Node implements it.
type RPCHandler interface {
	HandleRequestVote(req RequestVoteRequest) RequestVoteResponse
	HandleAppendEntries(req AppendEntriesRequest) AppendEntriesResponse
	HandleInstallSnapshot(req InstallSnapshotRequest) InstallSnapshotResponse
}

// Hooks are optional observation callbacks for metrics and structured
// state-transition logs. They are invoked while Raft holds its internal lock,
// so they must be fast and must never call back into the Node.
type Hooks struct {
	// OnStateChange fires on every role change (and on leader discovery).
	OnStateChange func(term uint64, state State, leader NodeID)
	// OnElectionStarted fires when this node becomes a candidate.
	OnElectionStarted func(term uint64)
	// OnSnapshot fires when a snapshot is created locally (installed=false)
	// or installed from the leader (installed=true).
	OnSnapshot func(index, term uint64, installed bool)
	// OnConfigChange fires when a configuration takes effect (appended) and
	// again when it commits.
	OnConfigChange func(index uint64, m Membership, committed bool)
	// OnCommit fires when commitIndex advances.
	OnCommit func(commitIndex uint64)
}
