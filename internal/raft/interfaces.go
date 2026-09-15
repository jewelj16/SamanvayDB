package raft

// StateMachine is the seam between Raft and the storage layer.
// Raft calls Apply() only for entries it has determined are committed,
// and in strictly increasing index order. Apply must be:
//   - Deterministic: no wall-clock reads, no randomness, no non-deterministic
//     map iteration — every node must reach the same state given the same log.
//   - Pure with respect to Raft: it must not call back into Raft or modify
//     Raft state.
//
// In Phase 4 this interface is satisfied by the KV engine (storage.Engine
// wrapped with the apply logic). In tests it is satisfied by a mock.
type StateMachine interface {
	Apply(entry LogEntry) error
	// Snapshot returns a point-in-time snapshot of the state machine for
	// InstallSnapshot (Phase 3 snapshots, wired to the real engine in Phase 4).
	Snapshot() ([]byte, error)
	// Restore replaces the state machine state with a snapshot previously
	// produced by Snapshot().
	Restore(snapshot []byte) error
}

// Transport abstracts the network layer used by Raft to send RPCs to peers.
// The real implementation uses gRPC (Phase 3+); tests inject a simulator.
type Transport interface {
	SendRequestVote(to NodeID, req RequestVoteRequest) (RequestVoteResponse, error)
	SendAppendEntries(to NodeID, req AppendEntriesRequest) (AppendEntriesResponse, error)
	SendInstallSnapshot(to NodeID, req InstallSnapshotRequest) (InstallSnapshotResponse, error)
}
