package raft

import "errors"

var (
	// ErrNotLeader is returned by Propose/ReadIndex when the node is not the
	// current leader. Callers should redirect the client to Status().Leader.
	ErrNotLeader = errors.New("raft: not the leader")

	// ErrLeadershipLost is delivered to a ProposeAndWait caller whose entry
	// was overwritten by a different leader's entry at the same index. The
	// command was NOT applied; it is safe to retry.
	ErrLeadershipLost = errors.New("raft: leadership lost; entry was not committed")

	// ErrStopped is returned once the node has been stopped.
	ErrStopped = errors.New("raft: node stopped")

	// ErrConfigChangeInProgress is returned when a membership change is
	// proposed while a previous one has not yet committed. SamanvayDB uses
	// single-server changes, which are only safe one at a time.
	ErrConfigChangeInProgress = errors.New("raft: a configuration change is already in progress")

	// ErrLeaderNotReady is returned when a new leader has not yet committed
	// an entry from its own term (its no-op), so it cannot yet serve
	// ReadIndex or configuration changes safely.
	ErrLeaderNotReady = errors.New("raft: leader has not yet committed an entry in its term")

	// ErrInvalidConfChange is returned for a nonsensical membership change.
	ErrInvalidConfChange = errors.New("raft: invalid configuration change")
)
