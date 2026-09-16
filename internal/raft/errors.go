package raft

import "errors"

// ErrNotLeader is returned by Propose when the node is not the current leader.
// Callers should redirect the client to the known leader (tracked in LeaderID
// fields on AppendEntries responses).
var ErrNotLeader = errors.New("raft: not the leader")
