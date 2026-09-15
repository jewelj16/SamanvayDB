package raft

// election.go — RequestVote RPC handler, vote-granting rule, and the full
// leader election state machine (election timeouts, heartbeats).

// HandleRequestVote processes an incoming RequestVote RPC.
// This is the voter side of the election — it decides whether to grant its vote.
//
// A vote is granted only if ALL of the following hold:
//  1. candidate's term >= voter's currentTerm
//  2. voter hasn't already voted for a different candidate this term
//  3. candidate's log is at least as up-to-date as the voter's
//
// Rule 3 is a core safety mechanism, not an optimization. It prevents a node
// missing recent committed entries from ever becoming leader and overwriting them.
//
// votedFor must be persisted (fsynced) before responding — see persistence.go.
func (n *Node) HandleRequestVote(req RequestVoteRequest) RequestVoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Universal term rule first.
	n.observeTerm(req.Term)

	resp := RequestVoteResponse{Term: n.ps.CurrentTerm}

	// Reject if candidate's term is stale.
	if req.Term < n.ps.CurrentTerm {
		resp.VoteGranted = false
		return resp
	}

	// Check if we've already voted for someone else this term.
	alreadyVotedForOther := n.ps.VotedFor != nil && *n.ps.VotedFor != req.CandidateID
	if alreadyVotedForOther {
		resp.VoteGranted = false
		return resp
	}

	// Check log up-to-date: compare last log term first, then last log index.
	// A candidate's log is "at least as up-to-date" as ours if:
	//   - its last log term is higher than ours, OR
	//   - its last log term equals ours AND its last log index >= ours
	ourLastTerm := n.lastLogTerm()
	ourLastIdx := n.lastLogIndex()

	candidateLogOK := req.LastLogTerm > ourLastTerm ||
		(req.LastLogTerm == ourLastTerm && req.LastLogIndex >= ourLastIdx)

	if !candidateLogOK {
		resp.VoteGranted = false
		return resp
	}

	// Grant the vote.
	n.ps.VotedFor = &req.CandidateID
	// NOTE: In the full implementation (persistence.go / Commit 31), we fsync
	// votedFor here before returning. For now the field is in memory only.

	resp.VoteGranted = true
	n.logger.Info("vote granted", "candidate", req.CandidateID, "term", req.Term)
	return resp
}
