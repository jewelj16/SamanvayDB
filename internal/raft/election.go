package raft

import (
	"time"
)

// election.go — leader election.
//
//	Follower -> (election timeout, no heartbeat/AppendEntries received) -> Pre-Vote
//	    -> majority would vote for us -> Candidate
//	        currentTerm += 1; votedFor = self (both fsynced)
//	        send RequestVote to all voters
//	        -> majority votes (incl. self) -> Leader (appends a no-op, starts heartbeats)
//	        -> AppendEntries from term >= currentTerm -> Follower
//	        -> timeout again, no majority (split vote) -> new election, term += 1
//
// Election timeouts are re-randomized on every reset (default 150–300ms) so
// that split votes resolve on the next round instead of repeating forever.

// campaign runs one Pre-Vote round followed (if won) by one real election.
func (n *Node) campaign() {
	defer func() {
		n.mu.Lock()
		n.campaigning = false
		n.mu.Unlock()
	}()

	n.mu.Lock()
	if n.stopped || n.state == Leader || !n.config.IsVoter(n.id) {
		n.mu.Unlock()
		return
	}
	term := n.ps.CurrentTerm
	voters := n.otherVoters()
	quorum := n.config.Quorum()
	preVote := n.preVote && quorum > 1
	req := RequestVoteRequest{
		Term:         term + 1,
		CandidateID:  n.id,
		LastLogIndex: n.lastLogIndex(),
		LastLogTerm:  n.lastLogTerm(),
		PreVote:      true,
	}
	n.mu.Unlock()

	if preVote {
		if !n.collectVotes(req, voters, quorum) {
			return
		}
	}

	n.mu.Lock()
	// Abort if anything changed while we were pre-voting (a leader appeared,
	// a higher term was observed, we were stopped or removed).
	if n.stopped || n.ps.CurrentTerm != term || n.state == Leader || !n.config.IsVoter(n.id) ||
		(preVote && n.leader != "") {
		n.mu.Unlock()
		return
	}
	n.becomeCandidate()
	n.resetElectionTimer()
	if quorum == 1 {
		n.startLeading()
		n.mu.Unlock()
		return
	}
	req = RequestVoteRequest{
		Term:         n.ps.CurrentTerm,
		CandidateID:  n.id,
		LastLogIndex: n.lastLogIndex(),
		LastLogTerm:  n.lastLogTerm(),
	}
	voters = n.otherVoters()
	n.mu.Unlock()

	if !n.collectVotes(req, voters, quorum) {
		return
	}
	n.mu.Lock()
	if !n.stopped && n.state == Candidate && n.ps.CurrentTerm == req.Term {
		n.startLeading()
	}
	n.mu.Unlock()
}

// otherVoters returns the voting members other than self. Caller holds mu.
func (n *Node) otherVoters() []NodeID {
	var out []NodeID
	for _, v := range n.config.Voters() {
		if v != n.id {
			out = append(out, v)
		}
	}
	return out
}

// collectVotes sends req to every voter in parallel and returns true as soon
// as a quorum (counting our own vote) is granted, or false once that has
// become impossible or the election timeout elapses.
func (n *Node) collectVotes(req RequestVoteRequest, voters []NodeID, quorum int) bool {
	type result struct {
		resp RequestVoteResponse
		err  error
	}
	results := make(chan result, len(voters))
	for _, v := range voters {
		go func(p NodeID) {
			resp, err := n.transport.SendRequestVote(p, req)
			results <- result{resp, err}
		}(v)
	}

	granted, responded := 1, 0 // our own vote
	timeout := time.NewTimer(n.electionMin)
	defer timeout.Stop()
	for responded < len(voters) {
		select {
		case <-n.stopCh:
			return false
		case <-timeout.C:
			return false
		case r := <-results:
			responded++
			if r.err != nil {
				continue
			}
			n.mu.Lock()
			stepDown := n.observeTerm(r.resp.Term)
			stale := !req.PreVote && (n.state != Candidate || n.ps.CurrentTerm != req.Term)
			n.mu.Unlock()
			if stepDown || stale {
				return false
			}
			if r.resp.VoteGranted {
				granted++
				if granted >= quorum {
					return true
				}
			}
		}
		if granted+(len(voters)-responded) < quorum {
			return false // cannot win any more
		}
	}
	return granted >= quorum
}

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
// votedFor is persisted (fsynced) before the response is returned.
func (n *Node) HandleRequestVote(req RequestVoteRequest) RequestVoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return RequestVoteResponse{}
	}

	if req.PreVote {
		return n.handlePreVote(req)
	}

	// Universal term rule first.
	n.observeTerm(req.Term)

	resp := RequestVoteResponse{Term: n.ps.CurrentTerm}

	// Reject if candidate's term is stale.
	if req.Term < n.ps.CurrentTerm {
		return resp
	}

	// Check if we've already voted for someone else this term.
	if n.ps.VotedFor != nil && *n.ps.VotedFor != req.CandidateID {
		return resp
	}

	if !n.candidateLogOK(req) {
		return resp
	}

	// Grant the vote — durably, before replying.
	cand := req.CandidateID
	n.ps.VotedFor = &cand
	n.persistStateLocked()
	n.resetElectionTimer()

	resp.VoteGranted = true
	n.logger.Info("vote granted", "candidate", req.CandidateID, "term", req.Term)
	return resp
}

// handlePreVote answers "would you vote for me in term req.Term?" without
// changing any local state. Caller holds mu.
func (n *Node) handlePreVote(req RequestVoteRequest) RequestVoteResponse {
	resp := RequestVoteResponse{Term: n.ps.CurrentTerm}
	if req.Term < n.ps.CurrentTerm {
		return resp
	}
	// Leader stickiness: if we have a live leader, refuse to help unseat it.
	if n.state == Leader {
		return resp
	}
	if n.leader != "" && time.Since(n.lastLeaderContact) < n.electionMin {
		return resp
	}
	resp.VoteGranted = n.candidateLogOK(req)
	return resp
}

// candidateLogOK compares logs: a candidate's log is "at least as up-to-date"
// as ours if its last term is higher, or the terms tie and it is at least as
// long. Caller holds mu.
func (n *Node) candidateLogOK(req RequestVoteRequest) bool {
	ourLastTerm := n.lastLogTerm()
	ourLastIdx := n.lastLogIndex()
	return req.LastLogTerm > ourLastTerm ||
		(req.LastLogTerm == ourLastTerm && req.LastLogIndex >= ourLastIdx)
}
