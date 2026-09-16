package raft

import (
	"math/rand"
	"sync"
	"time"
)

// Timeout constants. The heartbeat interval must be well under the minimum
// election timeout — at least a 3–5× safety margin to prevent spurious
// elections under ordinary network jitter.
const (
	electionTimeoutMin = 150 * time.Millisecond
	electionTimeoutMax = 300 * time.Millisecond
	heartbeatInterval  = 50 * time.Millisecond // ≥3× under electionTimeoutMin
)

// randomElectionTimeout returns a duration uniformly distributed in
// [electionTimeoutMin, electionTimeoutMax). Randomization prevents split votes:
// if all nodes had the same timeout, they'd always time out simultaneously.
func randomElectionTimeout() time.Duration {
	span := int64(electionTimeoutMax - electionTimeoutMin)
	return electionTimeoutMin + time.Duration(rand.Int63n(span))
}

// Start launches the Raft background goroutines. Must be called once after
// NewNode. Blocks until Stop() is called.
func (n *Node) Start() {
	go n.runElectionTimer()
}

// runElectionTimer is the main election-timer loop for Followers and Candidates.
// It fires when no heartbeat or AppendEntries is received within the timeout.
func (n *Node) runElectionTimer() {
	for {
		timeout := randomElectionTimeout()
		select {
		case <-n.stopCh:
			return
		case <-time.After(timeout):
			n.mu.Lock()
			state := n.state
			n.mu.Unlock()

			if state == Leader {
				// Leaders don't run an election timer — they send heartbeats.
				continue
			}
			// Timeout fired without a heartbeat — start an election.
			n.startElection()
		case <-n.heartbeatCh:
			// Reset the timer on heartbeat receipt — loop again.
		}
	}
}

// startElection transitions to Candidate, increments term, votes for self,
// and sends RequestVote to all peers in parallel.
func (n *Node) startElection() {
	n.mu.Lock()
	n.becomeCandidate()
	term := n.ps.CurrentTerm
	lastIdx := n.lastLogIndex()
	lastTerm := n.lastLogTerm()
	peers := n.peers
	n.mu.Unlock()

	req := RequestVoteRequest{
		Term:         term,
		CandidateID:  n.id,
		LastLogIndex: lastIdx,
		LastLogTerm:  lastTerm,
	}

	votes := 1 // vote for self
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, peer := range peers {
		wg.Add(1)
		go func(p NodeID) {
			defer wg.Done()
			resp, err := n.transport.SendRequestVote(p, req)
			if err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			// If we've moved on (term changed, already won/lost), ignore.
			if n.state != Candidate || n.ps.CurrentTerm != term {
				return
			}

			// Universal term rule on the response.
			if n.observeTerm(resp.Term) {
				return // reverted to Follower
			}

			if resp.VoteGranted {
				mu.Lock()
				votes++
				currentVotes := votes
				mu.Unlock()

				if currentVotes > (len(peers)+1)/2 {
					// Majority reached — become leader.
					n.becomeLeader()
					go n.runHeartbeatLoop()
				}
			}
		}(peer)
	}
}

// runHeartbeatLoop sends periodic empty AppendEntries (heartbeats) to all peers
// to suppress their election timers while we're the leader.
func (n *Node) runHeartbeatLoop() {
	for {
		select {
		case <-n.stopCh:
			return
		case <-time.After(heartbeatInterval):
			n.mu.Lock()
			if n.state != Leader {
				n.mu.Unlock()
				return
			}
			term := n.ps.CurrentTerm
			commitIdx := n.vs.CommitIndex
			lastIdx := n.lastLogIndex()
			lastTerm := n.lastLogTerm()
			peers := n.peers
			n.mu.Unlock()

			for _, peer := range peers {
				go func(p NodeID) {
					req := AppendEntriesRequest{
						Term:         term,
						LeaderID:     n.id,
						PrevLogIndex: lastIdx,
						PrevLogTerm:  lastTerm,
						Entries:      nil, // heartbeat
						LeaderCommit: commitIdx,
					}
					resp, err := n.transport.SendAppendEntries(p, req)
					if err != nil {
						return
					}
					n.mu.Lock()
					defer n.mu.Unlock()
					n.observeTerm(resp.Term)
				}(peer)
			}
		}
	}
}

// NotifyHeartbeat is called by the AppendEntries handler to signal the
// election timer that a valid heartbeat was received, resetting the timeout.
func (n *Node) NotifyHeartbeat() {
	select {
	case n.heartbeatCh <- struct{}{}:
	default: // non-blocking — one signal is enough
	}
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
