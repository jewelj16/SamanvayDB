package raft

// replication.go — AppendEntries RPC handler and the leader's log replication loop.

// HandleAppendEntries processes an incoming AppendEntries RPC.
// This is the follower/candidate side. It handles both heartbeats (Entries==nil)
// and real log replication.
//
// Critical safety rule (enforced here): a leader must NEVER consider an entry
// from a previous term committed purely by counting replicas — it must wait
// until at least one entry of its OWN current term is replicated to a majority,
// then commit everything up to and including that point together.
func (n *Node) HandleAppendEntries(req AppendEntriesRequest) AppendEntriesResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Universal term rule.
	n.observeTerm(req.Term)

	resp := AppendEntriesResponse{Term: n.ps.CurrentTerm}

	// Reject if leader's term is stale.
	if req.Term < n.ps.CurrentTerm {
		resp.Success = false
		return resp
	}

	// Valid AppendEntries from current leader — reset election timer.
	n.NotifyHeartbeat()

	// If we were a Candidate and get AppendEntries with term >= ours, revert.
	if n.state == Candidate && req.Term >= n.ps.CurrentTerm {
		n.becomeFollower()
	}

	// Consistency check: does our log contain an entry at PrevLogIndex
	// with PrevLogTerm?
	if req.PrevLogIndex > 0 {
		if req.PrevLogIndex >= uint64(len(n.ps.Log)) {
			// We don't even have an entry at that index.
			resp.Success = false
			resp.ConflictIndex = uint64(len(n.ps.Log))
			resp.ConflictTerm = 0
			return resp
		}
		prevEntry := n.ps.Log[req.PrevLogIndex]
		if prevEntry.Term != req.PrevLogTerm {
			// Term mismatch — find the earliest index with this conflict term
			// so the leader can skip the whole conflicting term in one shot.
			resp.Success = false
			resp.ConflictTerm = prevEntry.Term
			resp.ConflictIndex = req.PrevLogIndex
			for resp.ConflictIndex > 1 && n.ps.Log[resp.ConflictIndex-1].Term == resp.ConflictTerm {
				resp.ConflictIndex--
			}
			return resp
		}
	}

	// Append new entries, truncating any conflicting existing entries.
	for i, entry := range req.Entries {
		logIdx := req.PrevLogIndex + 1 + uint64(i)
		if logIdx < uint64(len(n.ps.Log)) {
			if n.ps.Log[logIdx].Term != entry.Term {
				// Conflict — truncate from here.
				n.ps.Log = n.ps.Log[:logIdx]
			} else {
				continue // already have this entry
			}
		}
		n.ps.Log = append(n.ps.Log, entry)
	}

	// Advance commit index.
	if req.LeaderCommit > n.vs.CommitIndex {
		lastNewIndex := req.PrevLogIndex + uint64(len(req.Entries))
		if req.LeaderCommit < lastNewIndex {
			n.vs.CommitIndex = req.LeaderCommit
		} else {
			n.vs.CommitIndex = lastNewIndex
		}
		n.applyCommitted()
	}

	resp.Success = true
	return resp
}

// applyCommitted applies all log entries from lastApplied+1 to commitIndex
// to the state machine, in order. Caller must hold n.mu.
//
// Invariant: apply() is a pure, deterministic function of (state, entry).
// No wall-clock reads, no randomness inside this path.
func (n *Node) applyCommitted() {
	for n.vs.LastApplied < n.vs.CommitIndex {
		n.vs.LastApplied++
		entry := n.ps.Log[n.vs.LastApplied]
		if n.sm != nil {
			n.sm.Apply(entry)
		}
	}
}

// Propose appends a new command to the leader's log and begins replication.
// Returns an error if this node is not the leader.
func (n *Node) Propose(command []byte) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Leader {
		return ErrNotLeader
	}

	entry := LogEntry{
		Index:   n.lastLogIndex() + 1,
		Term:    n.ps.CurrentTerm,
		Command: command,
	}
	n.ps.Log = append(n.ps.Log, entry)

	// Trigger replication to all peers.
	go n.replicateLog()
	return nil
}

// replicateLog sends AppendEntries to all peers to replicate the leader's log.
// Called from Propose and from the heartbeat loop when there are pending entries.
func (n *Node) replicateLog() {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return
	}
	peers := n.peers
	term := n.ps.CurrentTerm
	leaderID := n.id
	commitIdx := n.vs.CommitIndex
	n.mu.Unlock()

	for _, peer := range peers {
		go func(p NodeID) {
			n.mu.Lock()
			if n.state != Leader || n.ps.CurrentTerm != term {
				n.mu.Unlock()
				return
			}
			nextIdx := n.ls.NextIndex[p]
			prevIdx := nextIdx - 1
			var prevTerm uint64
			if prevIdx > 0 && prevIdx < uint64(len(n.ps.Log)) {
				prevTerm = n.ps.Log[prevIdx].Term
			}
			entries := make([]LogEntry, len(n.ps.Log)-int(nextIdx))
			copy(entries, n.ps.Log[nextIdx:])
			n.mu.Unlock()

			req := AppendEntriesRequest{
				Term:         term,
				LeaderID:     leaderID,
				PrevLogIndex: prevIdx,
				PrevLogTerm:  prevTerm,
				Entries:      entries,
				LeaderCommit: commitIdx,
			}

			resp, err := n.transport.SendAppendEntries(p, req)
			if err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if n.observeTerm(resp.Term) {
				return
			}
			if n.state != Leader || n.ps.CurrentTerm != term {
				return
			}

			if resp.Success {
				// Update matchIndex and nextIndex for this peer.
				newMatch := req.PrevLogIndex + uint64(len(req.Entries))
				if newMatch > n.ls.MatchIndex[p] {
					n.ls.MatchIndex[p] = newMatch
					n.ls.NextIndex[p] = newMatch + 1
				}
				// Check if we can advance commitIndex.
				n.maybeAdvanceCommitIndex()
			} else {
				// Backtrack: use conflict hint if available.
				if resp.ConflictIndex > 0 {
					n.ls.NextIndex[p] = resp.ConflictIndex
				} else if n.ls.NextIndex[p] > 1 {
					n.ls.NextIndex[p]--
				}
			}
		}(peer)
	}
}

// maybeAdvanceCommitIndex checks whether a new entry can be committed —
// i.e., replicated to a majority — and advances commitIndex if so.
//
// CRITICAL SAFETY RULE: only commit entries from the CURRENT term by count.
// Entries from previous terms are committed implicitly when a current-term
// entry is committed (Raft §5.4.2). Violating this causes the "leader
// overwriting committed entries" bug.
//
// Caller must hold n.mu.
func (n *Node) maybeAdvanceCommitIndex() {
	if n.state != Leader {
		return
	}
	quorum := (len(n.peers) + 1 + 1) / 2 // majority of total cluster size

	// Walk candidate indices from the end of log down to commitIndex+1.
	for idx := n.lastLogIndex(); idx > n.vs.CommitIndex; idx-- {
		entry := n.ps.Log[idx]
		// Only commit entries from the current term (safety rule).
		if entry.Term != n.ps.CurrentTerm {
			continue
		}
		// Count replicas: self + peers with matchIndex >= idx.
		replicaCount := 1
		for _, peer := range n.peers {
			if n.ls.MatchIndex[peer] >= idx {
				replicaCount++
			}
		}
		if replicaCount >= quorum {
			n.vs.CommitIndex = idx
			n.applyCommitted()
			break
		}
	}
}
