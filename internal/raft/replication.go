package raft

import (
	"context"
	"fmt"
	"time"
)

// replication.go — AppendEntries RPC handler and the leader's log replication.
//
//	Leader: append {term, index, command} to local log (fsynced, uncommitted)
//	Leader sends AppendEntries{prevLogIndex, prevLogTerm, entries, leaderCommit}
//	  to every follower, in parallel (one replicator goroutine per follower)
//	Follower:
//	  reject if it doesn't have an entry at prevLogIndex with prevLogTerm
//	  otherwise: append new entries (truncating conflicting uncommitted
//	    entries at those indexes), fsync, ACK
//	Leader: on majority ACK (incl. self) of an entry FROM ITS OWN TERM ->
//	  that entry and everything before it is COMMITTED -> applied in order.
//
// Heartbeats are simply AppendEntries sent from each follower's nextIndex,
// so they double as the retry mechanism for lost replication messages.

// replicator owns the replication stream to one peer. At most one RPC is in
// flight per peer; trigger is a 1-slot "work available" signal.
type replicator struct {
	peer    NodeID
	trigger chan struct{}
	stop    chan struct{}
}

// syncReplicatorsLocked starts replicators for new members of the latest
// config and stops those for removed members. Leader only.
func (n *Node) syncReplicatorsLocked() {
	if n.state != Leader || n.ls == nil || n.stopped {
		return
	}
	want := map[NodeID]bool{}
	now := time.Now().UnixNano()
	for _, p := range n.peers() {
		want[p] = true
		if _, ok := n.replicators[p]; ok {
			continue
		}
		if _, ok := n.ls.NextIndex[p]; !ok {
			n.ls.NextIndex[p] = n.lastLogIndex() + 1
			n.ls.MatchIndex[p] = 0
			n.ls.lastContact[p] = now
		}
		r := &replicator{peer: p, trigger: make(chan struct{}, 1), stop: make(chan struct{})}
		n.replicators[p] = r
		n.wg.Add(1)
		go n.runReplicator(r, n.ps.CurrentTerm)
		r.signal()
	}
	for p, r := range n.replicators {
		if !want[p] {
			close(r.stop)
			delete(n.replicators, p)
			delete(n.ls.NextIndex, p)
			delete(n.ls.MatchIndex, p)
			delete(n.ls.lastContact, p)
			delete(n.ls.ackSeq, p)
		}
	}
}

func (n *Node) stopReplicatorsLocked() {
	for p, r := range n.replicators {
		close(r.stop)
		delete(n.replicators, p)
	}
}

func (r *replicator) signal() {
	select {
	case r.trigger <- struct{}{}:
	default:
	}
}

func (n *Node) triggerAllLocked() {
	for _, r := range n.replicators {
		r.signal()
	}
}

func (n *Node) runReplicator(r *replicator, term uint64) {
	defer n.wg.Done()
	for {
		select {
		case <-r.stop:
			return
		case <-n.stopCh:
			return
		case <-r.trigger:
		}
		// Keep sending while there is more to send (catch-up / backtracking).
		for i := 0; i < 1000; i++ {
			select {
			case <-r.stop:
				return
			default:
			}
			if !n.replicateOnce(r.peer, term) {
				break
			}
		}
	}
}

// replicateOnce sends one AppendEntries (or InstallSnapshot) to peer and
// processes the response. Returns true if another send should follow
// immediately.
func (n *Node) replicateOnce(peer NodeID, term uint64) bool {
	n.mu.Lock()
	if n.stopped || n.state != Leader || n.ps.CurrentTerm != term || n.ls == nil {
		n.mu.Unlock()
		return false
	}
	next, ok := n.ls.NextIndex[peer]
	if !ok {
		n.mu.Unlock()
		return false
	}
	if next <= n.firstIndex() && n.firstIndex() > 0 {
		n.mu.Unlock()
		return n.sendSnapshot(peer, term)
	}
	if next > n.lastLogIndex()+1 {
		next = n.lastLogIndex() + 1
		n.ls.NextIndex[peer] = next
	}
	prevIdx := next - 1
	prevTerm, _ := n.termAt(prevIdx)
	req := AppendEntriesRequest{
		Term:         term,
		LeaderID:     n.id,
		PrevLogIndex: prevIdx,
		PrevLogTerm:  prevTerm,
		Entries:      n.entriesFrom(next, n.maxEntries),
		LeaderCommit: n.vs.CommitIndex,
	}
	seq := n.ls.readSeq
	n.mu.Unlock()

	resp, err := n.transport.SendAppendEntries(peer, req)
	if err != nil {
		return false // retried on the next heartbeat
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.observeTerm(resp.Term) {
		return false
	}
	if n.stopped || n.state != Leader || n.ps.CurrentTerm != term || n.ls == nil {
		return false
	}
	if _, ok := n.ls.NextIndex[peer]; !ok {
		return false // removed from config meanwhile
	}
	n.ls.lastContact[peer] = time.Now().UnixNano()
	if seq > n.ls.ackSeq[peer] {
		n.ls.ackSeq[peer] = seq
		n.notifyLocked()
	}

	if resp.Success {
		match := req.PrevLogIndex + uint64(len(req.Entries))
		if match > n.ls.MatchIndex[peer] {
			n.ls.MatchIndex[peer] = match
		}
		if match+1 > n.ls.NextIndex[peer] {
			n.ls.NextIndex[peer] = match + 1
		}
		n.maybeAdvanceCommitIndex()
		if n.state != Leader || n.ls == nil { // may step down if it removed itself
			return false
		}
		return n.ls.NextIndex[peer] <= n.lastLogIndex()
	}

	// Rejected: back up. Use the follower's conflict hint to skip a whole
	// conflicting term at once instead of decrementing one index per RPC.
	newNext := resp.ConflictIndex
	if resp.ConflictTerm != 0 {
		// If we have entries from ConflictTerm, resume just after our last one.
		for i := len(n.ps.Log) - 1; i >= 1; i-- {
			if n.ps.Log[i].Term == resp.ConflictTerm {
				newNext = n.ps.Log[i].Index + 1
				break
			}
			if n.ps.Log[i].Term < resp.ConflictTerm {
				break
			}
		}
	}
	if newNext == 0 || newNext >= req.PrevLogIndex+1 {
		newNext = req.PrevLogIndex // guarantee progress: at least one step back
	}
	if newNext < 1 {
		newNext = 1
	}
	if newNext <= n.ls.MatchIndex[peer] {
		newNext = n.ls.MatchIndex[peer] + 1
	}
	n.ls.NextIndex[peer] = newNext
	return true
}

// HandleAppendEntries processes an incoming AppendEntries RPC.
// This is the follower/candidate side. It handles both heartbeats (Entries==nil)
// and real log replication. New entries are fsynced before Success is returned.
func (n *Node) HandleAppendEntries(req AppendEntriesRequest) AppendEntriesResponse {
	n.mu.Lock()
	resp, syncUpTo := n.appendEntriesLocked(req)
	if syncUpTo == 0 {
		n.mu.Unlock()
		return resp
	}
	// New entries were WRITTEN under the lock (keeping the file in log
	// order); fsync them OUTSIDE it so heartbeats/ReadIndex acks and reads
	// are not stalled behind the disk. Success is reported only after the
	// fsync, and only if we are still in the leader's term and still hold
	// those exact entries — a follower never acknowledges entries outside
	// its current term.
	wantTerm, _ := n.termAt(syncUpTo)
	n.mu.Unlock()
	err := n.storage.Sync()

	n.mu.Lock()
	defer n.mu.Unlock()
	if err != nil {
		n.logger.Error("fsync failed — not acknowledging", "error", err)
		return AppendEntriesResponse{Term: n.ps.CurrentTerm, ConflictIndex: req.PrevLogIndex + 1}
	}
	if t, ok := n.termAt(syncUpTo); n.stopped || n.ps.CurrentTerm != req.Term || !ok || t != wantTerm {
		return AppendEntriesResponse{Term: n.ps.CurrentTerm, ConflictIndex: req.PrevLogIndex + 1}
	}
	if syncUpTo > n.durableIndex {
		n.durableIndex = syncUpTo
	}
	return resp
}

// appendEntriesLocked does the work of HandleAppendEntries under n.mu. It
// returns syncUpTo > 0 if the response may only be sent after the log has
// been fsynced through that index.
func (n *Node) appendEntriesLocked(req AppendEntriesRequest) (resp AppendEntriesResponse, syncUpTo uint64) {
	if n.stopped {
		return AppendEntriesResponse{}, 0
	}

	// Universal term rule.
	n.observeTerm(req.Term)

	resp = AppendEntriesResponse{Term: n.ps.CurrentTerm}

	// Reject if leader's term is stale.
	if req.Term < n.ps.CurrentTerm {
		return resp, 0
	}

	if n.state == Leader {
		// Two leaders in the same term is structurally impossible (each
		// needs a majority of votes, and each node votes once per term).
		panic(fmt.Sprintf("raft: %s: leader received AppendEntries from %s in its own term %d — this is a bug",
			n.id, req.LeaderID, req.Term))
	}
	// A Candidate that hears from a leader of its term defers to it.
	if n.state == Candidate {
		n.becomeFollower()
	}
	n.recordLeaderContactLocked(req.LeaderID)

	prev, prevTerm, entries := req.PrevLogIndex, req.PrevLogTerm, req.Entries
	base := n.firstIndex()
	if prev < base {
		// Entries up to our snapshot are committed and identical by the Log
		// Matching property; skip the overlap.
		skip := base - prev
		if uint64(len(entries)) <= skip {
			entries = nil
		} else {
			entries = entries[skip:]
		}
		prev, prevTerm = base, n.ps.Log[0].Term
	}

	// Consistency check: does our log contain an entry at prev with prevTerm?
	if prev > n.lastLogIndex() {
		resp.ConflictIndex = n.lastLogIndex() + 1
		resp.ConflictTerm = 0
		return resp, 0
	}
	if t, _ := n.termAt(prev); t != prevTerm {
		// Term mismatch — report the first index of the conflicting term so
		// the leader can skip the whole term in one round trip.
		resp.ConflictTerm = t
		resp.ConflictIndex = prev
		for resp.ConflictIndex > base+1 {
			pt, _ := n.termAt(resp.ConflictIndex - 1)
			if pt != t {
				break
			}
			resp.ConflictIndex--
		}
		return resp, 0
	}

	// Append new entries, truncating any conflicting (uncommitted) entries.
	for i, entry := range entries {
		idx := prev + 1 + uint64(i)
		if idx <= n.lastLogIndex() {
			if t, _ := n.termAt(idx); t == entry.Term {
				continue // already have this entry (duplicate/retransmit)
			}
			if err := n.truncateFromLocked(idx); err != nil {
				n.logger.Error("truncate failed", "error", err)
				resp.ConflictIndex = idx
				return resp, 0
			}
		}
		newEntries := cloneEntries(entries[i:])
		if err := n.storage.Write(newEntries); err != nil {
			n.logger.Error("append failed — not acknowledging", "error", err)
			resp.ConflictIndex = idx
			return resp, 0
		}
		n.appendInMemoryLocked(newEntries)
		break
	}

	// Advance commit index (never backwards).
	lastNew := prev + uint64(len(entries))
	if req.LeaderCommit > n.vs.CommitIndex {
		newCommit := req.LeaderCommit
		if lastNew < newCommit {
			newCommit = lastNew
		}
		if newCommit > n.vs.CommitIndex {
			n.setCommitIndexLocked(newCommit)
		}
	}

	resp.Success = true
	// Anything up to lastNew that is not yet known durable (new entries, or
	// a retransmit of entries a previous, still-syncing handler wrote) must
	// be fsynced before we acknowledge.
	if lastNew > n.durableIndex {
		return resp, lastNew
	}
	return resp, 0
}

func (n *Node) recordLeaderContactLocked(leader NodeID) {
	n.lastLeaderContact = time.Now()
	n.resetElectionTimer()
	if n.leader != leader {
		n.leader = leader
		n.fireStateChange()
	}
}

func (n *Node) setCommitIndexLocked(idx uint64) {
	if idx < n.vs.CommitIndex {
		panic(fmt.Sprintf("raft: %s: commitIndex moving backwards %d -> %d — this is a bug", n.id, n.vs.CommitIndex, idx))
	}
	if idx > n.lastLogIndex() {
		panic(fmt.Sprintf("raft: %s: commitIndex %d beyond last log index %d — this is a bug", n.id, idx, n.lastLogIndex()))
	}
	n.vs.CommitIndex = idx
	if n.hooks.OnCommit != nil {
		n.hooks.OnCommit(idx)
	}
	n.applyCommitted()
	n.notifyLocked()
}

// applyCommitted applies all log entries from lastApplied+1 to commitIndex
// to the state machine, in order. Caller must hold n.mu.
//
// Invariant: apply() is a pure, deterministic function of (state, entry).
// No wall-clock reads, no randomness inside this path.
func (n *Node) applyCommitted() {
	for n.vs.LastApplied < n.vs.CommitIndex {
		idx := n.vs.LastApplied + 1
		entry, ok := n.entryAt(idx)
		if !ok {
			panic(fmt.Sprintf("raft: %s: committed entry %d missing from log — this is a bug", n.id, idx))
		}
		var result any
		if n.sm != nil {
			result = n.sm.Apply(entry)
		}
		n.vs.LastApplied = idx

		if entry.Type == EntryConfig {
			m, _ := DecodeMembership(entry.Command)
			n.logger.Info("membership change committed", "index", idx, "config", m.String(), "term", n.ps.CurrentTerm)
			if n.hooks.OnConfigChange != nil {
				n.hooks.OnConfigChange(idx, m, true)
			}
			// A leader that has removed itself steps down once the change
			// commits (it managed the cluster without counting itself until now).
			if n.state == Leader && idx == n.configIndex && !n.config.IsVoter(n.id) {
				n.logger.Info("removed from configuration — stepping down", "term", n.ps.CurrentTerm)
				n.leader = ""
				n.becomeFollower()
			}
		}

		if w, ok := n.waiters[idx]; ok {
			if w.term == entry.Term {
				w.ch <- proposalResult{value: result}
			} else {
				w.ch <- proposalResult{err: ErrLeadershipLost}
			}
			delete(n.waiters, idx)
		}
	}
	n.maybeSnapshotLocked()
}

// maybeAdvanceCommitIndex checks whether a new entry can be committed —
// i.e., replicated to a majority of VOTERS — and advances commitIndex if so.
//
// CRITICAL SAFETY RULE: only entries from the CURRENT term are committed by
// counting replicas. Entries from previous terms are committed implicitly
// when a current-term entry is committed (Raft §5.4.2). Violating this
// causes the "leader overwriting committed entries" bug (Figure 8).
//
// Caller must hold n.mu.
func (n *Node) maybeAdvanceCommitIndex() {
	if n.state != Leader || n.ls == nil {
		return
	}
	quorum := n.config.Quorum()
	voters := n.config.Voters()
	for idx := n.lastLogIndex(); idx > n.vs.CommitIndex; idx-- {
		t, _ := n.termAt(idx)
		if t != n.ps.CurrentTerm {
			// Terms are non-decreasing along the log, so every earlier
			// entry is also from a previous term: nothing to count.
			break
		}
		count := 0
		for _, v := range voters {
			if v == n.id {
				if idx <= n.durableIndex { // only once fsynced locally
					count++
				}
			} else if n.ls.MatchIndex[v] >= idx {
				count++
			}
		}
		if count >= quorum {
			n.setCommitIndexLocked(idx)
			return
		}
	}
}

// Propose appends a new command to the leader's log (durably) and begins
// replication. It returns the index and term the entry was assigned; the
// entry is committed only if it is later applied at that index with that
// term. Returns ErrNotLeader if this node is not the leader.
func (n *Node) Propose(command []byte) (uint64, uint64, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.proposeLocked(EntryNormal, command, nil)
}

// proposeLocked appends one entry as leader. If w is non-nil it is
// registered for the entry's index BEFORE the entry can possibly commit, so
// even a single-voter group (which commits inside this call) delivers the
// apply result to it.
func (n *Node) proposeLocked(typ EntryType, command []byte, w *waiter) (uint64, uint64, error) {
	if n.stopped {
		return 0, 0, ErrStopped
	}
	if n.state != Leader {
		return 0, 0, ErrNotLeader
	}
	entry := LogEntry{
		Index:   n.lastLogIndex() + 1,
		Term:    n.ps.CurrentTerm,
		Type:    typ,
		Command: cloneBytes(command),
	}
	if err := n.appendLocked([]LogEntry{entry}); err != nil {
		return 0, 0, fmt.Errorf("raft: persist proposal: %w", err)
	}
	if w != nil {
		w.term = entry.Term
		n.waiters[entry.Index] = w
	}
	n.maybeAdvanceCommitIndex() // single-voter groups commit immediately
	n.triggerAllLocked()
	return entry.Index, entry.Term, nil
}

// ProposeAndWait proposes command and blocks until it is committed and
// applied, returning the state machine's Apply result for it. If ctx expires
// first, the outcome is unknown: the entry may still commit later.
func (n *Node) ProposeAndWait(ctx context.Context, command []byte) (any, error) {
	return n.proposeAndWait(ctx, EntryNormal, command)
}

func (n *Node) proposeAndWait(ctx context.Context, typ EntryType, command []byte) (any, error) {
	w := &waiter{ch: make(chan proposalResult, 1)}
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return nil, ErrStopped
	}
	if n.state != Leader {
		n.mu.Unlock()
		return nil, ErrNotLeader
	}
	batching := n.started
	n.mu.Unlock()

	if batching {
		// Group commit: hand the proposal to the proposer goroutine, which
		// appends everything queued with ONE fsync.
		select {
		case n.proposeCh <- &pendingProposal{typ: typ, command: command, w: w}:
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-n.stopCh:
			return nil, ErrStopped
		}
	} else {
		n.mu.Lock()
		_, _, err := n.proposeLocked(typ, command, w)
		n.mu.Unlock()
		if err != nil {
			return nil, err
		}
	}
	select {
	case r := <-w.ch:
		return r.value, r.err
	case <-ctx.Done():
		n.mu.Lock()
		for idx, cur := range n.waiters {
			if cur == w {
				delete(n.waiters, idx)
			}
		}
		n.mu.Unlock()
		select { // the result may have raced in
		case r := <-w.ch:
			return r.value, r.err
		default:
		}
		return nil, ctx.Err()
	}
}

// pendingProposal is a queued ProposeAndWait call.
type pendingProposal struct {
	typ     EntryType
	command []byte
	w       *waiter
}

// maxBatch bounds one group-commit batch.
const maxBatch = 1024

// runProposer implements group commit. While one batch is being appended
// (and fsynced), new proposals queue up in proposeCh; the next iteration
// takes all of them at once. Every entry gets its own index and waiter, and
// every client in the batch is answered only after the shared fsync AND
// commit — acknowledging any of them earlier would defeat durability.
func (n *Node) runProposer() {
	defer n.wg.Done()
	for {
		var first *pendingProposal
		select {
		case <-n.stopCh:
			n.failQueuedProposals(ErrStopped)
			return
		case first = <-n.proposeCh:
		}
		batch := []*pendingProposal{first}
		bytes := len(first.command)
	drain:
		for len(batch) < maxBatch && bytes < maxAppendBytes {
			select {
			case p := <-n.proposeCh:
				batch = append(batch, p)
				bytes += len(p.command)
			default:
				break drain
			}
		}
		n.mu.Lock()
		if n.stopped || n.state != Leader {
			err := ErrNotLeader
			if n.stopped {
				err = ErrStopped
			}
			n.mu.Unlock()
			for _, p := range batch {
				p.w.ch <- proposalResult{err: err}
			}
			continue
		}
		entries := make([]LogEntry, len(batch))
		next := n.lastLogIndex() + 1
		for i, p := range batch {
			entries[i] = LogEntry{Index: next + uint64(i), Term: n.ps.CurrentTerm, Type: p.typ, Command: cloneBytes(p.command)}
		}
		// Write (not fsync) under the lock, so the file stays in log order
		// with every other append...
		if err := n.storage.Write(entries); err != nil {
			n.mu.Unlock()
			for _, p := range batch {
				p.w.ch <- proposalResult{err: fmt.Errorf("raft: persist proposal: %w", err)}
			}
			continue
		}
		n.appendInMemoryLocked(entries)
		for i, p := range batch {
			p.w.term = entries[i].Term
			n.waiters[entries[i].Index] = p.w
		}
		n.triggerAllLocked() // followers replicate in parallel with our fsync
		last := entries[len(entries)-1]
		n.mu.Unlock()

		// ...and fsync OUTSIDE it, so reads, heartbeats and replication
		// responses are not stalled behind the disk. The entries are not
		// counted as ours toward a majority until this returns.
		err := n.storage.Sync()

		n.mu.Lock()
		if err != nil {
			n.logger.Error("fsync of proposed entries failed; they will commit only via a follower majority", "error", err)
		} else if t, ok := n.termAt(last.Index); ok && t == last.Term && last.Index > n.durableIndex {
			n.durableIndex = last.Index // still the same entries: now durable
		}
		n.maybeAdvanceCommitIndex() // single-voter groups commit here
		n.triggerAllLocked()
		n.mu.Unlock()
	}
}

func (n *Node) failQueuedProposals(err error) {
	for {
		select {
		case p := <-n.proposeCh:
			p.w.ch <- proposalResult{err: err}
		default:
			return
		}
	}
}
