package raft

import "fmt"

// snapshot.go — log compaction and InstallSnapshot.
//
// Once SnapshotThreshold entries have been applied since the last snapshot,
// the node captures a consistent point-in-time snapshot of the ALREADY
// APPLIED state machine, records lastIncludedIndex/lastIncludedTerm and the
// configuration at that point, writes it durably, and ONLY THEN discards the
// log prefix it covers. A crash between the two steps is harmless: on
// restart, log entries at or below the snapshot index are ignored.
//
// A follower whose nextIndex falls at or below the leader's snapshot index
// can no longer be caught up with AppendEntries (those entries are gone), so
// the leader sends InstallSnapshot instead: the follower discards its state
// machine (and any conflicting log), loads the snapshot, and resumes normal
// replication from lastIncludedIndex + 1.

// TakeSnapshot forces a snapshot at the current applied index (no-op if one
// already covers it). Returns the snapshot index.
func (n *Node) TakeSnapshot() (uint64, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return 0, ErrStopped
	}
	if err := n.takeSnapshotLocked(); err != nil {
		return 0, err
	}
	return n.firstIndex(), nil
}

func (n *Node) maybeSnapshotLocked() {
	if n.snapshotThreshold == 0 || n.sm == nil {
		return
	}
	if n.vs.LastApplied-n.firstIndex() < n.snapshotThreshold {
		return
	}
	if err := n.takeSnapshotLocked(); err != nil {
		n.logger.Error("snapshot failed", "error", err)
	}
}

func (n *Node) takeSnapshotLocked() error {
	idx := n.vs.LastApplied
	base := n.firstIndex()
	if idx <= base || n.sm == nil {
		return nil
	}
	data, err := n.sm.Snapshot()
	if err != nil {
		return fmt.Errorf("raft: state machine snapshot: %w", err)
	}
	term, _ := n.termAt(idx)
	meta := SnapshotMeta{Index: idx, Term: term, Config: n.configAt(idx)}
	if err := n.storage.SaveSnapshot(meta, data); err != nil {
		return fmt.Errorf("raft: save snapshot: %w", err)
	}
	// Snapshot is durable — now (and only now) drop the covered log prefix.
	if err := n.storage.CompactTo(idx); err != nil {
		return fmt.Errorf("raft: compact log: %w", err)
	}
	rest := n.ps.Log[idx-base+1:]
	newLog := make([]LogEntry, 0, len(rest)+1)
	newLog = append(newLog, LogEntry{Index: idx, Term: term})
	n.ps.Log = append(newLog, rest...)
	n.snapshotData = data
	n.snapshotConfig = meta.Config
	if n.durableIndex < idx { // the snapshot itself is durable
		n.durableIndex = idx
	}
	n.logger.Info("snapshot created", "snapshot_index", idx, "snapshot_term", term,
		"bytes", len(data), "term", n.ps.CurrentTerm)
	if n.hooks.OnSnapshot != nil {
		n.hooks.OnSnapshot(idx, term, false)
	}
	return nil
}

// sendSnapshot ships the current snapshot to peer. Returns true if
// replication should continue immediately.
func (n *Node) sendSnapshot(peer NodeID, term uint64) bool {
	n.mu.Lock()
	if n.stopped || n.state != Leader || n.ps.CurrentTerm != term {
		n.mu.Unlock()
		return false
	}
	req := InstallSnapshotRequest{
		Term:              term,
		LeaderID:          n.id,
		LastIncludedIndex: n.firstIndex(),
		LastIncludedTerm:  n.ps.Log[0].Term,
		Config:            n.snapshotConfig.Clone(),
		Data:              n.snapshotData,
	}
	seq := n.ls.readSeq
	n.mu.Unlock()

	n.logger.Info("sending snapshot", "peer", peer, "snapshot_index", req.LastIncludedIndex, "bytes", len(req.Data))
	resp, err := n.transport.SendInstallSnapshot(peer, req)
	if err != nil {
		return false
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
		return false
	}
	n.ls.lastContact[peer] = timeNowNano()
	if seq > n.ls.ackSeq[peer] {
		n.ls.ackSeq[peer] = seq
		n.notifyLocked()
	}
	if req.LastIncludedIndex > n.ls.MatchIndex[peer] {
		n.ls.MatchIndex[peer] = req.LastIncludedIndex
	}
	n.ls.NextIndex[peer] = n.ls.MatchIndex[peer] + 1
	n.maybeAdvanceCommitIndex()
	if n.state != Leader || n.ls == nil {
		return false
	}
	return n.ls.NextIndex[peer] <= n.lastLogIndex()
}

// HandleInstallSnapshot processes an incoming InstallSnapshot RPC.
func (n *Node) HandleInstallSnapshot(req InstallSnapshotRequest) InstallSnapshotResponse {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return InstallSnapshotResponse{}
	}
	n.observeTerm(req.Term)
	resp := InstallSnapshotResponse{Term: n.ps.CurrentTerm}
	if req.Term < n.ps.CurrentTerm {
		return resp
	}
	if n.state == Leader {
		panic(fmt.Sprintf("raft: %s: leader received InstallSnapshot in its own term — this is a bug", n.id))
	}
	if n.state == Candidate {
		n.becomeFollower()
	}
	n.recordLeaderContactLocked(req.LeaderID)

	// Already have everything the snapshot covers (it is all committed here).
	if req.LastIncludedIndex <= n.vs.CommitIndex {
		return resp
	}

	if n.sm != nil {
		if err := n.sm.Restore(req.Data); err != nil {
			n.logger.Error("restore snapshot failed", "error", err)
			return resp
		}
	}
	meta := SnapshotMeta{Index: req.LastIncludedIndex, Term: req.LastIncludedTerm, Config: req.Config.Clone()}
	if err := n.storage.SaveSnapshot(meta, req.Data); err != nil {
		panic(fmt.Sprintf("raft: %s: cannot persist installed snapshot: %v", n.id, err))
	}

	// Retain the log suffix only if our log agrees with the snapshot at its
	// last index; otherwise the whole log is superseded.
	var keep []LogEntry
	if t, ok := n.termAt(req.LastIncludedIndex); ok && t == req.LastIncludedTerm {
		keep = append(keep, n.ps.Log[req.LastIncludedIndex-n.firstIndex()+1:]...)
	}
	if err := n.storage.CompactTo(req.LastIncludedIndex); err != nil {
		panic(fmt.Sprintf("raft: %s: compact after snapshot install: %v", n.id, err))
	}
	if len(keep) == 0 {
		if err := n.storage.TruncateFrom(req.LastIncludedIndex + 1); err != nil {
			panic(fmt.Sprintf("raft: %s: reset log after snapshot install: %v", n.id, err))
		}
	}
	n.ps.Log = append([]LogEntry{{Index: req.LastIncludedIndex, Term: req.LastIncludedTerm}}, keep...)
	if n.durableIndex < req.LastIncludedIndex || len(keep) == 0 {
		n.durableIndex = req.LastIncludedIndex
	}
	if n.durableIndex > n.lastLogIndex() {
		n.durableIndex = n.lastLogIndex()
	}
	n.snapshotData = cloneBytes(req.Data)
	n.snapshotConfig = meta.Config
	n.vs.CommitIndex = req.LastIncludedIndex
	n.vs.LastApplied = req.LastIncludedIndex
	for idx, w := range n.waiters {
		if idx <= req.LastIncludedIndex {
			w.ch <- proposalResult{err: ErrLeadershipLost}
			delete(n.waiters, idx)
		}
	}
	n.recomputeConfig()
	n.logger.Info("snapshot installed", "snapshot_index", req.LastIncludedIndex,
		"leader", req.LeaderID, "term", n.ps.CurrentTerm)
	if n.hooks.OnSnapshot != nil {
		n.hooks.OnSnapshot(req.LastIncludedIndex, req.LastIncludedTerm, true)
	}
	n.notifyLocked()
	return resp
}
