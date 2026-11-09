package raft

import (
	"context"
	"fmt"
	"time"
)

// membership.go — single-server membership changes.
//
// SamanvayDB uses the dissertation's simpler single-server approach instead
// of joint consensus: a configuration change adds or removes exactly ONE
// voter, and a new change may only be proposed once the previous one has
// committed. With that restriction any majority of the old configuration
// and any majority of the new one share at least one server, so two disjoint
// majorities (two leaders in one term) can never exist during the change.
// Joint consensus is the general solution for multi-server changes at
// larger scale; it is documented future work, not implemented.
//
// New servers join as non-voting LEARNERS: they receive the log/snapshot
// but are never counted toward any quorum, so an empty node cannot affect
// commit or election math before it holds any data. Once caught up, the
// learner is promoted to a voter by a second config change.
//
// Each change is just another log entry (EntryConfig, carrying the full new
// membership). It takes effect on each server as soon as that server
// appends it, and commits through normal replication. This ordering
// guarantee is exactly what editing a peer list in a config file and
// restarting cannot provide: there, nodes adopt the change at uncoordinated
// times and two majorities can briefly coexist.

// ConfChangeType enumerates supported membership changes.
type ConfChangeType int

const (
	// AddLearner adds a non-voting member.
	AddLearner ConfChangeType = iota
	// PromoteLearner turns a learner into a voter.
	PromoteLearner
	// RemoveMember removes a voter or learner.
	RemoveMember
)

func (t ConfChangeType) String() string {
	switch t {
	case AddLearner:
		return "add_learner"
	case PromoteLearner:
		return "promote"
	case RemoveMember:
		return "remove"
	default:
		return fmt.Sprintf("unknown(%d)", t)
	}
}

// ConfChange is one single-server membership change.
type ConfChange struct {
	Type ConfChangeType
	Node NodeID
	Addr string // for AddLearner
}

// ProposeConfChange proposes a membership change and waits for it to commit.
// It returns the index of the committed config entry.
func (n *Node) ProposeConfChange(ctx context.Context, cc ConfChange) (uint64, error) {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return 0, ErrStopped
	}
	if n.state != Leader {
		n.mu.Unlock()
		return 0, ErrNotLeader
	}
	if t, _ := n.termAt(n.vs.CommitIndex); t != n.ps.CurrentTerm {
		n.mu.Unlock()
		return 0, ErrLeaderNotReady
	}
	if n.configIndex > n.vs.CommitIndex {
		n.mu.Unlock()
		return 0, ErrConfigChangeInProgress
	}
	next, err := applyConfChange(n.config, cc)
	if err != nil {
		n.mu.Unlock()
		return 0, err
	}
	w := &waiter{ch: make(chan proposalResult, 1)}
	idx, _, err := n.proposeLocked(EntryConfig, next.Encode(), w)
	n.mu.Unlock()
	if err != nil {
		return 0, err
	}
	n.logger.Info("membership change proposed", "change", cc.Type.String(), "node", cc.Node, "index", idx)
	select {
	case r := <-w.ch:
		return idx, r.err
	case <-ctx.Done():
		return idx, ctx.Err()
	}
}

func applyConfChange(cur Membership, cc ConfChange) (Membership, error) {
	next := cur.Clone()
	switch cc.Type {
	case AddLearner:
		if cur.Contains(cc.Node) {
			return next, fmt.Errorf("%w: %s is already a member", ErrInvalidConfChange, cc.Node)
		}
		next.Members = append(next.Members, Member{ID: cc.Node, Addr: cc.Addr, Learner: true})
	case PromoteLearner:
		mem, ok := cur.Get(cc.Node)
		if !ok || !mem.Learner {
			return next, fmt.Errorf("%w: %s is not a learner", ErrInvalidConfChange, cc.Node)
		}
		for i := range next.Members {
			if next.Members[i].ID == cc.Node {
				next.Members[i].Learner = false
			}
		}
	case RemoveMember:
		if !cur.Contains(cc.Node) {
			return next, fmt.Errorf("%w: %s is not a member", ErrInvalidConfChange, cc.Node)
		}
		if cur.IsVoter(cc.Node) && len(cur.Voters()) == 1 {
			return next, fmt.Errorf("%w: cannot remove the last voter", ErrInvalidConfChange)
		}
		out := next.Members[:0]
		for _, m := range next.Members {
			if m.ID != cc.Node {
				out = append(out, m)
			}
		}
		next.Members = out
	default:
		return next, fmt.Errorf("%w: unknown type %d", ErrInvalidConfChange, cc.Type)
	}
	next.normalize()
	return next, nil
}

// ReadIndex implements linearizable reads without writing to the log
// (Raft dissertation §6.4):
//
//  1. The leader must have committed an entry from its current term (its
//     no-op), so its commitIndex is at least as large as any previously
//     committed index.
//  2. It records readIndex = commitIndex.
//  3. It confirms it is still the leader by getting a heartbeat round
//     acknowledged by a majority AFTER the read arrived (a deposed leader
//     cannot gather such a majority).
//  4. It waits until lastApplied >= readIndex.
//
// A read served from the state machine after ReadIndex returns therefore
// reflects every write committed before the read began.
func (n *Node) ReadIndex(ctx context.Context) (uint64, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for {
		if n.stopped {
			return 0, ErrStopped
		}
		if n.state != Leader {
			return 0, ErrNotLeader
		}
		if t, _ := n.termAt(n.vs.CommitIndex); t == n.ps.CurrentTerm {
			break
		}
		if err := n.waitChangeLocked(ctx); err != nil {
			return 0, err
		}
	}
	readIdx := n.vs.CommitIndex
	term := n.ps.CurrentTerm

	if len(n.otherVoters()) > 0 {
		n.ls.readSeq++
		target := n.ls.readSeq
		n.triggerAllLocked()
		for {
			if n.stopped {
				return 0, ErrStopped
			}
			if n.state != Leader || n.ps.CurrentTerm != term {
				return 0, ErrNotLeader
			}
			acks := 0
			for _, v := range n.config.Voters() {
				if v == n.id || n.ls.ackSeq[v] >= target {
					acks++
				}
			}
			if acks >= n.config.Quorum() {
				break
			}
			if err := n.waitChangeLocked(ctx); err != nil {
				return 0, err
			}
		}
	}

	for n.vs.LastApplied < readIdx {
		if n.stopped {
			return 0, ErrStopped
		}
		if err := n.waitChangeLocked(ctx); err != nil {
			return 0, err
		}
	}
	return readIdx, nil
}

// waitChangeLocked releases mu until the next progress notification, ctx
// expiry, or stop, then re-acquires it.
func (n *Node) waitChangeLocked(ctx context.Context) error {
	ch := n.changed
	n.mu.Unlock()
	defer n.mu.Lock()
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-n.stopCh:
		return ErrStopped
	}
}

func timeNowNano() int64 { return time.Now().UnixNano() }
