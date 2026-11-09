package raftsim

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"samanvaydb/internal/logging"
	"samanvaydb/internal/raft"
)

// RecordingSM is a deterministic test state machine that records every
// applied EntryNormal command in order.
type RecordingSM struct {
	mu      sync.Mutex
	applied []AppliedEntry
}

// AppliedEntry is one command applied by a RecordingSM.
type AppliedEntry struct {
	Index   uint64 `json:"i"`
	Term    uint64 `json:"t"`
	Command []byte `json:"c"`
}

// Apply records normal entries; it returns the entry index as the result.
func (s *RecordingSM) Apply(e raft.LogEntry) any {
	if e.Type != raft.EntryNormal {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.applied = append(s.applied, AppliedEntry{Index: e.Index, Term: e.Term, Command: append([]byte(nil), e.Command...)})
	return e.Index
}

// Snapshot serializes the applied history.
func (s *RecordingSM) Snapshot() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.applied)
}

// Restore replaces the applied history.
func (s *RecordingSM) Restore(b []byte) error {
	var a []AppliedEntry
	if len(b) > 0 {
		if err := json.Unmarshal(b, &a); err != nil {
			return err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.applied = a
	return nil
}

// Commands returns the applied commands in order.
func (s *RecordingSM) Commands() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.applied))
	for i, a := range s.applied {
		out[i] = string(a.Command)
	}
	return out
}

// Entries returns a copy of the applied history.
func (s *RecordingSM) Entries() []AppliedEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]AppliedEntry(nil), s.applied...)
}

// Options configures a simulated cluster.
type Options struct {
	Seed               int64
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration
	SnapshotThreshold  uint64
	DisablePreVote     bool
	// LogLevel for node loggers (default: error-only).
	LogLevel slog.Level
}

func (o *Options) defaults() {
	if o.ElectionTimeoutMin == 0 {
		o.ElectionTimeoutMin = 150 * time.Millisecond
	}
	if o.ElectionTimeoutMax == 0 {
		o.ElectionTimeoutMax = 300 * time.Millisecond
	}
	if o.HeartbeatInterval == 0 {
		o.HeartbeatInterval = 30 * time.Millisecond
	}
	if o.Seed == 0 {
		o.Seed = 1
	}
	if o.LogLevel == 0 {
		o.LogLevel = slog.LevelError + 4
	}
}

// Cluster runs N Raft nodes over a simulated Network. It records every
// leader per term so tests can assert Election Safety (at most one leader
// per term) across the whole run.
type Cluster struct {
	Net *Network
	IDs []raft.NodeID

	opts Options

	mu        sync.Mutex
	nodes     map[raft.NodeID]*raft.Node
	sms       map[raft.NodeID]*RecordingSM
	storages  map[raft.NodeID]raft.Storage
	leaders   map[uint64]map[raft.NodeID]bool // term -> leaders seen
	elections map[raft.NodeID]int
}

// NewCluster creates and starts an n-node cluster (node-1..node-n).
func NewCluster(n int, opts Options) (*Cluster, error) {
	opts.defaults()
	c := &Cluster{
		Net:       NewNetwork(opts.Seed),
		opts:      opts,
		nodes:     map[raft.NodeID]*raft.Node{},
		sms:       map[raft.NodeID]*RecordingSM{},
		storages:  map[raft.NodeID]raft.Storage{},
		leaders:   map[uint64]map[raft.NodeID]bool{},
		elections: map[raft.NodeID]int{},
	}
	for i := 1; i <= n; i++ {
		c.IDs = append(c.IDs, raft.NodeID(fmt.Sprintf("node-%d", i)))
	}
	for _, id := range c.IDs {
		c.storages[id] = raft.NewMemoryStorage()
		if err := c.start(id, nil, false); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// AddNode starts a new, empty node that joins later via membership change.
func (c *Cluster) AddNode(id raft.NodeID) error {
	c.mu.Lock()
	c.IDs = append(c.IDs, id)
	c.storages[id] = raft.NewMemoryStorage()
	c.mu.Unlock()
	return c.start(id, nil, true)
}

func (c *Cluster) start(id raft.NodeID, _ *raft.Membership, join bool) error {
	sm := &RecordingSM{}
	var peers []raft.NodeID
	for _, p := range c.IDs {
		if p != id {
			peers = append(peers, p)
		}
	}
	c.mu.Lock()
	st := c.storages[id]
	seed := c.opts.Seed*1000 + int64(len(c.IDs))*7 + int64(hash(string(id)))
	c.mu.Unlock()

	node, err := raft.NewNode(raft.Config{
		ID:                 id,
		Peers:              peers,
		Join:               join,
		StateMachine:       sm,
		Transport:          c.Net.Transport(id),
		Storage:            st,
		Logger:             logging.NewLogger(logging.Options{NodeID: string(id), Level: c.opts.LogLevel}),
		ElectionTimeoutMin: c.opts.ElectionTimeoutMin,
		ElectionTimeoutMax: c.opts.ElectionTimeoutMax,
		HeartbeatInterval:  c.opts.HeartbeatInterval,
		SnapshotThreshold:  c.opts.SnapshotThreshold,
		DisablePreVote:     c.opts.DisablePreVote,
		Seed:               seed,
		Hooks: raft.Hooks{
			OnStateChange: func(term uint64, s raft.State, _ raft.NodeID) {
				if s == raft.Leader {
					c.mu.Lock()
					if c.leaders[term] == nil {
						c.leaders[term] = map[raft.NodeID]bool{}
					}
					c.leaders[term][id] = true
					c.mu.Unlock()
				}
			},
			OnElectionStarted: func(uint64) {
				c.mu.Lock()
				c.elections[id]++
				c.mu.Unlock()
			},
		},
	})
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.nodes[id] = node
	c.sms[id] = sm
	c.mu.Unlock()
	c.Net.Register(id, node)
	node.Start()
	return nil
}

func hash(s string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}

// Node returns the live node for id (nil if crashed).
func (c *Cluster) Node(id raft.NodeID) *raft.Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[id]
}

// SM returns the state machine of the current incarnation of id.
func (c *Cluster) SM(id raft.NodeID) *RecordingSM {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sms[id]
}

// Crash stops id abruptly. Its persisted storage survives; its in-memory
// state machine does not (exactly like a process crash).
func (c *Cluster) Crash(id raft.NodeID) {
	c.mu.Lock()
	n := c.nodes[id]
	delete(c.nodes, id)
	c.mu.Unlock()
	c.Net.Unregister(id)
	if n != nil {
		n.Stop()
	}
}

// Restart brings a crashed node back from its persisted storage.
func (c *Cluster) Restart(id raft.NodeID) error {
	return c.start(id, nil, false)
}

// Shutdown stops every node.
func (c *Cluster) Shutdown() {
	c.mu.Lock()
	ids := make([]raft.NodeID, 0, len(c.nodes))
	for id := range c.nodes {
		ids = append(ids, id)
	}
	c.mu.Unlock()
	for _, id := range ids {
		c.Crash(id)
	}
}

// Live returns the IDs of running nodes.
func (c *Cluster) Live() []raft.NodeID {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []raft.NodeID
	for id := range c.nodes {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// Leader returns the live node that believes it is leader with the highest
// term, restricted to ids if given.
func (c *Cluster) Leader(ids ...raft.NodeID) (raft.NodeID, uint64, bool) {
	if len(ids) == 0 {
		ids = c.Live()
	}
	var (
		best     raft.NodeID
		bestTerm uint64
		found    bool
	)
	for _, id := range ids {
		n := c.Node(id)
		if n == nil {
			continue
		}
		st := n.Status()
		if st.State == raft.Leader && (!found || st.Term > bestTerm) {
			best, bestTerm, found = id, st.Term, true
		}
	}
	return best, bestTerm, found
}

// WaitForLeader waits until some node among ids (default: all live) is a
// leader that a majority of ids acknowledge.
func (c *Cluster) WaitForLeader(timeout time.Duration, ids ...raft.NodeID) (raft.NodeID, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if id, term, ok := c.Leader(ids...); ok {
			// Require a stable leader: it must still hold the role shortly after.
			time.Sleep(c.opts.HeartbeatInterval * 2)
			if n := c.Node(id); n != nil {
				st := n.Status()
				if st.State == raft.Leader && st.Term == term {
					return id, nil
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return "", fmt.Errorf("raftsim: no leader elected within %v", timeout)
}

// Propose submits cmd through whichever node is leader among ids (default:
// all live), retrying across leader changes until it commits or timeout.
func (c *Cluster) Propose(cmd []byte, timeout time.Duration, ids ...raft.NodeID) (uint64, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		id, _, ok := c.Leader(ids...)
		if !ok {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		n := c.Node(id)
		if n == nil {
			continue
		}
		remaining := time.Until(deadline)
		if remaining > time.Second {
			remaining = time.Second
		}
		ctx, cancel := context.WithTimeout(context.Background(), remaining)
		res, err := n.ProposeAndWait(ctx, cmd)
		cancel()
		if err == nil {
			idx, _ := res.(uint64)
			return idx, nil
		}
		if errors.Is(err, raft.ErrStopped) || errors.Is(err, raft.ErrNotLeader) ||
			errors.Is(err, raft.ErrLeadershipLost) || errors.Is(err, context.DeadlineExceeded) {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		return 0, err
	}
	return 0, fmt.Errorf("raftsim: proposal %q not committed within %v", cmd, timeout)
}

// WaitApplied waits until every node in ids has applied through index.
func (c *Cluster) WaitApplied(index uint64, timeout time.Duration, ids ...raft.NodeID) error {
	if len(ids) == 0 {
		ids = c.Live()
	}
	deadline := time.Now().Add(timeout)
	for _, id := range ids {
		n := c.Node(id)
		if n == nil {
			return fmt.Errorf("raftsim: %s is not running", id)
		}
		if !n.WaitApplied(index, time.Until(deadline)) {
			return fmt.Errorf("raftsim: %s did not apply index %d within %v (status %+v)", id, index, timeout, n.Status())
		}
	}
	return nil
}

// CheckElectionSafety returns an error if two different nodes were ever
// leader in the same term.
func (c *Cluster) CheckElectionSafety() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for term, ls := range c.leaders {
		if len(ls) > 1 {
			var ids []string
			for id := range ls {
				ids = append(ids, string(id))
			}
			sort.Strings(ids)
			return fmt.Errorf("election safety violated: term %d had leaders %v", term, ids)
		}
	}
	return nil
}

// Elections returns how many elections id has started.
func (c *Cluster) Elections(id raft.NodeID) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.elections[id]
}

// CheckLogsAgree verifies State Machine Safety: every pair of nodes agrees
// on every index both have applied.
func (c *Cluster) CheckLogsAgree(ids ...raft.NodeID) error {
	if len(ids) == 0 {
		ids = c.Live()
	}
	byIndex := map[uint64]string{}
	owner := map[uint64]raft.NodeID{}
	for _, id := range ids {
		sm := c.SM(id)
		if sm == nil {
			continue
		}
		for _, e := range sm.Entries() {
			if prev, ok := byIndex[e.Index]; ok && prev != string(e.Command) {
				return fmt.Errorf("state machine safety violated at index %d: %s applied %q, %s applied %q",
					e.Index, owner[e.Index], prev, id, e.Command)
			}
			byIndex[e.Index] = string(e.Command)
			owner[e.Index] = id
		}
	}
	return nil
}
