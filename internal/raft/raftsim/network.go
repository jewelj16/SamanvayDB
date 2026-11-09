// Package raftsim is an in-process network simulator and cluster harness for
// deterministic, repeatable Raft failure testing.
//
// Several Raft nodes run as goroutines inside one test process, and every
// RPC between them is routed through Network, which can drop, delay,
// duplicate, or reorder specific messages, or partition the cluster, on
// command. It is the primary Raft test tool (the unit-test tier); the
// operational fault injector in internal/faultinjector covers real
// multi-process clusters.
package raftsim

import (
	"errors"
	"math/rand"
	"sync"
	"time"

	"samanvaydb/internal/raft"
)

// Errors returned to senders for undeliverable messages.
var (
	ErrUnreachable = errors.New("raftsim: destination unreachable")
	ErrDropped     = errors.New("raftsim: message dropped")
)

// NetworkSimulator is the fault-control surface of the simulator.
type NetworkSimulator interface {
	DropMessages(from, to raft.NodeID, rate float64)
	DelayMessages(from, to raft.NodeID, d time.Duration)
	Partition(sideA, sideB []raft.NodeID)
	HealPartition()
}

type link struct{ from, to raft.NodeID }

// LinkStats counts what happened to messages on one directed link.
type LinkStats struct {
	Delivered  int
	Dropped    int
	Blocked    int
	Duplicated int
}

// Network routes RPCs between registered handlers.
type Network struct {
	mu       sync.Mutex
	handlers map[raft.NodeID]raft.RPCHandler
	drop     map[link]float64
	delay    map[link]time.Duration
	jitter   map[link]time.Duration
	dup      map[link]float64
	blocked  map[link]bool
	stats    map[link]*LinkStats
	rng      *rand.Rand
}

var _ NetworkSimulator = (*Network)(nil)

// NewNetwork creates a simulator. All randomness (drop/duplicate decisions,
// jitter) comes from seed, so a scenario's fault decisions are reproducible.
func NewNetwork(seed int64) *Network {
	return &Network{
		handlers: map[raft.NodeID]raft.RPCHandler{},
		drop:     map[link]float64{},
		delay:    map[link]time.Duration{},
		jitter:   map[link]time.Duration{},
		dup:      map[link]float64{},
		blocked:  map[link]bool{},
		stats:    map[link]*LinkStats{},
		rng:      rand.New(rand.NewSource(seed)),
	}
}

// Register attaches a node's RPC handler under id.
func (n *Network) Register(id raft.NodeID, h raft.RPCHandler) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handlers[id] = h
}

// Unregister detaches id; messages to it fail as unreachable (a crash).
func (n *Network) Unregister(id raft.NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.handlers, id)
}

// Transport returns the raft.Transport a node with ID from should use.
func (n *Network) Transport(from raft.NodeID) raft.Transport {
	return &endpoint{net: n, from: from}
}

// DropMessages drops messages from->to with probability rate (0..1).
// Applies independently to the request and the response leg.
func (n *Network) DropMessages(from, to raft.NodeID, rate float64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.drop[link{from, to}] = rate
}

// DelayMessages delays every message from->to by d before delivery.
func (n *Network) DelayMessages(from, to raft.NodeID, d time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.delay[link{from, to}] = d
}

// JitterMessages adds a random extra delay in [0, max) to messages
// from->to, which reorders concurrently sent messages.
func (n *Network) JitterMessages(from, to raft.NodeID, max time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.jitter[link{from, to}] = max
}

// DuplicateMessages delivers requests from->to twice with probability rate.
func (n *Network) DuplicateMessages(from, to raft.NodeID, rate float64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.dup[link{from, to}] = rate
}

// Partition blocks all traffic between sideA and sideB (both directions).
// Traffic within each side is unaffected. Partitions accumulate until
// HealPartition.
func (n *Network) Partition(sideA, sideB []raft.NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, a := range sideA {
		for _, b := range sideB {
			n.blocked[link{a, b}] = true
			n.blocked[link{b, a}] = true
		}
	}
}

// Isolate cuts id off from every other registered node.
func (n *Network) Isolate(id raft.NodeID, all []raft.NodeID) {
	var others []raft.NodeID
	for _, o := range all {
		if o != id {
			others = append(others, o)
		}
	}
	n.Partition([]raft.NodeID{id}, others)
}

// HealPartition removes every partition.
func (n *Network) HealPartition() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.blocked = map[link]bool{}
}

// Reset removes every fault (partitions, drops, delays, duplication).
func (n *Network) Reset() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.blocked = map[link]bool{}
	n.drop = map[link]float64{}
	n.delay = map[link]time.Duration{}
	n.jitter = map[link]time.Duration{}
	n.dup = map[link]float64{}
}

// Stats returns a copy of the counters for link from->to.
func (n *Network) Stats(from, to raft.NodeID) LinkStats {
	n.mu.Lock()
	defer n.mu.Unlock()
	if s, ok := n.stats[link{from, to}]; ok {
		return *s
	}
	return LinkStats{}
}

func (n *Network) statsLocked(l link) *LinkStats {
	s, ok := n.stats[l]
	if !ok {
		s = &LinkStats{}
		n.stats[l] = s
	}
	return s
}

// deliver runs fn against the destination handler, applying faults to the
// request leg (before fn) and the response leg (after fn). A response lost
// after fn ran models "the receiver processed it but the sender never
// heard back" — the sender sees an error either way.
func (n *Network) deliver(from, to raft.NodeID, fn func(h raft.RPCHandler)) error {
	l := link{from, to}
	back := link{to, from}

	n.mu.Lock()
	h := n.handlers[to]
	st := n.statsLocked(l)
	if n.blocked[l] {
		st.Blocked++
		n.mu.Unlock()
		return ErrUnreachable
	}
	if h == nil {
		n.mu.Unlock()
		return ErrUnreachable
	}
	if r := n.drop[l]; r > 0 && n.rng.Float64() < r {
		st.Dropped++
		n.mu.Unlock()
		return ErrDropped
	}
	d := n.delay[l]
	if j := n.jitter[l]; j > 0 {
		d += time.Duration(n.rng.Int63n(int64(j)))
	}
	dup := n.dup[l] > 0 && n.rng.Float64() < n.dup[l]
	n.mu.Unlock()

	if d > 0 {
		time.Sleep(d)
	}
	// A partition created while the message was "in flight" also blocks it.
	n.mu.Lock()
	if n.blocked[l] {
		n.statsLocked(l).Blocked++
		n.mu.Unlock()
		return ErrUnreachable
	}
	n.mu.Unlock()

	fn(h)
	n.mu.Lock()
	n.statsLocked(l).Delivered++
	if dup {
		n.statsLocked(l).Duplicated++
	}
	n.mu.Unlock()
	if dup {
		fn(h) // duplicate delivery; the receiver must treat it idempotently
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.blocked[back] {
		n.statsLocked(back).Blocked++
		return ErrUnreachable
	}
	if r := n.drop[back]; r > 0 && n.rng.Float64() < r {
		n.statsLocked(back).Dropped++
		return ErrDropped
	}
	return nil
}

type endpoint struct {
	net  *Network
	from raft.NodeID
}

func (e *endpoint) SendRequestVote(to raft.NodeID, req raft.RequestVoteRequest) (raft.RequestVoteResponse, error) {
	var (
		resp raft.RequestVoteResponse
		got  bool
	)
	err := e.net.deliver(e.from, to, func(h raft.RPCHandler) {
		r := h.HandleRequestVote(req)
		if !got {
			resp, got = r, true
		}
	})
	return resp, err
}

func (e *endpoint) SendAppendEntries(to raft.NodeID, req raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	var (
		resp raft.AppendEntriesResponse
		got  bool
	)
	// Each delivery gets its own copy, as it would after real serialization.
	err := e.net.deliver(e.from, to, func(h raft.RPCHandler) {
		cp := req
		cp.Entries = append([]raft.LogEntry(nil), req.Entries...)
		r := h.HandleAppendEntries(cp)
		if !got {
			resp, got = r, true
		}
	})
	return resp, err
}

func (e *endpoint) SendInstallSnapshot(to raft.NodeID, req raft.InstallSnapshotRequest) (raft.InstallSnapshotResponse, error) {
	var (
		resp raft.InstallSnapshotResponse
		got  bool
	)
	err := e.net.deliver(e.from, to, func(h raft.RPCHandler) {
		r := h.HandleInstallSnapshot(req)
		if !got {
			resp, got = r, true
		}
	})
	return resp, err
}
