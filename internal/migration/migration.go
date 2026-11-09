// Package migration moves a shard's replica set from one group of nodes to
// another with no lost or duplicated writes, and rebalances replicas when
// nodes join.
//
// State progression (each transition is validated against the legal graph):
//
//	PENDING -> SNAPSHOTTING -> TRANSFERRING -> CATCHING_UP -> CUTOVER -> DONE
//	   \______________\_______________\______________\
//	                                                  -> ABORTED  (any pre-CUTOVER state)
//
// Every stage before CUTOVER is trivially abortable: the source group stays
// fully authoritative and keeps serving reads and writes, so a failure while
// snapshotting/transferring/catching up is just "throw away the half-built
// target replicas and try again" — zero risk to live data.
//
// How the target group is built — a deliberate design choice. Instead of
// bootstrapping a SEPARATE Raft group and replaying the source's tail into
// it (which would mint different log indexes and break fencing-token
// monotonicity for the lock subsystem), the target nodes join the SAME Raft
// group as non-voting learners, receive the source's snapshot through
// InstallSnapshot, and tail the log through ordinary AppendEntries. Cutover
// is then a sequence of single-server configuration changes (promote each
// target, remove each source, the current leader last). Consequences:
//
//   - zero gaps by construction: the target replicas hold the exact same log;
//   - no write freeze at all (better than freeze-and-flip): the only
//     unavailability is one leader election (~election timeout) when the
//     source leader removes itself;
//   - the shard map is updated (durably, through the metadata Raft group)
//     after the configuration has committed, and clients that still route
//     to a removed node simply get NOT_LEADER and refresh.
//
// Dual-write-with-idempotent-replay is the documented, more complex
// alternative; it is not implemented.
package migration

import (
	"fmt"
	"sync"
	"time"
)

// State is a migration stage.
type State string

const (
	Pending      State = "PENDING"
	Snapshotting State = "SNAPSHOTTING"
	Transferring State = "TRANSFERRING"
	CatchingUp   State = "CATCHING_UP"
	Cutover      State = "CUTOVER"
	Done         State = "DONE"
	Aborted      State = "ABORTED"
)

var legal = map[State][]State{
	Pending:      {Snapshotting, Aborted},
	Snapshotting: {Transferring, Aborted},
	Transferring: {CatchingUp, Aborted},
	CatchingUp:   {Cutover, Aborted},
	Cutover:      {Done},
}

// Abortable reports whether a migration in state s may still be aborted.
func Abortable(s State) bool {
	for _, to := range legal[s] {
		if to == Aborted {
			return true
		}
	}
	return false
}

// Transition records one state change.
type Transition struct {
	From, To State
	At       time.Time
}

// Migration tracks one shard migration.
type Migration struct {
	mu      sync.Mutex
	ShardID int32
	Source  []string
	Target  []string
	state   State
	history []Transition
	onTrans func(Transition)
}

// New creates a migration in PENDING.
func New(shard int32, source, target []string, onTransition func(Transition)) *Migration {
	return &Migration{ShardID: shard, Source: source, Target: target, state: Pending, onTrans: onTransition}
}

// State returns the current state.
func (m *Migration) State() State {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state
}

// History returns every transition so far.
func (m *Migration) History() []Transition {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]Transition(nil), m.history...)
}

// To moves to state `to`, rejecting illegal transitions.
func (m *Migration) To(to State) error {
	m.mu.Lock()
	from := m.state
	ok := false
	for _, allowed := range legal[from] {
		if allowed == to {
			ok = true
		}
	}
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("migration: illegal transition %s -> %s", from, to)
	}
	t := Transition{From: from, To: to, At: time.Now()}
	m.state = to
	m.history = append(m.history, t)
	cb := m.onTrans
	m.mu.Unlock()
	if cb != nil {
		cb(t)
	}
	return nil
}
