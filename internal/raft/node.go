package raft

import (
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"samanvaydb/internal/logging"
)

// Config configures a Node.
type Config struct {
	ID NodeID

	// Peers are the other voting members of a brand-new group. Used only
	// when Storage holds no prior state; afterwards membership comes from
	// the log/snapshot. Ignored if Bootstrap is set or Join is true.
	Peers []NodeID
	// Bootstrap is an explicit initial membership (with addresses/learners).
	Bootstrap *Membership
	// Join starts the node with an EMPTY membership: it never campaigns and
	// simply waits for an existing leader to add it as a learner and stream
	// it the log/snapshot.
	Join bool

	StateMachine StateMachine
	Transport    Transport
	// Storage defaults to a fresh MemoryStorage.
	Storage Storage
	Logger  *logging.Logger
	Hooks   Hooks

	// Timing. Zero values take the defaults below. The heartbeat interval
	// must stay well under the minimum election timeout (≥3–5×), or ordinary
	// network jitter triggers spurious elections against a healthy leader.
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration

	// SnapshotThreshold triggers a snapshot once this many entries have been
	// applied since the last one. 0 disables automatic snapshots.
	SnapshotThreshold uint64
	// MaxEntriesPerAppend bounds a single AppendEntries batch.
	MaxEntriesPerAppend int

	// DisablePreVote turns off the Pre-Vote phase (tests of the classic
	// algorithm only).
	DisablePreVote bool
	// Seed seeds this node's election-timeout randomness (0 = time-based).
	Seed int64
}

// Default timing (milliseconds): election 150–300, heartbeat 50 — a 3×
// margin below the minimum election timeout.
const (
	DefaultElectionTimeoutMin = 150 * time.Millisecond
	DefaultElectionTimeoutMax = 300 * time.Millisecond
	DefaultHeartbeatInterval  = 50 * time.Millisecond
	defaultMaxEntries         = 512
)

// Node is a single Raft participant. It holds all state (persistent + volatile)
// and exposes RPC handlers (RequestVote, AppendEntries, InstallSnapshot).
//
// All exported methods are safe for concurrent use; the internal mu protects
// all mutable state. RPC handlers run in goroutines dispatched by the transport.
type Node struct {
	mu sync.Mutex

	// Identity
	id NodeID

	// Persistent state — saved to stable storage before responding to RPCs.
	ps PersistentState

	// Volatile state — safely re-derived on restart.
	vs VolatileState

	// durableIndex: every log entry at or below it is known to be fsynced
	// locally. Entries above it may have been written but not yet synced
	// (leader group commit). A leader counts ITSELF toward a commit majority
	// only for entries <= durableIndex.
	durableIndex uint64

	// Leader-only volatile state — re-initialized on each new election win.
	ls *LeaderVolatileState

	// Current role and known leader.
	state  State
	leader NodeID

	// Membership. config is the LATEST configuration in the log (in effect
	// as soon as appended); configIndex is the index of the entry holding it
	// (or the snapshot index if it came from a snapshot).
	config         Membership
	configIndex    uint64
	snapshotConfig Membership
	snapshotData   []byte

	sm        StateMachine
	transport Transport
	storage   Storage
	hooks     Hooks

	electionMin, electionMax, heartbeat time.Duration
	snapshotThreshold                   uint64
	maxEntries                          int
	preVote                             bool
	rng                                 *rand.Rand

	electionDeadline  time.Time
	lastLeaderContact time.Time
	lastHeartbeat     time.Time
	lastQuorumCheck   time.Time
	campaigning       bool

	replicators map[NodeID]*replicator
	waiters     map[uint64]*waiter
	proposeCh   chan *pendingProposal // group-commit queue (see runProposer)
	changed     chan struct{}         // closed & replaced whenever commit/apply/acks advance

	started  bool
	stopped  bool
	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup

	logger *logging.Logger
}

type waiter struct {
	term uint64
	ch   chan proposalResult
}

type proposalResult struct {
	value any
	err   error
}

// NewNode creates a Raft node, recovering any state found in cfg.Storage.
// The node starts as a Follower. Call Start() to begin its timers.
func NewNode(cfg Config) (*Node, error) {
	if cfg.ID == "" {
		return nil, fmt.Errorf("raft: config: ID is required")
	}
	if cfg.Storage == nil {
		cfg.Storage = NewMemoryStorage()
	}
	if cfg.Logger == nil {
		cfg.Logger = logging.NewLogger(logging.Options{NodeID: string(cfg.ID), Level: slog.LevelError})
	}
	if cfg.ElectionTimeoutMin == 0 {
		cfg.ElectionTimeoutMin = DefaultElectionTimeoutMin
	}
	if cfg.ElectionTimeoutMax <= cfg.ElectionTimeoutMin {
		cfg.ElectionTimeoutMax = 2 * cfg.ElectionTimeoutMin
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = DefaultHeartbeatInterval
	}
	if cfg.MaxEntriesPerAppend <= 0 {
		cfg.MaxEntriesPerAppend = defaultMaxEntries
	}
	seed := cfg.Seed
	if seed == 0 {
		seed = time.Now().UnixNano() ^ int64(len(cfg.ID))<<32 ^ int64(hashID(cfg.ID))
	}

	n := &Node{
		id:                cfg.ID,
		state:             Follower,
		sm:                cfg.StateMachine,
		transport:         cfg.Transport,
		storage:           cfg.Storage,
		hooks:             cfg.Hooks,
		electionMin:       cfg.ElectionTimeoutMin,
		electionMax:       cfg.ElectionTimeoutMax,
		heartbeat:         cfg.HeartbeatInterval,
		snapshotThreshold: cfg.SnapshotThreshold,
		maxEntries:        cfg.MaxEntriesPerAppend,
		preVote:           !cfg.DisablePreVote,
		rng:               rand.New(rand.NewSource(seed)),
		replicators:       make(map[NodeID]*replicator),
		waiters:           make(map[uint64]*waiter),
		proposeCh:         make(chan *pendingProposal, 4*maxBatch),
		changed:           make(chan struct{}),
		stopCh:            make(chan struct{}),
		logger:            cfg.Logger.WithComponent("raft"),
	}
	n.ps.Log = []LogEntry{{Index: 0, Term: 0}}

	if err := n.recover(cfg); err != nil {
		return nil, err
	}
	n.resetElectionTimer()
	return n, nil
}

func hashID(id NodeID) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(id); i++ {
		h ^= uint32(id[i])
		h *= 16777619
	}
	return h
}

// recover loads persisted state: snapshot first, then the log tail after it.
func (n *Node) recover(cfg Config) error {
	hs, meta, snap, entries, ok, err := n.storage.InitialState()
	if err != nil {
		return fmt.Errorf("raft: recover: %w", err)
	}
	if !ok {
		// Brand-new group member: persist the bootstrap configuration as an
		// index-0 snapshot so a restart never "re-bootstraps" differently.
		var boot Membership
		switch {
		case cfg.Join:
			boot = Membership{}
		case cfg.Bootstrap != nil:
			boot = cfg.Bootstrap.Clone()
			boot.normalize()
		default:
			boot = NewMembership(append([]NodeID{cfg.ID}, cfg.Peers...)...)
		}
		if err := n.storage.SaveSnapshot(SnapshotMeta{Config: boot}, nil); err != nil {
			return fmt.Errorf("raft: persist bootstrap config: %w", err)
		}
		n.config, n.snapshotConfig = boot, boot
		return nil
	}

	n.ps.CurrentTerm = hs.Term
	if hs.VotedFor != "" {
		v := hs.VotedFor
		n.ps.VotedFor = &v
	}
	if snap != nil && n.sm != nil {
		if err := n.sm.Restore(snap); err != nil {
			return fmt.Errorf("raft: recover: restore snapshot: %w", err)
		}
	}
	n.snapshotData = snap
	n.snapshotConfig = meta.Config
	n.ps.Log = []LogEntry{{Index: meta.Index, Term: meta.Term}}
	n.vs.CommitIndex, n.vs.LastApplied = meta.Index, meta.Index

	// Keep only the contiguous tail that directly follows the snapshot and
	// is consistent with it; anything else is a remnant of a crash between
	// "snapshot saved" and "log compacted/reset" and is repaired here.
	var tail []LogEntry
	next := meta.Index + 1
	for _, e := range entries {
		if e.Index <= meta.Index {
			if e.Index == meta.Index && meta.Index > 0 && e.Term != meta.Term {
				tail, next = nil, 0 // log diverges from snapshot: discard all
				break
			}
			continue
		}
		if e.Index != next {
			break
		}
		tail = append(tail, e)
		next++
	}
	if len(entries) > 0 {
		if err := n.storage.CompactTo(meta.Index); err != nil {
			return err
		}
		if err := n.storage.TruncateFrom(meta.Index + uint64(len(tail)) + 1); err != nil {
			return err
		}
	}
	n.ps.Log = append(n.ps.Log, tail...)
	n.durableIndex = n.lastLogIndex()
	n.config, n.configIndex = n.snapshotConfig, meta.Index
	n.recomputeConfig()
	n.logger.Info("recovered persistent state",
		"term", n.ps.CurrentTerm, "snapshot_index", meta.Index,
		"last_index", n.lastLogIndex(), "config", n.config.String())
	return nil
}

// Start launches the background ticker. Safe to call once.
func (n *Node) Start() {
	n.mu.Lock()
	if n.started || n.stopped {
		n.mu.Unlock()
		return
	}
	n.started = true
	n.mu.Unlock()
	n.wg.Add(2)
	go n.run()
	go n.runProposer()
}

// Stop shuts down all background goroutines and fails pending proposals.
// It is idempotent. After Stop the node ignores incoming RPCs.
func (n *Node) Stop() {
	n.stopOnce.Do(func() {
		n.mu.Lock()
		n.stopped = true
		close(n.stopCh)
		n.stopReplicatorsLocked()
		for idx, w := range n.waiters {
			w.ch <- proposalResult{err: ErrStopped}
			delete(n.waiters, idx)
		}
		n.notifyLocked()
		n.mu.Unlock()
		n.wg.Wait()
	})
}

// ID returns this node's ID.
func (n *Node) ID() NodeID { return n.id }

// ---- main loop ----

func (n *Node) tickInterval() time.Duration {
	t := n.heartbeat / 5
	if t < time.Millisecond {
		t = time.Millisecond
	}
	if t > 10*time.Millisecond {
		t = 10 * time.Millisecond
	}
	return t
}

func (n *Node) run() {
	defer n.wg.Done()
	ticker := time.NewTicker(n.tickInterval())
	defer ticker.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.tick()
		}
	}
}

func (n *Node) tick() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return
	}
	now := time.Now()
	if n.state == Leader {
		if now.Sub(n.lastHeartbeat) >= n.heartbeat {
			n.lastHeartbeat = now
			n.triggerAllLocked()
		}
		if now.Sub(n.lastQuorumCheck) >= n.electionMax {
			n.lastQuorumCheck = now
			n.checkQuorumLocked(now)
		}
		return
	}
	if !n.campaigning && now.After(n.electionDeadline) {
		n.resetElectionTimer()
		// No contact within an election timeout: the leader we knew is
		// presumed dead (stop redirecting clients to it).
		if n.leader != "" {
			n.leader = ""
			n.fireStateChange()
		}
		if n.config.IsVoter(n.id) {
			n.campaigning = true
			n.wg.Add(1)
			go func() {
				defer n.wg.Done()
				n.campaign()
			}()
		}
	}
}

// checkQuorumLocked makes a leader step down if it has not heard from a
// majority of voters within an election timeout. Without it, a leader cut
// off in a minority partition keeps believing it leads (accepting writes
// that can never commit) until the partition heals.
func (n *Node) checkQuorumLocked(now time.Time) {
	recent := 0
	for _, v := range n.config.Voters() {
		if v == n.id {
			recent++
			continue
		}
		if ts, ok := n.ls.lastContact[v]; ok && now.Sub(time.Unix(0, ts)) <= n.electionMax {
			recent++
		}
	}
	if recent < n.config.Quorum() {
		n.logger.Warn("check-quorum failed — stepping down", "term", n.ps.CurrentTerm,
			"reachable_voters", recent, "quorum", n.config.Quorum())
		n.leader = ""
		n.becomeFollower()
	}
}

// ---- Universal term rule ----
// Every RPC handler calls this first. If we see a higher term, we immediately
// revert to Follower — even if we thought we were the Leader. This is the
// single most important safety rule in Raft.

// observeTerm checks if term > currentTerm and, if so, updates currentTerm,
// clears votedFor, persists both, and reverts to Follower. Returns true if a
// reversion occurred. Caller must hold n.mu.
func (n *Node) observeTerm(term uint64) bool {
	if term > n.ps.CurrentTerm {
		n.logger.Info("higher term observed — reverting to Follower",
			"term", term, "old_term", n.ps.CurrentTerm, "role", "Follower")
		n.ps.CurrentTerm = term
		n.ps.VotedFor = nil
		n.persistStateLocked()
		n.leader = ""
		n.becomeFollower()
		return true
	}
	return false
}

// persistStateLocked durably records currentTerm/votedFor. Raft cannot
// safely continue if this fails (it could vote twice in a term after a
// restart), so a failure here is fatal.
func (n *Node) persistStateLocked() {
	hs := HardState{Term: n.ps.CurrentTerm}
	if n.ps.VotedFor != nil {
		hs.VotedFor = *n.ps.VotedFor
	}
	if err := n.storage.SaveState(hs); err != nil {
		panic(fmt.Sprintf("raft: %s: cannot persist term/vote: %v", n.id, err))
	}
}

// becomeFollower transitions node to Follower state.
// Caller must hold n.mu.
func (n *Node) becomeFollower() {
	wasLeader := n.state == Leader
	n.state = Follower
	n.ls = nil
	n.stopReplicatorsLocked()
	if wasLeader {
		n.logger.Info("stepped down from leader", "term", n.ps.CurrentTerm, "role", "Follower")
	}
	n.fireStateChange()
	n.notifyLocked()
}

// becomeCandidate transitions node to Candidate, increments term, votes for
// self, and persists both before any RequestVote is sent.
// Caller must hold n.mu.
func (n *Node) becomeCandidate() {
	n.ps.CurrentTerm++
	self := n.id
	n.ps.VotedFor = &self
	n.persistStateLocked()
	n.state = Candidate
	n.leader = ""
	n.ls = nil
	n.logger.Info("election started", "term", n.ps.CurrentTerm, "role", "Candidate")
	if n.hooks.OnElectionStarted != nil {
		n.hooks.OnElectionStarted(n.ps.CurrentTerm)
	}
	n.fireStateChange()
}

// becomeLeader transitions node to Leader and initializes per-follower tracking.
// Caller must hold n.mu. (startLeading additionally appends the no-op entry
// and starts replication; this split keeps unit tests of the bookkeeping
// simple.)
func (n *Node) becomeLeader() {
	n.state = Leader
	n.leader = n.id
	lastIdx := n.lastLogIndex()
	now := time.Now()
	n.ls = &LeaderVolatileState{
		NextIndex:   make(map[NodeID]uint64),
		MatchIndex:  make(map[NodeID]uint64),
		lastContact: make(map[NodeID]int64),
		ackSeq:      make(map[NodeID]uint64),
	}
	for _, peer := range n.peers() {
		n.ls.NextIndex[peer] = lastIdx + 1
		n.ls.MatchIndex[peer] = 0
		n.ls.lastContact[peer] = now.UnixNano()
	}
	n.lastHeartbeat = time.Time{}
	n.lastQuorumCheck = now
	n.logger.Info("became leader", "term", n.ps.CurrentTerm, "role", "Leader")
	n.fireStateChange()
}

// startLeading completes a won election: append a no-op entry for the new
// term (a leader may only commit earlier-term entries indirectly, by
// committing one of its own), then start replicating.
func (n *Node) startLeading() {
	n.becomeLeader()
	noop := LogEntry{Index: n.lastLogIndex() + 1, Term: n.ps.CurrentTerm, Type: EntryNoop}
	if err := n.appendLocked([]LogEntry{noop}); err != nil {
		n.logger.Error("failed to append leader no-op; stepping down", "error", err)
		n.becomeFollower()
		return
	}
	n.syncReplicatorsLocked()
	n.maybeAdvanceCommitIndex()
	n.triggerAllLocked()
}

func (n *Node) fireStateChange() {
	if n.hooks.OnStateChange != nil {
		n.hooks.OnStateChange(n.ps.CurrentTerm, n.state, n.leader)
	}
}

// notifyLocked wakes everyone waiting on commit/apply/ack progress.
func (n *Node) notifyLocked() {
	close(n.changed)
	n.changed = make(chan struct{})
}

func (n *Node) resetElectionTimer() {
	span := int64(n.electionMax - n.electionMin)
	n.electionDeadline = time.Now().Add(n.electionMin + time.Duration(n.rng.Int63n(span)))
}

// peers returns every other member (voters and learners) of the latest config.
func (n *Node) peers() []NodeID {
	var out []NodeID
	for _, m := range n.config.Members {
		if m.ID != n.id {
			out = append(out, m.ID)
		}
	}
	return out
}

// ---- Log helpers ----
// Caller must hold n.mu. Log[0] is the snapshot sentinel.

func (n *Node) firstIndex() uint64 { return n.ps.Log[0].Index }

func (n *Node) lastLogIndex() uint64 {
	return n.ps.Log[len(n.ps.Log)-1].Index
}

func (n *Node) lastLogTerm() uint64 {
	return n.ps.Log[len(n.ps.Log)-1].Term
}

// termAt returns the term of the entry at index, if known (the snapshot
// sentinel counts; compacted entries before it do not).
func (n *Node) termAt(index uint64) (uint64, bool) {
	base := n.firstIndex()
	if index < base || index > n.lastLogIndex() {
		return 0, false
	}
	return n.ps.Log[index-base].Term, true
}

// entryAt returns the log entry at the given index (never the sentinel).
func (n *Node) entryAt(index uint64) (LogEntry, bool) {
	base := n.firstIndex()
	if index <= base || index > n.lastLogIndex() {
		return LogEntry{}, false
	}
	e := n.ps.Log[index-base]
	if e.Index != index {
		// Invariant violation — log is corrupted or accessed incorrectly.
		panic("raft: log index mismatch — this is a bug")
	}
	return e, true
}

// maxAppendBytes bounds the payload of one AppendEntries message (well
// under the transport's message size limit); at least one entry is always
// sent so a single large entry still makes progress.
const maxAppendBytes = 8 << 20

// entriesFrom returns up to max entries (and ~maxAppendBytes) starting at
// index (copied).
func (n *Node) entriesFrom(index uint64, max int) []LogEntry {
	base := n.firstIndex()
	if index <= base || index > n.lastLogIndex() {
		return nil
	}
	src := n.ps.Log[index-base:]
	if len(src) > max {
		src = src[:max]
	}
	size := 0
	for i, e := range src {
		size += len(e.Command) + 32
		if size > maxAppendBytes && i > 0 {
			src = src[:i]
			break
		}
	}
	out := make([]LogEntry, len(src))
	copy(out, src)
	return out
}

// appendLocked durably appends entries to the log, then to memory, and
// applies any configuration they carry.
func (n *Node) appendLocked(entries []LogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	if err := n.storage.Append(entries); err != nil {
		return err
	}
	n.appendInMemoryLocked(entries)
	n.durableIndex = n.lastLogIndex() // Append fsyncs everything written so far
	return nil
}

// appendInMemoryLocked adds entries to the in-memory log and applies any
// configuration they carry (configs take effect when appended).
func (n *Node) appendInMemoryLocked(entries []LogEntry) {
	n.ps.Log = append(n.ps.Log, entries...)
	for _, e := range entries {
		if e.Type == EntryConfig {
			m, err := DecodeMembership(e.Command)
			if err != nil {
				panic(fmt.Sprintf("raft: corrupt config entry at %d: %v", e.Index, err))
			}
			n.setConfigLocked(m, e.Index, false)
		}
	}
}

// truncateFromLocked durably removes entries >= index. Truncating a
// committed entry is a safety violation and panics.
func (n *Node) truncateFromLocked(index uint64) error {
	if index <= n.vs.CommitIndex {
		panic(fmt.Sprintf("raft: %s: attempt to truncate committed entry %d (commitIndex=%d) — this is a bug",
			n.id, index, n.vs.CommitIndex))
	}
	if err := n.storage.TruncateFrom(index); err != nil {
		return err
	}
	n.ps.Log = n.ps.Log[:index-n.firstIndex()]
	if n.durableIndex >= index {
		n.durableIndex = index - 1
	}
	// Fail proposals whose entries were just discarded.
	for idx, w := range n.waiters {
		if idx >= index {
			w.ch <- proposalResult{err: ErrLeadershipLost}
			delete(n.waiters, idx)
		}
	}
	if n.configIndex >= index {
		n.recomputeConfig()
	}
	return nil
}

// recomputeConfig resets config to the latest config entry in the log,
// falling back to the snapshot's configuration.
func (n *Node) recomputeConfig() {
	for i := len(n.ps.Log) - 1; i >= 1; i-- {
		e := n.ps.Log[i]
		if e.Type == EntryConfig {
			m, err := DecodeMembership(e.Command)
			if err != nil {
				panic(fmt.Sprintf("raft: corrupt config entry at %d: %v", e.Index, err))
			}
			n.setConfigLocked(m, e.Index, false)
			return
		}
	}
	n.setConfigLocked(n.snapshotConfig, n.firstIndex(), false)
}

// configAt returns the configuration in effect at index (latest config
// entry at or before index, else the snapshot config).
func (n *Node) configAt(index uint64) Membership {
	base := n.firstIndex()
	for i := int(index - base); i >= 1; i-- {
		if i >= len(n.ps.Log) {
			continue
		}
		e := n.ps.Log[i]
		if e.Type == EntryConfig {
			m, _ := DecodeMembership(e.Command)
			return m
		}
	}
	return n.snapshotConfig.Clone()
}

func (n *Node) setConfigLocked(m Membership, index uint64, committed bool) {
	n.config = m.Clone()
	n.configIndex = index
	if n.state == Leader {
		n.syncReplicatorsLocked()
	}
	if n.hooks.OnConfigChange != nil {
		n.hooks.OnConfigChange(index, n.config.Clone(), committed)
	}
}

// ---- Introspection ----

// CurrentTerm returns the node's current term.
func (n *Node) CurrentTerm() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.ps.CurrentTerm
}

// CurrentState returns the node's current role.
func (n *Node) CurrentState() State {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state
}

// Leader returns the ID of the leader this node currently recognizes ("" if
// unknown), for client redirects.
func (n *Node) Leader() NodeID {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leader
}

// IsLeader reports whether this node currently believes it is the leader.
func (n *Node) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state == Leader
}

// Membership returns the latest configuration.
func (n *Node) Membership() Membership {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.config.Clone()
}

// Status returns a snapshot of the node's Raft state.
func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	st := Status{
		ID:            n.id,
		State:         n.state,
		Term:          n.ps.CurrentTerm,
		Leader:        n.leader,
		CommitIndex:   n.vs.CommitIndex,
		LastApplied:   n.vs.LastApplied,
		LastLogIndex:  n.lastLogIndex(),
		SnapshotIndex: n.firstIndex(),
		Config:        n.config.Clone(),
	}
	st.LastLeaderContact = n.lastLeaderContact
	if n.state == Leader {
		st.LastLeaderContact = time.Now()
	}
	if n.state == Leader && n.ls != nil {
		now := time.Now()
		st.Progress = make(map[NodeID]PeerProgress)
		for _, p := range n.peers() {
			mem, _ := n.config.Get(p)
			st.Progress[p] = PeerProgress{
				Match:   n.ls.MatchIndex[p],
				Next:    n.ls.NextIndex[p],
				Learner: mem.Learner,
				Recent:  now.Sub(time.Unix(0, n.ls.lastContact[p])) <= n.electionMax,
			}
		}
	}
	return st
}

// WaitApplied blocks until lastApplied >= index, the node stops, or the
// timeout elapses. Returns whether the index was reached.
func (n *Node) WaitApplied(index uint64, timeout time.Duration) bool {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		n.mu.Lock()
		if n.vs.LastApplied >= index {
			n.mu.Unlock()
			return true
		}
		ch := n.changed
		stopped := n.stopped
		n.mu.Unlock()
		if stopped {
			return false
		}
		select {
		case <-ch:
		case <-deadline.C:
			return false
		}
	}
}
