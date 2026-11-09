// Package shard wires one Raft group to one storage engine.
//
// A Shard owns exactly one raft.Node and one storage.Engine (wrapped by
// FSM, the raft.StateMachine). The write path is exactly:
//
//	Client -> PUT k v -> Leader
//	Leader appends {PUT,k,v} as a new Raft log entry (fsynced, uncommitted)
//	Replication to followers
//	MAJORITY ack -> entry COMMITTED
//	Leader applies it (FSM.Apply -> engine.Put) and only THEN responds OK
//	Followers apply it on learning the new commitIndex (next AppendEntries)
//
// The leader never writes to its engine before commit "for speed": a
// concurrent read in that window would observe an uncommitted, possibly
// doomed value, and there is no safe rollback.
package shard

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"samanvaydb/internal/command"
	"samanvaydb/internal/logging"
	"samanvaydb/internal/raft"
	"samanvaydb/internal/storage"
)

// MetaShardID is the reserved ID of the metadata Raft group that stores the
// cluster's shard map. It is a regular KV shard hosted on a fixed subset of
// nodes; it never holds user data.
const MetaShardID int32 = -1

// Consistency selects how a read is served.
type Consistency int

const (
	// ConsistencyLeader is linearizable: the leader confirms its leadership
	// with a ReadIndex round before reading its applied state.
	ConsistencyLeader Consistency = iota
	// ConsistencyFollowerOK reads any replica's local applied state; it may
	// be stale by the replication lag.
	ConsistencyFollowerOK
)

// Errors returned by Shard operations. Callers map them to API codes.
var (
	// ErrUnavailable means no quorum could be reached in time (or the
	// proposing leader lost leadership). Retry with backoff. For writes the
	// outcome may be unknown; PUT/DELETE are idempotent, so retrying is safe.
	ErrUnavailable = errors.New("shard: unavailable (no quorum reachable)")
	// ErrInvalid means the request itself is malformed. Never retry.
	ErrInvalid = errors.New("shard: invalid request")
	// ErrStopped means the shard replica is shutting down.
	ErrStopped = errors.New("shard: stopped")
)

// NotLeaderError is returned when this replica is not the shard leader.
type NotLeaderError struct {
	Leader raft.NodeID // "" if unknown
}

func (e *NotLeaderError) Error() string {
	if e.Leader == "" {
		return "shard: not leader (leader unknown)"
	}
	return fmt.Sprintf("shard: not leader; leader is %s", e.Leader)
}

// Config configures a Shard replica.
type Config struct {
	ShardID int32
	NodeID  raft.NodeID
	// RaftDir holds this replica's Raft log/state/snapshot. Empty means
	// in-memory Raft storage (tests).
	RaftDir   string
	NoSync    bool
	Bootstrap *raft.Membership
	Join      bool
	Transport raft.Transport
	Logger    *logging.Logger
	Hooks     raft.Hooks

	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration
	SnapshotThreshold  uint64

	// ExpiryInterval is how often the leader scans for expired TTL keys and
	// locks (default 500ms).
	ExpiryInterval time.Duration
	// Now returns the leader's wall clock (default time.Now); used ONLY at
	// propose time, never inside Apply.
	Now func() time.Time
	// OnApplyLag observes per-entry apply lag on this replica.
	OnApplyLag func(time.Duration)
}

// Shard is one replica of one shard's Raft group plus its storage engine.
type Shard struct {
	id      int32
	nodeID  raft.NodeID
	node    *raft.Node
	fsm     *FSM
	engine  storage.Engine
	store   raft.Storage
	waiters *WaiterRegistry
	logger  *logging.Logger
	now     func() time.Time

	expiryInterval time.Duration
	pendingMu      sync.Mutex
	pendingExpiry  map[string]time.Time

	stopOnce sync.Once
	stopCh   chan struct{}
	wg       sync.WaitGroup
	raftDir  string
}

// New creates a shard replica, recovering any persisted Raft state (the
// engine is rebuilt from the latest snapshot plus the log tail).
func New(cfg Config) (*Shard, error) {
	if cfg.Logger == nil {
		return nil, fmt.Errorf("shard: logger required")
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.ExpiryInterval == 0 {
		cfg.ExpiryInterval = 500 * time.Millisecond
	}
	logger := cfg.Logger.WithShard(cfg.ShardID)
	engine := storage.NewMemEngine()
	fsm := NewFSM(engine)
	s := &Shard{
		id:             cfg.ShardID,
		nodeID:         cfg.NodeID,
		fsm:            fsm,
		engine:         engine,
		waiters:        NewWaiterRegistry(),
		logger:         logger,
		now:            cfg.Now,
		expiryInterval: cfg.ExpiryInterval,
		pendingExpiry:  map[string]time.Time{},
		stopCh:         make(chan struct{}),
		raftDir:        cfg.RaftDir,
	}
	fsm.OnRelease = s.waiters.Wake
	fsm.OnApplyLag = cfg.OnApplyLag

	var st raft.Storage
	if cfg.RaftDir == "" {
		st = raft.NewMemoryStorage()
	} else {
		fs, err := raft.OpenFileStorage(cfg.RaftDir, raft.FileStorageOptions{NoSync: cfg.NoSync})
		if err != nil {
			return nil, err
		}
		st = fs
	}
	s.store = st

	node, err := raft.NewNode(raft.Config{
		ID:                 cfg.NodeID,
		Bootstrap:          cfg.Bootstrap,
		Join:               cfg.Join,
		StateMachine:       fsm,
		Transport:          cfg.Transport,
		Storage:            st,
		Logger:             logger,
		Hooks:              cfg.Hooks,
		ElectionTimeoutMin: cfg.ElectionTimeoutMin,
		ElectionTimeoutMax: cfg.ElectionTimeoutMax,
		HeartbeatInterval:  cfg.HeartbeatInterval,
		SnapshotThreshold:  cfg.SnapshotThreshold,
	})
	if err != nil {
		st.Close()
		return nil, fmt.Errorf("shard %d: %w", cfg.ShardID, err)
	}
	s.node = node
	return s, nil
}

// Start begins Raft timers and the leader expiry ticker.
func (s *Shard) Start() {
	s.node.Start()
	s.wg.Add(1)
	go s.runExpiryTicker()
}

// Stop halts the replica (Raft first, then storage). Idempotent.
func (s *Shard) Stop() {
	s.stopOnce.Do(func() {
		close(s.stopCh)
		s.node.Stop()
		s.wg.Wait()
		s.waiters.CloseAll()
		s.store.Close()
	})
}

// Destroy stops the replica and deletes its on-disk state. Only safe once
// it has been removed from the group's configuration.
func (s *Shard) Destroy() error {
	s.Stop()
	if s.raftDir == "" {
		return nil
	}
	return os.RemoveAll(s.raftDir)
}

// ID returns the shard ID.
func (s *Shard) ID() int32 { return s.id }

// Raft returns the underlying Raft node (for RPC dispatch and admin).
func (s *Shard) Raft() *raft.Node { return s.node }

// FSM returns the state machine (read-only use by status/tests).
func (s *Shard) FSM() *FSM { return s.fsm }

// ---- write path ----

func (s *Shard) nowMs() uint64 { return uint64(s.now().UnixMilli()) }

func (s *Shard) notLeader() error {
	return &NotLeaderError{Leader: s.node.Leader()}
}

// propose stamps the leader's time into cmd (the ONLY place a clock is read
// on the write path), proposes it, and waits for it to be committed and
// applied. The returned Result is the deterministic apply outcome.
func (s *Shard) propose(ctx context.Context, cmd command.Command) (Result, error) {
	if !s.node.IsLeader() {
		return Result{}, s.notLeader()
	}
	if cmd.TimestampMs == 0 {
		cmd.TimestampMs = s.nowMs()
	}
	if ref, ok := clientFrom(ctx); ok {
		cmd.ClientID, cmd.Seq, cmd.MinSeq = ref.ID, ref.Seq, ref.MinSeq
	}
	data, err := cmd.Marshal()
	if err != nil {
		return Result{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	res, err := s.node.ProposeAndWait(ctx, data)
	if err != nil {
		switch {
		case errors.Is(err, raft.ErrNotLeader):
			return Result{}, s.notLeader()
		case errors.Is(err, raft.ErrStopped):
			return Result{}, ErrStopped
		case errors.Is(err, raft.ErrLeadershipLost),
			errors.Is(err, context.DeadlineExceeded),
			errors.Is(err, context.Canceled):
			return Result{}, ErrUnavailable
		}
		return Result{}, err
	}
	r, _ := res.(Result)
	if r.Code == ResultInvalid {
		return r, ErrInvalid
	}
	return r, nil
}

func validateUserKey(key []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("%w: empty key", ErrInvalid)
	}
	if IsReservedKey(key) {
		return fmt.Errorf("%w: key prefix %q is reserved", ErrInvalid, LockPrefix)
	}
	return nil
}

// Put writes key=value. ttl > 0 attaches an expiry deadline computed once,
// on the leader.
func (s *Shard) Put(ctx context.Context, key, value []byte, ttl time.Duration) error {
	if err := validateUserKey(key); err != nil {
		return err
	}
	now := s.nowMs()
	cmd := command.Command{Type: command.CmdPut, Key: key, Value: value, TimestampMs: now}
	if ttl > 0 {
		cmd.ExpiresAtMs = now + uint64(ttl.Milliseconds())
	}
	_, err := s.propose(ctx, cmd)
	return err
}

// Delete removes key (idempotent).
func (s *Shard) Delete(ctx context.Context, key []byte) error {
	if err := validateUserKey(key); err != nil {
		return err
	}
	_, err := s.propose(ctx, command.Command{Type: command.CmdDelete, Key: key})
	return err
}

// Get reads key with the requested consistency.
func (s *Shard) Get(ctx context.Context, key []byte, c Consistency) ([]byte, bool, error) {
	if err := validateUserKey(key); err != nil {
		return nil, false, err
	}
	if c == ConsistencyLeader {
		if _, err := s.node.ReadIndex(ctx); err != nil {
			switch {
			case errors.Is(err, raft.ErrNotLeader):
				return nil, false, s.notLeader()
			case errors.Is(err, raft.ErrStopped):
				return nil, false, ErrStopped
			default:
				return nil, false, ErrUnavailable
			}
		}
	}
	v, found := s.fsm.Get(key)
	return v, found, nil
}

// CAS sets key to newValue iff it currently holds expected (or is absent,
// with expectAbsent). The leader first checks its own applied state and
// fails fast without proposing; the authoritative check is re-done at apply
// time, so concurrent CAS calls are resolved by Raft log order.
func (s *Shard) CAS(ctx context.Context, key, expected []byte, expectAbsent bool, newValue []byte) (Result, error) {
	if err := validateUserKey(key); err != nil {
		return Result{}, err
	}
	if !s.node.IsLeader() {
		return Result{}, s.notLeader()
	}
	if r, ok := s.cached(ctx); ok {
		return r, nil
	}
	cur, found := s.fsm.Get(key)
	if (expectAbsent && found) || (!expectAbsent && (!found || string(cur) != string(expected))) {
		return Result{Code: ResultCASFailed, Found: found, Value: cur}, nil
	}
	return s.propose(ctx, command.Command{Type: command.CmdCAS, Key: key, Value: newValue,
		Expected: expected, ExpectAbsent: expectAbsent})
}

// cached returns the recorded outcome if this exact client request (a
// retry) was already applied, so pre-checks never re-evaluate it.
func (s *Shard) cached(ctx context.Context) (Result, bool) {
	ref, ok := clientFrom(ctx)
	if !ok {
		return Result{}, false
	}
	return s.fsm.CachedResult(ref.ID, ref.Seq)
}

// ---- locks ----

// LockAcquire takes lock name for owner with a lease of ttl.
func (s *Shard) LockAcquire(ctx context.Context, name, owner string, ttl time.Duration) (Result, error) {
	if err := validateLockArgs(name, owner, ttl, true); err != nil {
		return Result{}, err
	}
	if !s.node.IsLeader() {
		return Result{}, s.notLeader()
	}
	if r, ok := s.cached(ctx); ok {
		return r, nil
	}
	// Leader-side fast check (no proposal if obviously held). NOT what makes
	// this correct — see FSM.Apply's re-validation.
	if rec, held := s.fsm.GetLock(name); held && rec.ExpiresAtMs > s.nowMs() {
		return Result{Code: ResultAlreadyHeld, Holder: rec.OwnerID, ExpiresAtMs: rec.ExpiresAtMs, Token: rec.FencingToken}, nil
	}
	return s.propose(ctx, command.Command{Type: command.CmdLockAcquire, Key: LockKey(name),
		Owner: []byte(owner), TTLMs: uint64(ttl.Milliseconds())})
}

// LockRenew extends owner's lease. The fencing token is unchanged.
func (s *Shard) LockRenew(ctx context.Context, name, owner string, ttl time.Duration) (Result, error) {
	if err := validateLockArgs(name, owner, ttl, true); err != nil {
		return Result{}, err
	}
	if !s.node.IsLeader() {
		return Result{}, s.notLeader()
	}
	if r, ok := s.cached(ctx); ok {
		return r, nil
	}
	rec, held := s.fsm.GetLock(name)
	if !held || string(rec.OwnerID) != owner {
		return Result{Code: ResultNotOwner}, nil
	}
	if rec.ExpiresAtMs <= s.nowMs() {
		return Result{Code: ResultLockExpired}, nil
	}
	return s.propose(ctx, command.Command{Type: command.CmdLockRenew, Key: LockKey(name),
		Owner: []byte(owner), TTLMs: uint64(ttl.Milliseconds())})
}

// LockRelease releases the lock if owner holds it (idempotent).
func (s *Shard) LockRelease(ctx context.Context, name, owner string) (Result, error) {
	if err := validateLockArgs(name, owner, 0, false); err != nil {
		return Result{}, err
	}
	if !s.node.IsLeader() {
		return Result{}, s.notLeader()
	}
	if r, ok := s.cached(ctx); ok {
		return r, nil
	}
	rec, held := s.fsm.GetLock(name)
	if !held {
		return Result{Code: ResultOK}, nil
	}
	if string(rec.OwnerID) != owner {
		return Result{Code: ResultNotOwner, Holder: rec.OwnerID}, nil
	}
	return s.propose(ctx, command.Command{Type: command.CmdLockRelease, Key: LockKey(name), Owner: []byte(owner)})
}

// LockWait blocks until lock name is released/expired (true) or ctx ends
// (false). Waiters are leader-local and NOT replicated: a leader failover
// drops them, and the caller gets an error and must re-issue the wait
// against the new leader. This is a deliberate best-effort trade-off.
func (s *Shard) LockWait(ctx context.Context, name string) (bool, error) {
	if name == "" {
		return false, fmt.Errorf("%w: empty lock name", ErrInvalid)
	}
	if !s.node.IsLeader() {
		return false, s.notLeader()
	}
	key := string(LockKey(name))
	ch, cancel := s.waiters.Register(key) // register BEFORE checking: no lost wake-up
	defer cancel()
	if _, held := s.fsm.GetLock(name); !held {
		return true, nil
	}
	select {
	case _, ok := <-ch:
		if !ok {
			return false, ErrStopped
		}
		return true, nil
	case <-ctx.Done():
		if !s.node.IsLeader() {
			return false, s.notLeader()
		}
		return false, nil
	case <-s.stopCh:
		return false, ErrStopped
	}
}

func validateLockArgs(name, owner string, ttl time.Duration, needTTL bool) error {
	if name == "" || owner == "" {
		return fmt.Errorf("%w: lock name and owner are required", ErrInvalid)
	}
	if needTTL && ttl <= 0 {
		return fmt.Errorf("%w: lock TTL must be positive", ErrInvalid)
	}
	return nil
}

// ---- expiry ticker ----

// runExpiryTicker runs on every replica but acts only while this replica is
// the leader. Each tick it re-derives the expired set from COMMITTED state
// and proposes CmdExpire/CmdLockExpire through Raft. Replicas never expire
// anything by their own clock; after a failover the new leader's ticker
// simply picks up the same work.
func (s *Shard) runExpiryTicker() {
	defer s.wg.Done()
	t := time.NewTicker(s.expiryInterval)
	defer t.Stop()
	for {
		select {
		case <-s.stopCh:
			return
		case <-t.C:
			if s.node.IsLeader() {
				s.expireOnce()
			}
		}
	}
}

func (s *Shard) expireOnce() {
	now := s.nowMs()
	cands := s.fsm.Expired(now, 256)
	if len(cands) == 0 {
		return
	}
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	wall := time.Now()
	for k, at := range s.pendingExpiry {
		if wall.Sub(at) > 3*s.expiryInterval {
			delete(s.pendingExpiry, k)
		}
	}
	for _, c := range cands {
		if _, pending := s.pendingExpiry[string(c.Key)]; pending {
			continue
		}
		cmd := command.Command{Type: command.CmdExpire, Key: c.Key, ExpiresAtMs: c.ExpiresAt, TimestampMs: now}
		if c.IsLock {
			cmd = command.Command{Type: command.CmdLockExpire, Key: c.Key, Owner: c.Owner, Token: c.Token, TimestampMs: now}
		}
		data, err := cmd.Marshal()
		if err != nil {
			continue
		}
		if _, _, err := s.node.Propose(data); err != nil {
			return
		}
		s.pendingExpiry[string(c.Key)] = wall
	}
}

// ---- status ----

// Status returns Raft status plus the replica's key count.
func (s *Shard) Status() (raft.Status, int) {
	return s.node.Status(), s.fsm.Len()
}

// ---- waiter registry ----

// WaiterRegistry is the leader-local registry of clients blocked in
// LockWait. It is deliberately NOT replicated through Raft (liveness/UX
// feature, not durability-requiring).
type WaiterRegistry struct {
	mu      sync.Mutex
	waiters map[string]map[chan struct{}]struct{}
	closed  bool
}

// NewWaiterRegistry returns an empty registry.
func NewWaiterRegistry() *WaiterRegistry {
	return &WaiterRegistry{waiters: map[string]map[chan struct{}]struct{}{}}
}

// Register adds a waiter for key; cancel removes it.
func (w *WaiterRegistry) Register(key string) (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		close(ch)
		return ch, func() {}
	}
	if w.waiters[key] == nil {
		w.waiters[key] = map[chan struct{}]struct{}{}
	}
	w.waiters[key][ch] = struct{}{}
	return ch, func() {
		w.mu.Lock()
		delete(w.waiters[key], ch)
		if len(w.waiters[key]) == 0 {
			delete(w.waiters, key)
		}
		w.mu.Unlock()
	}
}

// Wake signals every waiter on key.
func (w *WaiterRegistry) Wake(key string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for ch := range w.waiters[key] {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// Count returns the number of registered waiters on key.
func (w *WaiterRegistry) Count(key string) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.waiters[key])
}

// CloseAll fails every waiter (shard shutting down).
func (w *WaiterRegistry) CloseAll() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
	for key, set := range w.waiters {
		for ch := range set {
			close(ch)
		}
		delete(w.waiters, key)
	}
}
