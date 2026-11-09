package shard

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"samanvaydb/internal/logging"
	"samanvaydb/internal/raft"
)

// Manager hosts S independent Raft groups ("Multi-Raft") in one process.
//
// Each Shard runs its own, fully independent instance of the Raft state
// machine — its own currentTerm, log, election timers and leader —
// parameterized only by shard ID and peer set. There is NO shared mutable
// Raft state between groups; they share only infrastructure (the process,
// the gRPC transport, the address book).
//
// Each replica's Raft state lives under a per-shard directory,
//
//	<data_dir>/shard-<id>/raft/     (meta group: <data_dir>/shard-meta/raft/)
//
// so a bug or corruption in one shard's files cannot contaminate another's.
//
// The set of shards a node hosts is recorded in <data_dir>/node-manifest.json.
// It is written once from the initial cluster topology on first boot and
// afterwards changed ONLY by replica create/destroy operations — never
// re-derived from the config file on restart. (Re-deriving it would let a
// node that was removed from a group resurrect itself with a stale peer
// list: exactly the out-of-band membership change Raft forbids.)
type Manager struct {
	nodeID  raft.NodeID
	dataDir string
	factory func(cfg Config) Config
	logger  *logging.Logger

	mu     sync.Mutex
	shards map[int32]*Shard
}

// ManagerConfig configures a Manager.
type ManagerConfig struct {
	NodeID  raft.NodeID
	DataDir string // empty: in-memory (tests)
	Logger  *logging.Logger
	// Configure fills in per-shard settings (transport, timing, hooks) for
	// a replica about to be created. ShardID/NodeID/RaftDir/Bootstrap/Join
	// are already set.
	Configure func(cfg Config) Config
}

// NewManager creates an empty manager.
func NewManager(cfg ManagerConfig) *Manager {
	return &Manager{
		nodeID:  cfg.NodeID,
		dataDir: cfg.DataDir,
		factory: cfg.Configure,
		logger:  cfg.Logger.WithComponent("shard-manager"),
		shards:  map[int32]*Shard{},
	}
}

// ShardDir returns the per-shard directory for id.
func (m *Manager) ShardDir(id int32) string {
	if m.dataDir == "" {
		return ""
	}
	if id == MetaShardID {
		return filepath.Join(m.dataDir, "shard-meta")
	}
	return filepath.Join(m.dataDir, fmt.Sprintf("shard-%d", id))
}

// Open creates (or recovers) and starts a replica of shard id. bootstrap is
// used only if the replica has no persisted state; join starts it as an
// empty replica waiting to be added by the group leader.
func (m *Manager) Open(id int32, bootstrap *raft.Membership, join bool) (*Shard, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s, ok := m.shards[id]; ok {
		return s, nil
	}
	cfg := Config{ShardID: id, NodeID: m.nodeID, Bootstrap: bootstrap, Join: join, Logger: m.logger}
	if dir := m.ShardDir(id); dir != "" {
		cfg.RaftDir = filepath.Join(dir, "raft")
	}
	if m.factory != nil {
		cfg = m.factory(cfg)
	}
	s, err := New(cfg)
	if err != nil {
		return nil, err
	}
	m.shards[id] = s
	if err := m.saveManifestLocked(); err != nil {
		delete(m.shards, id)
		s.Stop()
		return nil, err
	}
	s.Start()
	st := s.Raft().Status()
	m.logger.Info("shard replica opened", "shard_id", id, "term", st.Term,
		"config", st.Config.String(), "join", join)
	return s, nil
}

// Get returns the replica of shard id hosted here.
func (m *Manager) Get(id int32) (*Shard, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.shards[id]
	return s, ok
}

// IDs returns the hosted shard IDs in ascending order.
func (m *Manager) IDs() []int32 {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]int32, 0, len(m.shards))
	for id := range m.shards {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// Destroy stops replica id and deletes its data. The caller must have
// removed it from the group's Raft configuration first.
func (m *Manager) Destroy(id int32) error {
	m.mu.Lock()
	s, ok := m.shards[id]
	if ok {
		delete(m.shards, id)
	}
	err := m.saveManifestLocked()
	m.mu.Unlock()
	if !ok {
		return nil
	}
	if derr := s.Destroy(); derr != nil && err == nil {
		err = derr
	}
	if dir := m.ShardDir(id); dir != "" {
		os.RemoveAll(dir)
	}
	m.logger.Info("shard replica destroyed", "shard_id", id)
	return err
}

// Close stops every replica (data is kept).
func (m *Manager) Close() {
	m.mu.Lock()
	shards := make([]*Shard, 0, len(m.shards))
	for _, s := range m.shards {
		shards = append(shards, s)
	}
	m.mu.Unlock()
	var wg sync.WaitGroup
	for _, s := range shards {
		wg.Add(1)
		go func(s *Shard) { defer wg.Done(); s.Stop() }(s)
	}
	wg.Wait()
}

// ---- node manifest ----

type manifest struct {
	NodeID string  `json:"node_id"`
	Shards []int32 `json:"shards"`
}

func (m *Manager) manifestPath() string { return filepath.Join(m.dataDir, "node-manifest.json") }

// LoadManifest returns the shard IDs recorded as hosted by this node, and
// whether a manifest exists at all (false on a brand-new data dir).
func (m *Manager) LoadManifest() ([]int32, bool, error) {
	if m.dataDir == "" {
		return nil, false, nil
	}
	b, err := os.ReadFile(m.manifestPath())
	if os.IsNotExist(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("shard: read manifest: %w", err)
	}
	var mf manifest
	if err := json.Unmarshal(b, &mf); err != nil {
		return nil, false, fmt.Errorf("shard: parse manifest: %w", err)
	}
	if mf.NodeID != string(m.nodeID) {
		return nil, false, fmt.Errorf("shard: data dir belongs to node %q, not %q", mf.NodeID, m.nodeID)
	}
	return mf.Shards, true, nil
}

func (m *Manager) saveManifestLocked() error {
	if m.dataDir == "" {
		return nil
	}
	if err := os.MkdirAll(m.dataDir, 0o755); err != nil {
		return err
	}
	mf := manifest{NodeID: string(m.nodeID)}
	for id := range m.shards {
		mf.Shards = append(mf.Shards, id)
	}
	sort.Slice(mf.Shards, func(i, j int) bool { return mf.Shards[i] < mf.Shards[j] })
	b, _ := json.MarshalIndent(mf, "", "  ")
	tmp := m.manifestPath() + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	f.Close()
	return os.Rename(tmp, m.manifestPath())
}
