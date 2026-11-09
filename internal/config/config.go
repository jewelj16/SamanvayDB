// Package config provides declarative, file-based configuration for SamanvayDB nodes.
// Configuration is YAML-based rather than flag-based so that multi-node clusters
// (9+ nodes) can be managed without hand-typed CLI flags.
package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config holds all configuration for a single SamanvayDB node.
type Config struct {
	// NodeID uniquely identifies this node within the cluster.
	NodeID string `yaml:"node_id"`

	// ListenAddr is the gRPC address (host:port) serving client KV, Admin
	// and inter-node Raft RPCs.
	ListenAddr string `yaml:"listen_addr"`

	// HTTPAddr serves /healthz, /metrics and /status. Optional.
	HTTPAddr string `yaml:"http_addr,omitempty"`

	// DataDir holds this node's Raft logs, snapshots and node manifest.
	DataDir string `yaml:"data_dir"`

	// Join marks a node added to a running cluster: it bootstraps no Raft
	// groups itself and waits to be added to groups by the admin API.
	Join bool `yaml:"join,omitempty"`

	// Cluster is the initial cluster topology. Every node of a cluster must
	// be started with the same cluster section. If omitted, the node runs as
	// a single-node cluster with one shard.
	Cluster ClusterConfig `yaml:"cluster,omitempty"`

	// Raft tunes consensus timing and durability.
	Raft RaftConfig `yaml:"raft,omitempty"`

	// RaftPeers is accepted for backward compatibility with Phase-0 configs
	// and ignored; topology now lives in Cluster.
	RaftPeers []string `yaml:"raft_peers,omitempty"`
}

// NodeEntry is one node of the initial cluster topology.
type NodeEntry struct {
	ID       string `yaml:"id"`
	Addr     string `yaml:"addr"`
	HTTPAddr string `yaml:"http_addr,omitempty"`
}

// ClusterConfig is the bootstrap topology (version 1 of the shard map).
// After bootstrap, the authoritative topology lives in the replicated
// metadata group and changes only through the admin API.
type ClusterConfig struct {
	Nodes             []NodeEntry        `yaml:"nodes,omitempty"`
	NumShards         int32              `yaml:"num_shards,omitempty"`
	ReplicationFactor int                `yaml:"replication_factor,omitempty"`
	MetaReplicas      []string           `yaml:"meta_replicas,omitempty"`
	Placement         map[int32][]string `yaml:"placement,omitempty"`
}

// RaftConfig tunes Raft. Zero values take defaults.
type RaftConfig struct {
	ElectionTimeoutMinMs int    `yaml:"election_timeout_min_ms,omitempty"`
	ElectionTimeoutMaxMs int    `yaml:"election_timeout_max_ms,omitempty"`
	HeartbeatIntervalMs  int    `yaml:"heartbeat_interval_ms,omitempty"`
	SnapshotThreshold    uint64 `yaml:"snapshot_threshold,omitempty"`
	// NoSync disables fsync — benchmarks only; voids durability.
	NoSync bool `yaml:"no_sync,omitempty"`
}

// ElectionTimeouts returns the configured election timeout range.
func (r RaftConfig) ElectionTimeouts() (time.Duration, time.Duration) {
	min, max := 150*time.Millisecond, 300*time.Millisecond
	if r.ElectionTimeoutMinMs > 0 {
		min = time.Duration(r.ElectionTimeoutMinMs) * time.Millisecond
	}
	if r.ElectionTimeoutMaxMs > 0 {
		max = time.Duration(r.ElectionTimeoutMaxMs) * time.Millisecond
	}
	if max <= min {
		max = 2 * min
	}
	return min, max
}

// Heartbeat returns the configured heartbeat interval.
func (r RaftConfig) Heartbeat() time.Duration {
	if r.HeartbeatIntervalMs > 0 {
		return time.Duration(r.HeartbeatIntervalMs) * time.Millisecond
	}
	return 50 * time.Millisecond
}

// Snapshot returns the snapshot threshold (default 10000 entries).
func (r RaftConfig) Snapshot() uint64 {
	if r.SnapshotThreshold > 0 {
		return r.SnapshotThreshold
	}
	return 10000
}

// Validate checks that all required fields are present and consistent.
func (c *Config) Validate() error {
	if c.NodeID == "" {
		return fmt.Errorf("config: node_id is required")
	}
	if c.ListenAddr == "" {
		return fmt.Errorf("config: listen_addr is required")
	}
	if c.DataDir == "" {
		return fmt.Errorf("config: data_dir is required")
	}
	seen := map[string]bool{}
	self := false
	for _, n := range c.Cluster.Nodes {
		if n.ID == "" || n.Addr == "" {
			return fmt.Errorf("config: cluster node entries need id and addr")
		}
		if seen[n.ID] {
			return fmt.Errorf("config: duplicate cluster node %q", n.ID)
		}
		seen[n.ID] = true
		if n.ID == c.NodeID {
			self = true
		}
	}
	if len(c.Cluster.Nodes) > 0 && !self && !c.Join {
		return fmt.Errorf("config: node %q is not listed in cluster.nodes (set join: true for a node added later)", c.NodeID)
	}
	if c.Cluster.NumShards < 0 {
		return fmt.Errorf("config: cluster.num_shards must be positive")
	}
	if c.Cluster.ReplicationFactor > len(c.Cluster.Nodes) && len(c.Cluster.Nodes) > 0 {
		return fmt.Errorf("config: replication_factor %d exceeds %d nodes", c.Cluster.ReplicationFactor, len(c.Cluster.Nodes))
	}
	return nil
}

// ApplyDefaults fills in a single-node cluster when no topology is given.
func (c *Config) ApplyDefaults() {
	if len(c.Cluster.Nodes) == 0 && !c.Join {
		c.Cluster.Nodes = []NodeEntry{{ID: c.NodeID, Addr: c.ListenAddr, HTTPAddr: c.HTTPAddr}}
	}
	if c.Cluster.NumShards == 0 {
		c.Cluster.NumShards = 1
	}
	if c.Cluster.ReplicationFactor == 0 {
		c.Cluster.ReplicationFactor = 3
		if n := len(c.Cluster.Nodes); n < 3 {
			c.Cluster.ReplicationFactor = n
		}
	}
}

// LoadConfig reads and parses a YAML configuration file at the given path.
// It returns an error (not a panic) for missing files, permission errors,
// malformed YAML, or missing required fields.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: failed to read file %q: %w", path, err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("config: failed to parse YAML from %q: %w", path, err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	cfg.ApplyDefaults()
	return &cfg, nil
}

// Marshal serializes a Config to YAML bytes.
func (c *Config) Marshal() ([]byte, error) {
	data, err := yaml.Marshal(c)
	if err != nil {
		return nil, fmt.Errorf("config: failed to marshal to YAML: %w", err)
	}
	return data, nil
}
