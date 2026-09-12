// Package config provides declarative, file-based configuration for SamanvayDB nodes.
// Configuration is YAML-based rather than flag-based so that multi-node clusters
// (up to 9+ nodes by Phase 6) can be managed without hand-typed CLI flags.
package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config holds all configuration for a single SamanvayDB node.
// Fields are tagged for YAML serialization and designed to be extended
// (e.g. raft_peers in Phase 3) without breaking the format.
type Config struct {
	// NodeID uniquely identifies this node within the cluster.
	NodeID string `yaml:"node_id"`

	// ListenAddr is the address (host:port) the node listens on for client
	// and inter-node RPCs.
	ListenAddr string `yaml:"listen_addr"`

	// DataDir is the filesystem path where this node stores its data
	// (WAL, SSTables, Raft log, snapshots).
	DataDir string `yaml:"data_dir"`

	// RaftPeers will hold peer addresses for Raft consensus (Phase 3+).
	// Left as an optional slice so the config format doesn't break when
	// peers are added later.
	RaftPeers []string `yaml:"raft_peers,omitempty"`
}

// Validate checks that all required fields are present and non-empty.
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
	return nil
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
