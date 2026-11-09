package config

import (
	"os"
	"path/filepath"
	"testing"
)

// TestLoadConfig_RoundTrip verifies that a Config struct survives
// marshal → write → LoadConfig → compare without data loss.
func TestLoadConfig_RoundTrip(t *testing.T) {
	original := &Config{
		NodeID:     "node-1",
		ListenAddr: "127.0.0.1:7000",
		DataDir:    "/tmp/samanvaydb/node-1",
		RaftPeers:  []string{"127.0.0.1:7001", "127.0.0.1:7002"},
	}

	data, err := original.Marshal()
	if err != nil {
		t.Fatalf("Marshal() failed: %v", err)
	}

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("failed to write temp config: %v", err)
	}

	loaded, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig() failed: %v", err)
	}

	if loaded.NodeID != original.NodeID {
		t.Errorf("NodeID: got %q, want %q", loaded.NodeID, original.NodeID)
	}
	if loaded.ListenAddr != original.ListenAddr {
		t.Errorf("ListenAddr: got %q, want %q", loaded.ListenAddr, original.ListenAddr)
	}
	if loaded.DataDir != original.DataDir {
		t.Errorf("DataDir: got %q, want %q", loaded.DataDir, original.DataDir)
	}
	if len(loaded.RaftPeers) != len(original.RaftPeers) {
		t.Fatalf("RaftPeers length: got %d, want %d", len(loaded.RaftPeers), len(original.RaftPeers))
	}
	for i, peer := range loaded.RaftPeers {
		if peer != original.RaftPeers[i] {
			t.Errorf("RaftPeers[%d]: got %q, want %q", i, peer, original.RaftPeers[i])
		}
	}
}

// TestLoadConfig_RoundTripWithoutOptionalFields verifies round-trip
// works when optional fields (RaftPeers) are omitted.
func TestLoadConfig_RoundTripWithoutOptionalFields(t *testing.T) {
	original := &Config{
		NodeID:     "node-solo",
		ListenAddr: "0.0.0.0:7000",
		DataDir:    "/var/lib/samanvaydb",
	}

	data, err := original.Marshal()
	if err != nil {
		t.Fatalf("Marshal() failed: %v", err)
	}

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("failed to write temp config: %v", err)
	}

	loaded, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig() failed: %v", err)
	}

	if loaded.NodeID != original.NodeID {
		t.Errorf("NodeID: got %q, want %q", loaded.NodeID, original.NodeID)
	}
	if len(loaded.RaftPeers) != 0 {
		t.Errorf("RaftPeers: expected empty, got %v", loaded.RaftPeers)
	}
}

// TestLoadConfig_MissingFile returns an error, not a panic.
func TestLoadConfig_MissingFile(t *testing.T) {
	_, err := LoadConfig("/nonexistent/path/config.yaml")
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
}

// TestLoadConfig_MalformedYAML returns an error, not a panic.
func TestLoadConfig_MalformedYAML(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "bad.yaml")
	// Write invalid YAML (a bare tab character followed by nonsense)
	if err := os.WriteFile(path, []byte(":\t:\n\t- [\ninvalid{{}}}"), 0644); err != nil {
		t.Fatalf("failed to write malformed file: %v", err)
	}

	_, err := LoadConfig(path)
	if err == nil {
		t.Fatal("expected error for malformed YAML, got nil")
	}
}

// TestLoadConfig_MissingRequiredFields returns an error for each missing field.
func TestLoadConfig_MissingRequiredFields(t *testing.T) {
	tests := []struct {
		name    string
		yaml    string
		wantErr string
	}{
		{
			name:    "missing node_id",
			yaml:    "listen_addr: ':7000'\ndata_dir: /tmp/data\n",
			wantErr: "node_id is required",
		},
		{
			name:    "missing listen_addr",
			yaml:    "node_id: node-1\ndata_dir: /tmp/data\n",
			wantErr: "listen_addr is required",
		},
		{
			name:    "missing data_dir",
			yaml:    "node_id: node-1\nlisten_addr: ':7000'\n",
			wantErr: "data_dir is required",
		},
		{
			name:    "empty file",
			yaml:    "",
			wantErr: "node_id is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			path := filepath.Join(tmpDir, "config.yaml")
			if err := os.WriteFile(path, []byte(tt.yaml), 0644); err != nil {
				t.Fatalf("failed to write config: %v", err)
			}

			_, err := LoadConfig(path)
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.wantErr)
			}
			if !contains(err.Error(), tt.wantErr) {
				t.Errorf("error %q does not contain %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// TestLoadConfig_ClusterTopology verifies the cluster section round-trips
// and is validated.
func TestLoadConfig_ClusterTopology(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	good := write("good.yaml", `
node_id: node-2
listen_addr: 127.0.0.1:7002
http_addr: 127.0.0.1:8002
data_dir: /tmp/n2
cluster:
  num_shards: 3
  replication_factor: 3
  nodes:
    - {id: node-1, addr: 127.0.0.1:7001}
    - {id: node-2, addr: 127.0.0.1:7002}
    - {id: node-3, addr: 127.0.0.1:7003}
  placement:
    0: [node-1, node-2, node-3]
raft:
  election_timeout_min_ms: 200
  heartbeat_interval_ms: 40
`)
	cfg, err := LoadConfig(good)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Cluster.Nodes) != 3 || cfg.Cluster.NumShards != 3 || len(cfg.Cluster.Placement[0]) != 3 {
		t.Fatalf("cluster = %+v", cfg.Cluster)
	}
	if min, max := cfg.Raft.ElectionTimeouts(); min.Milliseconds() != 200 || max.Milliseconds() != 300 {
		t.Errorf("election timeouts %v-%v", min, max)
	}
	if cfg.Raft.Heartbeat().Milliseconds() != 40 {
		t.Errorf("heartbeat %v", cfg.Raft.Heartbeat())
	}

	tests := []struct{ name, body string }{
		{"self_not_listed", "node_id: x\nlisten_addr: a:1\ndata_dir: d\ncluster:\n  nodes:\n    - {id: y, addr: a:2}\n"},
		{"duplicate_node", "node_id: y\nlisten_addr: a:1\ndata_dir: d\ncluster:\n  nodes:\n    - {id: y, addr: a:2}\n    - {id: y, addr: a:3}\n"},
		{"rf_too_large", "node_id: y\nlisten_addr: a:1\ndata_dir: d\ncluster:\n  replication_factor: 5\n  nodes:\n    - {id: y, addr: a:2}\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := LoadConfig(write(tt.name+".yaml", tt.body)); err == nil {
				t.Error("expected validation error")
			}
		})
	}
}

// TestLoadConfig_SingleNodeDefaults verifies a config without a cluster
// section becomes a one-node, one-shard cluster.
func TestLoadConfig_SingleNodeDefaults(t *testing.T) {
	p := filepath.Join(t.TempDir(), "c.yaml")
	os.WriteFile(p, []byte("node_id: solo\nlisten_addr: 127.0.0.1:7000\ndata_dir: d\n"), 0o644)
	cfg, err := LoadConfig(p)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Cluster.Nodes) != 1 || cfg.Cluster.Nodes[0].ID != "solo" || cfg.Cluster.NumShards != 1 || cfg.Cluster.ReplicationFactor != 1 {
		t.Fatalf("defaults = %+v", cfg.Cluster)
	}
}
