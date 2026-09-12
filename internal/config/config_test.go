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
