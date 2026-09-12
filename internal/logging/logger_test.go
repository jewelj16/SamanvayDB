package logging

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

// TestLogger_NodeIDPresent verifies that every log line contains the node_id field.
func TestLogger_NodeIDPresent(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(Options{
		NodeID: "test-node-42",
		Level:  slog.LevelInfo,
		Output: &buf,
		JSON:   false,
	})

	logger.Info("server starting", "port", 7000)

	output := buf.String()
	if !strings.Contains(output, "node_id=test-node-42") {
		t.Errorf("log output missing node_id field.\nGot: %s", output)
	}
	if !strings.Contains(output, "server starting") {
		t.Errorf("log output missing message.\nGot: %s", output)
	}
	if !strings.Contains(output, "port=7000") {
		t.Errorf("log output missing port field.\nGot: %s", output)
	}
}

// TestLogger_JSONFormat verifies JSON output contains structured fields.
func TestLogger_JSONFormat(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(Options{
		NodeID: "json-node",
		Level:  slog.LevelInfo,
		Output: &buf,
		JSON:   true,
	})

	logger.Info("health check passed")

	output := buf.String()
	// JSON output should contain the node_id as a JSON field
	if !strings.Contains(output, `"node_id":"json-node"`) {
		t.Errorf("JSON log output missing node_id field.\nGot: %s", output)
	}
	if !strings.Contains(output, `"msg":"health check passed"`) {
		t.Errorf("JSON log output missing msg field.\nGot: %s", output)
	}
}

// TestLogger_WithRaftContext verifies that term and role are added to log lines.
func TestLogger_WithRaftContext(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(Options{
		NodeID: "raft-node-1",
		Level:  slog.LevelInfo,
		Output: &buf,
		JSON:   false,
	})

	raftLogger := logger.WithRaftContext(5, "Leader")
	raftLogger.Info("sending heartbeat")

	output := buf.String()
	if !strings.Contains(output, "node_id=raft-node-1") {
		t.Errorf("log output missing node_id.\nGot: %s", output)
	}
	if !strings.Contains(output, "term=5") {
		t.Errorf("log output missing term.\nGot: %s", output)
	}
	if !strings.Contains(output, "role=Leader") {
		t.Errorf("log output missing role.\nGot: %s", output)
	}
}

// TestLogger_WithComponent verifies component scoping.
func TestLogger_WithComponent(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(Options{
		NodeID: "node-1",
		Level:  slog.LevelInfo,
		Output: &buf,
		JSON:   false,
	})

	storageLogger := logger.WithComponent("storage")
	storageLogger.Info("flushing memtable")

	output := buf.String()
	if !strings.Contains(output, "node_id=node-1") {
		t.Errorf("log output missing node_id.\nGot: %s", output)
	}
	if !strings.Contains(output, "component=storage") {
		t.Errorf("log output missing component.\nGot: %s", output)
	}
}

// TestLogger_LevelFiltering verifies that log levels below the threshold are suppressed.
func TestLogger_LevelFiltering(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(Options{
		NodeID: "node-1",
		Level:  slog.LevelWarn,
		Output: &buf,
		JSON:   false,
	})

	logger.Info("this should be suppressed")
	if buf.Len() != 0 {
		t.Errorf("expected no output for Info at Warn level, got: %s", buf.String())
	}

	logger.Warn("this should appear")
	if buf.Len() == 0 {
		t.Error("expected output for Warn at Warn level, got nothing")
	}
}
