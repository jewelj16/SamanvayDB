package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"samanvaydb/internal/logging"
)

// TestHealthz_ReturnsOKWhenReady starts the server, marks it ready,
// and verifies /healthz returns 200 with status "ok".
func TestHealthz_ReturnsOKWhenReady(t *testing.T) {
	logger := logging.NewLogger(logging.Options{
		NodeID: "test-node",
		Level:  slog.LevelInfo,
	})

	hs := NewHealthServer("127.0.0.1:0", "test-node", logger)

	addr, _, err := hs.Start()
	if err != nil {
		t.Fatalf("Start() failed: %v", err)
	}
	defer hs.Shutdown(context.Background())

	// Mark ready (config loaded, logger initialized).
	hs.SetReady()

	url := fmt.Sprintf("http://%s/healthz", addr.String())
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET /healthz failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status code = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	var body HealthResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if body.Status != "ok" {
		t.Errorf("status = %q, want %q", body.Status, "ok")
	}
	if body.NodeID != "test-node" {
		t.Errorf("node_id = %q, want %q", body.NodeID, "test-node")
	}
}

// TestHealthz_Returns503BeforeReady verifies /healthz returns 503 before
// SetReady() is called.
func TestHealthz_Returns503BeforeReady(t *testing.T) {
	logger := logging.NewLogger(logging.Options{
		NodeID: "test-node",
		Level:  slog.LevelInfo,
	})

	hs := NewHealthServer("127.0.0.1:0", "test-node", logger)

	addr, _, err := hs.Start()
	if err != nil {
		t.Fatalf("Start() failed: %v", err)
	}
	defer hs.Shutdown(context.Background())

	// Do NOT call SetReady().

	url := fmt.Sprintf("http://%s/healthz", addr.String())
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET /healthz failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status code = %d, want %d", resp.StatusCode, http.StatusServiceUnavailable)
	}

	var body HealthResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if body.Status != "not_ready" {
		t.Errorf("status = %q, want %q", body.Status, "not_ready")
	}
}

// TestHealthz_GracefulShutdown verifies the server shuts down cleanly.
func TestHealthz_GracefulShutdown(t *testing.T) {
	logger := logging.NewLogger(logging.Options{
		NodeID: "test-node",
		Level:  slog.LevelInfo,
	})

	hs := NewHealthServer("127.0.0.1:0", "test-node", logger)

	addr, _, err := hs.Start()
	if err != nil {
		t.Fatalf("Start() failed: %v", err)
	}

	hs.SetReady()

	// Verify it's serving.
	url := fmt.Sprintf("http://%s/healthz", addr.String())
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET /healthz failed: %v", err)
	}
	resp.Body.Close()

	// Shutdown.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := hs.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() failed: %v", err)
	}

	// After shutdown, connection should fail.
	_, err = http.Get(url)
	if err == nil {
		t.Error("expected connection error after shutdown, got nil")
	}
}
