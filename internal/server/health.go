// Package server provides the HTTP health-check endpoint and will later host
// the gRPC service for SamanvayDB.
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"

	"samanvaydb/internal/logging"
)

// HealthServer exposes a /healthz HTTP endpoint that returns OK once the node
// has loaded its configuration and initialized its logger.
type HealthServer struct {
	httpServer *http.Server
	logger     *logging.Logger
	ready      bool
	mu         sync.RWMutex
}

// HealthResponse is the JSON body returned by /healthz.
type HealthResponse struct {
	Status string `json:"status"`
	NodeID string `json:"node_id,omitempty"`
}

// NewHealthServer creates a health-check server on the given address.
func NewHealthServer(addr string, nodeID string, logger *logging.Logger) *HealthServer {
	hs := &HealthServer{
		logger: logger,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", hs.handleHealth(nodeID))

	hs.httpServer = &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	return hs
}

// handleHealth returns the /healthz handler.
func (hs *HealthServer) handleHealth(nodeID string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		hs.mu.RLock()
		ready := hs.ready
		hs.mu.RUnlock()

		w.Header().Set("Content-Type", "application/json")

		if !ready {
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(HealthResponse{Status: "not_ready"})
			return
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(HealthResponse{
			Status: "ok",
			NodeID: nodeID,
		})
	}
}

// SetReady marks the server as ready to serve. Called once config is loaded
// and the logger is initialized.
func (hs *HealthServer) SetReady() {
	hs.mu.Lock()
	hs.ready = true
	hs.mu.Unlock()
}

// Start begins listening. It returns the actual listener address (useful when
// port 0 is used for tests) and a channel that receives the server error
// when it stops.
func (hs *HealthServer) Start() (net.Addr, <-chan error, error) {
	ln, err := net.Listen("tcp", hs.httpServer.Addr)
	if err != nil {
		return nil, nil, fmt.Errorf("health server: listen: %w", err)
	}

	errCh := make(chan error, 1)
	go func() {
		hs.logger.Info("health server listening", "addr", ln.Addr().String())
		if err := hs.httpServer.Serve(ln); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
		close(errCh)
	}()

	return ln.Addr(), errCh, nil
}

// Shutdown gracefully stops the health server.
func (hs *HealthServer) Shutdown(ctx context.Context) error {
	return hs.httpServer.Shutdown(ctx)
}
