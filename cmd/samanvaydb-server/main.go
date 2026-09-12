package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"samanvaydb/internal/config"
	"samanvaydb/internal/logging"
	"samanvaydb/internal/server"
)

func main() {
	configPath := flag.String("config", "configs/single-node.yaml", "path to YAML config file")
	healthAddr := flag.String("health-addr", "127.0.0.1:8080", "address for the /healthz HTTP endpoint")
	flag.Parse()

	// Load configuration.
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}

	// Initialize structured logger with node_id from config.
	logger := logging.NewLogger(logging.Options{
		NodeID: cfg.NodeID,
		JSON:   false,
	})

	logger.Info("SamanvayDB server starting",
		"listen_addr", cfg.ListenAddr,
		"data_dir", cfg.DataDir,
	)

	// Start the health-check HTTP server.
	hs := server.NewHealthServer(*healthAddr, cfg.NodeID, logger)
	addr, _, err := hs.Start()
	if err != nil {
		logger.Error("failed to start health server", "error", err)
		os.Exit(1)
	}

	// Config loaded, logger initialized — mark as ready.
	hs.SetReady()
	logger.Info("health server ready", "addr", addr.String())

	// Wait for shutdown signal.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh

	logger.Info("shutting down", "signal", sig.String())
	hs.Shutdown(context.Background())
}
