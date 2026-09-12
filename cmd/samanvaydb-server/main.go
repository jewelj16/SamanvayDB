package main

import (
	"flag"
	"fmt"
	"os"

	"samanvaydb/internal/config"
	"samanvaydb/internal/logging"
)

func main() {
	configPath := flag.String("config", "configs/single-node.yaml", "path to YAML config file")
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
}
