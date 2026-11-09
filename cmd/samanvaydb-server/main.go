// Command samanvaydb-server runs one SamanvayDB cluster node.
//
//	samanvaydb-server -config configs/single-node.yaml
//
// The node reads its YAML config, starts a structured logger carrying its
// node_id, recovers (or bootstraps) every Raft group it hosts from its data
// directory, then serves:
//
//	listen_addr  gRPC: client KV API, Admin API, inter-node Raft RPCs
//	http_addr    HTTP: /healthz, /metrics (Prometheus), /status (JSON)
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"samanvaydb/internal/config"
	"samanvaydb/internal/logging"
	"samanvaydb/internal/node"
)

func main() {
	configPath := flag.String("config", "configs/single-node.yaml", "path to YAML config file")
	healthAddr := flag.String("health-addr", "", "address for the HTTP endpoints (/healthz, /metrics, /status); overrides http_addr")
	logJSON := flag.Bool("log-json", false, "emit JSON log lines")
	logLevel := flag.String("log-level", "info", "debug | info | warn | error")
	flag.Parse()

	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}
	if *healthAddr != "" {
		cfg.HTTPAddr = *healthAddr
	}
	if cfg.HTTPAddr == "" {
		cfg.HTTPAddr = "127.0.0.1:8080"
	}

	var level slog.Level
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		fmt.Fprintf(os.Stderr, "bad -log-level: %v\n", err)
		os.Exit(1)
	}
	logger := logging.NewLogger(logging.Options{NodeID: cfg.NodeID, JSON: *logJSON, Level: level})
	logger.Info("SamanvayDB server starting",
		"listen_addr", cfg.ListenAddr, "http_addr", cfg.HTTPAddr, "data_dir", cfg.DataDir,
		"shards", cfg.Cluster.NumShards, "replication_factor", cfg.Cluster.ReplicationFactor,
		"cluster_nodes", len(cfg.Cluster.Nodes), "join", cfg.Join)

	n, err := node.New(node.Options{Config: cfg, Logger: logger})
	if err != nil {
		logger.Error("failed to start node", "error", err)
		os.Exit(1)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	logger.Info("shutting down", "signal", sig.String())
	n.Stop()
}
