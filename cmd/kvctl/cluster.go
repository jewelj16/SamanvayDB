package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"samanvaydb/internal/faultinjector"
	"samanvaydb/pkg/client"
	"samanvaydb/proto/adminpb"
)

// emitProto prints a protobuf message as JSON or via human().
func (a *app) emitProto(m proto.Message, human func(w io.Writer)) {
	if a.json() {
		b, _ := protojson.MarshalOptions{Indent: "  ", EmitUnpopulated: true, UseProtoNames: true}.Marshal(m)
		fmt.Fprintln(a.out, string(b))
		return
	}
	human(a.out)
}

func shardName(id int32) string {
	if id < 0 {
		return "meta"
	}
	return fmt.Sprint(id)
}

func printShard(w io.Writer, s *adminpb.ShardInfo) {
	leader := s.Leader
	if leader == "" {
		leader = "(none)"
	}
	title := "Shard " + shardName(s.ShardId)
	if s.ShardId < 0 {
		title = "Metadata group"
	}
	state := ""
	if s.State != "" && s.State != "STABLE" {
		state = "  [" + s.State + "]"
	}
	fmt.Fprintf(w, "%s%s\n", title, state)
	fmt.Fprintf(w, "  Leader:   %-8s (term %d, committed index %d)\n", leader, s.Term, s.CommittedIndex)
	var parts []string
	for _, r := range s.ReplicaStatus {
		switch r.Role {
		case "leader":
			parts = append(parts, fmt.Sprintf("%s (leader)", r.NodeId))
		case "follower", "learner":
			parts = append(parts, fmt.Sprintf("%s (%s, lag %d)", r.NodeId, r.Role, r.Lag))
		default:
			parts = append(parts, fmt.Sprintf("%s (%s)", r.NodeId, r.Role))
		}
	}
	fmt.Fprintf(w, "  Replicas: %s\n", strings.Join(parts, ", "))
}

func clusterCmd(a *app) *cobra.Command {
	cluster := &cobra.Command{Use: "cluster", Short: "Cluster introspection and lifecycle"}

	cluster.AddCommand(&cobra.Command{
		Use:   "status",
		Short: "Show every shard's leader, term, committed index and replica lag",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := a.ctx()
			defer cancel()
			cl, err := a.client(ctx)
			if err != nil {
				return err
			}
			defer cl.Close()
			st, err := cl.ClusterStatus(ctx)
			if err != nil {
				return withCode(exitUnavailable, err)
			}
			a.emitProto(st, func(w io.Writer) {
				fmt.Fprintf(w, "Shard map version %d\n\n", st.ShardMapVersion)
				for _, s := range st.Shards {
					printShard(w, s)
					fmt.Fprintln(w)
				}
				tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
				fmt.Fprintln(tw, "NODE\tSTATUS\tADDR\tSHARDS")
				for _, n := range st.Nodes {
					status := "up"
					if !n.Up {
						status = "DOWN"
					}
					var hs []string
					for _, h := range n.HostedShards {
						hs = append(hs, shardName(h))
					}
					fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", n.NodeId, status, n.Addr, strings.Join(hs, ","))
				}
				tw.Flush()
			})
			return nil
		},
	})

	var metricsNode string
	metrics := &cobra.Command{
		Use:   "metrics",
		Short: "Print the samanvaydb_* Prometheus metrics of every node",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := a.ctx()
			defer cancel()
			cl, err := a.client(ctx)
			if err != nil {
				return err
			}
			defer cl.Close()
			type nodeMetrics struct {
				Node  string   `json:"node"`
				Lines []string `json:"metrics,omitempty"`
				Error string   `json:"error,omitempty"`
			}
			var all []nodeMetrics
			for _, n := range cl.ShardMap().Nodes {
				if metricsNode != "" && n.ID != metricsNode {
					continue
				}
				nm := nodeMetrics{Node: n.ID}
				lines, err := scrape(ctx, n.HTTPAddr)
				if err != nil {
					nm.Error = err.Error()
				}
				nm.Lines = lines
				all = append(all, nm)
			}
			a.emit(all, func(w io.Writer) {
				for _, nm := range all {
					fmt.Fprintf(w, "# node %s\n", nm.Node)
					if nm.Error != "" {
						fmt.Fprintf(w, "#   unreachable: %s\n", nm.Error)
					}
					for _, l := range nm.Lines {
						fmt.Fprintln(w, l)
					}
				}
			})
			return nil
		},
	}
	metrics.Flags().StringVar(&metricsNode, "node", "", "only this node")
	cluster.AddCommand(metrics)

	var nodes, rf, basePort int
	var shards int32
	var binary string
	start := &cobra.Command{
		Use:   "start",
		Short: "Start a local multi-process cluster (state in --local-dir)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if binary == "" {
				binary = defaultServerBinary()
			}
			if _, err := os.Stat(binary); err != nil {
				return withCode(exitUsage, fmt.Errorf("server binary %q not found (build it with `make build` or pass --binary): %w", binary, err))
			}
			lc, err := faultinjector.CreateLocal(faultinjector.LocalOptions{Dir: a.localDir, Binary: binary,
				Nodes: nodes, Shards: shards, RF: rf, BasePort: basePort})
			if err != nil {
				return err
			}
			// Wait until every shard has a leader.
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			cl, err := client.New(ctx, client.Config{Seeds: lc.Seeds()})
			if err != nil {
				return err
			}
			defer cl.Close()
			st, err := waitAllLeaders(ctx, cl)
			if err != nil {
				return withCode(exitUnavailable, err)
			}
			a.emitProto(st, func(w io.Writer) {
				fmt.Fprintf(w, "Started %d nodes, %d shards, replication factor %d (state: %s)\n\n", nodes, shards, rf, lc.Dir)
				for _, s := range st.Shards {
					printShard(w, s)
				}
				fmt.Fprintf(w, "\nexport SAMANVAY_SERVERS=%s\n", strings.Join(lc.Seeds(), ","))
			})
			return nil
		},
	}
	start.Flags().IntVar(&nodes, "nodes", 3, "number of nodes")
	start.Flags().Int32Var(&shards, "shards", 1, "number of shards")
	start.Flags().IntVar(&rf, "replication-factor", 3, "replicas per shard")
	start.Flags().StringVar(&binary, "binary", "", "path to samanvaydb-server (default: ./bin/samanvaydb-server)")
	start.Flags().IntVar(&basePort, "base-port", 0, "gRPC ports are base+i, HTTP ports base+1000+i (0 = random free ports)")
	cluster.AddCommand(start)

	var purge bool
	stop := &cobra.Command{
		Use:   "stop",
		Short: "Kill every process of the local cluster",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			lc, err := faultinjector.OpenLocal(a.localDir)
			if err != nil {
				return err
			}
			if err := lc.StopAll(purge); err != nil {
				return err
			}
			a.emit(map[string]any{"ok": true, "stopped": lc.NodeIDs(), "purged": purge},
				func(w io.Writer) { fmt.Fprintf(w, "Stopped %d nodes\n", len(lc.NodeIDs())) })
			return nil
		},
	}
	stop.Flags().BoolVar(&purge, "purge", false, "also delete all data")
	cluster.AddCommand(stop)
	return cluster
}

func defaultServerBinary() string {
	name := "samanvaydb-server"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	return filepath.Join("bin", name)
}

func waitAllLeaders(ctx context.Context, cl *client.Client) (*adminpb.ClusterStatusResponse, error) {
	for {
		st, err := cl.ClusterStatus(ctx)
		if err == nil {
			ok := true
			for _, s := range st.Shards {
				if s.Leader == "" {
					ok = false
				}
			}
			if ok {
				return st, nil
			}
		}
		select {
		case <-ctx.Done():
			if err == nil {
				err = errors.New("not every shard has a leader yet")
			}
			return st, err
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func scrape(ctx context.Context, httpAddr string) ([]string, error) {
	if httpAddr == "" {
		return nil, errors.New("node has no http_addr")
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+httpAddr+"/metrics", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var out []string
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		if l := sc.Text(); strings.HasPrefix(l, "samanvaydb_") {
			out = append(out, l)
		}
	}
	sort.Strings(out)
	return out, sc.Err()
}

func shardCmd(a *app) *cobra.Command {
	shard := &cobra.Command{Use: "shard", Short: "Shard introspection and migration"}
	var id int32
	status := &cobra.Command{
		Use:   "status",
		Short: "Show one shard's leader, term, committed index and replicas",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := a.ctx()
			defer cancel()
			admin, err := a.admin(ctx)
			if err != nil {
				return err
			}
			resp, err := admin.ShardStatus(ctx, &adminpb.ShardStatusRequest{ShardId: id})
			if err != nil {
				return withCode(exitUnavailable, err)
			}
			a.emitProto(resp, func(w io.Writer) { printShard(w, resp.Shard) })
			return nil
		},
	}
	status.Flags().Int32Var(&id, "shard", 0, "shard ID (-1 = metadata group)")
	shard.AddCommand(status)

	var mid int32
	var targets string
	migrate := &cobra.Command{
		Use:   "migrate",
		Short: "Move a shard's replica set to the target nodes (no downtime)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			defer cancel()
			admin, err := a.admin(ctx)
			if err != nil {
				return err
			}
			resp, err := admin.MigrateShard(ctx, &adminpb.MigrateShardRequest{ShardId: mid, Targets: splitList(targets)})
			return a.operation(resp, err)
		},
	}
	migrate.Flags().Int32Var(&mid, "shard", 0, "shard ID")
	migrate.Flags().StringVar(&targets, "target", "", "comma-separated target nodes")
	migrate.MarkFlagRequired("target")
	shard.AddCommand(migrate)
	return shard
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// admin returns an Admin client for the first reachable seed.
func (a *app) admin(ctx context.Context) (adminpb.AdminClient, error) {
	cl, err := a.client(ctx)
	if err != nil {
		return nil, err
	}
	// The client stays open for the process lifetime (kvctl is short-lived).
	var lastErr error
	for _, addr := range append(a.seeds(), nodeAddrs(cl)...) {
		adm, err := cl.AdminFor(addr)
		if err != nil {
			lastErr = err
			continue
		}
		pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		_, err = adm.NodeStatus(pctx, &adminpb.NodeStatusRequest{})
		cancel()
		if err == nil {
			return adm, nil
		}
		lastErr = err
	}
	return nil, withCode(exitUnavailable, fmt.Errorf("no node reachable: %v", lastErr))
}

func nodeAddrs(cl *client.Client) []string {
	var out []string
	for _, n := range cl.ShardMap().Nodes {
		out = append(out, n.Addr)
	}
	return out
}

// operation prints an orchestration result.
func (a *app) operation(resp *adminpb.OperationResponse, err error) error {
	if err != nil {
		return withCode(exitUnavailable, err)
	}
	a.emitProto(resp, func(w io.Writer) {
		for _, l := range resp.Log {
			fmt.Fprintln(w, l)
		}
		if resp.Ok {
			fmt.Fprintln(w, "OK")
		}
	})
	if !resp.Ok {
		return errors.New(resp.Error)
	}
	return nil
}

func rebalanceCmd(a *app) *cobra.Command {
	return &cobra.Command{
		Use:   "rebalance",
		Short: "Greedily even out replica counts across nodes",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			defer cancel()
			admin, err := a.admin(ctx)
			if err != nil {
				return err
			}
			resp, err := admin.Rebalance(ctx, &adminpb.RebalanceRequest{})
			return a.operation(resp, err)
		},
	}
}
