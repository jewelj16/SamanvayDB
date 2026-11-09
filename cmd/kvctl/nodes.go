package main

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"samanvaydb/internal/faultinjector"
	"samanvaydb/proto/adminpb"
)

func nodeCmd(a *app) *cobra.Command {
	nodeC := &cobra.Command{Use: "node", Short: "Node introspection, lifecycle and fault simulation"}

	var statusNode string
	status := &cobra.Command{
		Use:   "status",
		Short: "Show the replicas hosted on one node",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := a.ctx()
			defer cancel()
			cl, err := a.client(ctx)
			if err != nil {
				return err
			}
			defer cl.Close()
			info, ok := cl.ShardMap().Node(statusNode)
			if !ok {
				return withCode(exitInvalid, fmt.Errorf("unknown node %q", statusNode))
			}
			adm, err := cl.AdminFor(info.Addr)
			if err != nil {
				return err
			}
			resp, err := adm.NodeStatus(ctx, &adminpb.NodeStatusRequest{})
			if err != nil {
				return withCode(exitUnavailable, fmt.Errorf("node %s unreachable: %w", statusNode, err))
			}
			a.emitProto(resp, func(w io.Writer) {
				fmt.Fprintf(w, "Node %s (%s)\n", resp.Node.NodeId, resp.Node.Addr)
				tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
				fmt.Fprintln(tw, "SHARD\tROLE\tTERM\tLEADER\tCOMMIT\tAPPLIED\tSNAPSHOT\tKEYS")
				for _, s := range resp.Shards {
					fmt.Fprintf(tw, "%s\t%s\t%d\t%s\t%d\t%d\t%d\t%d\n", shardName(s.ShardId), s.Role, s.Term, s.Leader,
						s.CommitIndex, s.AppliedIndex, s.SnapshotIndex, s.KeyCount)
				}
				tw.Flush()
			})
			return nil
		},
	}
	status.Flags().StringVar(&statusNode, "node", "", "node ID")
	status.MarkFlagRequired("node")
	nodeC.AddCommand(status)

	var addID, addAddr, addHTTP string
	var noRebalance bool
	add := &cobra.Command{
		Use:   "add",
		Short: "Add a node to the cluster and rebalance replicas onto it",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if addAddr == "" { // local cluster: start the process ourselves
				lc, err := faultinjector.OpenLocal(a.localDir)
				if err != nil {
					return withCode(exitUsage, fmt.Errorf("--addr is required outside a `kvctl cluster start` cluster: %w", err))
				}
				ln, err := lc.AddNode(addID)
				if err != nil {
					return err
				}
				addAddr, addHTTP = ln.Addr, ln.HTTPAddr
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			defer cancel()
			adm, err := a.admin(ctx)
			if err != nil {
				return err
			}
			resp, err := adm.AddNode(ctx, &adminpb.AddNodeRequest{NodeId: addID, Addr: addAddr, HttpAddr: addHTTP, Rebalance: !noRebalance})
			return a.operation(resp, err)
		},
	}
	add.Flags().StringVar(&addID, "node", "", "new node ID")
	add.Flags().StringVar(&addAddr, "addr", "", "gRPC address of the (already running) node; omit for a local cluster")
	add.Flags().StringVar(&addHTTP, "http-addr", "", "HTTP address of the node")
	add.Flags().BoolVar(&noRebalance, "no-rebalance", false, "only register the node")
	add.MarkFlagRequired("node")
	nodeC.AddCommand(add)

	var rmID string
	remove := &cobra.Command{
		Use:   "remove",
		Short: "Decommission a node (its replicas are moved elsewhere first)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			defer cancel()
			adm, err := a.admin(ctx)
			if err != nil {
				return err
			}
			resp, err := adm.RemoveNode(ctx, &adminpb.RemoveNodeRequest{NodeId: rmID})
			return a.operation(resp, err)
		},
	}
	remove.Flags().StringVar(&rmID, "node", "", "node ID")
	remove.MarkFlagRequired("node")
	nodeC.AddCommand(remove)

	var failed, newID, newAddr, newHTTP string
	replace := &cobra.Command{
		Use:   "replace",
		Short: "Replace a failed node with a new one (remove + add per shard)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if newAddr == "" {
				lc, err := faultinjector.OpenLocal(a.localDir)
				if err != nil {
					return withCode(exitUsage, fmt.Errorf("--addr is required outside a local cluster: %w", err))
				}
				ln, err := lc.AddNode(newID)
				if err != nil {
					return err
				}
				newAddr, newHTTP = ln.Addr, ln.HTTPAddr
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			defer cancel()
			adm, err := a.admin(ctx)
			if err != nil {
				return err
			}
			resp, err := adm.ReplaceNode(ctx, &adminpb.ReplaceNodeRequest{FailedNodeId: failed, NewNodeId: newID, NewAddr: newAddr, NewHttpAddr: newHTTP})
			return a.operation(resp, err)
		},
	}
	replace.Flags().StringVar(&failed, "failed", "", "failed node ID")
	replace.Flags().StringVar(&newID, "node", "", "replacement node ID")
	replace.Flags().StringVar(&newAddr, "addr", "", "gRPC address of the replacement (omit for a local cluster)")
	replace.Flags().StringVar(&newHTTP, "http-addr", "", "HTTP address of the replacement")
	replace.MarkFlagRequired("failed")
	replace.MarkFlagRequired("node")
	nodeC.AddCommand(replace)

	// ---- fault simulation (wraps internal/faultinjector) ----
	localOp := func(use, short string, fn func(lc *faultinjector.LocalCluster, id string) error) *cobra.Command {
		return &cobra.Command{
			Use: use + " <node-id>", Short: short, Args: cobra.ExactArgs(1),
			RunE: func(cmd *cobra.Command, args []string) error {
				lc, err := faultinjector.OpenLocal(a.localDir)
				if err != nil {
					return withCode(exitUsage, err)
				}
				start := time.Now()
				if err := fn(lc, args[0]); err != nil {
					return err
				}
				a.emit(map[string]any{"ok": true, "op": use, "node": args[0], "took_ms": time.Since(start).Milliseconds()},
					func(w io.Writer) {
						fmt.Fprintf(w, "%s: %s OK (%v)\n", use, args[0], time.Since(start).Round(time.Millisecond))
					})
				return nil
			},
		}
	}
	nodeC.AddCommand(localOp("kill", "Crash a node (SIGKILL / TerminateProcess — no graceful shutdown)",
		func(lc *faultinjector.LocalCluster, id string) error { return lc.KillNode(id) }))
	nodeC.AddCommand(localOp("restart", "Restart a killed node from its data directory",
		func(lc *faultinjector.LocalCluster, id string) error { return lc.RestartNode(id) }))

	var latency time.Duration
	slow := &cobra.Command{
		Use: "slow <node-id>", Short: "Inject latency into every request a node handles (0 to clear)", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			net, err := a.network()
			if err != nil {
				return err
			}
			if err := net.SlowNode(args[0], latency); err != nil {
				return withCode(exitUnavailable, err)
			}
			a.emit(map[string]any{"ok": true, "node": args[0], "latency_ms": latency.Milliseconds()},
				func(w io.Writer) { fmt.Fprintf(w, "%s now delays every request by %v\n", args[0], latency) })
			return nil
		},
	}
	slow.Flags().DurationVar(&latency, "latency", 200*time.Millisecond, "added latency")
	nodeC.AddCommand(slow)
	return nodeC
}

// network returns an AdminNetwork over every node in the shard map.
func (a *app) network() (*faultinjector.AdminNetwork, error) {
	ctx, cancel := a.ctx()
	defer cancel()
	cl, err := a.client(ctx)
	if err != nil {
		return nil, err
	}
	nodes := map[string]string{}
	for _, n := range cl.ShardMap().Nodes {
		nodes[n.ID] = n.Addr
	}
	cl.Close()
	return &faultinjector.AdminNetwork{Nodes: func() map[string]string { return nodes }}, nil
}

func partitionCmd(a *app) *cobra.Command {
	p := &cobra.Command{Use: "partition", Short: "Network partitions"}
	var sideA, sideB string
	create := &cobra.Command{
		Use: "create", Short: "Block all traffic between two groups of nodes", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			net, err := a.network()
			if err != nil {
				return err
			}
			A, B := splitList(sideA), splitList(sideB)
			if len(B) == 0 { // default side B: everyone else
				for id := range net.Nodes() {
					if !contains(A, id) {
						B = append(B, id)
					}
				}
				sort.Strings(B)
			}
			if err := net.CreatePartition(A, B); err != nil {
				return withCode(exitUnavailable, err)
			}
			a.emit(map[string]any{"ok": true, "side_a": A, "side_b": B},
				func(w io.Writer) {
					fmt.Fprintf(w, "Partitioned [%s] | [%s]\n", strings.Join(A, ","), strings.Join(B, ","))
				})
			return nil
		},
	}
	create.Flags().StringVar(&sideA, "side-a", "", "comma-separated nodes on side A")
	create.Flags().StringVar(&sideB, "side-b", "", "comma-separated nodes on side B (default: all others)")
	create.MarkFlagRequired("side-a")
	p.AddCommand(create)
	p.AddCommand(&cobra.Command{
		Use: "heal", Short: "Remove every network fault on every node", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			net, err := a.network()
			if err != nil {
				return err
			}
			if err := net.HealPartition(); err != nil {
				return withCode(exitUnavailable, err)
			}
			a.emit(map[string]any{"ok": true}, func(w io.Writer) { fmt.Fprintln(w, "Healed") })
			return nil
		},
	})
	return p
}

func netCmd(a *app) *cobra.Command {
	n := &cobra.Command{Use: "net", Short: "Fine-grained network faults (delay, drop)"}
	var from, to string
	var latency time.Duration
	var rate float64
	delay := &cobra.Command{
		Use: "delay", Short: "Delay messages from one node to another", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			net, err := a.network()
			if err != nil {
				return err
			}
			if err := net.DelayMessages(from, to, latency); err != nil {
				return withCode(exitUnavailable, err)
			}
			a.emit(map[string]any{"ok": true}, func(w io.Writer) { fmt.Fprintf(w, "%s -> %s delayed by %v\n", from, to, latency) })
			return nil
		},
	}
	drop := &cobra.Command{
		Use: "drop", Short: "Drop a fraction of messages from one node to another", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			net, err := a.network()
			if err != nil {
				return err
			}
			if err := net.DropMessages(from, to, rate); err != nil {
				return withCode(exitUnavailable, err)
			}
			a.emit(map[string]any{"ok": true}, func(w io.Writer) { fmt.Fprintf(w, "%s -> %s dropping %.0f%%\n", from, to, rate*100) })
			return nil
		},
	}
	for _, c := range []*cobra.Command{delay, drop} {
		c.Flags().StringVar(&from, "from", "", "sending node")
		c.Flags().StringVar(&to, "to", "*", "receiving node (* = any)")
		c.MarkFlagRequired("from")
	}
	delay.Flags().DurationVar(&latency, "latency", 100*time.Millisecond, "delay")
	drop.Flags().Float64Var(&rate, "rate", 0.5, "drop probability 0..1")
	n.AddCommand(delay, drop)
	return n
}

func contains(list []string, x string) bool {
	for _, v := range list {
		if v == x {
			return true
		}
	}
	return false
}
