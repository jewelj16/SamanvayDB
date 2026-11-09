// Package faultinjector is the operational fault injector used for demos
// and end-to-end tests against real clusters (multi-process or Docker). It
// is distinct from internal/raft/raftsim, the in-process simulator that
// remains the fast unit-test tier.
//
// Network faults (partition, delay, drop, slow node) are enforced inside
// each node's transport (see Rules) and driven remotely through the Admin
// API, so they work identically on every OS with no root/iptables. Process
// faults (kill/restart) use real OS processes: KillNode sends SIGKILL on
// Unix and TerminateProcess on Windows — a true crash with no graceful
// shutdown, never SIGTERM.
package faultinjector

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"samanvaydb/proto/adminpb"
)

// FaultInjector is implemented by every fault backend.
type FaultInjector interface {
	KillNode(nodeID string) error
	RestartNode(nodeID string) error
	CreatePartition(sideA, sideB []string) error
	HealPartition() error
	DelayMessages(from, to string, duration time.Duration) error
	DropMessages(from, to string, rate float64) error
}

// SlowInjector is implemented by backends that support the "slow node"
// fault (artificial latency in a node's request handling path).
type SlowInjector interface {
	SlowNode(nodeID string, latency time.Duration) error
}

// AdminNetwork implements the network half of FaultInjector by sending
// SetFaults/ClearFaults to every node's Admin API. Each node then enforces
// the rules on its own sends AND receives.
type AdminNetwork struct {
	// Nodes returns nodeID -> gRPC address for every node to configure.
	Nodes func() map[string]string

	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

func (a *AdminNetwork) client(addr string) (adminpb.AdminClient, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.conns == nil {
		a.conns = map[string]*grpc.ClientConn{}
	}
	cc, ok := a.conns[addr]
	if !ok {
		var err error
		cc, err = grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		a.conns[addr] = cc
	}
	return adminpb.NewAdminClient(cc), nil
}

// broadcast sends req to every node. Unreachable (killed) nodes are skipped:
// they will not see traffic anyway, and rules are re-applied by callers
// after a restart if needed.
func (a *AdminNetwork) broadcast(only []string, fn func(ctx context.Context, cl adminpb.AdminClient) (*adminpb.AdminResponse, error)) error {
	var errs []string
	reached := 0
	nodes := a.Nodes()
	for id, addr := range nodes {
		if only != nil && !contains(only, id) {
			continue
		}
		cl, err := a.client(addr)
		if err != nil {
			errs = append(errs, id+": "+err.Error())
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		resp, err := fn(ctx, cl)
		cancel()
		if err != nil {
			errs = append(errs, id+": "+err.Error())
			continue
		}
		if !resp.Ok {
			errs = append(errs, id+": "+resp.Error)
			continue
		}
		reached++
	}
	if reached == 0 && len(errs) > 0 {
		return fmt.Errorf("faultinjector: no node accepted the fault: %s", strings.Join(errs, "; "))
	}
	return nil
}

func contains(list []string, x string) bool {
	for _, v := range list {
		if v == x {
			return true
		}
	}
	return false
}

// CreatePartition blocks all traffic between sideA and sideB.
func (a *AdminNetwork) CreatePartition(sideA, sideB []string) error {
	return a.broadcast(nil, func(ctx context.Context, cl adminpb.AdminClient) (*adminpb.AdminResponse, error) {
		return cl.SetFaults(ctx, &adminpb.SetFaultsRequest{Kind: "partition", SideA: sideA, SideB: sideB})
	})
}

// HealPartition removes every network fault on every node.
func (a *AdminNetwork) HealPartition() error {
	return a.broadcast(nil, func(ctx context.Context, cl adminpb.AdminClient) (*adminpb.AdminResponse, error) {
		return cl.ClearFaults(ctx, &adminpb.ClearFaultsRequest{})
	})
}

// DelayMessages delays messages from->to (enforced by the sender).
func (a *AdminNetwork) DelayMessages(from, to string, d time.Duration) error {
	return a.broadcast([]string{from}, func(ctx context.Context, cl adminpb.AdminClient) (*adminpb.AdminResponse, error) {
		return cl.SetFaults(ctx, &adminpb.SetFaultsRequest{Kind: "delay", From: from, To: to, DelayMs: uint64(d.Milliseconds())})
	})
}

// DropMessages drops messages from->to with probability rate.
func (a *AdminNetwork) DropMessages(from, to string, rate float64) error {
	return a.broadcast([]string{from}, func(ctx context.Context, cl adminpb.AdminClient) (*adminpb.AdminResponse, error) {
		return cl.SetFaults(ctx, &adminpb.SetFaultsRequest{Kind: "drop", From: from, To: to, DropRate: rate})
	})
}

// SlowNode injects latency into every request nodeID handles.
func (a *AdminNetwork) SlowNode(nodeID string, latency time.Duration) error {
	return a.broadcast([]string{nodeID}, func(ctx context.Context, cl adminpb.AdminClient) (*adminpb.AdminResponse, error) {
		return cl.SetFaults(ctx, &adminpb.SetFaultsRequest{Kind: "slow", DelayMs: uint64(latency.Milliseconds())})
	})
}

// Close releases connections.
func (a *AdminNetwork) Close() {
	a.mu.Lock()
	defer a.mu.Unlock()
	for k, cc := range a.conns {
		cc.Close()
		delete(a.conns, k)
	}
}

// ---- leader-specific failures ----

// LeaderLookup returns the current leader node of a shard.
type LeaderLookup func(shard int32) (string, error)

// AdminLeaderLookup finds shard leaders through ClusterStatus on any of addrs.
func AdminLeaderLookup(addrs func() []string) LeaderLookup {
	return func(shard int32) (string, error) {
		var lastErr error = errors.New("no node reachable")
		for _, addr := range addrs() {
			cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				lastErr = err
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			resp, err := adminpb.NewAdminClient(cc).ShardStatus(ctx, &adminpb.ShardStatusRequest{ShardId: shard})
			cancel()
			cc.Close()
			if err != nil {
				lastErr = err
				continue
			}
			if resp.Shard.Leader == "" {
				lastErr = fmt.Errorf("shard %d has no leader", shard)
				continue
			}
			return resp.Shard.Leader, nil
		}
		return "", lastErr
	}
}

// KillLeader kills whichever node currently leads shard; returns its ID.
func KillLeader(fi FaultInjector, lookup LeaderLookup, shard int32) (string, error) {
	leader, err := lookup(shard)
	if err != nil {
		return "", err
	}
	return leader, fi.KillNode(leader)
}

// IsolateLeader partitions shard's leader away from every other node.
func IsolateLeader(fi FaultInjector, lookup LeaderLookup, shard int32, all []string) (string, error) {
	leader, err := lookup(shard)
	if err != nil {
		return "", err
	}
	var rest []string
	for _, n := range all {
		if n != leader {
			rest = append(rest, n)
		}
	}
	return leader, fi.CreatePartition([]string{leader}, rest)
}
