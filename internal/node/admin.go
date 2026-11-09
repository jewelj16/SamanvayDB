package node

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"samanvaydb/internal/membership"
	"samanvaydb/internal/migration"
	"samanvaydb/internal/raft"
	"samanvaydb/internal/router"
	"samanvaydb/internal/shard"
	"samanvaydb/proto/adminpb"
)

// adminServer implements the Admin gRPC service.
type adminServer struct {
	adminpb.UnimplementedAdminServer
	n *Node
}

// opsTimeout bounds long-running orchestration (migrations, node changes).
const opsTimeout = 10 * time.Minute

func (a *adminServer) ClusterStatus(ctx context.Context, _ *adminpb.ClusterStatusRequest) (*adminpb.ClusterStatusResponse, error) {
	return a.n.clusterStatus(ctx)
}

func (a *adminServer) NodeStatus(_ context.Context, _ *adminpb.NodeStatusRequest) (*adminpb.NodeStatusResponse, error) {
	return a.n.localStatus(), nil
}

func (a *adminServer) ShardStatus(ctx context.Context, req *adminpb.ShardStatusRequest) (*adminpb.ShardStatusResponse, error) {
	cs, err := a.n.clusterStatus(ctx)
	if err != nil {
		return nil, err
	}
	for _, s := range cs.Shards {
		if s.ShardId == req.ShardId {
			return &adminpb.ShardStatusResponse{Shard: s}, nil
		}
	}
	return nil, fmt.Errorf("unknown shard %d", req.ShardId)
}

func (a *adminServer) GetShardMap(ctx context.Context, req *adminpb.GetShardMapRequest) (*adminpb.GetShardMapResponse, error) {
	if req.Cached {
		return &adminpb.GetShardMapResponse{ShardMapJson: a.n.cachedMap().Encode()}, nil
	}
	m, err := a.n.readShardMap(ctx)
	if err != nil {
		return &adminpb.GetShardMapResponse{Error: err.Error()}, nil
	}
	return &adminpb.GetShardMapResponse{ShardMapJson: m.Encode()}, nil
}

func (a *adminServer) UpdateShardMap(ctx context.Context, req *adminpb.UpdateShardMapRequest) (*adminpb.AdminResponse, error) {
	m, err := router.DecodeShardMap(req.ShardMapJson)
	if err == nil {
		err = a.n.updateShardMap(ctx, req.ExpectedVersion, m)
	}
	return adminResp(err), nil
}

func adminResp(err error) *adminpb.AdminResponse {
	if err != nil {
		return &adminpb.AdminResponse{Error: err.Error()}
	}
	return &adminpb.AdminResponse{Ok: true}
}

func (a *adminServer) CreateReplica(_ context.Context, req *adminpb.CreateReplicaRequest) (*adminpb.AdminResponse, error) {
	_, err := a.n.manager.Open(req.ShardId, nil, true)
	return adminResp(err), nil
}

// DestroyReplica deletes a replica. Callers must only do this after the
// replica's removal from the group configuration has COMMITTED (the
// orchestrator guarantees it). The replica itself cannot be asked: a removed
// follower often never receives the entry that removes it, because the
// leader stops replicating to it the moment that entry is appended.
func (a *adminServer) DestroyReplica(_ context.Context, req *adminpb.DestroyReplicaRequest) (*adminpb.AdminResponse, error) {
	if s, ok := a.n.manager.Get(req.ShardId); ok && s.Raft().IsLeader() {
		return adminResp(fmt.Errorf("refusing to destroy shard %d: this replica is currently the leader", req.ShardId)), nil
	}
	return adminResp(a.n.manager.Destroy(req.ShardId)), nil
}

func (a *adminServer) ChangeMembership(ctx context.Context, req *adminpb.ChangeMembershipRequest) (*adminpb.ChangeMembershipResponse, error) {
	idx, err := a.n.changeMembershipLocal(ctx, req)
	if err == nil {
		return &adminpb.ChangeMembershipResponse{Ok: true, Index: idx}, nil
	}
	var nl *shard.NotLeaderError
	if errors.As(err, &nl) && !isForwarded(ctx) {
		// Forward to the leader we know of (once).
		targets := []string{string(nl.Leader)}
		if nl.Leader == "" {
			targets = a.n.replicaCandidates(req.ShardId)
		}
		for _, t := range targets {
			if t == "" || t == a.n.id {
				continue
			}
			cl, cerr := a.n.adminClient(t)
			if cerr != nil {
				continue
			}
			resp, ferr := cl.ChangeMembership(forwardCtx(ctx), req)
			if ferr == nil && resp.Ok {
				return resp, nil
			}
			if ferr == nil {
				err = errors.New(resp.Error)
			}
		}
	}
	resp := &adminpb.ChangeMembershipResponse{Error: err.Error()}
	if errors.As(err, &nl) {
		resp.RedirectLeader = string(nl.Leader)
	}
	return resp, nil
}

func (n *Node) replicaCandidates(id int32) []string {
	m := n.cachedMap()
	if id == shard.MetaShardID {
		return m.MetaReplicas
	}
	if meta, ok := m.Shard(id); ok {
		return meta.Replicas
	}
	return nil
}

// changeMembershipLocal executes a membership op on the local replica,
// which must be the shard leader.
func (n *Node) changeMembershipLocal(ctx context.Context, req *adminpb.ChangeMembershipRequest) (uint64, error) {
	s, ok := n.manager.Get(req.ShardId)
	if !ok || !s.Raft().IsLeader() {
		leader := raft.NodeID("")
		if ok {
			leader = s.Raft().Leader()
		}
		return 0, &shard.NotLeaderError{Leader: leader}
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var cc raft.ConfChange
	switch req.Op {
	case "snapshot":
		return s.Raft().TakeSnapshot()
	case "add_learner":
		n.book.Set(req.NodeId, req.NodeAddr)
		cc = raft.ConfChange{Type: raft.AddLearner, Node: raft.NodeID(req.NodeId), Addr: req.NodeAddr}
	case "promote":
		cc = raft.ConfChange{Type: raft.PromoteLearner, Node: raft.NodeID(req.NodeId)}
	case "remove":
		cc = raft.ConfChange{Type: raft.RemoveMember, Node: raft.NodeID(req.NodeId)}
	default:
		return 0, fmt.Errorf("unknown membership op %q", req.Op)
	}
	idx, err := s.Raft().ProposeConfChange(ctx, cc)
	if errors.Is(err, raft.ErrNotLeader) {
		return 0, &shard.NotLeaderError{Leader: s.Raft().Leader()}
	}
	if err == nil {
		n.logger.Info("membership change committed", "shard_id", req.ShardId, "op", req.Op,
			"member", req.NodeId, "index", idx, "term", s.Raft().CurrentTerm())
	}
	return idx, err
}

// ---- orchestration ----

type opLog struct {
	mu    sync.Mutex
	lines []string
	n     *Node
}

func (l *opLog) logf(format string, args ...any) {
	line := fmt.Sprintf(format, args...)
	l.mu.Lock()
	l.lines = append(l.lines, line)
	l.mu.Unlock()
	l.n.logger.Info(line, "component", "orchestrator")
}

func (l *opLog) resp(err error) *adminpb.OperationResponse {
	l.mu.Lock()
	defer l.mu.Unlock()
	r := &adminpb.OperationResponse{Ok: err == nil, Log: append([]string(nil), l.lines...)}
	if err != nil {
		r.Error = err.Error()
	}
	return r
}

func (a *adminServer) opCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, opsTimeout)
}

func (a *adminServer) AddNode(ctx context.Context, req *adminpb.AddNodeRequest) (*adminpb.OperationResponse, error) {
	ctx, cancel := a.opCtx(ctx)
	defer cancel()
	l := &opLog{n: a.n}
	ops := a.n.Ops()
	_, err := membership.RegisterNode(ctx, ops, router.NodeInfo{ID: req.NodeId, Addr: req.Addr, HTTPAddr: req.HttpAddr})
	if err == nil {
		l.logf("node %s registered (%s)", req.NodeId, req.Addr)
		if req.Rebalance {
			_, err = migration.Rebalance(ctx, ops, l.logf)
		}
	}
	return l.resp(err), nil
}

func (a *adminServer) RemoveNode(ctx context.Context, req *adminpb.RemoveNodeRequest) (*adminpb.OperationResponse, error) {
	ctx, cancel := a.opCtx(ctx)
	defer cancel()
	l := &opLog{n: a.n}
	return l.resp(membership.RemoveNode(ctx, a.n.Ops(), req.NodeId, l.logf)), nil
}

func (a *adminServer) ReplaceNode(ctx context.Context, req *adminpb.ReplaceNodeRequest) (*adminpb.OperationResponse, error) {
	ctx, cancel := a.opCtx(ctx)
	defer cancel()
	l := &opLog{n: a.n}
	err := membership.ReplaceNode(ctx, a.n.Ops(), req.FailedNodeId,
		router.NodeInfo{ID: req.NewNodeId, Addr: req.NewAddr, HTTPAddr: req.NewHttpAddr}, l.logf)
	return l.resp(err), nil
}

func (a *adminServer) MigrateShard(ctx context.Context, req *adminpb.MigrateShardRequest) (*adminpb.OperationResponse, error) {
	ctx, cancel := a.opCtx(ctx)
	defer cancel()
	l := &opLog{n: a.n}
	_, err := migration.Run(ctx, a.n.Ops(), req.ShardId, req.Targets, l.logf)
	return l.resp(err), nil
}

func (a *adminServer) Rebalance(ctx context.Context, _ *adminpb.RebalanceRequest) (*adminpb.OperationResponse, error) {
	ctx, cancel := a.opCtx(ctx)
	defer cancel()
	l := &opLog{n: a.n}
	_, err := migration.Rebalance(ctx, a.n.Ops(), l.logf)
	return l.resp(err), nil
}

// ---- fault injection ----

func (a *adminServer) SetFaults(_ context.Context, req *adminpb.SetFaultsRequest) (*adminpb.AdminResponse, error) {
	f := a.n.faults
	switch strings.ToLower(req.Kind) {
	case "partition":
		f.Partition(req.SideA, req.SideB)
	case "heal":
		f.HealPartition()
	case "delay":
		f.Delay(orAny(req.From), orAny(req.To), time.Duration(req.DelayMs)*time.Millisecond)
	case "drop":
		f.Drop(orAny(req.From), orAny(req.To), req.DropRate)
	case "slow":
		f.Slow(time.Duration(req.DelayMs) * time.Millisecond)
	default:
		return adminResp(fmt.Errorf("unknown fault kind %q", req.Kind)), nil
	}
	a.n.logger.Warn("network fault injected", "kind", req.Kind, "side_a", fmt.Sprint(req.SideA),
		"side_b", fmt.Sprint(req.SideB), "from", req.From, "to", req.To, "delay_ms", req.DelayMs, "drop_rate", req.DropRate)
	return adminResp(nil), nil
}

func orAny(s string) string {
	if s == "" {
		return "*"
	}
	return s
}

func (a *adminServer) ClearFaults(context.Context, *adminpb.ClearFaultsRequest) (*adminpb.AdminResponse, error) {
	a.n.faults.Clear()
	a.n.logger.Warn("network faults cleared")
	return adminResp(nil), nil
}
