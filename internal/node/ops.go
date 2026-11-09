package node

import (
	"context"
	"errors"

	"samanvaydb/internal/membership"
	"samanvaydb/internal/router"
	"samanvaydb/proto/adminpb"
)

// clusterOps implements membership.Cluster for orchestration running on
// this node, by calling the Admin API on whichever node each step targets.
type clusterOps struct{ n *Node }

// Ops returns this node's orchestration control surface.
func (n *Node) Ops() membership.Cluster { return &clusterOps{n: n} }

func (c *clusterOps) ShardMap(ctx context.Context) (*router.ShardMap, error) {
	return c.n.readShardMap(ctx)
}

func (c *clusterOps) UpdateShardMap(ctx context.Context, expected uint64, m *router.ShardMap) error {
	return c.n.updateShardMap(ctx, expected, m)
}

func (c *clusterOps) CreateReplica(ctx context.Context, node string, shardID int32) error {
	if node == c.n.id {
		_, err := c.n.manager.Open(shardID, nil, true)
		return err
	}
	cl, err := c.n.adminClient(node)
	if err != nil {
		return err
	}
	resp, err := cl.CreateReplica(ctx, &adminpb.CreateReplicaRequest{ShardId: shardID})
	if err != nil {
		return err
	}
	if !resp.Ok {
		return errors.New(resp.Error)
	}
	return nil
}

func (c *clusterOps) DestroyReplica(ctx context.Context, node string, shardID int32) error {
	cl, err := c.n.adminClient(node)
	if err != nil {
		return err
	}
	resp, err := cl.DestroyReplica(ctx, &adminpb.DestroyReplicaRequest{ShardId: shardID})
	if err != nil {
		return err
	}
	if !resp.Ok {
		return errors.New(resp.Error)
	}
	return nil
}

func (c *clusterOps) ChangeMembership(ctx context.Context, shardID int32, op, node, addr string) error {
	req := &adminpb.ChangeMembershipRequest{ShardId: shardID, Op: op, NodeId: node, NodeAddr: addr}
	// Prefer the current leader; fall back to any replica (which forwards).
	targets := []string{}
	if p, err := c.n.shardProgress(ctx, shardID); err == nil {
		targets = append(targets, p.Leader)
	}
	targets = append(targets, c.n.replicaCandidates(shardID)...)
	var lastErr error = errors.New("no replica reachable")
	for _, t := range targets {
		var resp *adminpb.ChangeMembershipResponse
		var err error
		if t == c.n.id {
			resp, err = (&adminServer{n: c.n}).ChangeMembership(ctx, req)
		} else {
			cl, cerr := c.n.adminClient(t)
			if cerr != nil {
				lastErr = cerr
				continue
			}
			resp, err = cl.ChangeMembership(ctx, req)
		}
		if err != nil {
			lastErr = err
			continue
		}
		if resp.Ok {
			return nil
		}
		lastErr = errors.New(resp.Error)
		if resp.RedirectLeader == "" {
			break // a real error from the leader, not a redirect
		}
	}
	return lastErr
}

func (c *clusterOps) Progress(ctx context.Context, shardID int32) (membership.Progress, error) {
	return c.n.shardProgress(ctx, shardID)
}
