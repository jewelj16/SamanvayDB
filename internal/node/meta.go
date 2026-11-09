package node

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc/metadata"

	"samanvaydb/internal/router"
	"samanvaydb/internal/shard"
	"samanvaydb/proto/adminpb"
)

// meta.go — the shard map lives in the metadata Raft group (shard ID -1)
// under a single key, and is only ever changed by compare-and-swap. Any
// node can serve GetShardMap/UpdateShardMap: if it is not the meta leader
// it forwards the call (once) to a node that might be.

var shardMapKey = []byte("shardmap")

const forwardedHeader = "x-samanvay-forwarded"

var errVersionConflict = errors.New("shard map version conflict")

func isForwarded(ctx context.Context) bool {
	md, ok := metadata.FromIncomingContext(ctx)
	return ok && len(md.Get(forwardedHeader)) > 0
}

func forwardCtx(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, forwardedHeader, "1")
}

// metaLeaderLocal returns the local meta replica if it is the leader.
func (n *Node) metaLeaderLocal() (*shard.Shard, bool) {
	s, ok := n.manager.Get(shard.MetaShardID)
	if !ok || !s.Raft().IsLeader() {
		return nil, false
	}
	return s, true
}

// metaCandidates lists nodes to forward meta operations to, best first.
func (n *Node) metaCandidates() []string {
	var out []string
	if s, ok := n.manager.Get(shard.MetaShardID); ok {
		if l := string(s.Raft().Leader()); l != "" && l != n.id {
			out = append(out, l)
		}
	}
	for _, r := range n.cachedMap().MetaReplicas {
		if r != n.id && !contains(out, r) {
			out = append(out, r)
		}
	}
	return out
}

// readLocal reads the map from the local meta LEADER (linearizably).
func (n *Node) readLocal(ctx context.Context, s *shard.Shard) (*router.ShardMap, []byte, error) {
	raw, found, err := s.Get(ctx, shardMapKey, shard.ConsistencyLeader)
	if err != nil {
		return nil, nil, err
	}
	if !found {
		return n.initial.Clone(), nil, nil // never updated: version 1 from config
	}
	m, err := router.DecodeShardMap(raw)
	return m, raw, err
}

// readShardMap returns the authoritative shard map and refreshes the cache.
func (n *Node) readShardMap(ctx context.Context) (*router.ShardMap, error) {
	if s, ok := n.metaLeaderLocal(); ok {
		m, _, err := n.readLocal(ctx, s)
		if err == nil {
			n.setCachedMap(m)
			return m, nil
		}
	}
	if isForwarded(ctx) {
		return nil, fmt.Errorf("node %s is not the metadata leader", n.id)
	}
	var lastErr error = errors.New("no metadata replica reachable")
	for _, id := range n.metaCandidates() {
		cl, err := n.adminClient(id)
		if err != nil {
			lastErr = err
			continue
		}
		cctx, cancel := context.WithTimeout(forwardCtx(ctx), time.Second)
		resp, err := cl.GetShardMap(cctx, &adminpb.GetShardMapRequest{})
		cancel()
		if err != nil {
			lastErr = err
			continue
		}
		if resp.Error != "" {
			lastErr = errors.New(resp.Error)
			continue
		}
		m, err := router.DecodeShardMap(resp.ShardMapJson)
		if err != nil {
			return nil, err
		}
		n.setCachedMap(m)
		return m, nil
	}
	return nil, fmt.Errorf("read shard map: %w", lastErr)
}

// updateShardMap stores m iff the current version is expected.
func (n *Node) updateShardMap(ctx context.Context, expected uint64, m *router.ShardMap) error {
	if m.Version != expected+1 {
		return fmt.Errorf("new shard map version must be %d, got %d", expected+1, m.Version)
	}
	if err := m.Validate(); err != nil {
		return err
	}
	if s, ok := n.metaLeaderLocal(); ok {
		cur, raw, err := n.readLocal(ctx, s)
		if err != nil {
			return err
		}
		if cur.Version != expected {
			return fmt.Errorf("%w: stored version %d, expected %d", errVersionConflict, cur.Version, expected)
		}
		r, err := s.CAS(ctx, shardMapKey, raw, raw == nil, m.Encode())
		if err != nil {
			return err
		}
		if r.Code != shard.ResultOK {
			return fmt.Errorf("%w: concurrent update", errVersionConflict)
		}
		n.setCachedMap(m)
		n.logger.Info("shard map updated", "version", m.Version)
		return nil
	}
	if isForwarded(ctx) {
		return fmt.Errorf("node %s is not the metadata leader", n.id)
	}
	var lastErr error = errors.New("no metadata replica reachable")
	for _, id := range n.metaCandidates() {
		cl, err := n.adminClient(id)
		if err != nil {
			lastErr = err
			continue
		}
		cctx, cancel := context.WithTimeout(forwardCtx(ctx), 3*time.Second)
		resp, err := cl.UpdateShardMap(cctx, &adminpb.UpdateShardMapRequest{ExpectedVersion: expected, ShardMapJson: m.Encode()})
		cancel()
		if err != nil {
			lastErr = err
			continue
		}
		if !resp.Ok {
			lastErr = errors.New(resp.Error)
			continue
		}
		n.setCachedMap(m)
		return nil
	}
	return fmt.Errorf("update shard map: %w", lastErr)
}

func (n *Node) adminClient(id string) (adminpb.AdminClient, error) {
	addr, ok := n.book.Get(id)
	if !ok {
		return nil, fmt.Errorf("no address for node %s", id)
	}
	cc, err := n.pool.Get(addr)
	if err != nil {
		return nil, err
	}
	return adminpb.NewAdminClient(cc), nil
}
