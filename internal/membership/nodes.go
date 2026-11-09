package membership

import (
	"context"
	"fmt"
	"sort"

	"samanvaydb/internal/router"
)

// add.go / remove.go / replace.go of the plan live here together: they are
// thin compositions of AddReplica and RemoveReplica, deliberately adding no
// new (separately risky) Raft logic.

// RegisterNode adds a node's addresses to the shard map so that it can be
// targeted by AddReplica. It hosts nothing until replicas are added.
func RegisterNode(ctx context.Context, c Cluster, n router.NodeInfo) (*router.ShardMap, error) {
	return MutateShardMap(ctx, c, func(m *router.ShardMap) error {
		for i, existing := range m.Nodes {
			if existing.ID == n.ID {
				m.Nodes[i] = n // idempotent re-registration (address update)
				return nil
			}
		}
		m.Nodes = append(m.Nodes, n)
		return nil
	})
}

// AddShardReplica adds node to shard's group and records it in the map.
func AddShardReplica(ctx context.Context, c Cluster, shard int32, node string, logf Logf) error {
	if err := AddReplica(ctx, c, shard, node, logf); err != nil {
		return err
	}
	_, err := MutateShardMap(ctx, c, func(m *router.ShardMap) error {
		reps := replicasOf(m, shard)
		if !contains(reps, node) {
			setReplicas(m, shard, append(reps, node))
		}
		return nil
	})
	return err
}

// RemoveShardReplica removes node from shard's group and from the map.
func RemoveShardReplica(ctx context.Context, c Cluster, shard int32, node string, logf Logf) error {
	if err := RemoveReplica(ctx, c, shard, node, logf); err != nil {
		return err
	}
	_, err := MutateShardMap(ctx, c, func(m *router.ShardMap) error {
		setReplicas(m, shard, without(replicasOf(m, shard), node))
		return nil
	})
	return err
}

// MoveReplica moves one replica of shard from -> to: add first (the group
// temporarily has one extra voter), then remove, so the replication factor
// never drops below its target during the move.
func MoveReplica(ctx context.Context, c Cluster, shard int32, from, to string, logf Logf) error {
	if err := AddShardReplica(ctx, c, shard, to, logf); err != nil {
		return err
	}
	return RemoveShardReplica(ctx, c, shard, from, logf)
}

// hostedShards returns every shard (meta included) with a replica on node.
func hostedShards(m *router.ShardMap, node string) []int32 {
	var out []int32
	if contains(m.MetaReplicas, node) {
		out = append(out, metaShard)
	}
	return append(out, m.ShardsOn(node)...)
}

// leastLoaded picks the node with the fewest replicas that is not excluded.
func leastLoaded(m *router.ShardMap, exclude map[string]bool) (string, bool) {
	counts := m.ReplicaCounts()
	var ids []string
	for id := range counts {
		if !exclude[id] {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return "", false
	}
	sort.Slice(ids, func(i, j int) bool {
		if counts[ids[i]] != counts[ids[j]] {
			return counts[ids[i]] < counts[ids[j]]
		}
		return ids[i] < ids[j]
	})
	return ids[0], true
}

// RemoveNode decommissions a node: every replica it hosts is first
// re-created on the least-loaded other node (when one is available), then
// removed from the departing node; finally the node leaves the shard map.
func RemoveNode(ctx context.Context, c Cluster, node string, logf Logf) error {
	if logf == nil {
		logf = nopLog
	}
	m, err := c.ShardMap(ctx)
	if err != nil {
		return err
	}
	if _, ok := m.Node(node); !ok {
		return fmt.Errorf("membership: unknown node %q", node)
	}
	for _, shard := range hostedShards(m, node) {
		m, err = c.ShardMap(ctx)
		if err != nil {
			return err
		}
		exclude := map[string]bool{node: true}
		for _, r := range replicasOf(m, shard) {
			exclude[r] = true
		}
		if repl, ok := leastLoaded(m, exclude); ok {
			logf("shard %d: moving replica %s -> %s", shard, node, repl)
			if err := MoveReplica(ctx, c, shard, node, repl, logf); err != nil {
				return err
			}
			continue
		}
		if len(replicasOf(m, shard)) <= 1 {
			return fmt.Errorf("membership: %s holds the only replica of shard %d and no other node can take it", node, shard)
		}
		logf("shard %d: no spare node; shrinking the group", shard)
		if err := RemoveShardReplica(ctx, c, shard, node, logf); err != nil {
			return err
		}
	}
	_, err = MutateShardMap(ctx, c, func(m *router.ShardMap) error {
		var nodes []router.NodeInfo
		for _, n := range m.Nodes {
			if n.ID != node {
				nodes = append(nodes, n)
			}
		}
		m.Nodes = nodes
		return nil
	})
	if err == nil {
		logf("node %s removed from the cluster", node)
	}
	return err
}

// ReplaceNode replaces a failed node with a new one: for every shard the
// failed node hosted, Remove(failed) followed by Add(new) — a composition of
// the two primitives, reusing AddReplica's snapshot-streaming catch-up to
// populate the new node. Removing the failed node first means the group
// never counts a dead voter in a temporarily larger quorum.
func ReplaceNode(ctx context.Context, c Cluster, failed string, newNode router.NodeInfo, logf Logf) error {
	if logf == nil {
		logf = nopLog
	}
	m, err := RegisterNode(ctx, c, newNode)
	if err != nil {
		return err
	}
	for _, shard := range hostedShards(m, failed) {
		logf("shard %d: replacing %s with %s", shard, failed, newNode.ID)
		if err := RemoveShardReplica(ctx, c, shard, failed, logf); err != nil {
			return err
		}
		if err := AddShardReplica(ctx, c, shard, newNode.ID, logf); err != nil {
			return err
		}
	}
	_, err = MutateShardMap(ctx, c, func(m *router.ShardMap) error {
		var nodes []router.NodeInfo
		for _, n := range m.Nodes {
			if n.ID != failed {
				nodes = append(nodes, n)
			}
		}
		m.Nodes = nodes
		return nil
	})
	return err
}
