package migration

import (
	"context"
	"sort"

	"samanvaydb/internal/membership"
	"samanvaydb/internal/router"
)

// Move relocates one replica of one shard.
type Move struct {
	Shard    int32
	From, To string
}

// Plan computes a greedy, count-based rebalancing plan: repeatedly move one
// replica from the most-loaded node to the least-loaded node (choosing a
// shard the least-loaded node does not already host) until the max-min
// spread of replica counts is at most 1.
//
// This is a legitimate first version. Load-aware rebalancing (by CPU, disk
// or QPS rather than replica count) is documented future work.
func Plan(m *router.ShardMap) []Move {
	sim := m.Clone()
	var moves []Move
	for iter := 0; iter < 1000; iter++ {
		counts := sim.ReplicaCounts()
		ids := make([]string, 0, len(counts))
		for id := range counts {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool {
			if counts[ids[i]] != counts[ids[j]] {
				return counts[ids[i]] > counts[ids[j]]
			}
			return ids[i] < ids[j]
		})
		if len(ids) < 2 {
			return moves
		}
		min := ids[len(ids)-1]
		moved := false
		for _, max := range ids[:len(ids)-1] {
			if counts[max]-counts[min] <= 1 {
				break
			}
			for i := range sim.Shards {
				s := &sim.Shards[i]
				if contains(s.Replicas, max) && !contains(s.Replicas, min) && s.State == router.StateStable {
					s.Replicas = append(without(s.Replicas, max), min)
					sort.Strings(s.Replicas)
					moves = append(moves, Move{Shard: s.ShardID, From: max, To: min})
					moved = true
					break
				}
			}
			if moved {
				break
			}
		}
		if !moved {
			return moves
		}
	}
	return moves
}

// Spread returns max-min replica count across nodes.
func Spread(m *router.ShardMap) int {
	first := true
	var lo, hi int
	for _, c := range m.ReplicaCounts() {
		if first {
			lo, hi, first = c, c, false
		}
		if c < lo {
			lo = c
		}
		if c > hi {
			hi = c
		}
	}
	return hi - lo
}

// Rebalance computes a plan from the live shard map and executes each move
// (add on the destination, then remove from the source).
func Rebalance(ctx context.Context, c membership.Cluster, logf membership.Logf) ([]Move, error) {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	m, err := c.ShardMap(ctx)
	if err != nil {
		return nil, err
	}
	plan := Plan(m)
	logf("rebalance: %d move(s) planned (spread %d)", len(plan), Spread(m))
	for _, mv := range plan {
		logf("rebalance: shard %d %s -> %s", mv.Shard, mv.From, mv.To)
		if err := membership.MoveReplica(ctx, c, mv.Shard, mv.From, mv.To, logf); err != nil {
			return plan, err
		}
	}
	return plan, nil
}

func without(list []string, x string) []string {
	var out []string
	for _, v := range list {
		if v != x {
			out = append(out, v)
		}
	}
	return out
}
