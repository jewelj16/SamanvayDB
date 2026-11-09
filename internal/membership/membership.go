// Package membership implements node add/remove/replace for running Raft
// groups, using single-server configuration changes (see
// internal/raft/membership.go for the safety argument).
//
// Why not just edit a peer list in a config file and restart? Because that
// gives no ordering guarantee about WHEN each node adopts the new peer list:
// for a window, some nodes count majorities over the old set and some over
// the new one, and two disjoint majorities (two leaders in one term) become
// possible. Routing every change through the replicated log orders it with
// respect to every other entry, on every replica.
package membership

import (
	"context"
	"fmt"
	"sort"
	"time"

	"samanvaydb/internal/router"
)

// Progress is a shard leader's view of its group.
type Progress struct {
	Leader    string
	LastIndex uint64
	Match     map[string]uint64
	Voters    []string
	Learners  []string
}

func (p Progress) has(list []string, n string) bool {
	for _, x := range list {
		if x == n {
			return true
		}
	}
	return false
}

// IsVoter reports whether n is a voter.
func (p Progress) IsVoter(n string) bool { return p.has(p.Voters, n) }

// IsLearner reports whether n is a learner.
func (p Progress) IsLearner(n string) bool { return p.has(p.Learners, n) }

// Cluster is the control surface the orchestration code drives. The node
// implements it over the Admin gRPC API; tests implement it in memory.
type Cluster interface {
	ShardMap(ctx context.Context) (*router.ShardMap, error)
	UpdateShardMap(ctx context.Context, expectedVersion uint64, m *router.ShardMap) error
	CreateReplica(ctx context.Context, node string, shard int32) error
	DestroyReplica(ctx context.Context, node string, shard int32) error
	// ChangeMembership runs op (add_learner|promote|remove|snapshot) on the
	// shard's leader and returns once the change has committed.
	ChangeMembership(ctx context.Context, shard int32, op, node, addr string) error
	Progress(ctx context.Context, shard int32) (Progress, error)
}

// Logf receives human-readable progress lines.
type Logf func(format string, args ...any)

func nopLog(string, ...any) {}

// CatchUpThreshold is how close (in entries) a learner must be to the
// leader's last index before it is promoted to voter.
const CatchUpThreshold = 32

// retry runs fn until it succeeds or ctx ends, backing off between tries.
// Membership RPCs fail transiently during elections or while a previous
// change is still committing.
func retry(ctx context.Context, fn func() error) error {
	backoff := 50 * time.Millisecond
	for {
		err := fn()
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w (last error: %v)", ctx.Err(), err)
		case <-time.After(backoff):
		}
		if backoff < time.Second {
			backoff *= 2
		}
	}
}

// change applies op idempotently: done() is checked first (and after any
// error), so re-running an interrupted operation is always safe.
func change(ctx context.Context, c Cluster, shard int32, op, node, addr string, done func(Progress) bool) error {
	return retry(ctx, func() error {
		if p, err := c.Progress(ctx, shard); err == nil && done(p) {
			return nil
		}
		return c.ChangeMembership(ctx, shard, op, node, addr)
	})
}

// WaitCaughtUp blocks until node's replica of shard is within
// CatchUpThreshold entries of the leader.
func WaitCaughtUp(ctx context.Context, c Cluster, shard int32, node string) error {
	return retry(ctx, func() error {
		p, err := c.Progress(ctx, shard)
		if err != nil {
			return err
		}
		m, ok := p.Match[node]
		if !ok {
			return fmt.Errorf("shard %d: leader has no progress for %s yet", shard, node)
		}
		if p.LastIndex > m+CatchUpThreshold {
			return fmt.Errorf("shard %d: %s lagging by %d entries", shard, node, p.LastIndex-m)
		}
		return nil
	})
}

// AddReplica brings node into shard's Raft group safely:
//
//  1. start an EMPTY replica on node (it never campaigns: no config yet)
//  2. add it as a non-voting LEARNER (does not count toward quorum)
//  3. the leader streams it a snapshot + subsequent entries
//  4. once caught up, promote it to voter via a second config change
//
// Each step is idempotent, so an interrupted AddReplica can be re-run.
func AddReplica(ctx context.Context, c Cluster, shard int32, node string, logf Logf) error {
	if logf == nil {
		logf = nopLog
	}
	m, err := c.ShardMap(ctx)
	if err != nil {
		return err
	}
	info, ok := m.Node(node)
	if !ok {
		return fmt.Errorf("membership: unknown node %q (register it first)", node)
	}
	if err := retry(ctx, func() error { return c.CreateReplica(ctx, node, shard) }); err != nil {
		return fmt.Errorf("create replica of shard %d on %s: %w", shard, node, err)
	}
	logf("shard %d: %s added as LEARNER", shard, node)
	if err := change(ctx, c, shard, "add_learner", node, info.Addr, func(p Progress) bool {
		return p.IsLearner(node) || p.IsVoter(node)
	}); err != nil {
		return fmt.Errorf("add learner %s to shard %d: %w", node, shard, err)
	}
	if err := WaitCaughtUp(ctx, c, shard, node); err != nil {
		return fmt.Errorf("learner %s catch-up on shard %d: %w", node, shard, err)
	}
	if err := change(ctx, c, shard, "promote", node, "", func(p Progress) bool { return p.IsVoter(node) }); err != nil {
		return fmt.Errorf("promote %s in shard %d: %w", node, shard, err)
	}
	logf("shard %d: %s caught up and promoted to VOTER", shard, node)
	return nil
}

// RemoveReplica removes node from shard's group via a config-change entry,
// then deletes its replica. If node was the leader it steps down once the
// change commits and the remaining voters elect a new leader. Destroying
// the replica is best-effort (the node may be dead — that is often why it
// is being removed).
func RemoveReplica(ctx context.Context, c Cluster, shard int32, node string, logf Logf) error {
	if logf == nil {
		logf = nopLog
	}
	if err := change(ctx, c, shard, "remove", node, "", func(p Progress) bool {
		return p.Leader != "" && p.Leader != node && !p.IsVoter(node) && !p.IsLearner(node)
	}); err != nil {
		return fmt.Errorf("remove %s from shard %d: %w", node, shard, err)
	}
	logf("shard %d: %s removed from the voting set", shard, node)
	dctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := c.DestroyReplica(dctx, node, shard); err != nil {
		logf("shard %d: could not destroy replica on %s (%v) — it is no longer a member; clean it up later", shard, node, err)
	}
	return nil
}

// MutateShardMap applies fn to the current shard map and stores the result
// with compare-and-swap on the version, retrying on concurrent updates and
// transient failures. Errors from fn or validation are returned at once.
func MutateShardMap(ctx context.Context, c Cluster, fn func(m *router.ShardMap) error) (*router.ShardMap, error) {
	backoff := 50 * time.Millisecond
	for {
		cur, err := c.ShardMap(ctx)
		if err == nil {
			next := cur.Clone()
			if ferr := fn(next); ferr != nil {
				return nil, ferr
			}
			next.Version = cur.Version + 1
			if verr := next.Validate(); verr != nil {
				return nil, verr
			}
			if err = c.UpdateShardMap(ctx, cur.Version, next); err == nil {
				return next, nil
			}
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("update shard map: %w (last error: %v)", ctx.Err(), err)
		case <-time.After(backoff):
		}
		if backoff < time.Second {
			backoff *= 2
		}
	}
}

// replicasOf returns a shard's replica list from the map (meta included).
func replicasOf(m *router.ShardMap, shard int32) []string {
	if shard == metaShard {
		return m.MetaReplicas
	}
	if s, ok := m.Shard(shard); ok {
		return s.Replicas
	}
	return nil
}

func setReplicas(m *router.ShardMap, shard int32, reps []string) {
	reps = append([]string(nil), reps...)
	sort.Strings(reps)
	if shard == metaShard {
		m.MetaReplicas = reps
		return
	}
	m.Shards[shard].Replicas = reps
}

const metaShard int32 = -1

func without(list []string, x string) []string {
	var out []string
	for _, v := range list {
		if v != x {
			out = append(out, v)
		}
	}
	return out
}

func contains(list []string, x string) bool {
	for _, v := range list {
		if v == x {
			return true
		}
	}
	return false
}
