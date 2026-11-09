package migration

import (
	"context"
	"fmt"
	"sort"
	"time"

	"samanvaydb/internal/membership"
	"samanvaydb/internal/router"
)

// transfer.go + cutover.go of the plan: the end-to-end migration driver.

// Run migrates shard to the target node set, driving the state machine and
// recording progress in the shard map (State=MIGRATING, Source/Target groups,
// MigrationPhase) so `kvctl cluster status` shows it live.
func Run(ctx context.Context, c membership.Cluster, shard int32, targets []string, logf membership.Logf) (*Migration, error) {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	if len(targets) == 0 {
		return nil, fmt.Errorf("migration: no target nodes")
	}
	m0, err := c.ShardMap(ctx)
	if err != nil {
		return nil, err
	}
	meta, ok := m0.Shard(shard)
	if !ok {
		return nil, fmt.Errorf("migration: unknown shard %d", shard)
	}
	if meta.State != router.StateStable {
		return nil, fmt.Errorf("migration: shard %d is already %s", shard, meta.State)
	}
	for _, t := range targets {
		if _, ok := m0.Node(t); !ok {
			return nil, fmt.Errorf("migration: unknown target node %q", t)
		}
	}
	targets = sortedUnique(targets)
	source := append([]string(nil), meta.Replicas...)
	var added []string // targets that were not already members
	for _, t := range targets {
		if !contains(source, t) {
			added = append(added, t)
		}
	}

	mig := New(shard, source, targets, func(t Transition) {
		logf("shard %d: %s -> %s", shard, t.From, t.To)
	})
	setPhase := func(st State) error {
		_, err := membership.MutateShardMap(ctx, c, func(m *router.ShardMap) error {
			s := &m.Shards[shard]
			s.State = router.StateMigrating
			s.SourceGroup, s.TargetGroup = source, targets
			s.MigrationPhase = string(st)
			return nil
		})
		return err
	}
	abort := func(cause error) (*Migration, error) {
		logf("shard %d: aborting migration: %v", shard, cause)
		actx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		for _, n := range added { // discard the half-built target replicas
			_ = membership.RemoveReplica(actx, c, shard, n, logf)
		}
		_, _ = membership.MutateShardMap(actx, c, func(m *router.ShardMap) error {
			s := &m.Shards[shard]
			s.State, s.SourceGroup, s.TargetGroup, s.MigrationPhase = router.StateStable, nil, nil, ""
			s.Replicas = source // the untouched, still-authoritative source
			return nil
		})
		_ = mig.To(Aborted)
		return mig, fmt.Errorf("migration of shard %d aborted: %w", shard, cause)
	}

	// PENDING -> SNAPSHOTTING: capture a point-in-time snapshot on the
	// leader so the new replicas bootstrap from it.
	if err := setPhase(Pending); err != nil {
		return mig, err
	}
	if err := mig.To(Snapshotting); err != nil {
		return mig, err
	}
	setPhase(Snapshotting)
	if err := c.ChangeMembership(ctx, shard, "snapshot", "", ""); err != nil {
		return abort(fmt.Errorf("snapshot: %w", err))
	}

	// SNAPSHOTTING -> TRANSFERRING: start empty target replicas and add them
	// as learners; the leader ships them the snapshot via InstallSnapshot.
	if err := mig.To(Transferring); err != nil {
		return mig, err
	}
	setPhase(Transferring)
	for _, n := range added {
		info, _ := m0.Node(n)
		if err := c.CreateReplica(ctx, n, shard); err != nil {
			return abort(fmt.Errorf("create replica on %s: %w", n, err))
		}
		if err := c.ChangeMembership(ctx, shard, "add_learner", n, info.Addr); err != nil {
			if p, perr := c.Progress(ctx, shard); perr != nil || !(p.IsLearner(n) || p.IsVoter(n)) {
				return abort(fmt.Errorf("add learner %s: %w", n, err))
			}
		}
		logf("shard %d: %s joined as learner", shard, n)
	}

	// TRANSFERRING -> CATCHING_UP: source keeps accepting writes; targets
	// tail the log until their lag is small.
	if err := mig.To(CatchingUp); err != nil {
		return mig, err
	}
	setPhase(CatchingUp)
	for _, n := range added {
		if err := membership.WaitCaughtUp(ctx, c, shard, n); err != nil {
			return abort(fmt.Errorf("catch-up of %s: %w", n, err))
		}
		logf("shard %d: %s caught up", shard, n)
	}

	// CATCHING_UP -> CUTOVER: past this point we roll FORWARD on errors.
	if err := mig.To(Cutover); err != nil {
		return mig, err
	}
	setPhase(Cutover)
	cctx := context.Background() // cutover must complete even if the caller gives up
	for _, n := range added {
		if err := promote(cctx, c, shard, n); err != nil {
			return mig, fmt.Errorf("cutover: promote %s: %w", n, err)
		}
		logf("shard %d: %s promoted to voter", shard, n)
	}
	var removed []string
	for _, n := range source {
		if !contains(targets, n) {
			removed = append(removed, n)
		}
	}
	// Remove the current leader last, so at most one election happens.
	if p, err := c.Progress(cctx, shard); err == nil {
		sort.SliceStable(removed, func(i, j int) bool { return removed[j] == p.Leader })
	}
	for _, n := range removed {
		if err := membership.RemoveReplica(cctx, c, shard, n, logf); err != nil {
			return mig, fmt.Errorf("cutover: remove %s: %w", n, err)
		}
	}
	if _, err := membership.MutateShardMap(cctx, c, func(m *router.ShardMap) error {
		s := &m.Shards[shard]
		s.Replicas = targets
		s.State, s.SourceGroup, s.TargetGroup, s.MigrationPhase = router.StateStable, nil, nil, ""
		return nil
	}); err != nil {
		return mig, fmt.Errorf("cutover: update shard map: %w", err)
	}
	if err := mig.To(Done); err != nil {
		return mig, err
	}
	return mig, nil
}

func promote(ctx context.Context, c membership.Cluster, shard int32, n string) error {
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	for {
		if p, err := c.Progress(ctx, shard); err == nil && p.IsVoter(n) {
			return nil
		}
		err := c.ChangeMembership(ctx, shard, "promote", n, "")
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func contains(list []string, x string) bool {
	for _, v := range list {
		if v == x {
			return true
		}
	}
	return false
}

func sortedUnique(in []string) []string {
	set := map[string]bool{}
	var out []string
	for _, s := range in {
		if s != "" && !set[s] {
			set[s] = true
			out = append(out, s)
		}
	}
	sort.Strings(out)
	return out
}
