package migration

import (
	"fmt"
	"testing"

	"samanvaydb/internal/router"
)

func TestStateMachine_LegalPathOnly(t *testing.T) {
	m := New(0, []string{"a"}, []string{"b"}, nil)
	path := []State{Snapshotting, Transferring, CatchingUp, Cutover, Done}
	for _, s := range path {
		if err := m.To(s); err != nil {
			t.Fatalf("legal transition to %s rejected: %v", s, err)
		}
	}
	if len(m.History()) != len(path) {
		t.Fatalf("history has %d entries", len(m.History()))
	}
	for _, s := range []State{Pending, Aborted, Cutover} {
		if err := m.To(s); err == nil {
			t.Fatalf("transition out of DONE to %s accepted", s)
		}
	}
}

func TestStateMachine_IllegalSkipsRejected(t *testing.T) {
	tests := []struct{ from, to State }{
		{Pending, Transferring},
		{Pending, Cutover},
		{Snapshotting, CatchingUp},
		{Transferring, Done},
		{CatchingUp, Done},
	}
	for _, tt := range tests {
		m := New(0, nil, nil, nil)
		for _, s := range []State{Snapshotting, Transferring, CatchingUp} {
			if m.State() == tt.from {
				break
			}
			m.To(s)
		}
		if m.State() != tt.from {
			t.Fatalf("setup: at %s, want %s", m.State(), tt.from)
		}
		if err := m.To(tt.to); err == nil {
			t.Errorf("%s -> %s accepted", tt.from, tt.to)
		}
	}
}

// TestStateMachine_AbortFromEveryPreCutoverState verifies ABORTED is
// reachable from every stage before CUTOVER and never after.
func TestStateMachine_AbortFromEveryPreCutoverState(t *testing.T) {
	order := []State{Pending, Snapshotting, Transferring, CatchingUp, Cutover}
	for i, at := range order {
		m := New(0, nil, nil, nil)
		for _, s := range order[1 : i+1] {
			if err := m.To(s); err != nil {
				t.Fatal(err)
			}
		}
		err := m.To(Aborted)
		if at == Cutover {
			if err == nil || Abortable(at) {
				t.Fatal("abort after CUTOVER must be rejected")
			}
			continue
		}
		if err != nil || !Abortable(at) {
			t.Fatalf("abort from %s rejected: %v", at, err)
		}
	}
}

func mapWith(nodes int, shards int32, rf int) *router.ShardMap {
	var ns []router.NodeInfo
	for i := 1; i <= nodes; i++ {
		ns = append(ns, router.NodeInfo{ID: fmt.Sprintf("n%d", i), Addr: fmt.Sprintf("a:%d", i)})
	}
	m, err := router.InitialShardMap(ns, shards, rf, nil, nil)
	if err != nil {
		panic(err)
	}
	return m
}

// TestPlan_AddingNodeReducesSpread: adding an empty node to a loaded
// cluster yields moves that reduce the max-min replica spread to <= 1.
func TestPlan_AddingNodeReducesSpread(t *testing.T) {
	m := mapWith(3, 6, 3) // every node hosts 6 replicas
	m.Nodes = append(m.Nodes, router.NodeInfo{ID: "n4", Addr: "a:4"})
	before := Spread(m)
	plan := Plan(m)
	if len(plan) == 0 {
		t.Fatal("no moves planned for an unbalanced cluster")
	}
	sim := m.Clone()
	for _, mv := range plan {
		s := &sim.Shards[mv.Shard]
		if !contains(s.Replicas, mv.From) || contains(s.Replicas, mv.To) {
			t.Fatalf("invalid move %+v for replicas %v", mv, s.Replicas)
		}
		s.Replicas = append(without(s.Replicas, mv.From), mv.To)
		if len(s.Replicas) != 3 {
			t.Fatalf("move changed replication factor: %v", s.Replicas)
		}
	}
	after := Spread(sim)
	if before != 6 || after > 1 {
		t.Fatalf("spread before=%d after=%d (want 6 -> <=1)", before, after)
	}
	if got := sim.ReplicaCounts()["n4"]; got < 4 {
		t.Fatalf("new node received only %d replicas", got)
	}
}

func TestPlan_BalancedClusterNoMoves(t *testing.T) {
	if plan := Plan(mapWith(9, 3, 3)); len(plan) != 0 {
		t.Fatalf("balanced cluster produced moves: %+v", plan)
	}
}

func TestPlan_SkipsMigratingShards(t *testing.T) {
	m := mapWith(3, 1, 3)
	m.Nodes = append(m.Nodes, router.NodeInfo{ID: "n4", Addr: "a:4"})
	m.Shards[0].State = router.StateMigrating
	if plan := Plan(m); len(plan) != 0 {
		t.Fatalf("planned moves for a migrating shard: %+v", plan)
	}
}
