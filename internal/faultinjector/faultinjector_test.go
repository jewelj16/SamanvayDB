package faultinjector

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestRules_PartitionIsSymmetricUntilHealed(t *testing.T) {
	r := NewRules("a", 1)
	r.Partition([]string{"a"}, []string{"b", "c"})
	for _, l := range [][2]string{{"a", "b"}, {"b", "a"}, {"a", "c"}, {"c", "a"}} {
		if _, err := r.Check(l[0], l[1]); !errors.Is(err, ErrInjected) {
			t.Fatalf("%s->%s not blocked", l[0], l[1])
		}
	}
	if _, err := r.Check("b", "c"); err != nil {
		t.Fatal("same-side traffic blocked")
	}
	r.HealPartition()
	if _, err := r.Check("a", "b"); err != nil {
		t.Fatal("still blocked after heal")
	}
}

func TestRules_DelayedMessageArrivesAfterDelay(t *testing.T) {
	r := NewRules("a", 1)
	r.Delay("a", "b", 60*time.Millisecond)
	start := time.Now()
	if err := r.Apply("a", "b"); err != nil {
		t.Fatal(err)
	}
	if el := time.Since(start); el < 60*time.Millisecond {
		t.Fatalf("delivered after %v, want >= 60ms", el)
	}
	if d, _ := r.Check("a", "c"); d != 0 {
		t.Fatal("delay leaked to another link")
	}
}

func TestRules_DropRateIsStatistical(t *testing.T) {
	r := NewRules("a", 42)
	r.Drop("a", "*", 0.25)
	dropped := 0
	const n = 8000
	for i := 0; i < n; i++ {
		if _, err := r.Check("a", "b"); err != nil {
			dropped++
		}
	}
	if rate := float64(dropped) / n; rate < 0.22 || rate > 0.28 {
		t.Fatalf("drop rate %.3f, want ~0.25", rate)
	}
}

func TestRules_SlowAndClear(t *testing.T) {
	r := NewRules("a", 1)
	r.Slow(25 * time.Millisecond)
	r.Partition([]string{"a"}, []string{"b"})
	if r.SlowDelay() != 25*time.Millisecond || len(r.Describe().Partitions) != 1 {
		t.Fatalf("describe: %+v", r.Describe())
	}
	r.Clear()
	if r.SlowDelay() != 0 || len(r.Describe().Partitions) != 0 {
		t.Fatal("Clear left faults behind")
	}
}

// TestScenario_SameSeedSameFaults — Commit 59: running the same seeded
// scenario twice produces an identical sequence of injected faults.
func TestScenario_SameSeedSameFaults(t *testing.T) {
	nodes := []string{"node-3", "node-1", "node-5", "node-2", "node-4"}
	run := func(seed int64) []string {
		rec := &Recorder{Inner: Nop{}}
		s := NewScenario(seed)
		for i := 0; i < 5; i++ {
			if err := ChaosRound(rec, s, nodes, func() {}); err != nil {
				t.Fatal(err)
			}
			rec.DelayMessages(s.Pick(nodes), s.Pick(nodes), s.Duration(10*time.Millisecond, 50*time.Millisecond))
		}
		return rec.Log()
	}
	a, b := run(1234), run(1234)
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("same seed, different faults:\n%v\n%v", a, b)
	}
	if reflect.DeepEqual(a, run(999)) {
		t.Fatal("different seeds produced identical sequences (suspicious)")
	}
	if len(a) != 25 {
		t.Fatalf("recorded %d faults, want 25", len(a))
	}
}
