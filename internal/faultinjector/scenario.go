package faultinjector

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// Reproducibility: every fault sequence used for testing or demos is a
// checked-in program (here, or a script in scripts/), never hand-typed.
// Wherever a scenario randomly picks a target (which node to kill, when),
// it draws from a Scenario seeded explicitly — and the seed is logged — so a
// failing run can be replayed exactly by re-running with the same seed.

// Scenario is a seeded source of fault decisions.
type Scenario struct {
	Seed int64
	rng  *rand.Rand
	mu   sync.Mutex
}

// NewScenario creates a scenario. seed 0 picks a time-based seed (which is
// then reported by Seed so the run can be reproduced).
func NewScenario(seed int64) *Scenario {
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	return &Scenario{Seed: seed, rng: rand.New(rand.NewSource(seed))}
}

// Pick returns one element of candidates, chosen by the seeded RNG. The
// candidates are sorted first so map-iteration order cannot leak in.
func (s *Scenario) Pick(candidates []string) string {
	c := append([]string(nil), candidates...)
	sort.Strings(c)
	s.mu.Lock()
	defer s.mu.Unlock()
	return c[s.rng.Intn(len(c))]
}

// Intn returns a seeded int in [0,n).
func (s *Scenario) Intn(n int) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rng.Intn(n)
}

// Duration returns a seeded duration in [min, max).
func (s *Scenario) Duration(min, max time.Duration) time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return min + time.Duration(s.rng.Int63n(int64(max-min)))
}

// Recorder wraps a FaultInjector and records every injected fault, so a
// run's exact fault sequence can be logged, compared, and replayed.
type Recorder struct {
	Inner FaultInjector
	mu    sync.Mutex
	log   []string
}

func (r *Recorder) rec(format string, args ...any) {
	r.mu.Lock()
	r.log = append(r.log, fmt.Sprintf(format, args...))
	r.mu.Unlock()
}

// Log returns the recorded fault sequence.
func (r *Recorder) Log() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.log...)
}

func (r *Recorder) KillNode(id string) error {
	r.rec("kill %s", id)
	return r.Inner.KillNode(id)
}

func (r *Recorder) RestartNode(id string) error {
	r.rec("restart %s", id)
	return r.Inner.RestartNode(id)
}

func (r *Recorder) CreatePartition(a, b []string) error {
	r.rec("partition %v | %v", a, b)
	return r.Inner.CreatePartition(a, b)
}

func (r *Recorder) HealPartition() error {
	r.rec("heal")
	return r.Inner.HealPartition()
}

func (r *Recorder) DelayMessages(from, to string, d time.Duration) error {
	r.rec("delay %s->%s %v", from, to, d)
	return r.Inner.DelayMessages(from, to, d)
}

func (r *Recorder) DropMessages(from, to string, rate float64) error {
	r.rec("drop %s->%s %.2f", from, to, rate)
	return r.Inner.DropMessages(from, to, rate)
}

// Nop is a FaultInjector that does nothing (dry runs, recorder tests).
type Nop struct{}

func (Nop) KillNode(string) error                             { return nil }
func (Nop) RestartNode(string) error                          { return nil }
func (Nop) CreatePartition([]string, []string) error          { return nil }
func (Nop) HealPartition() error                              { return nil }
func (Nop) DelayMessages(string, string, time.Duration) error { return nil }
func (Nop) DropMessages(string, string, float64) error        { return nil }

// ChaosRound is a canned seeded scenario used by tests and scripts: kill a
// random node, partition a random minority, heal, restart. It returns after
// injecting the full sequence (callers interleave workload and checks).
func ChaosRound(fi FaultInjector, s *Scenario, nodes []string, pause func()) error {
	victim := s.Pick(nodes)
	if err := fi.KillNode(victim); err != nil {
		return err
	}
	pause()
	if err := fi.RestartNode(victim); err != nil {
		return err
	}
	pause()
	minority := s.Pick(nodes)
	var rest []string
	for _, n := range nodes {
		if n != minority {
			rest = append(rest, n)
		}
	}
	if err := fi.CreatePartition([]string{minority}, rest); err != nil {
		return err
	}
	pause()
	return fi.HealPartition()
}
