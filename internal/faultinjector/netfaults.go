package faultinjector

import (
	"errors"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// ErrInjected is returned for a message the fault injector suppressed.
var ErrInjected = errors.New("faultinjector: message suppressed by injected network fault")

// Rules is the per-node network fault table, enforced by internal/transport
// on BOTH the send path (from this node) and the receive path (to this node,
// using the sender ID carried in every RPC). Enforcing on both sides makes a
// partition hold even if only one side's rules were updated.
//
// This is the "proxy layer" implementation of Phase 9's network faults: it
// works identically on Linux, macOS and Windows and needs no root/iptables.
type Rules struct {
	mu      sync.RWMutex
	self    string
	blocked map[[2]string]bool
	delay   map[[2]string]time.Duration
	drop    map[[2]string]float64
	slow    time.Duration
	rng     *rand.Rand
}

// NewRules returns an empty rule table for node self.
func NewRules(self string, seed int64) *Rules {
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	return &Rules{
		self:    self,
		blocked: map[[2]string]bool{},
		delay:   map[[2]string]time.Duration{},
		drop:    map[[2]string]float64{},
		rng:     rand.New(rand.NewSource(seed)),
	}
}

// Partition blocks traffic between every node in a and every node in b.
func (r *Rules) Partition(a, b []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, x := range a {
		for _, y := range b {
			r.blocked[[2]string{x, y}] = true
			r.blocked[[2]string{y, x}] = true
		}
	}
}

// HealPartition removes all partitions.
func (r *Rules) HealPartition() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.blocked = map[[2]string]bool{}
}

// Delay holds messages from->to for d ("*" matches any node).
func (r *Rules) Delay(from, to string, d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.delay[[2]string{from, to}] = d
}

// Drop discards messages from->to with probability rate ("*" = any).
func (r *Rules) Drop(from, to string, rate float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.drop[[2]string{from, to}] = rate
}

// Slow injects latency into every request this node handles (the guide's
// "slow node" fault: an artificial sleep in the request path).
func (r *Rules) Slow(d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.slow = d
}

// Clear removes every fault.
func (r *Rules) Clear() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.blocked = map[[2]string]bool{}
	r.delay = map[[2]string]time.Duration{}
	r.drop = map[[2]string]float64{}
	r.slow = 0
}

func lookup[T any](m map[[2]string]T, from, to string) (T, bool) {
	for _, k := range [][2]string{{from, to}, {from, "*"}, {"*", to}} {
		if v, ok := m[k]; ok {
			return v, true
		}
	}
	var zero T
	return zero, false
}

// Check decides the fate of one message from->to: an error if it must be
// suppressed, otherwise how long to delay it.
func (r *Rules) Check(from, to string) (time.Duration, error) {
	r.mu.RLock()
	blocked := r.blocked[[2]string{from, to}]
	d, _ := lookup(r.delay, from, to)
	rate, _ := lookup(r.drop, from, to)
	r.mu.RUnlock()
	if blocked {
		return 0, ErrInjected
	}
	if rate > 0 {
		r.mu.Lock()
		hit := r.rng.Float64() < rate
		r.mu.Unlock()
		if hit {
			return 0, ErrInjected
		}
	}
	return d, nil
}

// Apply enforces Check, sleeping for any injected delay.
func (r *Rules) Apply(from, to string) error {
	d, err := r.Check(from, to)
	if err != nil {
		return err
	}
	if d > 0 {
		time.Sleep(d)
	}
	return nil
}

// SlowDelay returns the injected per-request latency for this node.
func (r *Rules) SlowDelay() time.Duration {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.slow
}

// Snapshot describes the active faults (for status output).
type Snapshot struct {
	Partitions []string           `json:"partitions,omitempty"`
	Delays     map[string]string  `json:"delays,omitempty"`
	Drops      map[string]float64 `json:"drops,omitempty"`
	Slow       string             `json:"slow,omitempty"`
}

// Describe returns the active faults.
func (r *Rules) Describe() Snapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s := Snapshot{Delays: map[string]string{}, Drops: map[string]float64{}}
	for k := range r.blocked {
		if k[0] < k[1] {
			s.Partitions = append(s.Partitions, k[0]+"<->"+k[1])
		}
	}
	sort.Strings(s.Partitions)
	for k, v := range r.delay {
		s.Delays[k[0]+"->"+k[1]] = v.String()
	}
	for k, v := range r.drop {
		s.Drops[k[0]+"->"+k[1]] = v
	}
	if r.slow > 0 {
		s.Slow = r.slow.String()
	}
	return s
}
