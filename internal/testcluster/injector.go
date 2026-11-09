package testcluster

import (
	"fmt"
	"time"

	"samanvaydb/internal/faultinjector"
)

// Injector returns a FaultInjector backed by this in-process cluster:
// KillNode stops a node abruptly, RestartNode recovers it from disk, and
// network faults are applied to every live node's transport rules.
func (c *Cluster) Injector() faultinjector.FaultInjector { return &inproc{c: c} }

// LeaderLookup returns a leader lookup over this cluster.
func (c *Cluster) LeaderLookup() faultinjector.LeaderLookup {
	return func(shard int32) (string, error) {
		if l, _, ok := c.ShardLeader(shard); ok {
			return l, nil
		}
		return "", fmt.Errorf("shard %d has no leader", shard)
	}
}

type inproc struct{ c *Cluster }

func (i *inproc) KillNode(id string) error {
	if i.c.Node(id) == nil {
		return fmt.Errorf("%s is not running", id)
	}
	i.c.Kill(id)
	return nil
}

func (i *inproc) RestartNode(id string) error { return i.c.Restart(id) }

func (i *inproc) CreatePartition(a, b []string) error {
	i.c.Partition(a, b)
	return nil
}

func (i *inproc) HealPartition() error {
	i.c.Heal()
	return nil
}

func (i *inproc) DelayMessages(from, to string, d time.Duration) error {
	n := i.c.Node(from)
	if n == nil {
		return fmt.Errorf("%s is not running", from)
	}
	n.Faults().Delay(from, to, d)
	return nil
}

func (i *inproc) DropMessages(from, to string, rate float64) error {
	n := i.c.Node(from)
	if n == nil {
		return fmt.Errorf("%s is not running", from)
	}
	n.Faults().Drop(from, to, rate)
	return nil
}

func (i *inproc) SlowNode(id string, d time.Duration) error {
	n := i.c.Node(id)
	if n == nil {
		return fmt.Errorf("%s is not running", id)
	}
	n.Faults().Slow(d)
	return nil
}
