package router

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Outcome classifies the result of one attempt against one node.
type Outcome int

const (
	// Done: the request completed (successfully or with a final,
	// non-retryable answer such as CAS_FAILED or INVALID_REQUEST).
	Done Outcome = iota
	// NotLeader: the node is not the shard leader; Hint names the leader
	// if the node knows it.
	NotLeader
	// Unavailable: transient failure (node down, no quorum, timeout).
	// Retry elsewhere / with backoff.
	Unavailable
)

// Attempt is what an attempt function reports back to the router.
type Attempt struct {
	Outcome Outcome
	Hint    string // leader node ID, for NotLeader
	Err     error  // for Done: final error (nil on success); otherwise context
}

// ErrNoRoute means no replica could serve the request before the deadline.
var ErrNoRoute = errors.New("router: no replica could serve the request")

// Router routes requests for a key to the presumed leader of the key's shard
// and recovers transparently from stale leader hints:
//
//  1. compute shardId(key)
//  2. look up the shard -> {leader hint, replica set}
//  3. send to the presumed leader
//  4. on NOT_LEADER (with a hint): record the hint and retry there; on
//     NOT_LEADER without a hint or UNAVAILABLE: try the next replica,
//     refreshing the shard map and backing off between full rounds.
//
// It is safe for concurrent use.
type Router struct {
	mu      sync.RWMutex
	smap    *ShardMap
	hints   map[int32]string
	fetch   func(ctx context.Context) (*ShardMap, error)
	backoff time.Duration
}

// New creates a router. fetch retrieves the current shard map (from the
// admin API); it is called on construction failures and when routing
// repeatedly fails, so the router never trusts a stale cache for long.
func New(initial *ShardMap, fetch func(ctx context.Context) (*ShardMap, error)) *Router {
	return &Router{smap: initial, hints: map[int32]string{}, fetch: fetch, backoff: 50 * time.Millisecond}
}

// Map returns the current shard map.
func (r *Router) Map() *ShardMap {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.smap
}

// Refresh re-fetches the shard map; the newer version wins.
func (r *Router) Refresh(ctx context.Context) error {
	if r.fetch == nil {
		return nil
	}
	m, err := r.fetch(ctx)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.smap == nil || m.Version >= r.smap.Version {
		r.smap = m
	}
	return nil
}

// LeaderHint returns the cached leader hint for a shard.
func (r *Router) LeaderHint(shard int32) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.hints[shard]
}

// SetLeaderHint records a leader learned from a redirect or success.
func (r *Router) SetLeaderHint(shard int32, leader string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if leader == "" {
		delete(r.hints, shard)
	} else {
		r.hints[shard] = leader
	}
}

// candidates returns nodes to try for shard: hint first, then replicas.
func (r *Router) candidates(shard int32) []NodeInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.smap == nil {
		return nil
	}
	var ids []string
	if h := r.hints[shard]; h != "" {
		ids = append(ids, h)
	}
	if meta, ok := r.smap.Shard(shard); ok {
		ids = append(ids, meta.Replicas...)
		ids = append(ids, meta.TargetGroup...)
	} else if shard < 0 {
		ids = append(ids, r.smap.MetaReplicas...)
	}
	seen := map[string]bool{}
	var out []NodeInfo
	for _, id := range ids {
		if seen[id] {
			continue
		}
		seen[id] = true
		if n, ok := r.smap.Node(id); ok {
			out = append(out, n)
		}
	}
	return out
}

// ShardFor maps key to a shard under the current map.
func (r *Router) ShardFor(key []byte) int32 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.smap.ShardFor(key)
}

// Do routes one request for shard, calling attempt against candidate nodes
// until it reports Done or ctx expires. It returns the final error and the
// node that served the request.
func (r *Router) Do(ctx context.Context, shard int32, attempt func(ctx context.Context, n NodeInfo) Attempt) (NodeInfo, error) {
	var lastErr error
	backoff := r.backoff
	for round := 0; ; round++ {
		if r.Map() == nil {
			if err := r.Refresh(ctx); err != nil {
				lastErr = err
			}
		}
		tried := map[string]bool{}
		queue := r.candidates(shard)
		for len(queue) > 0 {
			if err := ctx.Err(); err != nil {
				return NodeInfo{}, fmt.Errorf("%w: %v (last error: %v)", ErrNoRoute, err, lastErr)
			}
			n := queue[0]
			queue = queue[1:]
			if tried[n.ID] {
				continue
			}
			tried[n.ID] = true
			a := attempt(ctx, n)
			switch a.Outcome {
			case Done:
				if a.Err == nil {
					r.SetLeaderHint(shard, n.ID)
				}
				return n, a.Err
			case NotLeader:
				lastErr = fmt.Errorf("%s: not leader (hint %q)", n.ID, a.Hint)
				if a.Hint != "" && a.Hint != n.ID {
					r.SetLeaderHint(shard, a.Hint)
					if hn, ok := r.Map().Node(a.Hint); ok && !tried[hn.ID] {
						queue = append([]NodeInfo{hn}, queue...) // follow the redirect next
					}
				}
			case Unavailable:
				lastErr = a.Err
				if r.LeaderHint(shard) == n.ID {
					r.SetLeaderHint(shard, "")
				}
			}
		}
		// A full round failed: back off, refresh topology, and retry.
		select {
		case <-ctx.Done():
			return NodeInfo{}, fmt.Errorf("%w: %v (last error: %v)", ErrNoRoute, ctx.Err(), lastErr)
		case <-time.After(backoff):
		}
		if backoff < time.Second {
			backoff *= 2
		}
		_ = r.Refresh(ctx)
	}
}
