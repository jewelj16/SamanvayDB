package node

import (
	"context"
	"time"

	"samanvaydb/internal/kvserver"
	"samanvaydb/internal/shard"
)

// Node implements kvserver.Backend: each request is routed to the replica
// of the key's shard hosted here. If this node hosts no replica, or its
// replica is not the leader, the KV server answers NOT_LEADER (with the
// leader's ID and address when known) and the client follows the redirect.

var _ kvserver.Backend = (*Node)(nil)

func (n *Node) withDeadline(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, n.opts.DefaultRequestTimeout)
}

func (n *Node) route(key []byte) (int32, *shard.Shard, error) {
	sid := n.cachedMap().ShardFor(key)
	s, ok := n.manager.Get(sid)
	if !ok {
		return sid, nil, kvserver.ErrNotHosted
	}
	return sid, s, nil
}

// Put implements kvserver.Backend.
func (n *Node) Put(ctx context.Context, key, value []byte, ttl time.Duration) (int32, error) {
	sid, s, err := n.route(key)
	if err != nil {
		return sid, err
	}
	ctx, cancel := n.withDeadline(ctx)
	defer cancel()
	return sid, s.Put(ctx, key, value, ttl)
}

// Get implements kvserver.Backend.
func (n *Node) Get(ctx context.Context, key []byte, c shard.Consistency) (int32, []byte, bool, error) {
	sid, s, err := n.route(key)
	if err != nil {
		return sid, nil, false, err
	}
	ctx, cancel := n.withDeadline(ctx)
	defer cancel()
	v, found, err := s.Get(ctx, key, c)
	return sid, v, found, err
}

// Delete implements kvserver.Backend.
func (n *Node) Delete(ctx context.Context, key []byte) (int32, error) {
	sid, s, err := n.route(key)
	if err != nil {
		return sid, err
	}
	ctx, cancel := n.withDeadline(ctx)
	defer cancel()
	return sid, s.Delete(ctx, key)
}

// CAS implements kvserver.Backend.
func (n *Node) CAS(ctx context.Context, key, expected []byte, expectAbsent bool, newValue []byte) (int32, shard.Result, error) {
	sid, s, err := n.route(key)
	if err != nil {
		return sid, shard.Result{}, err
	}
	ctx, cancel := n.withDeadline(ctx)
	defer cancel()
	r, err := s.CAS(ctx, key, expected, expectAbsent, newValue)
	return sid, r, err
}

// LockAcquire implements kvserver.Backend.
func (n *Node) LockAcquire(ctx context.Context, name, owner string, ttl time.Duration) (int32, shard.Result, error) {
	sid, s, err := n.route(shard.LockKey(name))
	if err != nil {
		return sid, shard.Result{}, err
	}
	ctx, cancel := n.withDeadline(ctx)
	defer cancel()
	r, err := s.LockAcquire(ctx, name, owner, ttl)
	return sid, r, err
}

// LockRenew implements kvserver.Backend.
func (n *Node) LockRenew(ctx context.Context, name, owner string, ttl time.Duration) (int32, shard.Result, error) {
	sid, s, err := n.route(shard.LockKey(name))
	if err != nil {
		return sid, shard.Result{}, err
	}
	ctx, cancel := n.withDeadline(ctx)
	defer cancel()
	r, err := s.LockRenew(ctx, name, owner, ttl)
	return sid, r, err
}

// LockRelease implements kvserver.Backend.
func (n *Node) LockRelease(ctx context.Context, name, owner string) (int32, shard.Result, error) {
	sid, s, err := n.route(shard.LockKey(name))
	if err != nil {
		return sid, shard.Result{}, err
	}
	ctx, cancel := n.withDeadline(ctx)
	defer cancel()
	r, err := s.LockRelease(ctx, name, owner)
	return sid, r, err
}

// LockWait implements kvserver.Backend. It blocks up to the caller's
// deadline (no default timeout is imposed beyond the RPC's own).
func (n *Node) LockWait(ctx context.Context, name string) (int32, bool, error) {
	sid, s, err := n.route(shard.LockKey(name))
	if err != nil {
		return sid, false, err
	}
	ok, err := s.LockWait(ctx, name)
	return sid, ok, err
}

// AddrOf implements kvserver.Backend.
func (n *Node) AddrOf(id string) string {
	a, _ := n.book.Get(id)
	return a
}
