package client

import (
	"context"
	"errors"
	"sync"
	"time"

	"samanvaydb/proto/kvpb"
)

// LockInfo is the outcome of a low-level lock call.
type LockInfo struct {
	FencingToken uint64
	ExpiresAtMs  uint64
	Holder       string
}

func lockShardKey(name string) []byte { return []byte("__lock__:" + name) }

func (c *Client) lockCall(ctx context.Context, name string, req *kvpb.LockRequest,
	call func(ctx context.Context, kv kvpb.KVClient, req *kvpb.LockRequest) (*kvpb.LockResponse, error)) (LockInfo, error) {
	var resp *kvpb.LockResponse
	seq := c.beginOp()
	defer c.endOp(seq)
	req.ClientId, req.Seq = c.id, seq
	_, err := c.do(ctx, c.ShardFor(lockShardKey(name)), func(ctx context.Context, kv kvpb.KVClient) (respEnvelope, error) {
		req.MinSeq = c.minOutstanding()
		r, err := call(ctx, kv, req)
		if err != nil {
			return respEnvelope{}, err
		}
		resp = r
		return respEnvelope{r.Code, r.Error, r.RedirectLeader, r.ShardId}, nil
	})
	if resp == nil {
		return LockInfo{}, err
	}
	return LockInfo{FencingToken: resp.FencingToken, ExpiresAtMs: resp.ExpiresAtMs, Holder: resp.Holder}, err
}

// TryLock makes one attempt to acquire lock name for owner. A retry of an
// acquire whose first attempt committed sees ALREADY_HELD by owner itself,
// which is treated as success.
func (c *Client) TryLock(ctx context.Context, name, owner string, ttl time.Duration) (LockInfo, error) {
	info, err := c.lockCall(ctx, name, &kvpb.LockRequest{Name: name, OwnerId: owner, TtlMs: uint64(ttl.Milliseconds())},
		func(ctx context.Context, kv kvpb.KVClient, req *kvpb.LockRequest) (*kvpb.LockResponse, error) {
			return kv.LockAcquire(ctx, req)
		})
	if errors.Is(err, ErrAlreadyHeld) && info.Holder == owner {
		return info, nil
	}
	return info, err
}

// RenewLock extends owner's lease (the fencing token is unchanged).
func (c *Client) RenewLock(ctx context.Context, name, owner string, ttl time.Duration) (LockInfo, error) {
	return c.lockCall(ctx, name, &kvpb.LockRequest{Name: name, OwnerId: owner, TtlMs: uint64(ttl.Milliseconds())},
		func(ctx context.Context, kv kvpb.KVClient, req *kvpb.LockRequest) (*kvpb.LockResponse, error) {
			return kv.LockRenew(ctx, req)
		})
}

// Unlock releases the lock (idempotent).
func (c *Client) Unlock(ctx context.Context, name, owner string) error {
	_, err := c.lockCall(ctx, name, &kvpb.LockRequest{Name: name, OwnerId: owner},
		func(ctx context.Context, kv kvpb.KVClient, req *kvpb.LockRequest) (*kvpb.LockResponse, error) {
			return kv.LockRelease(ctx, req)
		})
	return err
}

// WaitLock long-polls until the lock is released/expired or timeout passes.
// Waiters are leader-local; if the leader fails over mid-wait, the call is
// transparently re-issued against the new leader (through the router).
func (c *Client) WaitLock(ctx context.Context, name string, timeout time.Duration) (bool, error) {
	deadline := time.Now().Add(timeout)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return false, nil
		}
		var released bool
		cctx, cancel := context.WithTimeout(ctx, remaining+time.Second)
		_, err := c.doLong(cctx, c.ShardFor(lockShardKey(name)), remaining, func(ctx context.Context, kv kvpb.KVClient) (respEnvelope, error) {
			r, err := kv.LockWait(ctx, &kvpb.LockWaitRequest{Name: name, TimeoutMs: uint64(remaining.Milliseconds())})
			if err != nil {
				return respEnvelope{}, err
			}
			released = r.Released
			return respEnvelope{r.Code, r.Error, r.RedirectLeader, r.ShardId}, nil
		})
		cancel()
		if err == nil {
			return released, nil
		}
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		time.Sleep(50 * time.Millisecond) // failover: reissue against the new leader
	}
}

// doLong is do() with a per-attempt timeout long enough for a long-poll.
func (c *Client) doLong(ctx context.Context, shard int32, wait time.Duration, call func(ctx context.Context, kv kvpb.KVClient) (respEnvelope, error)) (string, error) {
	return c.doWith(ctx, shard, wait+time.Second, wait+c.cfg.RequestTimeout, call)
}

// ---- LockHandle ----

// LockHandle is a held lock with background auto-renewal. It prevents the
// common lock bugs by construction: forgetting to renew (renewed every
// ttl/3), forgetting that renewal can fail (Lost/Valid), and racing renewal
// against release (Release stops renewal first).
//
// Safety contract for application code: check Valid() before every critical
// action AND pass FencingToken() to the protected resource, which must
// reject any token lower than the highest it has seen. The token check is
// the real backstop: a goroutine paused (GC, slow disk, descheduled VM) past
// the TTL cannot know it lost the lock, but its stale token will be refused.
type LockHandle struct {
	c       *Client
	name    string
	owner   string
	ttl     time.Duration
	token   uint64
	stopCh  chan struct{}
	doneCh  chan struct{}
	lostCh  chan struct{}
	mu      sync.Mutex
	lost    bool
	stopped bool

	// renewHook, if set (tests), replaces the renewal RPC.
	renewHook func(ctx context.Context) error
}

// AcquireLock blocks until it acquires lock name (or ctx ends), then starts
// auto-renewal.
func (c *Client) AcquireLock(ctx context.Context, name, owner string, ttl time.Duration) (*LockHandle, error) {
	for {
		info, err := c.TryLock(ctx, name, owner, ttl)
		if err == nil {
			h := &LockHandle{c: c, name: name, owner: owner, ttl: ttl, token: info.FencingToken,
				stopCh: make(chan struct{}), doneCh: make(chan struct{}), lostCh: make(chan struct{})}
			go h.autoRenewLoop()
			return h, nil
		}
		if !errors.Is(err, ErrAlreadyHeld) {
			return nil, err
		}
		wait := ttl
		if dl, ok := ctx.Deadline(); ok && time.Until(dl) < wait {
			wait = time.Until(dl)
		}
		if wait <= 0 {
			return nil, ErrAlreadyHeld
		}
		if _, werr := c.WaitLock(ctx, name, wait); werr != nil {
			return nil, werr
		}
	}
}

// FencingToken returns the token minted for this acquisition. Pass it to
// every protected resource.
func (h *LockHandle) FencingToken() uint64 { return h.token }

// Valid reports whether the handle still believes it holds the lock.
func (h *LockHandle) Valid() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return !h.lost && !h.stopped
}

// Lost is closed when the lock is known (or presumed) lost.
func (h *LockHandle) Lost() <-chan struct{} { return h.lostCh }

func (h *LockHandle) markLost() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.lost {
		h.lost = true
		close(h.lostCh)
	}
}

func (h *LockHandle) autoRenewLoop() {
	defer close(h.doneCh)
	interval := h.ttl / 3
	if interval < 10*time.Millisecond {
		interval = 10 * time.Millisecond
	}
	lastOK := time.Now()
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-h.stopCh:
			return
		case <-t.C:
		}
		ctx, cancel := context.WithTimeout(context.Background(), interval)
		var err error
		if h.renewHook != nil {
			err = h.renewHook(ctx)
		} else {
			_, err = h.c.RenewLock(ctx, h.name, h.owner, h.ttl)
		}
		cancel()
		switch {
		case err == nil:
			lastOK = time.Now()
		case errors.Is(err, ErrNotOwner) || errors.Is(err, ErrLockExpired):
			h.markLost() // definitively lost
			return
		default:
			// Transient failure. Do NOT assume the lock is still held: after
			// two missed renewal intervals we give it up proactively, well
			// before the server-side TTL (3 intervals) can lapse.
			if time.Since(lastOK) >= 2*interval {
				h.markLost()
				return
			}
		}
	}
}

// Release stops renewal and releases the lock.
func (h *LockHandle) Release(ctx context.Context) error {
	h.mu.Lock()
	if h.stopped {
		h.mu.Unlock()
		return nil
	}
	h.stopped = true
	close(h.stopCh)
	h.mu.Unlock()
	<-h.doneCh
	return h.c.Unlock(ctx, h.name, h.owner)
}
