// Package client is the Go client library for SamanvayDB — the only package
// external users, tests and benchmarks should import to talk to a cluster.
//
// It routes each key to its shard (hash(key) mod NUM_SHARDS), sends the
// request to the shard's presumed leader, and transparently follows
// NOT_LEADER redirects, refreshing its shard map when routing keeps failing.
//
// Error taxonomy (never conflate these — doing so causes either retry storms
// or clients giving up on transient conditions):
//
//	ErrNotLeader      handled internally: retry against the redirect target
//	ErrUnavailable    no majority reachable right now: retried with backoff
//	                  until the request timeout, then returned
//	ErrInvalidRequest the request itself is wrong: returned at once, never retried
//
// Timeouts: each request's overall timeout (default 5s) is deliberately
// LARGER than the Raft election timeout (150–300ms), so a client waiting
// through a leader failover does not give up before a new leader exists.
//
// Exactly-once retries: each Client has a session ID and numbers every write
// operation; all retries of one operation carry the same number, and the
// cluster applies a numbered operation at most once, answering duplicates
// with the original result. Retrying after a timeout or a lost reply is
// therefore safe for PUT, DELETE, CAS and lock calls alike, and keeps the
// history linearizable (a duplicate PUT can never "re-apply" after another
// client's write). Sessions idle for an hour are forgotten.
//
// Consistency: Get defaults to LEADER (linearizable: the leader confirms its
// leadership with a ReadIndex round before reading). WithFollowerOK() reads
// any replica's local state — faster, possibly stale by the replication lag.
package client

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"samanvaydb/internal/router"
	"samanvaydb/proto/adminpb"
	"samanvaydb/proto/kvpb"
)

// Sentinel errors. Use errors.Is.
var (
	ErrNotLeader      = errors.New("samanvaydb: not leader")
	ErrUnavailable    = errors.New("samanvaydb: unavailable (no quorum reachable)")
	ErrInvalidRequest = errors.New("samanvaydb: invalid request")
	ErrCASFailed      = errors.New("samanvaydb: compare-and-swap precondition failed")
	ErrAlreadyHeld    = errors.New("samanvaydb: lock already held")
	ErrNotOwner       = errors.New("samanvaydb: not the lock owner")
	ErrLockExpired    = errors.New("samanvaydb: lock expired")
	ErrInternal       = errors.New("samanvaydb: internal server error")
)

// Error carries the server's code and message; errors.Is matches the
// corresponding sentinel.
type Error struct {
	Code    kvpb.ErrorCode
	Message string
	ShardID int32
	Node    string
}

func (e *Error) Error() string {
	return fmt.Sprintf("samanvaydb: %s (shard %d, node %s): %s", e.Code, e.ShardID, e.Node, e.Message)
}

// Is maps codes onto sentinels.
func (e *Error) Is(target error) bool { return codeErr(e.Code) == target }

func codeErr(c kvpb.ErrorCode) error {
	switch c {
	case kvpb.ErrorCode_NOT_LEADER:
		return ErrNotLeader
	case kvpb.ErrorCode_UNAVAILABLE:
		return ErrUnavailable
	case kvpb.ErrorCode_INVALID_REQUEST:
		return ErrInvalidRequest
	case kvpb.ErrorCode_CAS_FAILED:
		return ErrCASFailed
	case kvpb.ErrorCode_ALREADY_HELD:
		return ErrAlreadyHeld
	case kvpb.ErrorCode_NOT_OWNER:
		return ErrNotOwner
	case kvpb.ErrorCode_LOCK_EXPIRED:
		return ErrLockExpired
	case kvpb.ErrorCode_INTERNAL:
		return ErrInternal
	}
	return nil
}

// Config configures a Client.
type Config struct {
	// Seeds are gRPC addresses of any cluster nodes; the client discovers
	// the full topology from them.
	Seeds []string
	// RequestTimeout bounds each operation end to end, including retries
	// (default 5s — above the election timeout on purpose).
	RequestTimeout time.Duration
	// AttemptTimeout bounds one RPC attempt (default 2s).
	AttemptTimeout time.Duration
}

// Client is safe for concurrent use.
type Client struct {
	cfg    Config
	router *router.Router

	mu    sync.Mutex
	conns map[string]*grpc.ClientConn

	// Exactly-once session: every write carries (id, seq); retries of one
	// logical operation reuse its seq, so the cluster applies it at most
	// once even if an earlier attempt committed but its reply was lost.
	id          []byte
	seqMu       sync.Mutex
	nextSeq     uint64
	outstanding map[uint64]struct{}
}

// beginOp allocates the sequence number for one logical write.
func (c *Client) beginOp() uint64 {
	c.seqMu.Lock()
	defer c.seqMu.Unlock()
	c.nextSeq++
	c.outstanding[c.nextSeq] = struct{}{}
	return c.nextSeq
}

// endOp marks a write finished (its outcome no longer needs remembering).
func (c *Client) endOp(seq uint64) {
	c.seqMu.Lock()
	defer c.seqMu.Unlock()
	delete(c.outstanding, seq)
}

// minOutstanding is the lowest sequence number still in flight; the server
// may forget results below it.
func (c *Client) minOutstanding() uint64 {
	c.seqMu.Lock()
	defer c.seqMu.Unlock()
	min := c.nextSeq + 1
	for s := range c.outstanding {
		if s < min {
			min = s
		}
	}
	return min
}

// New creates a client. It fetches the shard map from a seed eagerly and
// returns an error if no seed answers.
func New(ctx context.Context, cfg Config) (*Client, error) {
	if len(cfg.Seeds) == 0 {
		return nil, fmt.Errorf("client: at least one seed address is required")
	}
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = 5 * time.Second
	}
	if cfg.AttemptTimeout == 0 {
		cfg.AttemptTimeout = 2 * time.Second
	}
	id := make([]byte, 16)
	if _, err := rand.Read(id); err != nil {
		return nil, fmt.Errorf("client: generate session id: %w", err)
	}
	c := &Client{cfg: cfg, conns: map[string]*grpc.ClientConn{},
		id: []byte(hex.EncodeToString(id)), outstanding: map[uint64]struct{}{}}
	c.router = router.New(nil, c.fetchShardMap)
	if err := c.router.Refresh(ctx); err != nil {
		c.Close()
		return nil, fmt.Errorf("client: fetch shard map: %w", err)
	}
	return c, nil
}

// Close releases connections.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for a, cc := range c.conns {
		cc.Close()
		delete(c.conns, a)
	}
	return nil
}

func (c *Client) conn(addr string) (*grpc.ClientConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if cc, ok := c.conns[addr]; ok {
		return cc, nil
	}
	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	c.conns[addr] = cc
	return cc, nil
}

// fetchShardMap asks each known node (seeds first, then map members) for
// the authoritative shard map.
func (c *Client) fetchShardMap(ctx context.Context) (*router.ShardMap, error) {
	addrs := append([]string(nil), c.cfg.Seeds...)
	if c.router != nil {
		if m := c.router.Map(); m != nil {
			for _, n := range m.Nodes {
				addrs = append(addrs, n.Addr)
			}
		}
	}
	var lastErr error = errors.New("no seed reachable")
	seen := map[string]bool{}
	for _, a := range addrs {
		if seen[a] {
			continue
		}
		seen[a] = true
		cc, err := c.conn(a)
		if err != nil {
			lastErr = err
			continue
		}
		actx, cancel := context.WithTimeout(ctx, c.cfg.AttemptTimeout)
		resp, err := adminpb.NewAdminClient(cc).GetShardMap(actx, &adminpb.GetShardMapRequest{})
		if err == nil && resp.Error != "" {
			// Metadata quorum unavailable: fall back to the node's cached copy.
			resp, err = adminpb.NewAdminClient(cc).GetShardMap(actx, &adminpb.GetShardMapRequest{Cached: true})
		}
		cancel()
		if err != nil {
			lastErr = err
			continue
		}
		return router.DecodeShardMap(resp.ShardMapJson)
	}
	return nil, lastErr
}

// ShardMap returns the client's current view of the topology.
func (c *Client) ShardMap() *router.ShardMap { return c.router.Map() }

// RefreshShardMap re-fetches the topology.
func (c *Client) RefreshShardMap(ctx context.Context) error { return c.router.Refresh(ctx) }

// ShardFor returns the shard a key maps to.
func (c *Client) ShardFor(key []byte) int32 { return c.router.ShardFor(key) }

// LeaderHint returns the client's cached leader for a shard.
func (c *Client) LeaderHint(shard int32) string { return c.router.LeaderHint(shard) }

// respEnvelope is the common part of every KV response.
type respEnvelope struct {
	code   kvpb.ErrorCode
	msg    string
	leader string
	shard  int32
}

// do runs one KV operation with routing, redirects, and retries.
func (c *Client) do(ctx context.Context, shard int32, call func(ctx context.Context, kv kvpb.KVClient) (respEnvelope, error)) (string, error) {
	return c.doWith(ctx, shard, c.cfg.AttemptTimeout, c.cfg.RequestTimeout, call)
}

func (c *Client) doWith(ctx context.Context, shard int32, attemptTimeout, requestTimeout time.Duration,
	call func(ctx context.Context, kv kvpb.KVClient) (respEnvelope, error)) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	var final *Error
	n, err := c.router.Do(ctx, shard, func(ctx context.Context, n router.NodeInfo) router.Attempt {
		cc, err := c.conn(n.Addr)
		if err != nil {
			return router.Attempt{Outcome: router.Unavailable, Err: err}
		}
		actx, acancel := context.WithTimeout(ctx, attemptTimeout)
		defer acancel()
		env, err := call(actx, kvpb.NewKVClient(cc))
		if err != nil { // transport failure: node down / partitioned / slow
			return router.Attempt{Outcome: router.Unavailable, Err: err}
		}
		switch env.code {
		case kvpb.ErrorCode_OK:
			return router.Attempt{Outcome: router.Done}
		case kvpb.ErrorCode_NOT_LEADER:
			return router.Attempt{Outcome: router.NotLeader, Hint: env.leader}
		case kvpb.ErrorCode_UNAVAILABLE:
			return router.Attempt{Outcome: router.Unavailable, Err: &Error{Code: env.code, Message: env.msg, ShardID: env.shard, Node: n.ID}}
		default:
			final = &Error{Code: env.code, Message: env.msg, ShardID: env.shard, Node: n.ID}
			return router.Attempt{Outcome: router.Done, Err: final}
		}
	})
	if err != nil {
		if final != nil {
			return n.ID, final
		}
		if errors.Is(err, router.ErrNoRoute) {
			return "", fmt.Errorf("%w: %v", ErrUnavailable, err)
		}
		return n.ID, err
	}
	return n.ID, nil
}

// Put writes key=value.
func (c *Client) Put(ctx context.Context, key, value []byte) error {
	return c.PutTTL(ctx, key, value, 0)
}

// PutTTL writes key=value that expires after ttl (0 = never). Expiry is
// committed through Raft by the shard leader, so it is consistent on every
// replica.
func (c *Client) PutTTL(ctx context.Context, key, value []byte, ttl time.Duration) error {
	seq := c.beginOp()
	defer c.endOp(seq)
	_, err := c.do(ctx, c.ShardFor(key), func(ctx context.Context, kv kvpb.KVClient) (respEnvelope, error) {
		r, err := kv.Put(ctx, &kvpb.PutRequest{Key: key, Value: value, TtlMs: uint64(ttl.Milliseconds()),
			ClientId: c.id, Seq: seq, MinSeq: c.minOutstanding()})
		if err != nil {
			return respEnvelope{}, err
		}
		return respEnvelope{r.Code, r.Error, r.RedirectLeader, r.ShardId}, nil
	})
	return err
}

// GetOption tunes a Get.
type GetOption func(*kvpb.GetRequest)

// WithFollowerOK allows any replica to serve the read from its local,
// possibly stale, state.
func WithFollowerOK() GetOption {
	return func(r *kvpb.GetRequest) { r.Consistency = kvpb.Consistency_FOLLOWER_OK }
}

// Get reads key. found=false (with err=nil) means the key does not exist.
func (c *Client) Get(ctx context.Context, key []byte, opts ...GetOption) (value []byte, found bool, err error) {
	req := &kvpb.GetRequest{Key: key}
	for _, o := range opts {
		o(req)
	}
	var resp *kvpb.GetResponse
	_, err = c.do(ctx, c.ShardFor(key), func(ctx context.Context, kv kvpb.KVClient) (respEnvelope, error) {
		r, err := kv.Get(ctx, req)
		if err != nil {
			return respEnvelope{}, err
		}
		resp = r
		return respEnvelope{r.Code, r.Error, r.RedirectLeader, r.ShardId}, nil
	})
	if err != nil {
		return nil, false, err
	}
	return resp.Value, resp.Found, nil
}

// Delete removes key (idempotent).
func (c *Client) Delete(ctx context.Context, key []byte) error {
	seq := c.beginOp()
	defer c.endOp(seq)
	_, err := c.do(ctx, c.ShardFor(key), func(ctx context.Context, kv kvpb.KVClient) (respEnvelope, error) {
		r, err := kv.Delete(ctx, &kvpb.DeleteRequest{Key: key, ClientId: c.id, Seq: seq, MinSeq: c.minOutstanding()})
		if err != nil {
			return respEnvelope{}, err
		}
		return respEnvelope{r.Code, r.Error, r.RedirectLeader, r.ShardId}, nil
	})
	return err
}

// CASResult reports the value present when a failed CAS was evaluated.
type CASResult struct {
	CurrentFound bool
	CurrentValue []byte
}

// CompareAndSwap sets key to newValue iff it currently equals expected
// (expected == nil means "key must be absent"). On precondition failure it
// returns ErrCASFailed and the current value.
func (c *Client) CompareAndSwap(ctx context.Context, key, expected, newValue []byte) (CASResult, error) {
	var resp *kvpb.CASResponse
	seq := c.beginOp()
	defer c.endOp(seq)
	_, err := c.do(ctx, c.ShardFor(key), func(ctx context.Context, kv kvpb.KVClient) (respEnvelope, error) {
		r, err := kv.CompareAndSwap(ctx, &kvpb.CASRequest{Key: key, Expected: expected, ExpectAbsent: expected == nil,
			NewValue: newValue, ClientId: c.id, Seq: seq, MinSeq: c.minOutstanding()})
		if err != nil {
			return respEnvelope{}, err
		}
		resp = r
		return respEnvelope{r.Code, r.Error, r.RedirectLeader, r.ShardId}, nil
	})
	if resp == nil {
		return CASResult{}, err
	}
	return CASResult{CurrentFound: resp.CurrentFound, CurrentValue: resp.CurrentValue}, err
}

// ---- admin helpers ----

// AdminFor returns an Admin client for any reachable node (for kvctl).
func (c *Client) AdminFor(addr string) (adminpb.AdminClient, error) {
	cc, err := c.conn(addr)
	if err != nil {
		return nil, err
	}
	return adminpb.NewAdminClient(cc), nil
}

// ClusterStatus asks the first reachable node for the cluster-wide status.
func (c *Client) ClusterStatus(ctx context.Context) (*adminpb.ClusterStatusResponse, error) {
	var lastErr error = errors.New("no node reachable")
	for _, a := range c.addrs() {
		cl, err := c.AdminFor(a)
		if err != nil {
			lastErr = err
			continue
		}
		actx, cancel := context.WithTimeout(ctx, 3*time.Second)
		r, err := cl.ClusterStatus(actx, &adminpb.ClusterStatusRequest{})
		cancel()
		if err == nil {
			return r, nil
		}
		lastErr = err
	}
	return nil, lastErr
}

func (c *Client) addrs() []string {
	out := append([]string(nil), c.cfg.Seeds...)
	if m := c.router.Map(); m != nil {
		for _, n := range m.Nodes {
			out = append(out, n.Addr)
		}
	}
	return out
}
