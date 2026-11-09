// Package kvserver implements the client-facing gRPC KV service.
//
// It translates gRPC requests into calls on a Backend and maps backend
// errors onto the API's error taxonomy:
//
//	*shard.NotLeaderError -> NOT_LEADER  (+ redirect_leader / redirect_addr)
//	shard.ErrUnavailable  -> UNAVAILABLE (no quorum; retry with backoff)
//	shard.ErrInvalid      -> INVALID_REQUEST (never retry)
//
// In a cluster the Backend routes each key to its shard's Raft group (see
// internal/node). NewStandalone keeps the original Phase-1 mode: one
// process, one storage.Engine, no replication.
package kvserver

import (
	"context"
	"errors"
	"fmt"
	"time"

	"samanvaydb/internal/logging"
	"samanvaydb/internal/shard"
	"samanvaydb/internal/storage"
	"samanvaydb/proto/kvpb"
)

// ErrNotHosted means this node hosts no replica of the key's shard; the
// client should try the shard's replicas from its shard map.
var ErrNotHosted = errors.New("kvserver: shard not hosted on this node")

// Backend executes KV operations. Each method returns the shard that owns
// the key so responses can report it.
type Backend interface {
	Put(ctx context.Context, key, value []byte, ttl time.Duration) (int32, error)
	Get(ctx context.Context, key []byte, c shard.Consistency) (int32, []byte, bool, error)
	Delete(ctx context.Context, key []byte) (int32, error)
	CAS(ctx context.Context, key, expected []byte, expectAbsent bool, newValue []byte) (int32, shard.Result, error)
	LockAcquire(ctx context.Context, name, owner string, ttl time.Duration) (int32, shard.Result, error)
	LockRenew(ctx context.Context, name, owner string, ttl time.Duration) (int32, shard.Result, error)
	LockRelease(ctx context.Context, name, owner string) (int32, shard.Result, error)
	LockWait(ctx context.Context, name string) (int32, bool, error)
	// AddrOf returns the gRPC address of a node, for redirect_addr.
	AddrOf(nodeID string) string
}

// Observer records request metrics (optional).
type Observer func(op, code string, d time.Duration)

// KVServer implements kvpb.KVServer.
type KVServer struct {
	kvpb.UnimplementedKVServer
	backend Backend
	logger  *logging.Logger
	observe Observer
}

// NewWithBackend creates a KV server over a Backend.
func NewWithBackend(b Backend, logger *logging.Logger, observe Observer) *KVServer {
	return &KVServer{backend: b, logger: logger.WithComponent("kv-server"), observe: observe}
}

// New creates a standalone (Phase-1, unreplicated) KV server over engine.
func New(engine storage.Engine, logger *logging.Logger) *KVServer {
	return NewWithBackend(&standalone{engine: engine}, logger, nil)
}

// NewStandalone is an explicit alias of New.
func NewStandalone(engine storage.Engine, logger *logging.Logger) *KVServer {
	return New(engine, logger)
}

// envelope is the common routing/error part of every response.
type envelope struct {
	code     kvpb.ErrorCode
	msg      string
	leader   string
	leaderAt string
}

func (s *KVServer) classify(err error) envelope {
	if err == nil {
		return envelope{code: kvpb.ErrorCode_OK}
	}
	var nl *shard.NotLeaderError
	switch {
	case errors.As(err, &nl):
		e := envelope{code: kvpb.ErrorCode_NOT_LEADER, msg: err.Error(), leader: string(nl.Leader)}
		if nl.Leader != "" {
			e.leaderAt = s.backend.AddrOf(string(nl.Leader))
		}
		return e
	case errors.Is(err, ErrNotHosted):
		return envelope{code: kvpb.ErrorCode_NOT_LEADER, msg: err.Error()}
	case errors.Is(err, shard.ErrUnavailable), errors.Is(err, shard.ErrStopped),
		errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return envelope{code: kvpb.ErrorCode_UNAVAILABLE, msg: err.Error()}
	case errors.Is(err, shard.ErrInvalid):
		return envelope{code: kvpb.ErrorCode_INVALID_REQUEST, msg: err.Error()}
	default:
		return envelope{code: kvpb.ErrorCode_INTERNAL, msg: err.Error()}
	}
}

func resultCode(r shard.Result) kvpb.ErrorCode {
	switch r.Code {
	case shard.ResultCASFailed:
		return kvpb.ErrorCode_CAS_FAILED
	case shard.ResultAlreadyHeld:
		return kvpb.ErrorCode_ALREADY_HELD
	case shard.ResultNotOwner:
		return kvpb.ErrorCode_NOT_OWNER
	case shard.ResultLockExpired:
		return kvpb.ErrorCode_LOCK_EXPIRED
	case shard.ResultInvalid:
		return kvpb.ErrorCode_INVALID_REQUEST
	}
	return kvpb.ErrorCode_OK
}

func (s *KVServer) done(op string, start time.Time, code kvpb.ErrorCode) {
	if s.observe != nil {
		s.observe(op, code.String(), time.Since(start))
	}
}

// withClient attaches the request's client session (if any) to ctx.
func withClient(ctx context.Context, id []byte, seq, minSeq uint64) context.Context {
	return shard.WithClient(ctx, shard.ClientRef{ID: id, Seq: seq, MinSeq: minSeq})
}

func (s *KVServer) Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	start := time.Now()
	ctx = withClient(ctx, req.ClientId, req.Seq, req.MinSeq)
	sid, err := s.backend.Put(ctx, req.Key, req.Value, time.Duration(req.TtlMs)*time.Millisecond)
	e := s.classify(err)
	s.done("put", start, e.code)
	if e.code == kvpb.ErrorCode_INTERNAL {
		s.logger.Error("Put failed", "key", fmt.Sprintf("%x", req.Key), "error", err)
	}
	return &kvpb.PutResponse{Ok: err == nil, Error: e.msg, Code: e.code, RedirectLeader: e.leader,
		RedirectAddr: e.leaderAt, ShardId: sid}, nil
}

func (s *KVServer) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	start := time.Now()
	c := shard.ConsistencyLeader
	if req.Consistency == kvpb.Consistency_FOLLOWER_OK {
		c = shard.ConsistencyFollowerOK
	}
	sid, v, found, err := s.backend.Get(ctx, req.Key, c)
	e := s.classify(err)
	s.done("get", start, e.code)
	return &kvpb.GetResponse{Found: found, Value: v, Error: e.msg, Code: e.code, RedirectLeader: e.leader,
		RedirectAddr: e.leaderAt, ShardId: sid}, nil
}

func (s *KVServer) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	start := time.Now()
	ctx = withClient(ctx, req.ClientId, req.Seq, req.MinSeq)
	sid, err := s.backend.Delete(ctx, req.Key)
	e := s.classify(err)
	s.done("delete", start, e.code)
	if e.code == kvpb.ErrorCode_INTERNAL {
		s.logger.Error("Delete failed", "key", fmt.Sprintf("%x", req.Key), "error", err)
	}
	return &kvpb.DeleteResponse{Ok: err == nil, Error: e.msg, Code: e.code, RedirectLeader: e.leader,
		RedirectAddr: e.leaderAt, ShardId: sid}, nil
}

func (s *KVServer) CompareAndSwap(ctx context.Context, req *kvpb.CASRequest) (*kvpb.CASResponse, error) {
	start := time.Now()
	ctx = withClient(ctx, req.ClientId, req.Seq, req.MinSeq)
	sid, r, err := s.backend.CAS(ctx, req.Key, req.Expected, req.ExpectAbsent, req.NewValue)
	e := s.classify(err)
	if err == nil {
		e.code = resultCode(r)
		if e.code != kvpb.ErrorCode_OK {
			e.msg = "compare-and-swap precondition failed"
		}
	}
	s.done("cas", start, e.code)
	return &kvpb.CASResponse{Ok: e.code == kvpb.ErrorCode_OK, Error: e.msg, Code: e.code, RedirectLeader: e.leader,
		RedirectAddr: e.leaderAt, ShardId: sid, CurrentFound: r.Found, CurrentValue: r.Value}, nil
}

func (s *KVServer) lockResp(op string, start time.Time, sid int32, r shard.Result, err error) *kvpb.LockResponse {
	e := s.classify(err)
	if err == nil {
		e.code = resultCode(r)
		if e.code != kvpb.ErrorCode_OK {
			e.msg = "lock " + e.code.String()
		}
	}
	s.done(op, start, e.code)
	return &kvpb.LockResponse{Ok: e.code == kvpb.ErrorCode_OK, Error: e.msg, Code: e.code, RedirectLeader: e.leader,
		RedirectAddr: e.leaderAt, ShardId: sid, FencingToken: r.Token, ExpiresAtMs: r.ExpiresAtMs, Holder: string(r.Holder)}
}

func (s *KVServer) LockAcquire(ctx context.Context, req *kvpb.LockRequest) (*kvpb.LockResponse, error) {
	start := time.Now()
	ctx = withClient(ctx, req.ClientId, req.Seq, req.MinSeq)
	sid, r, err := s.backend.LockAcquire(ctx, req.Name, req.OwnerId, time.Duration(req.TtlMs)*time.Millisecond)
	return s.lockResp("lock_acquire", start, sid, r, err), nil
}

func (s *KVServer) LockRenew(ctx context.Context, req *kvpb.LockRequest) (*kvpb.LockResponse, error) {
	start := time.Now()
	ctx = withClient(ctx, req.ClientId, req.Seq, req.MinSeq)
	sid, r, err := s.backend.LockRenew(ctx, req.Name, req.OwnerId, time.Duration(req.TtlMs)*time.Millisecond)
	return s.lockResp("lock_renew", start, sid, r, err), nil
}

func (s *KVServer) LockRelease(ctx context.Context, req *kvpb.LockRequest) (*kvpb.LockResponse, error) {
	start := time.Now()
	ctx = withClient(ctx, req.ClientId, req.Seq, req.MinSeq)
	sid, r, err := s.backend.LockRelease(ctx, req.Name, req.OwnerId)
	return s.lockResp("lock_release", start, sid, r, err), nil
}

func (s *KVServer) LockWait(ctx context.Context, req *kvpb.LockWaitRequest) (*kvpb.LockWaitResponse, error) {
	start := time.Now()
	if req.TimeoutMs > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(req.TimeoutMs)*time.Millisecond)
		defer cancel()
	}
	sid, released, err := s.backend.LockWait(ctx, req.Name)
	e := s.classify(err)
	s.done("lock_wait", start, e.code)
	return &kvpb.LockWaitResponse{Released: released, Error: e.msg, Code: e.code, RedirectLeader: e.leader,
		RedirectAddr: e.leaderAt, ShardId: sid}, nil
}

// ---- standalone (Phase-1) backend ----

type standalone struct{ engine storage.Engine }

var errStandalone = fmt.Errorf("%w: not supported by a standalone (unreplicated) server", shard.ErrInvalid)

func (b *standalone) Put(_ context.Context, key, value []byte, _ time.Duration) (int32, error) {
	if err := b.engine.Put(key, value); err != nil {
		return 0, fmt.Errorf("%w: %v", shard.ErrInvalid, err)
	}
	return 0, nil
}

func (b *standalone) Get(_ context.Context, key []byte, _ shard.Consistency) (int32, []byte, bool, error) {
	v, found, err := b.engine.Get(key)
	if err != nil {
		return 0, nil, false, fmt.Errorf("%w: %v", shard.ErrInvalid, err)
	}
	return 0, v, found, nil
}

func (b *standalone) Delete(_ context.Context, key []byte) (int32, error) {
	if err := b.engine.Delete(key); err != nil {
		return 0, fmt.Errorf("%w: %v", shard.ErrInvalid, err)
	}
	return 0, nil
}

func (b *standalone) CAS(context.Context, []byte, []byte, bool, []byte) (int32, shard.Result, error) {
	return 0, shard.Result{}, errStandalone
}
func (b *standalone) LockAcquire(context.Context, string, string, time.Duration) (int32, shard.Result, error) {
	return 0, shard.Result{}, errStandalone
}
func (b *standalone) LockRenew(context.Context, string, string, time.Duration) (int32, shard.Result, error) {
	return 0, shard.Result{}, errStandalone
}
func (b *standalone) LockRelease(context.Context, string, string) (int32, shard.Result, error) {
	return 0, shard.Result{}, errStandalone
}
func (b *standalone) LockWait(context.Context, string) (int32, bool, error) {
	return 0, false, errStandalone
}
func (b *standalone) AddrOf(string) string { return "" }
