// Package transport carries Raft RPCs between nodes over gRPC.
//
// One gRPC connection between two nodes carries the traffic of EVERY shard
// they co-host. Every message carries shard_id, and the receiving side
// dispatches on it to the right in-process Raft group (see dispatch.go) —
// never assuming one connection == one shard. Heartbeat coalescing across
// co-located groups is a documented future optimization; each group sends
// its own heartbeats today.
package transport

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"samanvaydb/internal/faultinjector"
	"samanvaydb/internal/raft"
	"samanvaydb/proto/raftpb"
)

// MaxMessageSize bounds a single gRPC message (snapshots are sent whole).
const MaxMessageSize = 256 << 20

// AddressBook maps node IDs to gRPC addresses. It is updated from the
// cluster topology and from Raft configuration entries (which carry member
// addresses), so a newly added node becomes reachable everywhere.
type AddressBook struct {
	mu    sync.RWMutex
	addrs map[string]string
	// Resolve is consulted for unknown IDs (e.g. refresh from the shard map).
	Resolve func(id string) (string, bool)
}

// NewAddressBook returns an empty book.
func NewAddressBook() *AddressBook { return &AddressBook{addrs: map[string]string{}} }

// Set records id's address.
func (b *AddressBook) Set(id, addr string) {
	if id == "" || addr == "" {
		return
	}
	b.mu.Lock()
	b.addrs[id] = addr
	b.mu.Unlock()
}

// Get returns id's address.
func (b *AddressBook) Get(id string) (string, bool) {
	b.mu.RLock()
	a, ok := b.addrs[id]
	resolve := b.Resolve
	b.mu.RUnlock()
	if ok {
		return a, true
	}
	if resolve != nil {
		if a, ok := resolve(id); ok {
			b.Set(id, a)
			return a, true
		}
	}
	return "", false
}

// All returns a copy of the book.
func (b *AddressBook) All() map[string]string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make(map[string]string, len(b.addrs))
	for k, v := range b.addrs {
		out[k] = v
	}
	return out
}

// Pool caches one gRPC client connection per remote address.
type Pool struct {
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

// NewPool returns an empty connection pool.
func NewPool() *Pool { return &Pool{conns: map[string]*grpc.ClientConn{}} }

// Get returns (creating lazily) a connection to addr.
func (p *Pool) Get(addr string) (*grpc.ClientConn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.conns[addr]; ok {
		return c, nil
	}
	c, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(MaxMessageSize), grpc.MaxCallSendMsgSize(MaxMessageSize)),
	)
	if err != nil {
		return nil, err
	}
	p.conns[addr] = c
	return c, nil
}

// Close closes every pooled connection.
func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for a, c := range p.conns {
		c.Close()
		delete(p.conns, a)
	}
}

// Client sends Raft RPCs for this node.
type Client struct {
	self   string
	book   *AddressBook
	pool   *Pool
	faults *faultinjector.Rules

	// Timeouts. RPC timeouts stay below the election timeout so a dead peer
	// never stalls elections; snapshots get much longer.
	VoteTimeout     time.Duration
	AppendTimeout   time.Duration
	SnapshotTimeout time.Duration
}

// NewClient creates the Raft RPC client for node self.
func NewClient(self string, book *AddressBook, pool *Pool, faults *faultinjector.Rules) *Client {
	return &Client{
		self: self, book: book, pool: pool, faults: faults,
		VoteTimeout:     150 * time.Millisecond,
		AppendTimeout:   500 * time.Millisecond,
		SnapshotTimeout: 30 * time.Second,
	}
}

// ForShard returns the raft.Transport used by this node's replica of shard.
func (c *Client) ForShard(shard int32) raft.Transport {
	return &shardTransport{c: c, shard: shard}
}

func (c *Client) conn(to raft.NodeID) (raftpb.RaftClient, error) {
	if c.faults != nil {
		if err := c.faults.Apply(c.self, string(to)); err != nil {
			return nil, err
		}
	}
	addr, ok := c.book.Get(string(to))
	if !ok {
		return nil, fmt.Errorf("transport: no address for node %s", to)
	}
	cc, err := c.pool.Get(addr)
	if err != nil {
		return nil, err
	}
	return raftpb.NewRaftClient(cc), nil
}

type shardTransport struct {
	c     *Client
	shard int32
}

func (t *shardTransport) SendRequestVote(to raft.NodeID, req raft.RequestVoteRequest) (raft.RequestVoteResponse, error) {
	cl, err := t.c.conn(to)
	if err != nil {
		return raft.RequestVoteResponse{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), t.c.VoteTimeout)
	defer cancel()
	resp, err := cl.RequestVote(ctx, &raftpb.RequestVoteRequest{
		ShardId: t.shard, From: t.c.self, Term: req.Term, CandidateId: string(req.CandidateID),
		LastLogIndex: req.LastLogIndex, LastLogTerm: req.LastLogTerm, PreVote: req.PreVote,
	})
	if err != nil {
		return raft.RequestVoteResponse{}, err
	}
	return raft.RequestVoteResponse{Term: resp.Term, VoteGranted: resp.VoteGranted}, nil
}

func (t *shardTransport) SendAppendEntries(to raft.NodeID, req raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	cl, err := t.c.conn(to)
	if err != nil {
		return raft.AppendEntriesResponse{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), t.c.AppendTimeout)
	defer cancel()
	pb := &raftpb.AppendEntriesRequest{
		ShardId: t.shard, From: t.c.self, Term: req.Term, LeaderId: string(req.LeaderID),
		PrevLogIndex: req.PrevLogIndex, PrevLogTerm: req.PrevLogTerm, LeaderCommit: req.LeaderCommit,
		Entries: make([]*raftpb.Entry, len(req.Entries)),
	}
	for i, e := range req.Entries {
		pb.Entries[i] = &raftpb.Entry{Index: e.Index, Term: e.Term, Type: uint32(e.Type), Data: e.Command}
	}
	resp, err := cl.AppendEntries(ctx, pb)
	if err != nil {
		return raft.AppendEntriesResponse{}, err
	}
	return raft.AppendEntriesResponse{Term: resp.Term, Success: resp.Success,
		ConflictIndex: resp.ConflictIndex, ConflictTerm: resp.ConflictTerm}, nil
}

func (t *shardTransport) SendInstallSnapshot(to raft.NodeID, req raft.InstallSnapshotRequest) (raft.InstallSnapshotResponse, error) {
	cl, err := t.c.conn(to)
	if err != nil {
		return raft.InstallSnapshotResponse{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), t.c.SnapshotTimeout)
	defer cancel()
	resp, err := cl.InstallSnapshot(ctx, &raftpb.InstallSnapshotRequest{
		ShardId: t.shard, From: t.c.self, Term: req.Term, LeaderId: string(req.LeaderID),
		LastIncludedIndex: req.LastIncludedIndex, LastIncludedTerm: req.LastIncludedTerm,
		Config: req.Config.Encode(), Data: req.Data,
	})
	if err != nil {
		return raft.InstallSnapshotResponse{}, err
	}
	return raft.InstallSnapshotResponse{Term: resp.Term}, nil
}
