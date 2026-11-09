package transport

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"samanvaydb/internal/faultinjector"
	"samanvaydb/internal/raft"
	"samanvaydb/proto/raftpb"
)

// Resolver finds the in-process Raft group for a shard ID.
type Resolver func(shardID int32) (raft.RPCHandler, bool)

// Server is the receive side of the Raft transport. It implements the gRPC
// Raft service and dispatches each RPC by its shard_id to the matching
// in-process Raft group.
type Server struct {
	raftpb.UnimplementedRaftServer
	self    string
	resolve Resolver
	faults  *faultinjector.Rules
}

// NewServer creates the Raft RPC server for node self.
func NewServer(self string, resolve Resolver, faults *faultinjector.Rules) *Server {
	return &Server{self: self, resolve: resolve, faults: faults}
}

func (s *Server) admit(from string, shard int32) (raft.RPCHandler, error) {
	if s.faults != nil {
		if err := s.faults.Apply(from, s.self); err != nil {
			return nil, status.Error(codes.Unavailable, err.Error())
		}
		if d := s.faults.SlowDelay(); d > 0 {
			time.Sleep(d)
		}
	}
	h, ok := s.resolve(shard)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "transport: node %s hosts no replica of shard %d", s.self, shard)
	}
	return h, nil
}

// RequestVote dispatches a RequestVote RPC.
func (s *Server) RequestVote(_ context.Context, in *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	h, err := s.admit(in.From, in.ShardId)
	if err != nil {
		return nil, err
	}
	r := h.HandleRequestVote(raft.RequestVoteRequest{
		Term: in.Term, CandidateID: raft.NodeID(in.CandidateId),
		LastLogIndex: in.LastLogIndex, LastLogTerm: in.LastLogTerm, PreVote: in.PreVote,
	})
	return &raftpb.RequestVoteResponse{Term: r.Term, VoteGranted: r.VoteGranted}, nil
}

// AppendEntries dispatches an AppendEntries RPC.
func (s *Server) AppendEntries(_ context.Context, in *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	h, err := s.admit(in.From, in.ShardId)
	if err != nil {
		return nil, err
	}
	req := raft.AppendEntriesRequest{
		Term: in.Term, LeaderID: raft.NodeID(in.LeaderId), PrevLogIndex: in.PrevLogIndex,
		PrevLogTerm: in.PrevLogTerm, LeaderCommit: in.LeaderCommit,
		Entries: make([]raft.LogEntry, len(in.Entries)),
	}
	for i, e := range in.Entries {
		req.Entries[i] = raft.LogEntry{Index: e.Index, Term: e.Term, Type: raft.EntryType(e.Type), Command: e.Data}
	}
	r := h.HandleAppendEntries(req)
	return &raftpb.AppendEntriesResponse{Term: r.Term, Success: r.Success,
		ConflictIndex: r.ConflictIndex, ConflictTerm: r.ConflictTerm}, nil
}

// InstallSnapshot dispatches an InstallSnapshot RPC.
func (s *Server) InstallSnapshot(_ context.Context, in *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	h, err := s.admit(in.From, in.ShardId)
	if err != nil {
		return nil, err
	}
	cfg, err := raft.DecodeMembership(in.Config)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	r := h.HandleInstallSnapshot(raft.InstallSnapshotRequest{
		Term: in.Term, LeaderID: raft.NodeID(in.LeaderId), LastIncludedIndex: in.LastIncludedIndex,
		LastIncludedTerm: in.LastIncludedTerm, Config: cfg, Data: in.Data,
	})
	return &raftpb.InstallSnapshotResponse{Term: r.Term}, nil
}
