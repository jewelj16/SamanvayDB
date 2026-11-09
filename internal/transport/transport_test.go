package transport

import (
	"net"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc"

	"samanvaydb/internal/faultinjector"
	"samanvaydb/internal/raft"
	"samanvaydb/proto/raftpb"
)

// recordingHandler counts the RPCs dispatched to one in-process group.
type recordingHandler struct {
	appends, votes, snaps atomic.Int32
	lastLeader            atomic.Value
}

func (h *recordingHandler) HandleRequestVote(r raft.RequestVoteRequest) raft.RequestVoteResponse {
	h.votes.Add(1)
	return raft.RequestVoteResponse{Term: r.Term, VoteGranted: r.PreVote}
}
func (h *recordingHandler) HandleAppendEntries(r raft.AppendEntriesRequest) raft.AppendEntriesResponse {
	h.appends.Add(1)
	h.lastLeader.Store(r.LeaderID)
	return raft.AppendEntriesResponse{Term: r.Term, Success: len(r.Entries) == 2 && string(r.Entries[1].Command) == "b"}
}
func (h *recordingHandler) HandleInstallSnapshot(r raft.InstallSnapshotRequest) raft.InstallSnapshotResponse {
	h.snaps.Add(1)
	return raft.InstallSnapshotResponse{Term: r.Term + uint64(len(r.Config.Voters()))}
}

// TestDispatch_ShardIDRoutesOnSharedConnection — Commit 45: RPCs for shard A
// and shard B travel over the SAME gRPC connection and are dispatched to the
// correct in-process Raft group by shard_id.
func TestDispatch_ShardIDRoutesOnSharedConnection(t *testing.T) {
	groups := map[int32]*recordingHandler{1: {}, 2: {}}
	srv := grpc.NewServer()
	raftpb.RegisterRaftServer(srv, NewServer("receiver", func(id int32) (raft.RPCHandler, bool) {
		h, ok := groups[id]
		return h, ok
	}, nil))
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve(lis)
	defer srv.Stop()

	book := NewAddressBook()
	book.Set("receiver", lis.Addr().String())
	pool := NewPool()
	defer pool.Close()
	cl := NewClient("sender", book, pool, nil)

	a, b := cl.ForShard(1), cl.ForShard(2)
	entries := []raft.LogEntry{{Index: 1, Term: 1, Command: []byte("a")}, {Index: 2, Term: 1, Type: raft.EntryConfig, Command: []byte("b")}}
	for i := 0; i < 3; i++ {
		if r, err := a.SendAppendEntries("receiver", raft.AppendEntriesRequest{Term: 5, LeaderID: "L1", Entries: entries}); err != nil || !r.Success || r.Term != 5 {
			t.Fatalf("shard 1 append: %+v %v", r, err)
		}
	}
	if _, err := b.SendAppendEntries("receiver", raft.AppendEntriesRequest{Term: 9, LeaderID: "L2"}); err != nil {
		t.Fatal(err)
	}
	if r, err := b.SendRequestVote("receiver", raft.RequestVoteRequest{Term: 9, PreVote: true}); err != nil || !r.VoteGranted {
		t.Fatalf("vote: %+v %v", r, err)
	}
	r, err := a.SendInstallSnapshot("receiver", raft.InstallSnapshotRequest{Term: 3, Config: raft.NewMembership("x", "y")})
	if err != nil || r.Term != 5 {
		t.Fatalf("snapshot: %+v %v (config must survive the wire)", r, err)
	}

	if groups[1].appends.Load() != 3 || groups[2].appends.Load() != 1 || groups[2].votes.Load() != 1 || groups[1].snaps.Load() != 1 {
		t.Fatalf("misdispatch: g1 appends=%d snaps=%d, g2 appends=%d votes=%d",
			groups[1].appends.Load(), groups[1].snaps.Load(), groups[2].appends.Load(), groups[2].votes.Load())
	}
	if groups[1].lastLeader.Load() != raft.NodeID("L1") || groups[2].lastLeader.Load() != raft.NodeID("L2") {
		t.Fatal("requests delivered to the wrong group")
	}
	if len(pool.conns) != 1 {
		t.Fatalf("%d connections; both shards must share one", len(pool.conns))
	}
	// Unknown shard: error, not a crash.
	if _, err := cl.ForShard(99).SendAppendEntries("receiver", raft.AppendEntriesRequest{}); err == nil {
		t.Fatal("RPC for an unhosted shard succeeded")
	}
}

// TestFaults_EnforcedOnSendAndReceive: a partition blocks traffic even if
// only the RECEIVER's rules know about it.
func TestFaults_EnforcedOnSendAndReceive(t *testing.T) {
	h := &recordingHandler{}
	rules := faultinjector.NewRules("receiver", 1)
	srv := grpc.NewServer()
	raftpb.RegisterRaftServer(srv, NewServer("receiver", func(int32) (raft.RPCHandler, bool) { return h, true }, rules))
	lis, _ := net.Listen("tcp", "127.0.0.1:0")
	go srv.Serve(lis)
	defer srv.Stop()
	book := NewAddressBook()
	book.Set("receiver", lis.Addr().String())
	pool := NewPool()
	defer pool.Close()
	cl := NewClient("sender", book, pool, nil) // sender has no rules

	rules.Partition([]string{"sender"}, []string{"receiver"})
	if _, err := cl.ForShard(0).SendAppendEntries("receiver", raft.AppendEntriesRequest{}); err == nil {
		t.Fatal("partition not enforced on the receive side")
	}
	rules.HealPartition()
	if _, err := cl.ForShard(0).SendAppendEntries("receiver", raft.AppendEntriesRequest{}); err != nil {
		t.Fatalf("after heal: %v", err)
	}
	if h.appends.Load() != 1 {
		t.Fatalf("handler saw %d appends, want 1", h.appends.Load())
	}
}
