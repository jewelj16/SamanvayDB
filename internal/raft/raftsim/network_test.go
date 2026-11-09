package raftsim

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"samanvaydb/internal/raft"
)

// countingHandler counts delivered messages.
type countingHandler struct{ n atomic.Int64 }

func (h *countingHandler) HandleRequestVote(raft.RequestVoteRequest) raft.RequestVoteResponse {
	h.n.Add(1)
	return raft.RequestVoteResponse{VoteGranted: true}
}
func (h *countingHandler) HandleAppendEntries(raft.AppendEntriesRequest) raft.AppendEntriesResponse {
	h.n.Add(1)
	return raft.AppendEntriesResponse{Success: true}
}
func (h *countingHandler) HandleInstallSnapshot(raft.InstallSnapshotRequest) raft.InstallSnapshotResponse {
	h.n.Add(1)
	return raft.InstallSnapshotResponse{}
}

func TestNetwork_DroppedMessageNeverArrives(t *testing.T) {
	net := NewNetwork(42)
	h := &countingHandler{}
	net.Register("b", h)
	net.DropMessages("a", "b", 1.0)
	for i := 0; i < 50; i++ {
		if _, err := net.Transport("a").SendAppendEntries("b", raft.AppendEntriesRequest{}); !errors.Is(err, ErrDropped) {
			t.Fatalf("send %d: err=%v, want ErrDropped", i, err)
		}
	}
	if h.n.Load() != 0 {
		t.Fatalf("%d dropped messages arrived", h.n.Load())
	}
}

func TestNetwork_DropRateIsStatistical(t *testing.T) {
	net := NewNetwork(7)
	h := &countingHandler{}
	net.Register("b", h)
	net.DropMessages("a", "b", 0.3)
	const total = 4000
	for i := 0; i < total; i++ {
		net.Transport("a").SendRequestVote("b", raft.RequestVoteRequest{})
	}
	got := float64(h.n.Load()) / total
	if got < 0.65 || got > 0.75 {
		t.Fatalf("delivery rate %.3f, want ~0.70", got)
	}
}

func TestNetwork_DelayedMessageArrivesAfterDelay(t *testing.T) {
	net := NewNetwork(1)
	h := &countingHandler{}
	net.Register("b", h)
	net.DelayMessages("a", "b", 80*time.Millisecond)
	start := time.Now()
	if _, err := net.Transport("a").SendAppendEntries("b", raft.AppendEntriesRequest{}); err != nil {
		t.Fatal(err)
	}
	if el := time.Since(start); el < 80*time.Millisecond {
		t.Fatalf("delivered after %v, want >= 80ms", el)
	}
	if h.n.Load() != 1 {
		t.Fatal("delayed message did not arrive")
	}
}

func TestNetwork_PartitionBlocksUntilHealed(t *testing.T) {
	net := NewNetwork(1)
	hb, hc := &countingHandler{}, &countingHandler{}
	net.Register("b", hb)
	net.Register("c", hc)
	net.Register("a", &countingHandler{})
	net.Partition([]raft.NodeID{"a"}, []raft.NodeID{"b"})

	if _, err := net.Transport("a").SendAppendEntries("b", raft.AppendEntriesRequest{}); !errors.Is(err, ErrUnreachable) {
		t.Fatalf("cross-partition a->b: err=%v", err)
	}
	if _, err := net.Transport("b").SendAppendEntries("a", raft.AppendEntriesRequest{}); !errors.Is(err, ErrUnreachable) {
		t.Fatalf("cross-partition b->a: err=%v", err)
	}
	if _, err := net.Transport("a").SendAppendEntries("c", raft.AppendEntriesRequest{}); err != nil {
		t.Fatalf("a->c is not partitioned: %v", err)
	}
	if hb.n.Load() != 0 {
		t.Fatal("message crossed the partition")
	}
	net.HealPartition()
	if _, err := net.Transport("a").SendAppendEntries("b", raft.AppendEntriesRequest{}); err != nil {
		t.Fatalf("after heal: %v", err)
	}
	if hb.n.Load() != 1 {
		t.Fatal("message not delivered after heal")
	}
}

func TestNetwork_DuplicateDeliversTwice(t *testing.T) {
	net := NewNetwork(1)
	h := &countingHandler{}
	net.Register("b", h)
	net.DuplicateMessages("a", "b", 1.0)
	net.Transport("a").SendAppendEntries("b", raft.AppendEntriesRequest{})
	if h.n.Load() != 2 {
		t.Fatalf("delivered %d times, want 2", h.n.Load())
	}
}

func TestNetwork_SeededFaultsAreReproducible(t *testing.T) {
	run := func() []bool {
		net := NewNetwork(99)
		net.Register("b", &countingHandler{})
		net.DropMessages("a", "b", 0.5)
		var out []bool
		for i := 0; i < 64; i++ {
			_, err := net.Transport("a").SendRequestVote("b", raft.RequestVoteRequest{})
			out = append(out, err == nil)
		}
		return out
	}
	a, b := run(), run()
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("same seed produced different fault sequences at message %d", i)
		}
	}
}
