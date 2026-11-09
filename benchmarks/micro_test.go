package benchmarks

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"samanvaydb/internal/command"
	"samanvaydb/internal/config"
	"samanvaydb/internal/logging"
	"samanvaydb/internal/raft"
	"samanvaydb/internal/shard"
	"samanvaydb/internal/storage"
	"samanvaydb/internal/testcluster"
	"samanvaydb/pkg/client"
)

func quiet() *logging.Logger {
	return logging.NewLogger(logging.Options{NodeID: "bench", Level: slog.LevelError + 4})
}

func testRaft() config.RaftConfig { return config.RaftConfig{SnapshotThreshold: 20000} }

// ---- storage engine (the map) ----

func BenchmarkEngine_Put(b *testing.B) {
	e := storage.NewMemEngine()
	v := make([]byte, 128)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		e.Put([]byte(fmt.Sprintf("k%d", i%100000)), v)
	}
}

func BenchmarkEngine_Get(b *testing.B) {
	e := storage.NewMemEngine()
	for i := 0; i < 100000; i++ {
		e.Put([]byte(fmt.Sprintf("k%d", i)), make([]byte, 128))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.Get([]byte(fmt.Sprintf("k%d", i%100000)))
	}
}

func BenchmarkCommand_Marshal(b *testing.B) {
	c := command.Command{Type: command.CmdPut, Key: []byte("user:1001"), Value: make([]byte, 128)}
	for i := 0; i < b.N; i++ {
		c.Marshal()
	}
}

// ---- write latency split: WAL fsync vs Raft round trip ----

// BenchmarkRaftLog_Append measures one durable Raft log append — the
// "WAL fsync" component of every write's latency.
func BenchmarkRaftLog_Append(b *testing.B) {
	for _, sync := range []bool{true, false} {
		b.Run(fmt.Sprintf("fsync=%v", sync), func(b *testing.B) {
			s, err := raft.OpenFileStorage(b.TempDir(), raft.FileStorageOptions{NoSync: !sync})
			if err != nil {
				b.Fatal(err)
			}
			defer s.Close()
			data := make([]byte, 150)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := s.Append([]raft.LogEntry{{Index: uint64(i + 1), Term: 1, Command: data}}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkPut_EndToEnd measures a client PUT through the full stack
// (gRPC -> leader -> fsync -> replicate -> follower fsync -> commit ->
// apply) for 1 and 3 replicas. Subtracting the fsync benchmark above
// approximates the Raft round-trip contribution.
func BenchmarkPut_EndToEnd(b *testing.B) {
	for _, rf := range []int{1, 3} {
		b.Run(fmt.Sprintf("replicas=%d", rf), func(b *testing.B) {
			c := testcluster.Start(b, testcluster.Options{Nodes: rf, Shards: 1, RF: rf, Sync: true, Raft: testRaft()})
			cl := c.Client()
			ctx := context.Background()
			v := make([]byte, 128)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := cl.Put(ctx, []byte(fmt.Sprintf("k%d", i)), v); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkGet_Consistency compares linearizable (ReadIndex) leader reads
// with follower reads.
func BenchmarkGet_Consistency(b *testing.B) {
	c := testcluster.Start(b, testcluster.Options{Nodes: 3, Shards: 1, Raft: testRaft()})
	cl := c.Client()
	ctx := context.Background()
	cl.Put(ctx, []byte("hot"), []byte("v"))
	b.Run("leader", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			cl.Get(ctx, []byte("hot"))
		}
	})
	b.Run("follower-ok", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			cl.Get(ctx, []byte("hot"), client.WithFollowerOK())
		}
	})
}

// ---- recovery time vs log entries since the last snapshot ----

// TestRecoveryTime reports how long a replica takes to become serviceable
// after a restart, as a function of the Raft log tail it must replay.
func TestRecoveryTime(t *testing.T) {
	if testing.Short() {
		t.Skip("slow")
	}
	logger := quiet()
	for _, n := range []int{1000, 10000, 50000} {
		dir := t.TempDir()
		mk := func() *shard.Shard {
			boot := raft.NewMembership("solo")
			s, err := shard.New(shard.Config{ShardID: 0, NodeID: "solo", RaftDir: dir, NoSync: true, Bootstrap: &boot,
				Logger: logger, ElectionTimeoutMin: 50 * time.Millisecond, ElectionTimeoutMax: 100 * time.Millisecond,
				HeartbeatInterval: 10 * time.Millisecond, SnapshotThreshold: 1 << 30})
			if err != nil {
				t.Fatal(err)
			}
			s.Start()
			return s
		}
		s := mk()
		for !s.Raft().IsLeader() {
			time.Sleep(5 * time.Millisecond)
		}
		for i := 0; i < n; i++ {
			data, _ := (&command.Command{Type: command.CmdPut, Key: []byte(fmt.Sprintf("k%d", i)), Value: make([]byte, 64)}).Marshal()
			if _, _, err := s.Raft().Propose(data); err != nil {
				t.Fatal(err)
			}
		}
		target := s.Raft().Status().LastLogIndex
		s.Raft().WaitApplied(target, 30*time.Second)
		s.Stop()

		start := time.Now()
		r := mk()
		if !r.Raft().WaitApplied(target, 60*time.Second) {
			t.Fatalf("recovery of %d entries did not finish", n)
		}
		el := time.Since(start)
		t.Logf("recovered %6d log entries (no snapshot) in %v (%.0f entries/s); keys=%d",
			n, el.Round(time.Millisecond), float64(n)/el.Seconds(), r.FSM().Len())
		r.Stop()
	}
}
