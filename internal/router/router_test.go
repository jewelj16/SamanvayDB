package router

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"
)

func TestShardFor_Deterministic(t *testing.T) {
	for i := 0; i < 1000; i++ {
		k := []byte(fmt.Sprintf("key-%d", i))
		if ShardFor(k, 7) != ShardFor(k, 7) {
			t.Fatal("same key mapped to different shards")
		}
		if s := ShardFor(k, 7); s < 0 || s >= 7 {
			t.Fatalf("shard %d out of range", s)
		}
	}
	if ShardFor([]byte("x"), 1) != 0 {
		t.Fatal("single shard must be 0")
	}
}

// TestShardFor_EvenDistribution: a large random sample spreads roughly
// evenly (chi-square-style bound: every bucket within 5% of the mean).
func TestShardFor_EvenDistribution(t *testing.T) {
	const n, shards = 200000, 8
	counts := make([]int, shards)
	for i := 0; i < n; i++ {
		counts[ShardFor([]byte(fmt.Sprintf("user:%d:%d", i, i*7919)), shards)]++
	}
	mean := float64(n) / shards
	for s, c := range counts {
		if math.Abs(float64(c)-mean)/mean > 0.05 {
			t.Errorf("shard %d got %d keys (mean %.0f)", s, c, mean)
		}
	}
}

func nodes(n int) []NodeInfo {
	var out []NodeInfo
	for i := 1; i <= n; i++ {
		out = append(out, NodeInfo{ID: fmt.Sprintf("node-%d", i), Addr: fmt.Sprintf("127.0.0.1:%d", 7000+i)})
	}
	return out
}

func TestInitialShardMap_DemoTopologyIsDisjoint(t *testing.T) {
	m, err := InitialShardMap(nodes(9), 3, 3, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]int32{}
	for _, s := range m.Shards {
		if len(s.Replicas) != 3 {
			t.Fatalf("shard %d has %d replicas", s.ShardID, len(s.Replicas))
		}
		for _, r := range s.Replicas {
			if prev, ok := seen[r]; ok {
				t.Fatalf("%s hosts shards %d and %d", r, prev, s.ShardID)
			}
			seen[r] = s.ShardID
		}
	}
	if len(m.MetaReplicas) != 3 {
		t.Fatalf("meta replicas %v", m.MetaReplicas)
	}
}

func TestInitialShardMap_SmallClusterReplicasDistinct(t *testing.T) {
	for _, tc := range []struct{ n, s, rf int }{{3, 3, 3}, {5, 3, 3}, {1, 2, 1}, {4, 5, 2}} {
		m, err := InitialShardMap(nodes(tc.n), int32(tc.s), tc.rf, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, s := range m.Shards {
			set := map[string]bool{}
			for _, r := range s.Replicas {
				if set[r] {
					t.Fatalf("%+v: shard %d has duplicate replica %s", tc, s.ShardID, r)
				}
				set[r] = true
			}
		}
	}
	if _, err := InitialShardMap(nodes(2), 1, 3, nil, nil); err == nil {
		t.Fatal("rf > nodes accepted")
	}
}

func TestShardMap_EncodeDecodeAndVersioning(t *testing.T) {
	m, _ := InitialShardMap(nodes(3), 2, 3, nil, nil)
	d, err := DecodeShardMap(m.Encode())
	if err != nil || d.Version != 1 || d.NumShards != 2 || len(d.Nodes) != 3 {
		t.Fatalf("decode: %+v %v", d, err)
	}
	c := m.Clone()
	c.Version++
	c.Shards[0].State = StateMigrating
	if m.Shards[0].State != StateStable || m.Version != 1 {
		t.Fatal("Clone shares state")
	}
	r := New(m, func(context.Context) (*ShardMap, error) { return c, nil })
	if err := r.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if r.Map().Version != 2 {
		t.Fatal("version bump not observed by router")
	}
}

// TestRouter_FollowsRedirectAndCachesHint: a request sent to a follower is
// redirected and succeeds; the next request goes straight to the leader.
func TestRouter_FollowsRedirectAndCachesHint(t *testing.T) {
	m, _ := InitialShardMap(nodes(3), 1, 3, nil, nil)
	r := New(m, nil)
	var calls []string
	attempt := func(_ context.Context, n NodeInfo) Attempt {
		calls = append(calls, n.ID)
		if n.ID != "node-3" {
			return Attempt{Outcome: NotLeader, Hint: "node-3"}
		}
		return Attempt{Outcome: Done}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := r.Do(ctx, 0, attempt); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 2 || calls[0] != "node-1" || calls[1] != "node-3" {
		t.Fatalf("first request path %v, want [node-1 node-3]", calls)
	}
	calls = nil
	if _, err := r.Do(ctx, 0, attempt); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 1 || calls[0] != "node-3" {
		t.Fatalf("second request path %v, want straight to node-3", calls)
	}
}

func TestRouter_SkipsUnavailableAndRetries(t *testing.T) {
	m, _ := InitialShardMap(nodes(3), 1, 3, nil, nil)
	r := New(m, nil)
	attempts := 0
	_, err := r.Do(context.Background(), 0, func(_ context.Context, n NodeInfo) Attempt {
		attempts++
		if attempts < 5 {
			return Attempt{Outcome: Unavailable, Err: errors.New("down")}
		}
		return Attempt{Outcome: Done}
	})
	if err != nil || attempts != 5 {
		t.Fatalf("err=%v attempts=%d", err, attempts)
	}
}

func TestRouter_FinalErrorNotRetried(t *testing.T) {
	m, _ := InitialShardMap(nodes(3), 1, 3, nil, nil)
	r := New(m, nil)
	invalid := errors.New("invalid request")
	n := 0
	_, err := r.Do(context.Background(), 0, func(context.Context, NodeInfo) Attempt {
		n++
		return Attempt{Outcome: Done, Err: invalid}
	})
	if !errors.Is(err, invalid) || n != 1 {
		t.Fatalf("err=%v attempts=%d", err, n)
	}
}

func TestRouter_DeadlineYieldsNoRoute(t *testing.T) {
	m, _ := InitialShardMap(nodes(3), 1, 3, nil, nil)
	r := New(m, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err := r.Do(ctx, 0, func(context.Context, NodeInfo) Attempt {
		return Attempt{Outcome: Unavailable, Err: errors.New("no quorum")}
	})
	if !errors.Is(err, ErrNoRoute) {
		t.Fatalf("err=%v, want ErrNoRoute", err)
	}
}
