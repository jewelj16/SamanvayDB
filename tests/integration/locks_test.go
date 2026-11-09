package integration

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"samanvaydb/internal/faultinjector"
	"samanvaydb/internal/testcluster"
	"samanvaydb/pkg/client"
)

// protectedResource is a mock resource guarded by fencing tokens: it
// rejects any request whose token is lower than the highest it has seen.
type protectedResource struct {
	mu       sync.Mutex
	highest  uint64
	counter  int
	accepted []access
	rejected []access
}

type access struct {
	client string
	token  uint64
}

func (r *protectedResource) Do(client string, token uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if token < r.highest {
		r.rejected = append(r.rejected, access{client, token})
		return false
	}
	r.highest = token
	r.counter++
	r.accepted = append(r.accepted, access{client, token})
	return true
}

func lockShardLeader(t *testing.T, c *testcluster.Cluster, cl *client.Client, name string) string {
	t.Helper()
	l, err := c.WaitLeader(cl.ShardFor([]byte("__lock__:"+name)), 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

// TestLocks_FailoverPreservesStateAndToken: acquire -> kill leader -> the
// new leader serves the same lock state, fencing token unchanged.
func TestLocks_FailoverPreservesStateAndToken(t *testing.T) {
	c := testcluster.Start(t, testcluster.Options{Nodes: 3, Shards: 2})
	cl := c.Client()
	cx := ctx(t, time.Minute)
	info, err := cl.TryLock(cx, "res", "alice", 10*time.Second)
	if err != nil || info.FencingToken == 0 {
		t.Fatalf("acquire: %+v %v", info, err)
	}
	c.Kill(lockShardLeader(t, c, cl, "res"))
	lockShardLeader(t, c, cl, "res")
	other, err := cl.TryLock(cx, "res", "bob", time.Second)
	if !errors.Is(err, client.ErrAlreadyHeld) || other.Holder != "alice" {
		t.Fatalf("after failover bob got %+v %v; want ALREADY_HELD by alice", other, err)
	}
	renew, err := cl.RenewLock(cx, "res", "alice", 10*time.Second)
	if err != nil || renew.FencingToken != info.FencingToken {
		t.Fatalf("renew after failover: %+v %v (token was %d)", renew, err, info.FencingToken)
	}
}

// TestLocks_TTLLapseExpiresViaCommittedCommand: acquire, never renew; the
// leader's ticker commits the expiry; a second client can then acquire,
// with a strictly larger fencing token.
func TestLocks_TTLLapseExpiresViaCommittedCommand(t *testing.T) {
	c := testcluster.Start(t, testcluster.Options{Nodes: 3, Shards: 1})
	cl := c.Client()
	cx := ctx(t, time.Minute)
	first, err := cl.TryLock(cx, "lease", "a", 400*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		second, err := cl.TryLock(cx, "lease", "b", 10*time.Second)
		if err == nil {
			if second.FencingToken <= first.FencingToken {
				t.Fatalf("token did not increase: %d -> %d", first.FencingToken, second.FencingToken)
			}
			break
		}
		if !errors.Is(err, client.ErrAlreadyHeld) || time.Now().After(deadline) {
			t.Fatalf("lock never expired: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	// "a" lost it: renew must fail.
	if _, err := cl.RenewLock(cx, "lease", "a", time.Second); !errors.Is(err, client.ErrNotOwner) {
		t.Fatalf("renew by expired holder: %v", err)
	}
}

// TestLocks_MinorityLeaderCannotGrant: a leader partitioned into the
// minority cannot commit an acquire; the majority-side leader can.
func TestLocks_MinorityLeaderCannotGrant(t *testing.T) {
	c := testcluster.Start(t, testcluster.Options{Nodes: 3, Shards: 1})
	cl := c.Client()
	cx := ctx(t, time.Minute)
	old, err := faultinjector.IsolateLeader(c.Injector(), c.LeaderLookup(), 0, c.IDs())
	if err != nil {
		t.Fatal(err)
	}
	s, _ := c.Node(old).Manager().Get(0)
	lctx, cancel := context.WithTimeout(cx, 700*time.Millisecond)
	_, err = s.LockAcquire(lctx, "m", "minority-client", 5*time.Second)
	cancel()
	if err == nil {
		t.Fatal("minority leader granted a lock")
	}
	if _, err := cl.TryLock(cx, "m", "majority-client", 5*time.Second); err != nil {
		t.Fatalf("majority side could not grant: %v", err)
	}
	c.Heal()
}

// TestLocks_FencingTokenCapstone — the lock-subsystem checkpoint: two
// clients race to hold the same named lock in a loop, incrementing a
// counter in a mock resource guarded by fencing tokens. Mid-run we kill the
// lock shard's leader AND simulate a GC pause: a third client acquires the
// lock, stalls past its TTL without renewing, then keeps issuing
// "protected" requests with its stale token. The resource must accept only
// one holder's requests at a time and refuse the stale-token requests.
func TestLocks_FencingTokenCapstone(t *testing.T) {
	c := testcluster.Start(t, testcluster.Options{Nodes: 3, Shards: 2})
	res := &protectedResource{}
	const lockName = "capstone"
	const ttl = 600 * time.Millisecond

	// The GC-pause client goes first so it is guaranteed to hold the lock once.
	gcClient := c.Client()
	cx := ctx(t, 2*time.Minute)
	gcLock, err := gcClient.TryLock(cx, lockName, "gc-client", ttl)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Do("gc-client", gcLock.FencingToken) {
		t.Fatal("first protected request refused")
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for _, owner := range []string{"worker-a", "worker-b"} {
		cl := c.Client()
		wg.Add(1)
		go func(owner string, cl *client.Client) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				actx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				h, err := cl.AcquireLock(actx, lockName, owner, ttl)
				cancel()
				if err != nil {
					continue // contention / failover: try again
				}
				for i := 0; i < 3 && h.Valid(); i++ {
					res.Do(owner, h.FencingToken())
					time.Sleep(20 * time.Millisecond)
				}
				rctx, rcancel := context.WithTimeout(context.Background(), 3*time.Second)
				h.Release(rctx)
				rcancel()
			}
		}(owner, cl)
	}

	// GC pause: the stalled client does nothing while its lease lapses.
	time.Sleep(3 * ttl)
	// Inject a leader kill while workers contend.
	victim := lockShardLeader(t, c, gcClient, lockName)
	c.Kill(victim)
	time.Sleep(1500 * time.Millisecond)
	// The stalled client "wakes up" believing it still holds the lock.
	staleRejected := 0
	for i := 0; i < 3; i++ {
		if !res.Do("gc-client", gcLock.FencingToken) {
			staleRejected++
		}
	}
	time.Sleep(500 * time.Millisecond)
	close(stop)
	wg.Wait()

	res.mu.Lock()
	defer res.mu.Unlock()
	t.Logf("accepted %d, rejected %d requests; highest token %d", len(res.accepted), len(res.rejected), res.highest)
	if staleRejected != 3 {
		t.Fatalf("stale-token requests accepted: %d of 3 rejected", staleRejected)
	}
	if res.counter != len(res.accepted) {
		t.Fatalf("counter %d != accepted %d", res.counter, len(res.accepted))
	}
	// One holder at a time: tokens never decrease, and every token's
	// accepted requests come from exactly one client.
	holder := map[uint64]string{}
	var last uint64
	for _, a := range res.accepted {
		if a.token < last {
			t.Fatalf("accepted token went backwards %d -> %d", last, a.token)
		}
		last = a.token
		if h, ok := holder[a.token]; ok && h != a.client {
			t.Fatalf("token %d used by both %s and %s", a.token, h, a.client)
		}
		holder[a.token] = a.client
	}
	if len(holder) < 3 {
		t.Fatalf("only %d distinct lock holders observed; the race did not exercise handoffs", len(holder))
	}
}

// TestLocks_HandleAutoRenewKeepsLockAlive: a LockHandle holds a short-TTL
// lock across many TTL periods via background renewal.
func TestLocks_HandleAutoRenewKeepsLockAlive(t *testing.T) {
	c := testcluster.Start(t, testcluster.Options{Nodes: 3, Shards: 1})
	cl := c.Client()
	cx := ctx(t, time.Minute)
	h, err := cl.AcquireLock(cx, "long", "holder", 300*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Second) // ~6 TTLs
	if !h.Valid() {
		t.Fatal("auto-renewal failed to keep the lock")
	}
	if _, err := cl.TryLock(cx, "long", "intruder", time.Second); !errors.Is(err, client.ErrAlreadyHeld) {
		t.Fatalf("intruder: %v", err)
	}
	if err := h.Release(cx); err != nil {
		t.Fatal(err)
	}
	if _, err := cl.TryLock(cx, "long", "intruder", time.Second); err != nil {
		t.Fatalf("after release: %v", err)
	}
}

// TestLocks_WaitUnblocksOnRelease exercises the WAIT long-poll end to end.
func TestLocks_WaitUnblocksOnRelease(t *testing.T) {
	c := testcluster.Start(t, testcluster.Options{Nodes: 3, Shards: 1})
	cl := c.Client()
	cx := ctx(t, time.Minute)
	if _, err := cl.TryLock(cx, "w", "a", 10*time.Second); err != nil {
		t.Fatal(err)
	}
	done := make(chan bool, 1)
	go func() {
		ok, _ := cl.WaitLock(cx, "w", 5*time.Second)
		done <- ok
	}()
	time.Sleep(200 * time.Millisecond)
	if err := cl.Unlock(cx, "w", "a"); err != nil {
		t.Fatal(err)
	}
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("wait returned without release")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("waiter not woken")
	}
}
