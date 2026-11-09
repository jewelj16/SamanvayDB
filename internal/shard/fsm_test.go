package shard

import (
	"bytes"
	"testing"

	"samanvaydb/internal/command"
	"samanvaydb/internal/raft"
	"samanvaydb/internal/storage"
)

func applyCmd(t *testing.T, f *FSM, idx uint64, c command.Command) Result {
	t.Helper()
	data, err := c.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	r, _ := f.Apply(raft.LogEntry{Index: idx, Term: 1, Type: raft.EntryNormal, Command: data}).(Result)
	return r
}

func TestFSM_PutGetDelete(t *testing.T) {
	f := NewFSM(storage.NewMemEngine())
	applyCmd(t, f, 1, command.Command{Type: command.CmdPut, Key: []byte("k"), Value: []byte("v")})
	if v, ok := f.Get([]byte("k")); !ok || string(v) != "v" {
		t.Fatalf("Get = %q %v", v, ok)
	}
	applyCmd(t, f, 2, command.Command{Type: command.CmdDelete, Key: []byte("k")})
	if _, ok := f.Get([]byte("k")); ok {
		t.Fatal("found after delete")
	}
}

func TestFSM_NonNormalEntriesIgnored(t *testing.T) {
	f := NewFSM(storage.NewMemEngine())
	f.Apply(raft.LogEntry{Index: 1, Type: raft.EntryNoop})
	f.Apply(raft.LogEntry{Index: 2, Type: raft.EntryConfig, Command: raft.NewMembership("a").Encode()})
	if f.Len() != 0 {
		t.Fatal("non-normal entries changed state")
	}
}

func TestFSM_CAS(t *testing.T) {
	f := NewFSM(storage.NewMemEngine())
	tests := []struct {
		name     string
		cmd      command.Command
		wantCode ResultCode
		wantVal  string
	}{
		{"absent_ok", command.Command{Type: command.CmdCAS, Key: []byte("k"), Value: []byte("1"), ExpectAbsent: true}, ResultOK, "1"},
		{"absent_fails_when_present", command.Command{Type: command.CmdCAS, Key: []byte("k"), Value: []byte("x"), ExpectAbsent: true}, ResultCASFailed, "1"},
		{"match_ok", command.Command{Type: command.CmdCAS, Key: []byte("k"), Value: []byte("2"), Expected: []byte("1")}, ResultOK, "2"},
		{"mismatch_fails", command.Command{Type: command.CmdCAS, Key: []byte("k"), Value: []byte("3"), Expected: []byte("1")}, ResultCASFailed, "2"},
	}
	for i, tt := range tests {
		r := applyCmd(t, f, uint64(i+1), tt.cmd)
		if r.Code != tt.wantCode {
			t.Errorf("%s: code=%v want %v", tt.name, r.Code, tt.wantCode)
		}
		if v, _ := f.Get([]byte("k")); string(v) != tt.wantVal {
			t.Errorf("%s: value=%q want %q", tt.name, v, tt.wantVal)
		}
	}
}

func TestFSM_TTLExpiryRevalidatesAtApply(t *testing.T) {
	f := NewFSM(storage.NewMemEngine())
	applyCmd(t, f, 1, command.Command{Type: command.CmdPut, Key: []byte("k"), Value: []byte("v"), TimestampMs: 100, ExpiresAtMs: 200})
	if c := f.Expired(150, 10); len(c) != 0 {
		t.Fatal("key reported expired before its deadline")
	}
	if c := f.Expired(250, 10); len(c) != 1 || c[0].ExpiresAt != 200 {
		t.Fatalf("Expired = %+v", c)
	}
	// Expired by wall clock, but still visible until the expiry COMMITS.
	if _, ok := f.Get([]byte("k")); !ok {
		t.Fatal("TTL key vanished without a committed expiry")
	}
	// A stale expiry (key overwritten with a new deadline) is a no-op.
	applyCmd(t, f, 2, command.Command{Type: command.CmdPut, Key: []byte("k"), Value: []byte("v2"), TimestampMs: 220, ExpiresAtMs: 900})
	if r := applyCmd(t, f, 3, command.Command{Type: command.CmdExpire, Key: []byte("k"), ExpiresAtMs: 200, TimestampMs: 260}); r.Code != ResultNoop {
		t.Fatalf("stale expiry applied: %v", r.Code)
	}
	if r := applyCmd(t, f, 4, command.Command{Type: command.CmdExpire, Key: []byte("k"), ExpiresAtMs: 900, TimestampMs: 950}); r.Code != ResultOK {
		t.Fatalf("valid expiry: %v", r.Code)
	}
	if _, ok := f.Get([]byte("k")); ok {
		t.Fatal("key still present after committed expiry")
	}
}

func TestFSM_LockLifecycle(t *testing.T) {
	f := NewFSM(storage.NewMemEngine())
	key := LockKey("L")
	acq := func(idx uint64, owner string, ts uint64) Result {
		return applyCmd(t, f, idx, command.Command{Type: command.CmdLockAcquire, Key: key, Owner: []byte(owner), TTLMs: 1000, TimestampMs: ts})
	}
	r := acq(10, "a", 100)
	if r.Code != ResultOK || r.Token != 10 || r.ExpiresAtMs != 1100 {
		t.Fatalf("acquire: %+v", r)
	}
	if r := acq(11, "b", 200); r.Code != ResultAlreadyHeld || string(r.Holder) != "a" {
		t.Fatalf("second acquire must lose: %+v", r)
	}
	// Renew keeps the token.
	r = applyCmd(t, f, 12, command.Command{Type: command.CmdLockRenew, Key: key, Owner: []byte("a"), TTLMs: 1000, TimestampMs: 500})
	if r.Code != ResultOK || r.Token != 10 || r.ExpiresAtMs != 1500 {
		t.Fatalf("renew: %+v", r)
	}
	if r := applyCmd(t, f, 13, command.Command{Type: command.CmdLockRenew, Key: key, Owner: []byte("b"), TTLMs: 1000, TimestampMs: 600}); r.Code != ResultNotOwner {
		t.Fatalf("renew by non-owner: %v", r.Code)
	}
	if r := applyCmd(t, f, 14, command.Command{Type: command.CmdLockRelease, Key: key, Owner: []byte("b")}); r.Code != ResultNotOwner {
		t.Fatalf("release by non-owner: %v", r.Code)
	}
	// Expiry is a no-op before the deadline or for a stale token.
	if r := applyCmd(t, f, 15, command.Command{Type: command.CmdLockExpire, Key: key, Owner: []byte("a"), Token: 10, TimestampMs: 1400}); r.Code != ResultNoop {
		t.Fatalf("early expire: %v", r.Code)
	}
	if r := applyCmd(t, f, 16, command.Command{Type: command.CmdLockExpire, Key: key, Owner: []byte("a"), Token: 9, TimestampMs: 1600}); r.Code != ResultNoop {
		t.Fatalf("stale-token expire: %v", r.Code)
	}
	woke := ""
	f.OnRelease = func(k string) { woke = k }
	if r := applyCmd(t, f, 17, command.Command{Type: command.CmdLockExpire, Key: key, Owner: []byte("a"), Token: 10, TimestampMs: 1600}); r.Code != ResultOK {
		t.Fatalf("expire: %v", r.Code)
	}
	if woke != string(key) {
		t.Error("expiry did not wake waiters")
	}
	// Re-acquire mints a strictly larger token.
	if r := acq(18, "b", 1700); r.Code != ResultOK || r.Token != 18 {
		t.Fatalf("re-acquire: %+v", r)
	}
	// Release is idempotent.
	applyCmd(t, f, 19, command.Command{Type: command.CmdLockRelease, Key: key, Owner: []byte("b")})
	if r := applyCmd(t, f, 20, command.Command{Type: command.CmdLockRelease, Key: key, Owner: []byte("b")}); r.Code != ResultOK {
		t.Fatalf("idempotent release: %v", r.Code)
	}
}

func TestFSM_LockRecordRoundTrip(t *testing.T) {
	in := LockRecord{OwnerID: []byte{0, 1, 'x'}, ExpiresAtMs: 7, FencingToken: 1 << 40, AcquiredAtMs: 3}
	out, err := DecodeLockRecord(EncodeLockRecord(in))
	if err != nil || !bytes.Equal(out.OwnerID, in.OwnerID) || out.ExpiresAtMs != 7 || out.FencingToken != 1<<40 || out.AcquiredAtMs != 3 {
		t.Fatalf("round trip: %+v %v", out, err)
	}
}

// TestFSM_DeterministicAcrossReplicas applies the same command stream to two
// FSMs and requires byte-identical snapshots.
func TestFSM_DeterministicAcrossReplicas(t *testing.T) {
	cmds := []command.Command{
		{Type: command.CmdPut, Key: []byte("a"), Value: []byte("1"), TimestampMs: 5, ExpiresAtMs: 50},
		{Type: command.CmdPut, Key: []byte("b"), Value: []byte("2")},
		{Type: command.CmdCAS, Key: []byte("b"), Value: []byte("3"), Expected: []byte("2")},
		{Type: command.CmdLockAcquire, Key: LockKey("l"), Owner: []byte("o"), TTLMs: 10, TimestampMs: 6},
		{Type: command.CmdLockAcquire, Key: LockKey("l"), Owner: []byte("p"), TTLMs: 10, TimestampMs: 7},
		{Type: command.CmdExpire, Key: []byte("a"), ExpiresAtMs: 50, TimestampMs: 60},
		{Type: command.CmdDelete, Key: []byte("zz")},
	}
	a, b := NewFSM(storage.NewMemEngine()), NewFSM(storage.NewMemEngine())
	for i, c := range cmds {
		ra := applyCmd(t, a, uint64(i+1), c)
		rb := applyCmd(t, b, uint64(i+1), c)
		if ra.Code != rb.Code || ra.Token != rb.Token {
			t.Fatalf("cmd %d: results differ %+v vs %+v", i, ra, rb)
		}
	}
	sa, _ := a.Snapshot()
	sb, _ := b.Snapshot()
	if !bytes.Equal(sa, sb) {
		t.Fatal("replicas diverged")
	}
	// Restore rebuilds the derived deadline index.
	c := NewFSM(storage.NewMemEngine())
	if err := c.Restore(sa); err != nil {
		t.Fatal(err)
	}
	if got := c.Expired(1000, 10); len(got) != 1 || !got[0].IsLock {
		t.Fatalf("deadline index not rebuilt: %+v", got)
	}
}

func TestReservedKeyRejected(t *testing.T) {
	if err := validateUserKey([]byte(LockPrefix + "x")); err == nil {
		t.Fatal("reserved prefix accepted")
	}
	if err := validateUserKey(nil); err == nil {
		t.Fatal("empty key accepted")
	}
	if err := validateUserKey([]byte("__lock_not_quite")); err != nil {
		t.Fatalf("near-miss rejected: %v", err)
	}
}

// TestFSM_SessionDedupPreventsDoubleApply reproduces the anomaly the
// linearizability checker found: a retried DELETE whose first attempt
// committed must not apply AGAIN after another client's write.
func TestFSM_SessionDedupPreventsDoubleApply(t *testing.T) {
	f := NewFSM(storage.NewMemEngine())
	del := command.Command{Type: command.CmdDelete, Key: []byte("w"), ClientID: []byte("c0"), Seq: 7, MinSeq: 7, TimestampMs: 1}
	applyCmd(t, f, 1, command.Command{Type: command.CmdPut, Key: []byte("w"), Value: []byte("old")})
	applyCmd(t, f, 2, del) // attempt 1 commits (reply lost)
	applyCmd(t, f, 3, command.Command{Type: command.CmdPut, Key: []byte("w"), Value: []byte("c2-1071"),
		ClientID: []byte("c2"), Seq: 1, MinSeq: 1, TimestampMs: 2})
	applyCmd(t, f, 4, del) // the retry commits too: must be a no-op
	if v, ok := f.Get([]byte("w")); !ok || string(v) != "c2-1071" {
		t.Fatalf("retried delete re-applied: w=%q found=%v", v, ok)
	}
	if r, ok := f.CachedResult([]byte("c0"), 7); !ok || r.Code != ResultOK {
		t.Fatalf("cached result: %+v %v", r, ok)
	}
}

func TestFSM_SessionCachesCASOutcomeAndPrunes(t *testing.T) {
	f := NewFSM(storage.NewMemEngine())
	applyCmd(t, f, 1, command.Command{Type: command.CmdPut, Key: []byte("k"), Value: []byte("a")})
	cas := command.Command{Type: command.CmdCAS, Key: []byte("k"), Expected: []byte("a"), Value: []byte("b"),
		ClientID: []byte("cl"), Seq: 3, MinSeq: 3}
	if r := applyCmd(t, f, 2, cas); r.Code != ResultOK {
		t.Fatalf("first CAS: %v", r.Code)
	}
	// The retry would now fail its precondition (k == b); it must instead
	// report the ORIGINAL outcome.
	if r := applyCmd(t, f, 3, cas); r.Code != ResultOK {
		t.Fatalf("retried CAS re-evaluated: %v", r.Code)
	}
	// A later request with MinSeq 10 lets the session forget seq 3.
	applyCmd(t, f, 4, command.Command{Type: command.CmdPut, Key: []byte("x"), Value: []byte("1"), ClientID: []byte("cl"), Seq: 10, MinSeq: 10})
	if rec := f.loadSession([]byte("cl")); len(rec.Results) != 1 || rec.MinSeq != 10 {
		t.Fatalf("session not pruned: %+v", rec)
	}
	// Session records are invisible to users and survive snapshot/restore.
	if len(f.Keys()) != 2 {
		t.Fatalf("user keys = %v", f.Keys())
	}
	snap, _ := f.Snapshot()
	g := NewFSM(storage.NewMemEngine())
	g.Restore(snap)
	if _, ok := g.CachedResult([]byte("cl"), 10); !ok {
		t.Fatal("session table lost across snapshot/restore")
	}
	if err := validateUserKey([]byte(SessionPrefix + "x")); err == nil {
		t.Fatal("session prefix not reserved")
	}
}

func TestFSM_IdleSessionsExpireDeterministically(t *testing.T) {
	f := NewFSM(storage.NewMemEngine())
	applyCmd(t, f, 5, command.Command{Type: command.CmdPut, Key: []byte("a"), Value: []byte("1"), ClientID: []byte("old"), Seq: 1, TimestampMs: 1000})
	later := uint64(1000 + SessionTTLMs + 1)
	// The purge runs at a fixed log index, using the leader's timestamp.
	applyCmd(t, f, sessionPurgeEvery*3, command.Command{Type: command.CmdPut, Key: []byte("b"), Value: []byte("2"),
		ClientID: []byte("new"), Seq: 1, TimestampMs: later})
	if _, ok := f.CachedResult([]byte("old"), 1); ok {
		t.Fatal("idle session not expired")
	}
	if _, ok := f.CachedResult([]byte("new"), 1); !ok {
		t.Fatal("active session expired")
	}
}
