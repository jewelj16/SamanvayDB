package shard

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"sync"
	"time"

	"samanvaydb/internal/command"
	"samanvaydb/internal/raft"
	"samanvaydb/internal/storage"
)

// LockPrefix is the reserved key prefix under which lock records live.
// Client PUT/GET/DELETE/CAS requests targeting it are rejected at the API
// boundary; only the lock commands may touch these keys.
const LockPrefix = "__lock__:"

// IsReservedKey reports whether key is in a reserved namespace (lock
// records, client sessions).
func IsReservedKey(key []byte) bool {
	return bytes.HasPrefix(key, []byte(LockPrefix)) || bytes.HasPrefix(key, []byte(SessionPrefix))
}

// LockKey returns the storage key for a lock name.
func LockKey(name string) []byte { return []byte(LockPrefix + name) }

// ResultCode is the deterministic outcome of applying one command.
type ResultCode int

const (
	ResultOK ResultCode = iota
	ResultCASFailed
	ResultAlreadyHeld
	ResultNotOwner
	ResultLockExpired
	ResultInvalid
	ResultNoop // expiry command found nothing (stale) to expire
)

func (c ResultCode) String() string {
	return [...]string{"OK", "CAS_FAILED", "ALREADY_HELD", "NOT_OWNER", "LOCK_EXPIRED", "INVALID", "NOOP"}[c]
}

// Result is what Apply returns for a command; it is delivered to the
// proposer and, for every replica, is a pure function of (state, command).
type Result struct {
	Code ResultCode
	// CAS: the value present when the CAS was applied.
	Found bool
	Value []byte
	// Locks.
	Token       uint64
	ExpiresAtMs uint64
	Holder      []byte
}

// LockRecord is the value stored under "__lock__:"+name.
type LockRecord struct {
	OwnerID      []byte
	ExpiresAtMs  uint64 // leader-assigned deadline
	FencingToken uint64 // == the Raft log index of the acquire entry
	AcquiredAtMs uint64 // informational only
}

// Value envelope stored in the engine:
//
//	kind(1) expiresAtMs(8) payload
//
// kind 0: plain value (payload = user value)
// kind 1: lock record (payload = encoded LockRecord minus ExpiresAtMs)
const (
	kindValue byte = 0
	kindLock  byte = 1
	envHeader      = 9
)

func encodeValue(v []byte, expiresAt uint64) []byte {
	out := make([]byte, envHeader+len(v))
	out[0] = kindValue
	binary.BigEndian.PutUint64(out[1:], expiresAt)
	copy(out[envHeader:], v)
	return out
}

// EncodeLockRecord serializes a lock record into an engine value.
func EncodeLockRecord(r LockRecord) []byte {
	out := make([]byte, envHeader, envHeader+4+len(r.OwnerID)+16)
	out[0] = kindLock
	binary.BigEndian.PutUint64(out[1:], r.ExpiresAtMs)
	out = binary.BigEndian.AppendUint32(out, uint32(len(r.OwnerID)))
	out = append(out, r.OwnerID...)
	out = binary.BigEndian.AppendUint64(out, r.FencingToken)
	out = binary.BigEndian.AppendUint64(out, r.AcquiredAtMs)
	return out
}

// DecodeLockRecord parses an engine value produced by EncodeLockRecord.
func DecodeLockRecord(b []byte) (LockRecord, error) {
	var r LockRecord
	if len(b) < envHeader+4+16 || b[0] != kindLock {
		return r, fmt.Errorf("shard: not a lock record")
	}
	r.ExpiresAtMs = binary.BigEndian.Uint64(b[1:])
	n := int(binary.BigEndian.Uint32(b[envHeader:]))
	off := envHeader + 4
	if off+n+16 != len(b) {
		return r, fmt.Errorf("shard: corrupt lock record")
	}
	r.OwnerID = append([]byte(nil), b[off:off+n]...)
	off += n
	r.FencingToken = binary.BigEndian.Uint64(b[off:])
	r.AcquiredAtMs = binary.BigEndian.Uint64(b[off+8:])
	return r, nil
}

func decodeEnvelope(b []byte) (kind byte, expiresAt uint64, payload []byte, err error) {
	if len(b) < envHeader {
		return 0, 0, nil, fmt.Errorf("shard: corrupt value envelope")
	}
	return b[0], binary.BigEndian.Uint64(b[1:]), b[envHeader:], nil
}

// ExpiryCandidate is a key whose leader-assigned deadline has passed.
type ExpiryCandidate struct {
	Key       []byte
	IsLock    bool
	ExpiresAt uint64
	Owner     []byte
	Token     uint64
}

// FSM is the shard's replicated state machine: a storage.Engine plus the
// command semantics. It implements raft.StateMachine.
//
// DETERMINISM: Apply is a pure function of (engine state, command). Every
// time comparison uses cmd.TimestampMs — the leader's clock reading frozen
// into the command at propose time — never this replica's clock. (A future
// "PUT with server-generated timestamp" feature must follow the same rule:
// decide the timestamp once on the leader and embed it in the command.)
// The only wall-clock read below feeds the apply-lag metric and never
// influences state.
type FSM struct {
	engine storage.Engine

	mu        sync.Mutex
	deadlines map[string]uint64 // derived index of keys with a deadline

	// OnRelease is called (outside determinism concerns: it only wakes
	// leader-local waiters) when a lock is released or expired.
	OnRelease func(key string)
	// OnApplyLag observes now - cmd.TimestampMs for each applied write
	// (replication + apply lag, for metrics only).
	OnApplyLag func(d time.Duration)
}

// NewFSM wraps an engine.
func NewFSM(engine storage.Engine) *FSM {
	return &FSM{engine: engine, deadlines: map[string]uint64{}}
}

// Engine exposes the underlying engine (read-only use by status/metrics).
func (f *FSM) Engine() storage.Engine { return f.engine }

var _ raft.StateMachine = (*FSM)(nil)

// Apply applies one committed Raft entry.
func (f *FSM) Apply(entry raft.LogEntry) any {
	if entry.Type != raft.EntryNormal {
		return nil
	}
	cmd, err := command.Unmarshal(entry.Command)
	if err != nil {
		return Result{Code: ResultInvalid}
	}
	if f.OnApplyLag != nil && cmd.TimestampMs > 0 {
		f.OnApplyLag(time.Since(time.UnixMilli(int64(cmd.TimestampMs))))
	}
	return f.applyWithSession(entry.Index, cmd, func() Result { return f.applyCommand(entry, cmd) })
}

// applyCommand applies one decoded command (after session dedup).
func (f *FSM) applyCommand(entry raft.LogEntry, cmd *command.Command) Result {
	ts := cmd.TimestampMs

	switch cmd.Type {
	case command.CmdPut:
		f.put(cmd.Key, encodeValue(cmd.Value, cmd.ExpiresAtMs), cmd.ExpiresAtMs)
		return Result{Code: ResultOK}

	case command.CmdDelete:
		f.del(cmd.Key)
		return Result{Code: ResultOK}

	case command.CmdCAS:
		cur, found := f.readValue(cmd.Key)
		ok := (cmd.ExpectAbsent && !found) || (!cmd.ExpectAbsent && found && bytes.Equal(cur, cmd.Expected))
		if !ok {
			return Result{Code: ResultCASFailed, Found: found, Value: cur}
		}
		f.put(cmd.Key, encodeValue(cmd.Value, 0), 0)
		return Result{Code: ResultOK}

	case command.CmdExpire:
		raw, found, _ := f.engine.Get(cmd.Key)
		if !found {
			return Result{Code: ResultNoop}
		}
		kind, exp, _, err := decodeEnvelope(raw)
		// Re-validate: only expire if the key still carries the deadline the
		// ticker saw (it may have been overwritten) and it has passed.
		if err != nil || kind != kindValue || exp == 0 || exp != cmd.ExpiresAtMs || exp > ts {
			return Result{Code: ResultNoop}
		}
		f.del(cmd.Key)
		return Result{Code: ResultOK}

	case command.CmdLockAcquire:
		// Apply-time re-validation: the leader-side check happened before
		// proposing, so two acquires can both pass it. Raft totally orders
		// them; whichever applies first wins on EVERY replica.
		if rec, held := f.readLock(cmd.Key); held && rec.ExpiresAtMs > ts {
			return Result{Code: ResultAlreadyHeld, Holder: rec.OwnerID, ExpiresAtMs: rec.ExpiresAtMs, Token: rec.FencingToken}
		}
		rec := LockRecord{
			OwnerID:      cmd.Owner,
			ExpiresAtMs:  ts + cmd.TTLMs,
			FencingToken: entry.Index, // minted at APPLY time from the log index
			AcquiredAtMs: ts,
		}
		f.put(cmd.Key, EncodeLockRecord(rec), rec.ExpiresAtMs)
		return Result{Code: ResultOK, Token: rec.FencingToken, ExpiresAtMs: rec.ExpiresAtMs}

	case command.CmdLockRenew:
		rec, held := f.readLock(cmd.Key)
		if !held || !bytes.Equal(rec.OwnerID, cmd.Owner) {
			return Result{Code: ResultNotOwner}
		}
		if rec.ExpiresAtMs <= ts {
			return Result{Code: ResultLockExpired}
		}
		rec.ExpiresAtMs = ts + cmd.TTLMs // fencing token carried over unchanged
		f.put(cmd.Key, EncodeLockRecord(rec), rec.ExpiresAtMs)
		return Result{Code: ResultOK, Token: rec.FencingToken, ExpiresAtMs: rec.ExpiresAtMs}

	case command.CmdLockRelease:
		rec, held := f.readLock(cmd.Key)
		if !held {
			return Result{Code: ResultOK} // already unlocked: idempotent
		}
		if !bytes.Equal(rec.OwnerID, cmd.Owner) {
			return Result{Code: ResultNotOwner, Holder: rec.OwnerID}
		}
		f.del(cmd.Key)
		f.released(cmd.Key)
		return Result{Code: ResultOK, Token: rec.FencingToken}

	case command.CmdLockExpire:
		rec, held := f.readLock(cmd.Key)
		// No-op if the lock was released, re-acquired (new token), renewed,
		// or the deadline has not passed as of the leader's timestamp.
		if !held || !bytes.Equal(rec.OwnerID, cmd.Owner) || rec.FencingToken != cmd.Token || rec.ExpiresAtMs > ts {
			return Result{Code: ResultNoop}
		}
		f.del(cmd.Key)
		f.released(cmd.Key)
		return Result{Code: ResultOK, Token: rec.FencingToken}
	}
	return Result{Code: ResultInvalid}
}

func (f *FSM) put(key, envelope []byte, expiresAt uint64) {
	if err := f.engine.Put(key, envelope); err != nil {
		// Keys were validated before proposing; an engine error here would
		// make replicas diverge, which Raft cannot recover from.
		panic(fmt.Sprintf("shard: engine put failed during apply: %v", err))
	}
	f.mu.Lock()
	if expiresAt > 0 {
		f.deadlines[string(key)] = expiresAt
	} else {
		delete(f.deadlines, string(key))
	}
	f.mu.Unlock()
}

func (f *FSM) del(key []byte) {
	if err := f.engine.Delete(key); err != nil {
		panic(fmt.Sprintf("shard: engine delete failed during apply: %v", err))
	}
	f.mu.Lock()
	delete(f.deadlines, string(key))
	f.mu.Unlock()
}

func (f *FSM) released(key []byte) {
	if f.OnRelease != nil {
		f.OnRelease(string(key))
	}
}

func (f *FSM) readValue(key []byte) ([]byte, bool) {
	raw, found, err := f.engine.Get(key)
	if err != nil || !found {
		return nil, false
	}
	kind, _, payload, err := decodeEnvelope(raw)
	if err != nil || kind != kindValue {
		return nil, false
	}
	return payload, true
}

func (f *FSM) readLock(key []byte) (LockRecord, bool) {
	raw, found, err := f.engine.Get(key)
	if err != nil || !found {
		return LockRecord{}, false
	}
	rec, err := DecodeLockRecord(raw)
	if err != nil {
		return LockRecord{}, false
	}
	return rec, true
}

// Get reads a user key from this replica's applied state. A key whose TTL
// has passed but whose expiry has not yet COMMITTED is still visible —
// expiry is decided by Raft agreement, not by local clocks.
func (f *FSM) Get(key []byte) ([]byte, bool) { return f.readValue(key) }

// GetLock reads a lock record from this replica's applied state.
func (f *FSM) GetLock(name string) (LockRecord, bool) { return f.readLock(LockKey(name)) }

// Len returns the number of stored keys (including lock records).
func (f *FSM) Len() int { return f.engine.Len() }

// Expired returns up to max keys whose deadline is <= nowMs.
func (f *FSM) Expired(nowMs uint64, max int) []ExpiryCandidate {
	f.mu.Lock()
	var keys []string
	for k, d := range f.deadlines {
		if d <= nowMs {
			keys = append(keys, k)
			if len(keys) >= max {
				break
			}
		}
	}
	f.mu.Unlock()
	var out []ExpiryCandidate
	for _, k := range keys {
		raw, found, _ := f.engine.Get([]byte(k))
		if !found {
			continue
		}
		kind, exp, _, err := decodeEnvelope(raw)
		if err != nil {
			continue
		}
		c := ExpiryCandidate{Key: []byte(k), ExpiresAt: exp, IsLock: kind == kindLock}
		if c.IsLock {
			rec, err := DecodeLockRecord(raw)
			if err != nil {
				continue
			}
			c.Owner, c.Token = rec.OwnerID, rec.FencingToken
		}
		out = append(out, c)
	}
	return out
}

// Snapshot captures the engine (which already contains deadlines and lock
// records inside value envelopes). Raft calls it only between applies.
func (f *FSM) Snapshot() ([]byte, error) { return f.engine.Snapshot() }

// Restore replaces all state and rebuilds the derived deadline index.
func (f *FSM) Restore(snap []byte) error {
	if snap == nil { // an empty snapshot (e.g. a group's bootstrap snapshot)
		empty, err := storage.NewMemEngine().Snapshot()
		if err != nil {
			return err
		}
		snap = empty
	}
	if err := f.engine.Restore(snap); err != nil {
		return err
	}
	deadlines := map[string]uint64{}
	f.engine.ForEach(func(k, v []byte) bool {
		if _, exp, _, err := decodeEnvelope(v); err == nil && exp > 0 {
			deadlines[string(k)] = exp
		}
		return true
	})
	f.mu.Lock()
	f.deadlines = deadlines
	f.mu.Unlock()
	return nil
}

// Keys returns every user-visible key (debug/test helper; not an API).
func (f *FSM) Keys() []string {
	var out []string
	f.engine.ForEach(func(k, v []byte) bool {
		if len(v) > 0 && v[0] == kindValue && !strings.HasPrefix(string(k), LockPrefix) {
			out = append(out, string(k))
		}
		return true
	})
	return out
}

// Dump returns user key -> value for every key (test helper).
func (f *FSM) Dump() map[string]string {
	out := map[string]string{}
	f.engine.ForEach(func(k, v []byte) bool {
		if kind, _, p, err := decodeEnvelope(v); err == nil && kind == kindValue {
			out[string(k)] = string(p)
		}
		return true
	})
	return out
}
