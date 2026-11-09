package shard

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"samanvaydb/internal/command"
)

// session.go — exactly-once semantics for retried client writes
// (Raft dissertation §6.3).
//
// Why: a client whose request times out (e.g. the leader crashed after
// committing but before replying) must retry, and the retry may commit a
// SECOND copy of the command. Even "idempotent" PUT/DELETE then break
// linearizability: put(X), another client's put(Y), and the retried put(X)
// make X take effect twice around Y. The correctness suite's
// linearizability checker caught exactly this with a retried DELETE.
//
// How: every client write carries (ClientID, Seq), reusing Seq on retries.
// The FSM records each session's results; a command whose (ClientID, Seq)
// was already applied is answered from the table instead of being applied
// again. MinSeq (the client's lowest outstanding Seq) lets the table forget
// older results, so concurrent requests from one client are supported.
// The table lives in the storage engine under a reserved prefix, so it is
// replicated, snapshotted and recovered exactly like user data, and every
// replica makes the same dedup decision.

// SessionPrefix is the reserved key prefix for session records.
const SessionPrefix = "__session__:"

// SessionTTLMs is how long an idle session is retained. Expiry is
// deterministic: it is evaluated at fixed log indexes using the leader's
// timestamp embedded in the command.
const SessionTTLMs = 60 * 60 * 1000

const sessionPurgeEvery = 1024 // log entries

const kindSession byte = 2

type sessionRecord struct {
	MinSeq  uint64            `json:"min"`
	LastTs  uint64            `json:"ts"`
	Results map[uint64]Result `json:"r"` // json sorts map keys: deterministic
}

func sessionKey(clientID []byte) []byte {
	return []byte(SessionPrefix + string(clientID))
}

func (f *FSM) loadSession(clientID []byte) sessionRecord {
	raw, found, _ := f.engine.Get(sessionKey(clientID))
	rec := sessionRecord{Results: map[uint64]Result{}}
	if !found || len(raw) < envHeader || raw[0] != kindSession {
		return rec
	}
	if err := json.Unmarshal(raw[envHeader:], &rec); err != nil || rec.Results == nil {
		rec.Results = map[uint64]Result{}
	}
	return rec
}

func (f *FSM) saveSession(clientID []byte, rec sessionRecord) {
	b, err := json.Marshal(rec)
	if err != nil {
		panic(fmt.Sprintf("shard: encode session: %v", err))
	}
	v := make([]byte, envHeader+len(b))
	v[0] = kindSession
	copy(v[envHeader:], b)
	f.put(sessionKey(clientID), v, 0)
}

// CachedResult returns the recorded outcome of (clientID, seq) if that
// command was already applied on this replica. Leader-side pre-checks
// consult it first, so a retried CAS/lock call is answered with its
// original outcome instead of being re-evaluated against newer state.
func (f *FSM) CachedResult(clientID []byte, seq uint64) (Result, bool) {
	if len(clientID) == 0 || seq == 0 {
		return Result{}, false
	}
	rec := f.loadSession(clientID)
	if seq < rec.MinSeq {
		return Result{Code: ResultOK}, true // completed long ago
	}
	r, ok := rec.Results[seq]
	return r, ok
}

// applyWithSession wraps apply with duplicate detection.
func (f *FSM) applyWithSession(index uint64, cmd *command.Command, apply func() Result) Result {
	if len(cmd.ClientID) == 0 || cmd.Seq == 0 {
		return apply()
	}
	rec := f.loadSession(cmd.ClientID)
	if cmd.Seq < rec.MinSeq {
		return Result{Code: ResultOK} // a late duplicate of a request the client finished
	}
	if r, ok := rec.Results[cmd.Seq]; ok {
		return r // duplicate: answer from the table, do NOT apply again
	}
	r := apply()
	rec.Results[cmd.Seq] = r
	if cmd.MinSeq > rec.MinSeq {
		rec.MinSeq = cmd.MinSeq
		for s := range rec.Results {
			if s < rec.MinSeq {
				delete(rec.Results, s)
			}
		}
	}
	if cmd.TimestampMs > rec.LastTs {
		rec.LastTs = cmd.TimestampMs
	}
	f.saveSession(cmd.ClientID, rec)
	if index%sessionPurgeEvery == 0 && cmd.TimestampMs > SessionTTLMs {
		f.purgeSessions(cmd.TimestampMs - SessionTTLMs)
	}
	return r
}

// purgeSessions deletes sessions idle since before cutoffMs.
func (f *FSM) purgeSessions(cutoffMs uint64) {
	var stale [][]byte
	f.engine.ForEach(func(k, v []byte) bool {
		if !strings.HasPrefix(string(k), SessionPrefix) || len(v) < envHeader || v[0] != kindSession {
			return true
		}
		var rec sessionRecord
		if json.Unmarshal(v[envHeader:], &rec) == nil && rec.LastTs < cutoffMs {
			stale = append(stale, append([]byte(nil), k...))
		}
		return true
	})
	for _, k := range stale {
		f.del(k)
	}
}

// ---- carrying the session through the request path ----

// ClientRef identifies one logical client request.
type ClientRef struct {
	ID     []byte
	Seq    uint64
	MinSeq uint64
}

type sessionCtxKey struct{}

// WithClient attaches a client session reference to ctx; Shard writes
// stamp it into the proposed command.
func WithClient(ctx context.Context, ref ClientRef) context.Context {
	if len(ref.ID) == 0 || ref.Seq == 0 {
		return ctx
	}
	return context.WithValue(ctx, sessionCtxKey{}, ref)
}

func clientFrom(ctx context.Context) (ClientRef, bool) {
	ref, ok := ctx.Value(sessionCtxKey{}).(ClientRef)
	return ref, ok
}
