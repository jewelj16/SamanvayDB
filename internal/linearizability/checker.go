// Package linearizability checks a recorded history of concurrent
// key-value operations against the specification of a linearizable
// register per key (Wing & Gong's algorithm with Lowe's memoization — the
// same approach as Knossos/Porcupine, hand-rolled and small).
//
// A history is linearizable iff there is a total order of the operations
// that (1) respects real time — if A returned before B was invoked, A comes
// first — and (2) is a legal sequential execution of a register: every read
// returns the value of the most recent preceding write (or "absent").
//
// Operations whose outcome is unknown (the client timed out) are modelled
// as Pending: a pending write may take effect at any point after its
// invocation — or never; a pending read constrains nothing and is dropped.
//
// Because registers for different keys are independent, the history is
// checked per key (linearizability is compositional / "local").
package linearizability

import (
	"fmt"
	"math"
	"sort"
	"strings"
)

// Kind is an operation type.
type Kind int

const (
	Put Kind = iota
	Get
	Delete
)

// Op is one client operation in the history.
type Op struct {
	Client int
	Kind   Kind
	Key    string
	// Value: the value written (Put), or the value read (Get).
	Value string
	// Found: for Get, whether the key existed.
	Found bool
	// Call and Return are monotonic timestamps (e.g. nanoseconds since the
	// test started). Return is ignored for Pending ops.
	Call, Return int64
	// Pending marks an operation with unknown outcome.
	Pending bool
	// Note is free-form diagnostic text (e.g. which node served the op).
	Note string
}

func (o Op) String() string {
	switch o.Kind {
	case Put:
		return fmt.Sprintf("c%d put(%s=%q)[%d,%d]%s", o.Client, o.Key, o.Value, o.Call, o.Return, pend(o))
	case Delete:
		return fmt.Sprintf("c%d del(%s)[%d,%d]%s", o.Client, o.Key, o.Call, o.Return, pend(o))
	default:
		if !o.Found {
			return fmt.Sprintf("c%d get(%s)=absent[%d,%d]%s", o.Client, o.Key, o.Call, o.Return, pend(o))
		}
		return fmt.Sprintf("c%d get(%s)=%q[%d,%d]%s", o.Client, o.Key, o.Value, o.Call, o.Return, pend(o))
	}
}

func pend(o Op) string {
	out := ""
	if o.Pending {
		out = "(pending)"
	}
	if o.Note != "" {
		out += " " + o.Note
	}
	return out
}

// Result of a check.
type Result struct {
	OK bool
	// Key and History describe the first non-linearizable key's history.
	Key     string
	History []Op
	// Explored is the number of search states visited (for diagnostics).
	Explored int
}

// Check verifies the whole history, key by key.
func Check(history []Op) Result {
	byKey := map[string][]Op{}
	for _, op := range history {
		if op.Kind == Get && op.Pending {
			continue // an unanswered read constrains nothing
		}
		byKey[op.Key] = append(byKey[op.Key], op)
	}
	keys := make([]string, 0, len(byKey))
	for k := range byKey {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	total := 0
	for _, k := range keys {
		ok, explored := checkKey(byKey[k])
		total += explored
		if !ok {
			return Result{OK: false, Key: k, History: byKey[k], Explored: total}
		}
	}
	return Result{OK: true, Explored: total}
}

type state struct {
	present bool
	value   string
}

func step(s state, op Op) (state, bool) {
	switch op.Kind {
	case Put:
		return state{true, op.Value}, true
	case Delete:
		return state{}, true
	default: // Get
		if op.Found != s.present {
			return s, false
		}
		if op.Found && op.Value != s.value {
			return s, false
		}
		return s, true
	}
}

// checkKey searches for a legal linearization of one key's operations.
func checkKey(ops []Op) (bool, int) {
	sort.SliceStable(ops, func(i, j int) bool { return ops[i].Call < ops[j].Call })
	n := len(ops)
	ret := make([]int64, n)
	known := 0
	for i, op := range ops {
		ret[i] = op.Return
		if op.Pending {
			ret[i] = math.MaxInt64
		} else {
			known++
		}
	}
	done := make([]bool, n)
	seen := map[string]bool{}
	explored := 0

	var bits strings.Builder
	cacheKey := func(s state) string {
		bits.Reset()
		for _, d := range done {
			if d {
				bits.WriteByte('1')
			} else {
				bits.WriteByte('0')
			}
		}
		if s.present {
			bits.WriteString("|P|")
			bits.WriteString(s.value)
		} else {
			bits.WriteString("|A|")
		}
		return bits.String()
	}

	var dfs func(s state, remaining int) bool
	dfs = func(s state, remaining int) bool {
		if remaining == 0 {
			return true // every op with a known outcome is linearized
		}
		k := cacheKey(s)
		if seen[k] {
			return false
		}
		seen[k] = true
		explored++
		// An op may go next only if it was invoked before every remaining
		// op's return (otherwise some op finished strictly before it began).
		minRet := int64(math.MaxInt64)
		for i := 0; i < n; i++ {
			if !done[i] && ret[i] < minRet {
				minRet = ret[i]
			}
		}
		for i := 0; i < n; i++ {
			if done[i] || ops[i].Call > minRet {
				continue
			}
			ns, ok := step(s, ops[i])
			if !ok {
				continue
			}
			done[i] = true
			r := remaining
			if !ops[i].Pending {
				r--
			}
			if dfs(ns, r) {
				return true
			}
			done[i] = false
		}
		return false
	}
	return dfs(state{}, known), explored
}
