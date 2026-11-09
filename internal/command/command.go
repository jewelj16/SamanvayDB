// Package command defines the internal Command type shared across the KV layer,
// Raft log entries, and the storage state machine. This type is intentionally
// designed early so that Raft ↔ storage integration is a rewiring exercise,
// not a redesign: a Command is exactly the payload serialized into a Raft log
// entry, and the shard state machine's Apply is a pure function of
// (state, Command).
//
// Design decision: empty keys are NOT legal. Every write path (KV server,
// Raft propose, apply) must reject commands with a zero-length key. This is
// enforced by Validate() and checked consistently through every layer.
//
// Determinism rule: anything time-dependent (TTL deadlines, lock expiry) is
// decided ONCE by the leader and embedded in the command (TimestampMs,
// ExpiresAtMs) before replication. Replicas never consult their own clocks
// inside Apply — that would let clock skew make replicas diverge.
package command

import (
	"encoding/binary"
	"fmt"
	"io"
)

// CommandType distinguishes write operations in the system.
type CommandType uint8

const (
	// CmdPut inserts or overwrites a key-value pair (optionally with a TTL
	// deadline in ExpiresAtMs).
	CmdPut CommandType = iota

	// CmdDelete removes a key. Deleting a missing key is a no-op.
	CmdDelete

	// CmdCAS writes Value only if the key currently holds Expected (or is
	// absent, when ExpectAbsent). The precondition is evaluated at apply time.
	CmdCAS

	// CmdExpire is proposed only by the shard leader's expiry ticker. It
	// deletes Key iff the key's current deadline still equals ExpiresAtMs
	// (i.e. it was not overwritten since the ticker observed it).
	CmdExpire

	// CmdLockAcquire takes the lock named by Key for Owner for TTLMs.
	CmdLockAcquire

	// CmdLockRenew extends the lock's deadline if Owner still holds it.
	CmdLockRenew

	// CmdLockRelease releases the lock if Owner holds it (idempotent).
	CmdLockRelease

	// CmdLockExpire is proposed only by the leader's expiry ticker; it
	// releases the lock iff it is still held by Owner with fencing token Token
	// and its deadline has passed as of TimestampMs.
	CmdLockExpire

	cmdTypeCount // sentinel — keep last
)

// String returns a human-readable name for the command type.
func (ct CommandType) String() string {
	switch ct {
	case CmdPut:
		return "PUT"
	case CmdDelete:
		return "DELETE"
	case CmdCAS:
		return "CAS"
	case CmdExpire:
		return "EXPIRE"
	case CmdLockAcquire:
		return "LOCK_ACQUIRE"
	case CmdLockRenew:
		return "LOCK_RENEW"
	case CmdLockRelease:
		return "LOCK_RELEASE"
	case CmdLockExpire:
		return "LOCK_EXPIRE"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", ct)
	}
}

// Command is the payload serialized into Raft log entries.
// Keys and values are opaque bytes — never string.
type Command struct {
	Type  CommandType
	Key   []byte
	Value []byte // empty/ignored for CmdDelete

	// Expected / ExpectAbsent are the CAS precondition.
	Expected     []byte
	ExpectAbsent bool

	// Owner identifies the lock holder for lock commands.
	Owner []byte

	// TTLMs is the requested lease length for lock acquire/renew.
	TTLMs uint64

	// TimestampMs is the leader's wall-clock time at propose time. It is the
	// ONLY notion of "now" that Apply may use.
	TimestampMs uint64

	// ExpiresAtMs is a leader-computed absolute deadline (PUT with TTL), or
	// the deadline being expired (CmdExpire). 0 means "never expires".
	ExpiresAtMs uint64

	// Token is the fencing token a CmdLockExpire targets.
	Token uint64

	// Client session (exactly-once semantics, Raft dissertation §6.3).
	// ClientID identifies the client; Seq numbers one logical operation and
	// is reused by every retry of it, so a retry whose first attempt already
	// committed is answered from the session table instead of being applied
	// twice. MinSeq is the client's lowest still-outstanding Seq: results
	// below it can be forgotten. Empty ClientID = no session (e.g. leader
	// expiry commands, which re-validate instead).
	ClientID []byte
	Seq      uint64
	MinSeq   uint64
}

// Validate checks that the command is well-formed.
// Empty keys are illegal — this invariant is enforced here and must be
// checked at every entry point (gRPC handler, Raft propose, apply).
func (c *Command) Validate() error {
	if len(c.Key) == 0 {
		return fmt.Errorf("command: empty key is not allowed")
	}
	if c.Type >= cmdTypeCount {
		return fmt.Errorf("command: unknown command type %d", c.Type)
	}
	switch c.Type {
	case CmdLockAcquire, CmdLockRenew:
		if len(c.Owner) == 0 {
			return fmt.Errorf("command: %s requires an owner", c.Type)
		}
		if c.TTLMs == 0 {
			return fmt.Errorf("command: %s requires a positive TTL", c.Type)
		}
	case CmdLockRelease, CmdLockExpire:
		if len(c.Owner) == 0 {
			return fmt.Errorf("command: %s requires an owner", c.Type)
		}
	}
	return nil
}

const (
	headerSize = 1 + 4 + 4
	// extension: TimestampMs(8) ExpiresAtMs(8) TTLMs(8) Token(8) Seq(8)
	//            MinSeq(8) Flags(1) ExpectedLen(4) Expected OwnerLen(4) Owner
	//            ClientIDLen(4) ClientID
	extFixedSize = 8*6 + 1 + 4 + 4 + 4

	flagExpectAbsent = 1 << 0
)

func (c *Command) hasExtension() bool {
	return c.TimestampMs != 0 || c.ExpiresAtMs != 0 || c.TTLMs != 0 || c.Token != 0 ||
		c.ExpectAbsent || len(c.Expected) > 0 || len(c.Owner) > 0 ||
		len(c.ClientID) > 0 || c.Seq != 0 || c.MinSeq != 0
}

// Marshal serializes a Command into the binary format stored in Raft log
// entries.
//
// Wire format (big-endian):
//
//	+----------+----------+----------+-----+-------+------------------------+
//	| Type (1B)| KeyLen(4)| ValLen(4)| Key | Value | [optional extension]   |
//	+----------+----------+----------+-----+-------+------------------------+
//
// The extension is present only when one of the CAS/TTL/lock fields is set,
// so a plain PUT/DELETE encodes exactly as the original Phase-1 format:
//
//	TimestampMs(8) ExpiresAtMs(8) TTLMs(8) Token(8) Flags(1)
//	ExpectedLen(4) Expected OwnerLen(4) Owner
func (c *Command) Marshal() ([]byte, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}

	size := headerSize + len(c.Key) + len(c.Value)
	ext := c.hasExtension()
	if ext {
		size += extFixedSize + len(c.Expected) + len(c.Owner) + len(c.ClientID)
	}
	buf := make([]byte, size)

	buf[0] = byte(c.Type)
	binary.BigEndian.PutUint32(buf[1:5], uint32(len(c.Key)))
	binary.BigEndian.PutUint32(buf[5:9], uint32(len(c.Value)))
	off := headerSize
	off += copy(buf[off:], c.Key)
	off += copy(buf[off:], c.Value)

	if ext {
		binary.BigEndian.PutUint64(buf[off:], c.TimestampMs)
		binary.BigEndian.PutUint64(buf[off+8:], c.ExpiresAtMs)
		binary.BigEndian.PutUint64(buf[off+16:], c.TTLMs)
		binary.BigEndian.PutUint64(buf[off+24:], c.Token)
		binary.BigEndian.PutUint64(buf[off+32:], c.Seq)
		binary.BigEndian.PutUint64(buf[off+40:], c.MinSeq)
		var flags byte
		if c.ExpectAbsent {
			flags |= flagExpectAbsent
		}
		buf[off+48] = flags
		off += 49
		binary.BigEndian.PutUint32(buf[off:], uint32(len(c.Expected)))
		off += 4
		off += copy(buf[off:], c.Expected)
		binary.BigEndian.PutUint32(buf[off:], uint32(len(c.Owner)))
		off += 4
		off += copy(buf[off:], c.Owner)
		binary.BigEndian.PutUint32(buf[off:], uint32(len(c.ClientID)))
		off += 4
		copy(buf[off:], c.ClientID)
	}
	return buf, nil
}

// Unmarshal deserializes a Command from its binary representation.
func Unmarshal(data []byte) (*Command, error) {
	if len(data) < headerSize {
		return nil, fmt.Errorf("command: data too short (%d bytes, need at least %d)", len(data), headerSize)
	}

	cmd := &Command{Type: CommandType(data[0])}
	keyLen := uint64(binary.BigEndian.Uint32(data[1:5]))
	valLen := uint64(binary.BigEndian.Uint32(data[5:9]))
	base := uint64(headerSize) + keyLen + valLen

	if base > uint64(len(data)) {
		return nil, fmt.Errorf("command: size mismatch: header says %d bytes, got %d", base, len(data))
	}

	cmd.Key = cloneBytes(data[headerSize : headerSize+keyLen])
	if valLen > 0 {
		cmd.Value = cloneBytes(data[headerSize+keyLen : base])
	}

	if rest := data[base:]; len(rest) > 0 {
		if err := cmd.unmarshalExt(rest); err != nil {
			return nil, err
		}
	}

	if err := cmd.Validate(); err != nil {
		return nil, err
	}
	return cmd, nil
}

func (c *Command) unmarshalExt(b []byte) error {
	if len(b) < extFixedSize {
		return fmt.Errorf("command: truncated extension (%d bytes)", len(b))
	}
	c.TimestampMs = binary.BigEndian.Uint64(b[0:])
	c.ExpiresAtMs = binary.BigEndian.Uint64(b[8:])
	c.TTLMs = binary.BigEndian.Uint64(b[16:])
	c.Token = binary.BigEndian.Uint64(b[24:])
	c.Seq = binary.BigEndian.Uint64(b[32:])
	c.MinSeq = binary.BigEndian.Uint64(b[40:])
	c.ExpectAbsent = b[48]&flagExpectAbsent != 0
	off := uint64(49)
	field := func(name string) ([]byte, error) {
		if off+4 > uint64(len(b)) {
			return nil, fmt.Errorf("command: truncated extension (%s length)", name)
		}
		n := uint64(binary.BigEndian.Uint32(b[off:]))
		off += 4
		if off+n > uint64(len(b)) {
			return nil, fmt.Errorf("command: extension %s length %d out of range", name, n)
		}
		var v []byte
		if n > 0 {
			v = cloneBytes(b[off : off+n])
		}
		off += n
		return v, nil
	}
	var err error
	if c.Expected, err = field("expected"); err != nil {
		return err
	}
	if c.Owner, err = field("owner"); err != nil {
		return err
	}
	if c.ClientID, err = field("client id"); err != nil {
		return err
	}
	if off != uint64(len(b)) {
		return fmt.Errorf("command: extension size mismatch")
	}
	return nil
}

// WriteTo writes the marshaled command to a writer.
func (c *Command) WriteTo(w io.Writer) (int64, error) {
	data, err := c.Marshal()
	if err != nil {
		return 0, err
	}
	n, err := w.Write(data)
	return int64(n), err
}

func cloneBytes(b []byte) []byte {
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
