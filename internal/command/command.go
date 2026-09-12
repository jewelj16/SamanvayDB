// Package command defines the internal Command type shared across the KV layer,
// WAL, Raft log entries, and the storage engine. This type is intentionally
// designed early so that Phase 4's Raft ↔ storage integration is a rewiring
// exercise, not a redesign.
//
// Design decision: empty keys are NOT legal. Every write path (KV server, WAL,
// Raft apply) must reject commands with a zero-length key. This is enforced by
// Validate() and must be checked consistently through every later phase.
package command

import (
	"encoding/binary"
	"fmt"
	"io"
)

// CommandType distinguishes write operations in the system.
type CommandType uint8

const (
	// CmdPut inserts or overwrites a key-value pair.
	CmdPut CommandType = iota

	// CmdDelete removes a key by writing a tombstone (same write path as
	// CmdPut, but Value is ignored/empty).
	CmdDelete
)

// String returns a human-readable name for the command type.
func (ct CommandType) String() string {
	switch ct {
	case CmdPut:
		return "PUT"
	case CmdDelete:
		return "DELETE"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", ct)
	}
}

// Command is the payload serialized into Raft log entries and WAL records.
// Keys and values are opaque bytes — never string.
type Command struct {
	Type  CommandType
	Key   []byte
	Value []byte // empty/ignored for CmdDelete
}

// Validate checks that the command is well-formed.
// Empty keys are illegal — this invariant is enforced here and must be
// checked at every entry point (gRPC handler, WAL writer, Raft propose).
func (c *Command) Validate() error {
	if len(c.Key) == 0 {
		return fmt.Errorf("command: empty key is not allowed")
	}
	if c.Type != CmdPut && c.Type != CmdDelete {
		return fmt.Errorf("command: unknown command type %d", c.Type)
	}
	return nil
}

// Marshal serializes a Command into a binary format suitable for WAL records
// and Raft log entries.
//
// Wire format:
//
//	+----------+----------+----------+-----------+-------+
//	| Type (1B)| KeyLen(4)| ValLen(4)| Key       | Value |
//	+----------+----------+----------+-----------+-------+
func (c *Command) Marshal() ([]byte, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}

	size := 1 + 4 + 4 + len(c.Key) + len(c.Value)
	buf := make([]byte, size)

	buf[0] = byte(c.Type)
	binary.BigEndian.PutUint32(buf[1:5], uint32(len(c.Key)))
	binary.BigEndian.PutUint32(buf[5:9], uint32(len(c.Value)))
	copy(buf[9:9+len(c.Key)], c.Key)
	copy(buf[9+len(c.Key):], c.Value)

	return buf, nil
}

// Unmarshal deserializes a Command from its binary representation.
func Unmarshal(data []byte) (*Command, error) {
	if len(data) < 9 {
		return nil, fmt.Errorf("command: data too short (%d bytes, need at least 9)", len(data))
	}

	cmd := &Command{
		Type: CommandType(data[0]),
	}

	keyLen := binary.BigEndian.Uint32(data[1:5])
	valLen := binary.BigEndian.Uint32(data[5:9])

	if uint64(9)+uint64(keyLen)+uint64(valLen) != uint64(len(data)) {
		return nil, fmt.Errorf("command: size mismatch: header says %d bytes, got %d",
			9+keyLen+valLen, len(data))
	}

	cmd.Key = make([]byte, keyLen)
	copy(cmd.Key, data[9:9+keyLen])

	if valLen > 0 {
		cmd.Value = make([]byte, valLen)
		copy(cmd.Value, data[9+keyLen:])
	}

	if err := cmd.Validate(); err != nil {
		return nil, err
	}

	return cmd, nil
}

// WriteTo writes the marshaled command to a writer. Useful for streaming
// into WAL files.
func (c *Command) WriteTo(w io.Writer) (int64, error) {
	data, err := c.Marshal()
	if err != nil {
		return 0, err
	}
	n, err := w.Write(data)
	return int64(n), err
}
