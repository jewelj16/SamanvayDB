package command

import (
	"bytes"
	"testing"
)

// TestCommand_MarshalUnmarshal_Put verifies a PUT command round-trips through
// serialization without data loss.
func TestCommand_MarshalUnmarshal_Put(t *testing.T) {
	original := &Command{
		Type:  CmdPut,
		Key:   []byte("user:1001"),
		Value: []byte("Alice"),
	}

	data, err := original.Marshal()
	if err != nil {
		t.Fatalf("Marshal() failed: %v", err)
	}

	decoded, err := Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal() failed: %v", err)
	}

	if decoded.Type != original.Type {
		t.Errorf("Type: got %v, want %v", decoded.Type, original.Type)
	}
	if !bytes.Equal(decoded.Key, original.Key) {
		t.Errorf("Key: got %q, want %q", decoded.Key, original.Key)
	}
	if !bytes.Equal(decoded.Value, original.Value) {
		t.Errorf("Value: got %q, want %q", decoded.Value, original.Value)
	}
}

// TestCommand_MarshalUnmarshal_Delete verifies a DELETE command round-trips
// correctly with an empty value.
func TestCommand_MarshalUnmarshal_Delete(t *testing.T) {
	original := &Command{
		Type:  CmdDelete,
		Key:   []byte("user:1001"),
		Value: nil,
	}

	data, err := original.Marshal()
	if err != nil {
		t.Fatalf("Marshal() failed: %v", err)
	}

	decoded, err := Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal() failed: %v", err)
	}

	if decoded.Type != CmdDelete {
		t.Errorf("Type: got %v, want DELETE", decoded.Type)
	}
	if !bytes.Equal(decoded.Key, original.Key) {
		t.Errorf("Key: got %q, want %q", decoded.Key, original.Key)
	}
	if len(decoded.Value) != 0 {
		t.Errorf("Value: got %q, want empty", decoded.Value)
	}
}

// TestCommand_MarshalUnmarshal_BinaryData verifies that arbitrary binary data
// (including null bytes) survives serialization — keys and values are opaque bytes.
func TestCommand_MarshalUnmarshal_BinaryData(t *testing.T) {
	original := &Command{
		Type:  CmdPut,
		Key:   []byte{0x00, 0xFF, 0x01, 0xFE},
		Value: []byte{0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x00},
	}

	data, err := original.Marshal()
	if err != nil {
		t.Fatalf("Marshal() failed: %v", err)
	}

	decoded, err := Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal() failed: %v", err)
	}

	if !bytes.Equal(decoded.Key, original.Key) {
		t.Errorf("Key: got %x, want %x", decoded.Key, original.Key)
	}
	if !bytes.Equal(decoded.Value, original.Value) {
		t.Errorf("Value: got %x, want %x", decoded.Value, original.Value)
	}
}

// TestCommand_MarshalUnmarshal_LargePayload verifies correctness with a
// non-trivial payload size.
func TestCommand_MarshalUnmarshal_LargePayload(t *testing.T) {
	key := bytes.Repeat([]byte("k"), 1024)
	value := bytes.Repeat([]byte("v"), 64*1024) // 64 KB

	original := &Command{
		Type:  CmdPut,
		Key:   key,
		Value: value,
	}

	data, err := original.Marshal()
	if err != nil {
		t.Fatalf("Marshal() failed: %v", err)
	}

	decoded, err := Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal() failed: %v", err)
	}

	if !bytes.Equal(decoded.Key, original.Key) {
		t.Errorf("Key length mismatch: got %d, want %d", len(decoded.Key), len(original.Key))
	}
	if !bytes.Equal(decoded.Value, original.Value) {
		t.Errorf("Value length mismatch: got %d, want %d", len(decoded.Value), len(original.Value))
	}
}

// TestCommand_EmptyKeyRejected verifies that empty keys are rejected by both
// Marshal and Validate.
func TestCommand_EmptyKeyRejected(t *testing.T) {
	cmd := &Command{
		Type:  CmdPut,
		Key:   []byte{},
		Value: []byte("value"),
	}

	if err := cmd.Validate(); err == nil {
		t.Error("Validate() should reject empty key, got nil")
	}

	if _, err := cmd.Marshal(); err == nil {
		t.Error("Marshal() should reject empty key, got nil")
	}
}

// TestCommand_NilKeyRejected verifies that nil keys are rejected.
func TestCommand_NilKeyRejected(t *testing.T) {
	cmd := &Command{
		Type:  CmdPut,
		Key:   nil,
		Value: []byte("value"),
	}

	if err := cmd.Validate(); err == nil {
		t.Error("Validate() should reject nil key, got nil")
	}
}

// TestCommand_UnmarshalTruncatedData returns an error for truncated input.
func TestCommand_UnmarshalTruncatedData(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{name: "empty", data: []byte{}},
		{name: "too_short", data: []byte{0x00, 0x01}},
		{name: "header_only", data: []byte{0x00, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x00}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Unmarshal(tt.data)
			if err == nil {
				t.Error("expected error for truncated data, got nil")
			}
		})
	}
}

// TestCommand_UnknownTypeRejected verifies unknown command types fail validation.
func TestCommand_UnknownTypeRejected(t *testing.T) {
	cmd := &Command{
		Type:  CommandType(99),
		Key:   []byte("key"),
		Value: []byte("val"),
	}

	if err := cmd.Validate(); err == nil {
		t.Error("Validate() should reject unknown type, got nil")
	}
}

// TestCommandType_String verifies human-readable names.
func TestCommandType_String(t *testing.T) {
	tests := []struct {
		ct   CommandType
		want string
	}{
		{CmdPut, "PUT"},
		{CmdDelete, "DELETE"},
		{CommandType(99), "UNKNOWN(99)"},
	}

	for _, tt := range tests {
		if got := tt.ct.String(); got != tt.want {
			t.Errorf("CommandType(%d).String() = %q, want %q", tt.ct, got, tt.want)
		}
	}
}
