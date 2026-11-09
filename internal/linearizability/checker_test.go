package linearizability

import "testing"

func TestCheck_Table(t *testing.T) {
	tests := []struct {
		name string
		ops  []Op
		want bool
	}{
		{"sequential_ok", []Op{
			{Kind: Put, Key: "x", Value: "1", Call: 0, Return: 10},
			{Kind: Get, Key: "x", Value: "1", Found: true, Call: 20, Return: 30},
		}, true},
		{"stale_read_after_write_returned", []Op{
			{Kind: Put, Key: "x", Value: "1", Call: 0, Return: 10},
			{Kind: Put, Key: "x", Value: "2", Call: 20, Return: 30},
			{Kind: Get, Key: "x", Value: "1", Found: true, Call: 40, Return: 50},
		}, false},
		{"concurrent_read_may_see_either", []Op{
			{Kind: Put, Key: "x", Value: "1", Call: 0, Return: 10},
			{Kind: Put, Key: "x", Value: "2", Call: 20, Return: 60},
			{Kind: Get, Key: "x", Value: "1", Found: true, Call: 30, Return: 40},
			{Kind: Get, Key: "x", Value: "2", Found: true, Call: 45, Return: 50},
		}, true},
		{"read_goes_back_in_time", []Op{
			{Kind: Put, Key: "x", Value: "1", Call: 0, Return: 10},
			{Kind: Put, Key: "x", Value: "2", Call: 20, Return: 60},
			{Kind: Get, Key: "x", Value: "2", Found: true, Call: 30, Return: 40},
			{Kind: Get, Key: "x", Value: "1", Found: true, Call: 45, Return: 50},
		}, false},
		{"read_of_never_written_value", []Op{
			{Kind: Get, Key: "x", Value: "9", Found: true, Call: 0, Return: 10},
		}, false},
		{"absent_then_delete", []Op{
			{Kind: Put, Key: "x", Value: "1", Call: 0, Return: 10},
			{Kind: Delete, Key: "x", Call: 20, Return: 30},
			{Kind: Get, Key: "x", Found: false, Call: 40, Return: 50},
		}, true},
		{"pending_write_may_apply_late", []Op{
			{Kind: Put, Key: "x", Value: "1", Call: 0, Pending: true},
			{Kind: Get, Key: "x", Found: false, Call: 5, Return: 6},
			{Kind: Get, Key: "x", Value: "1", Found: true, Call: 100, Return: 110},
		}, true},
		{"pending_write_may_never_apply", []Op{
			{Kind: Put, Key: "x", Value: "1", Call: 0, Pending: true},
			{Kind: Get, Key: "x", Found: false, Call: 100, Return: 110},
		}, true},
		{"pending_write_cannot_apply_before_call", []Op{
			{Kind: Get, Key: "x", Value: "1", Found: true, Call: 0, Return: 10},
			{Kind: Put, Key: "x", Value: "1", Call: 20, Pending: true},
		}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Check(tt.ops).OK; got != tt.want {
				t.Fatalf("Check = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCheck_KeysAreIndependent(t *testing.T) {
	ops := []Op{
		{Kind: Put, Key: "a", Value: "1", Call: 0, Return: 10},
		{Kind: Put, Key: "b", Value: "2", Call: 0, Return: 10},
		{Kind: Get, Key: "a", Value: "1", Found: true, Call: 20, Return: 30},
		{Kind: Get, Key: "b", Value: "1", Found: true, Call: 20, Return: 30}, // wrong
	}
	r := Check(ops)
	if r.OK || r.Key != "b" {
		t.Fatalf("result %+v, want failure on key b", r)
	}
}

func TestCheck_ManyConcurrentWritesTerminates(t *testing.T) {
	var ops []Op
	for i := 0; i < 14; i++ { // 14 fully concurrent writes, then a read
		ops = append(ops, Op{Kind: Put, Key: "x", Value: string(rune('a' + i)), Call: 0, Return: 100})
	}
	ops = append(ops, Op{Kind: Get, Key: "x", Value: "c", Found: true, Call: 200, Return: 210})
	if !Check(ops).OK {
		t.Fatal("expected linearizable")
	}
}
