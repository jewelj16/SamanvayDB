package raft

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// These tests simulate "kill -9 immediately after an fsync point" by simply
// abandoning a FileStorage without Close() and reopening the directory:
// whatever the reopened storage returns is exactly what survived on disk.

func openFS(t *testing.T, dir string) *FileStorage {
	t.Helper()
	s, err := OpenFileStorage(dir, FileStorageOptions{})
	if err != nil {
		t.Fatalf("OpenFileStorage: %v", err)
	}
	return s
}

func entries(from, to, term uint64) []LogEntry {
	var out []LogEntry
	for i := from; i <= to; i++ {
		out = append(out, LogEntry{Index: i, Term: term, Command: []byte{byte(i), 0x00, byte(i >> 8)}})
	}
	return out
}

func TestFileStorage_FreshDirIsEmpty(t *testing.T) {
	s := openFS(t, t.TempDir())
	defer s.Close()
	_, _, _, es, ok, err := s.InitialState()
	if err != nil || ok || len(es) != 0 {
		t.Fatalf("fresh storage: ok=%v entries=%d err=%v", ok, len(es), err)
	}
}

func TestFileStorage_TermAndVoteSurviveCrash(t *testing.T) {
	dir := t.TempDir()
	s := openFS(t, dir)
	if err := s.SaveState(HardState{Term: 7, VotedFor: "node-3"}); err != nil {
		t.Fatal(err)
	}
	// crash: no Close
	s2 := openFS(t, dir)
	defer s2.Close()
	hs, _, _, _, ok, err := s2.InitialState()
	if err != nil || !ok {
		t.Fatalf("InitialState: ok=%v err=%v", ok, err)
	}
	if hs.Term != 7 || hs.VotedFor != "node-3" {
		t.Errorf("hard state = %+v, want term 7 vote node-3", hs)
	}
	s.Close()
}

func TestFileStorage_LogEntriesSurviveCrash(t *testing.T) {
	dir := t.TempDir()
	s := openFS(t, dir)
	if err := s.Append(entries(1, 50, 2)); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(entries(51, 60, 3)); err != nil {
		t.Fatal(err)
	}
	s2 := openFS(t, dir)
	defer s2.Close()
	_, _, _, es, _, err := s2.InitialState()
	if err != nil {
		t.Fatal(err)
	}
	if len(es) != 60 || es[59].Index != 60 || es[59].Term != 3 || !bytes.Equal(es[9].Command, []byte{10, 0, 0}) {
		t.Fatalf("recovered %d entries; last=%+v", len(es), es[len(es)-1])
	}
	s.Close()
}

func TestFileStorage_TornWriteIsDiscarded(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(path string, size int64)
	}{
		{"truncated_mid_record", func(path string, size int64) {
			os.Truncate(path, size-3)
		}},
		{"garbage_tail", func(path string, _ int64) {
			f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
			f.Write([]byte{0xde, 0xad, 0xbe, 0xef, 0, 0, 0, 40, 1, 2})
			f.Close()
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			s := openFS(t, dir)
			if err := s.Append(entries(1, 10, 1)); err != nil {
				t.Fatal(err)
			}
			s.Close()
			path := filepath.Join(dir, logFile)
			info, _ := os.Stat(path)
			want := 10
			if tt.name == "truncated_mid_record" {
				want = 9 // the torn final record is discarded
			}
			tt.mutate(path, info.Size())

			s2 := openFS(t, dir)
			_, _, _, es, _, err := s2.InitialState()
			if err != nil {
				s2.Close()
				t.Fatalf("recovery must not fail on a torn tail: %v", err)
			}
			if len(es) != want {
				s2.Close()
				t.Fatalf("recovered %d entries, want %d", len(es), want)
			}
			// Appends after repair continue cleanly at a record boundary.
			err = s2.Append(entries(uint64(want)+1, uint64(want)+1, 5))
			s2.Close()
			if err != nil {
				t.Fatal(err)
			}
			s3 := openFS(t, dir)
			_, _, _, es, _, _ = s3.InitialState()
			s3.Close()
			if len(es) != want+1 || es[len(es)-1].Term != 5 {
				t.Fatalf("after repair+append: %d entries", len(es))
			}
		})
	}
}

func TestFileStorage_CorruptedMiddleRecordStopsReplay(t *testing.T) {
	dir := t.TempDir()
	s := openFS(t, dir)
	s.Append(entries(1, 5, 1))
	off := s.offsets[2]
	s.Close()
	path := filepath.Join(dir, logFile)
	b, _ := os.ReadFile(path)
	b[off+recordHeader+3] ^= 0xff // flip a payload byte of entry 3
	os.WriteFile(path, b, 0o644)

	s2 := openFS(t, dir)
	defer s2.Close()
	_, _, _, es, _, _ := s2.InitialState()
	if len(es) != 2 {
		t.Fatalf("replay must stop exactly at the corrupt record: got %d entries, want 2", len(es))
	}
}

func TestFileStorage_TruncateAndCompact(t *testing.T) {
	dir := t.TempDir()
	s := openFS(t, dir)
	s.Append(entries(1, 20, 1))
	if err := s.TruncateFrom(15); err != nil {
		t.Fatal(err)
	}
	s.Append(entries(15, 16, 2))
	if err := s.SaveSnapshot(SnapshotMeta{Index: 10, Term: 1, Config: NewMembership("a", "b")}, []byte("snap")); err != nil {
		t.Fatal(err)
	}
	if err := s.CompactTo(10); err != nil {
		t.Fatal(err)
	}
	s.Append(entries(17, 17, 2))

	s2 := openFS(t, dir)
	defer s2.Close()
	_, meta, snap, es, _, err := s2.InitialState()
	if err != nil {
		t.Fatal(err)
	}
	if meta.Index != 10 || string(snap) != "snap" || len(meta.Config.Voters()) != 2 {
		t.Errorf("snapshot meta=%+v data=%q", meta, snap)
	}
	if len(es) != 7 || es[0].Index != 11 || es[len(es)-1].Index != 17 || es[4].Term != 2 {
		t.Fatalf("entries after truncate/compact: %+v", es)
	}
	s.Close()
}

// TestFileStorage_SnapshotWriteIsAtomic simulates a crash after the new
// snapshot's temp file was written but before the rename: the old snapshot
// must remain intact and valid.
func TestFileStorage_SnapshotWriteIsAtomic(t *testing.T) {
	dir := t.TempDir()
	s := openFS(t, dir)
	s.SaveSnapshot(SnapshotMeta{Index: 5, Term: 1}, []byte("old"))
	os.WriteFile(filepath.Join(dir, snapshotFile+".tmp"), []byte("half-written garbage"), 0o644)
	s.Close()

	s2 := openFS(t, dir)
	defer s2.Close()
	_, meta, snap, _, _, err := s2.InitialState()
	if err != nil || meta.Index != 5 || string(snap) != "old" {
		t.Fatalf("old snapshot not intact: meta=%+v snap=%q err=%v", meta, snap, err)
	}
}

// TestNode_RecoversFromFileStorage verifies a node restarted on the same
// directory recovers term, vote, log, and snapshot, and repairs a log that
// still contains entries already covered by the snapshot.
func TestNode_RecoversFromFileStorage(t *testing.T) {
	dir := t.TempDir()
	st := openFS(t, dir)
	sm := &captureSM{applied: new([]LogEntry)}
	n, err := NewNode(Config{ID: "a", StateMachine: sm, Transport: &noopTransport{}, Storage: st, Logger: testLogger()})
	if err != nil {
		t.Fatal(err)
	}
	n.mu.Lock()
	n.becomeCandidate()
	n.startLeading()
	n.mu.Unlock()
	for i := 0; i < 5; i++ {
		if _, _, err := n.Propose([]byte{byte('a' + i)}); err != nil {
			t.Fatal(err)
		}
	}
	n.Stop()
	// "crash": abandon st without Close and reopen
	st2 := openFS(t, dir)
	defer st2.Close()
	sm2 := &captureSM{applied: new([]LogEntry)}
	n2, err := NewNode(Config{ID: "a", StateMachine: sm2, Transport: &noopTransport{}, Storage: st2, Logger: testLogger()})
	if err != nil {
		t.Fatal(err)
	}
	if n2.CurrentTerm() != 1 || n2.ps.VotedFor == nil || *n2.ps.VotedFor != "a" {
		t.Errorf("term/vote not recovered: term=%d vote=%v", n2.CurrentTerm(), n2.ps.VotedFor)
	}
	if n2.lastLogIndex() != 6 { // no-op + 5 proposals
		t.Errorf("lastLogIndex = %d, want 6", n2.lastLogIndex())
	}
	st.Close()
}
