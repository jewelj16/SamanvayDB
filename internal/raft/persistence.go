package raft

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// persistence.go — durable Raft state.
//
// Every change to currentTerm/votedFor and every log append is made durable
// (written AND fsynced) before the Node responds to the RPC that caused it.
// Because SamanvayDB's storage engine is an in-memory map, this Raft log is
// also the database's write-ahead log: a committed write exists on disk only
// here (or in a snapshot) until the next snapshot.

// HardState is the small, frequently rewritten part of persistent state.
type HardState struct {
	Term     uint64
	VotedFor NodeID // "" means none
}

// SnapshotMeta describes a stored snapshot.
type SnapshotMeta struct {
	Index  uint64     // last log index covered by the snapshot
	Term   uint64     // term of that entry
	Config Membership // configuration as of Index
}

// Storage is Raft's stable storage. Every mutating method must be durable
// (fsynced) when it returns nil.
type Storage interface {
	// InitialState returns everything persisted so far. A brand-new storage
	// returns zero values and ok=false.
	InitialState() (hs HardState, meta SnapshotMeta, snapshot []byte, entries []LogEntry, ok bool, err error)
	// SaveState durably records currentTerm and votedFor.
	SaveState(hs HardState) error
	// Append durably appends entries, which must directly follow the
	// current last stored entry (Write followed by Sync).
	Append(entries []LogEntry) error
	// Write appends entries WITHOUT making them durable. They are durable
	// only once a later Sync (or Append) returns. Used by the leader's group
	// commit, which writes under the Raft lock (preserving log order) and
	// fsyncs outside it.
	Write(entries []LogEntry) error
	// Sync makes every previously written entry durable.
	Sync() error
	// TruncateFrom durably deletes every entry with index >= index.
	TruncateFrom(index uint64) error
	// SaveSnapshot durably replaces the stored snapshot (atomically: a crash
	// leaves either the old or the new snapshot, never a mix).
	SaveSnapshot(meta SnapshotMeta, data []byte) error
	// CompactTo deletes every entry with index <= index. Called only after
	// a snapshot covering index has been saved.
	CompactTo(index uint64) error
	Close() error
}

// ---------------------------------------------------------------------------
// MemoryStorage
// ---------------------------------------------------------------------------

// MemoryStorage keeps "persistent" state in memory. It is used by unit tests
// and the in-process simulator: a simulated crash discards the Node but keeps
// the MemoryStorage, exactly modelling what survives a real crash.
type MemoryStorage struct {
	mu       sync.Mutex
	hs       HardState
	meta     SnapshotMeta
	snapshot []byte
	entries  []LogEntry
	used     bool
}

// NewMemoryStorage returns an empty MemoryStorage.
func NewMemoryStorage() *MemoryStorage { return &MemoryStorage{} }

func (s *MemoryStorage) InitialState() (HardState, SnapshotMeta, []byte, []LogEntry, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hs, SnapshotMeta{Index: s.meta.Index, Term: s.meta.Term, Config: s.meta.Config.Clone()},
		cloneBytes(s.snapshot), cloneEntries(s.entries), s.used, nil
}

func (s *MemoryStorage) SaveState(hs HardState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hs, s.used = hs, true
	return nil
}

func (s *MemoryStorage) Append(entries []LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(entries) == 0 {
		return nil
	}
	if n := len(s.entries); n > 0 && entries[0].Index != s.entries[n-1].Index+1 {
		return fmt.Errorf("raft: memory storage: non-contiguous append at %d after %d", entries[0].Index, s.entries[n-1].Index)
	}
	s.entries = append(s.entries, cloneEntries(entries)...)
	s.used = true
	return nil
}

// Write is Append for memory storage (there is no separate durability step).
func (s *MemoryStorage) Write(entries []LogEntry) error { return s.Append(entries) }

// Sync is a no-op for memory storage.
func (s *MemoryStorage) Sync() error { return nil }

func (s *MemoryStorage) TruncateFrom(index uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, e := range s.entries {
		if e.Index >= index {
			s.entries = s.entries[:i]
			break
		}
	}
	return nil
}

func (s *MemoryStorage) SaveSnapshot(meta SnapshotMeta, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.meta = SnapshotMeta{Index: meta.Index, Term: meta.Term, Config: meta.Config.Clone()}
	s.snapshot = cloneBytes(data)
	s.used = true
	return nil
}

func (s *MemoryStorage) CompactTo(index uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	i := 0
	for i < len(s.entries) && s.entries[i].Index <= index {
		i++
	}
	s.entries = append([]LogEntry(nil), s.entries[i:]...)
	return nil
}

func (s *MemoryStorage) Close() error { return nil }

// ---------------------------------------------------------------------------
// FileStorage
// ---------------------------------------------------------------------------

// FileStorage persists Raft state in a directory:
//
//	state     currentTerm + votedFor     (rewritten atomically: tmp+fsync+rename)
//	snapshot  latest snapshot + metadata  (rewritten atomically: tmp+fsync+rename)
//	log       append-only log records     (append + fsync; truncate + fsync)
//
// Log record format (big-endian):
//
//	+-----------+-----------+------------------------------------------+
//	| CRC32 (4B)| Len (4B)  | Index(8) Term(8) Type(1) Command(Len-17)  |
//	+-----------+-----------+------------------------------------------+
//
// CRC32 covers Len and the payload. On startup the log is read sequentially;
// the first record that is short or fails its CRC marks the end of valid
// data (a torn write from a crash mid-append) and the file is truncated
// there. Such a record was never acknowledged, because the fsync covering it
// never returned.
type FileStorage struct {
	mu      sync.Mutex
	dir     string
	noSync  bool
	log     *os.File
	gen     uint64  // incremented whenever the log file handle is replaced
	first   uint64  // index of the first record in the log file (0 if empty)
	offsets []int64 // offsets[i] = file offset of entry first+i
	size    int64   // current logical end of the log file
}

// FileStorageOptions tunes FileStorage.
type FileStorageOptions struct {
	// NoSync skips fsync calls. ONLY for tests/benchmarks that measure
	// something else — it voids every durability guarantee.
	NoSync bool
}

const (
	stateFile    = "state"
	snapshotFile = "snapshot"
	logFile      = "log"

	stateMagic    = 0x52535431 // "RST1"
	snapshotMagic = 0x52534e31 // "RSN1"
	recordHeader  = 8
	entryHeader   = 17
)

// OpenFileStorage opens (creating if needed) Raft storage in dir.
func OpenFileStorage(dir string, opts FileStorageOptions) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("raft: storage: mkdir %s: %w", dir, err)
	}
	s := &FileStorage{dir: dir, noSync: opts.NoSync}
	f, err := os.OpenFile(filepath.Join(dir, logFile), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("raft: storage: open log: %w", err)
	}
	s.log = f
	if _, err := s.loadLogLocked(); err != nil {
		f.Close()
		return nil, err
	}
	return s, nil
}

func (s *FileStorage) sync(f *os.File) error {
	if s.noSync {
		return nil
	}
	return f.Sync()
}

func (s *FileStorage) InitialState() (HardState, SnapshotMeta, []byte, []LogEntry, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var (
		hs   HardState
		meta SnapshotMeta
		snap []byte
		used bool
	)

	if b, err := os.ReadFile(filepath.Join(s.dir, stateFile)); err == nil {
		h, err := decodeState(b)
		if err != nil {
			return hs, meta, nil, nil, false, err
		}
		hs, used = h, true
	} else if !errors.Is(err, os.ErrNotExist) {
		return hs, meta, nil, nil, false, fmt.Errorf("raft: storage: read state: %w", err)
	}

	if b, err := os.ReadFile(filepath.Join(s.dir, snapshotFile)); err == nil {
		m, data, err := decodeSnapshot(b)
		if err != nil {
			return hs, meta, nil, nil, false, err
		}
		meta, snap, used = m, data, true
	} else if !errors.Is(err, os.ErrNotExist) {
		return hs, meta, nil, nil, false, fmt.Errorf("raft: storage: read snapshot: %w", err)
	}

	entries, err := s.loadLogLocked()
	if err != nil {
		return hs, meta, nil, nil, false, err
	}
	if len(entries) > 0 {
		used = true
	}
	return hs, meta, snap, entries, used, nil
}

// loadLogLocked reads every valid record, repairing a torn tail.
func (s *FileStorage) loadLogLocked() ([]LogEntry, error) {
	if _, err := s.log.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	r := bufio.NewReader(s.log)
	var (
		entries []LogEntry
		off     int64
		hdr     [recordHeader]byte
	)
	s.offsets = s.offsets[:0]
	s.first = 0
	for {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			break // clean EOF or torn header
		}
		sum := binary.BigEndian.Uint32(hdr[0:4])
		n := binary.BigEndian.Uint32(hdr[4:8])
		if n < entryHeader || n > 1<<30 {
			break
		}
		payload := make([]byte, n)
		if _, err := io.ReadFull(r, payload); err != nil {
			break // torn payload
		}
		h := crc32.NewIEEE()
		h.Write(hdr[4:8])
		h.Write(payload)
		if h.Sum32() != sum {
			break // corrupted / torn record: end of valid data
		}
		e := LogEntry{
			Index: binary.BigEndian.Uint64(payload[0:8]),
			Term:  binary.BigEndian.Uint64(payload[8:16]),
			Type:  EntryType(payload[16]),
		}
		if len(payload) > entryHeader {
			e.Command = payload[entryHeader:]
		}
		if len(entries) > 0 && e.Index != entries[len(entries)-1].Index+1 {
			break // non-contiguous: treat as end of valid data
		}
		if len(entries) == 0 {
			s.first = e.Index
		}
		entries = append(entries, e)
		s.offsets = append(s.offsets, off)
		off += int64(recordHeader) + int64(n)
	}
	// Drop any torn tail so future appends start at a clean boundary.
	if err := s.log.Truncate(off); err != nil {
		return nil, fmt.Errorf("raft: storage: repair log tail: %w", err)
	}
	if err := s.sync(s.log); err != nil {
		return nil, err
	}
	s.size = off
	return entries, nil
}

func encodeRecord(buf *bytes.Buffer, e LogEntry) {
	n := uint32(entryHeader + len(e.Command))
	var hdr [recordHeader + entryHeader]byte
	binary.BigEndian.PutUint32(hdr[4:8], n)
	binary.BigEndian.PutUint64(hdr[8:16], e.Index)
	binary.BigEndian.PutUint64(hdr[16:24], e.Term)
	hdr[24] = byte(e.Type)
	h := crc32.NewIEEE()
	h.Write(hdr[4:])
	h.Write(e.Command)
	binary.BigEndian.PutUint32(hdr[0:4], h.Sum32())
	buf.Write(hdr[:])
	buf.Write(e.Command)
}

func (s *FileStorage) Append(entries []LogEntry) error {
	if err := s.Write(entries); err != nil {
		return err
	}
	// The fsync is what makes the entries "exist". Only after it returns
	// may the caller acknowledge them to a leader or client.
	return s.Sync()
}

func (s *FileStorage) Write(entries []LogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.offsets) > 0 && entries[0].Index != s.first+uint64(len(s.offsets)) {
		return fmt.Errorf("raft: storage: non-contiguous append at %d (expected %d)",
			entries[0].Index, s.first+uint64(len(s.offsets)))
	}
	var buf bytes.Buffer
	offs := make([]int64, 0, len(entries))
	pos := s.size
	for _, e := range entries {
		offs = append(offs, pos)
		before := buf.Len()
		encodeRecord(&buf, e)
		pos += int64(buf.Len() - before)
	}
	if _, err := s.log.WriteAt(buf.Bytes(), s.size); err != nil {
		return fmt.Errorf("raft: storage: append: %w", err)
	}
	if len(s.offsets) == 0 {
		s.first = entries[0].Index
	}
	s.offsets = append(s.offsets, offs...)
	s.size = pos
	return nil
}

// Sync fsyncs the log. It does NOT hold the storage mutex during the fsync,
// so other storage calls are not stalled behind a slow disk flush.
func (s *FileStorage) Sync() error {
	s.mu.Lock()
	f, gen := s.log, s.gen
	s.mu.Unlock()
	if f == nil {
		return nil
	}
	if err := s.sync(f); err != nil {
		s.mu.Lock()
		replaced := s.gen != gen
		s.mu.Unlock()
		if replaced {
			// The log was compacted meanwhile: the replacement file (which
			// contains everything written so far) was fsynced before rename.
			return nil
		}
		return fmt.Errorf("raft: storage: fsync log: %w", err)
	}
	return nil
}

func (s *FileStorage) TruncateFrom(index uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.offsets) == 0 || index >= s.first+uint64(len(s.offsets)) {
		return nil
	}
	keep := 0
	if index > s.first {
		keep = int(index - s.first)
	}
	pos := int64(0)
	if keep < len(s.offsets) {
		pos = s.offsets[keep]
	}
	if err := s.log.Truncate(pos); err != nil {
		return fmt.Errorf("raft: storage: truncate log: %w", err)
	}
	if err := s.sync(s.log); err != nil {
		return err
	}
	s.offsets = s.offsets[:keep]
	if keep == 0 {
		s.first = 0
	}
	s.size = pos
	return nil
}

func (s *FileStorage) CompactTo(index uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.offsets) == 0 || index < s.first {
		return nil
	}
	drop := int(index - s.first + 1)
	if drop > len(s.offsets) {
		drop = len(s.offsets)
	}
	var tail []byte
	if drop < len(s.offsets) {
		start := s.offsets[drop]
		tail = make([]byte, s.size-start)
		if _, err := s.log.ReadAt(tail, start); err != nil {
			return fmt.Errorf("raft: storage: compact read: %w", err)
		}
	}
	// Write the retained tail to a temp file, fsync, then atomically rename
	// it over the old log. A crash at any point leaves either the old log
	// (entries <= index are skipped on load because the snapshot covers
	// them) or the new one.
	tmp := filepath.Join(s.dir, logFile+".tmp")
	if err := writeFileSync(tmp, tail, !s.noSync); err != nil {
		return err
	}
	if err := s.log.Close(); err != nil {
		return err
	}
	if err := renameRetry(tmp, filepath.Join(s.dir, logFile)); err != nil {
		return fmt.Errorf("raft: storage: rename compacted log: %w", err)
	}
	syncDir(s.dir)
	f, err := os.OpenFile(filepath.Join(s.dir, logFile), os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("raft: storage: reopen log: %w", err)
	}
	s.log = f
	s.gen++
	shift := int64(0)
	if drop < len(s.offsets) {
		shift = s.offsets[drop]
	}
	newOffs := make([]int64, 0, len(s.offsets)-drop)
	for _, o := range s.offsets[drop:] {
		newOffs = append(newOffs, o-shift)
	}
	s.offsets = newOffs
	if len(newOffs) > 0 {
		s.first += uint64(drop)
	} else {
		s.first = 0
	}
	s.size -= shift
	if len(newOffs) == 0 {
		s.size = 0
	}
	return nil
}

func (s *FileStorage) SaveState(hs HardState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return atomicWrite(s.dir, stateFile, encodeState(hs), !s.noSync)
}

func (s *FileStorage) SaveSnapshot(meta SnapshotMeta, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return atomicWrite(s.dir, snapshotFile, encodeSnapshot(meta, data), !s.noSync)
}

func (s *FileStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.log == nil {
		return nil
	}
	err := s.log.Close()
	s.log = nil
	return err
}

// ---- encoding helpers ----

func encodeState(hs HardState) []byte {
	body := make([]byte, 0, 16+len(hs.VotedFor))
	body = binary.BigEndian.AppendUint64(body, hs.Term)
	body = binary.BigEndian.AppendUint32(body, uint32(len(hs.VotedFor)))
	body = append(body, hs.VotedFor...)
	out := binary.BigEndian.AppendUint32(nil, stateMagic)
	out = binary.BigEndian.AppendUint32(out, crc32.ChecksumIEEE(body))
	return append(out, body...)
}

func decodeState(b []byte) (HardState, error) {
	var hs HardState
	if len(b) < 20 || binary.BigEndian.Uint32(b) != stateMagic {
		return hs, fmt.Errorf("raft: storage: corrupt state file")
	}
	body := b[8:]
	if crc32.ChecksumIEEE(body) != binary.BigEndian.Uint32(b[4:]) {
		return hs, fmt.Errorf("raft: storage: state file checksum mismatch")
	}
	hs.Term = binary.BigEndian.Uint64(body)
	n := binary.BigEndian.Uint32(body[8:])
	if int(12+n) != len(body) {
		return hs, fmt.Errorf("raft: storage: corrupt state file")
	}
	hs.VotedFor = NodeID(body[12:])
	return hs, nil
}

func encodeSnapshot(meta SnapshotMeta, data []byte) []byte {
	cfg := meta.Config.Encode()
	body := make([]byte, 0, 32+len(cfg)+len(data))
	body = binary.BigEndian.AppendUint64(body, meta.Index)
	body = binary.BigEndian.AppendUint64(body, meta.Term)
	body = binary.BigEndian.AppendUint32(body, uint32(len(cfg)))
	body = append(body, cfg...)
	if data == nil {
		body = append(body, 0)
	} else {
		body = append(body, 1)
	}
	body = binary.BigEndian.AppendUint64(body, uint64(len(data)))
	body = append(body, data...)
	out := binary.BigEndian.AppendUint32(nil, snapshotMagic)
	out = binary.BigEndian.AppendUint32(out, crc32.ChecksumIEEE(body))
	return append(out, body...)
}

func decodeSnapshot(b []byte) (SnapshotMeta, []byte, error) {
	var meta SnapshotMeta
	bad := fmt.Errorf("raft: storage: corrupt snapshot file")
	if len(b) < 8+29 || binary.BigEndian.Uint32(b) != snapshotMagic {
		return meta, nil, bad
	}
	body := b[8:]
	if crc32.ChecksumIEEE(body) != binary.BigEndian.Uint32(b[4:]) {
		return meta, nil, fmt.Errorf("raft: storage: snapshot checksum mismatch")
	}
	meta.Index = binary.BigEndian.Uint64(body)
	meta.Term = binary.BigEndian.Uint64(body[8:])
	cl := uint64(binary.BigEndian.Uint32(body[16:]))
	off := uint64(20)
	if off+cl+9 > uint64(len(body)) {
		return meta, nil, bad
	}
	cfg, err := DecodeMembership(body[off : off+cl])
	if err != nil {
		return meta, nil, err
	}
	meta.Config = cfg
	off += cl
	hasData := body[off] == 1
	off++
	dl := binary.BigEndian.Uint64(body[off:])
	off += 8
	if off+dl != uint64(len(body)) {
		return meta, nil, bad
	}
	if !hasData {
		return meta, nil, nil
	}
	return meta, cloneBytes(body[off:]), nil
}

// atomicWrite writes name via tmp-file + fsync + rename, so a crash at any
// point leaves either the complete old file or the complete new file.
func atomicWrite(dir, name string, data []byte, doSync bool) error {
	tmp := filepath.Join(dir, name+".tmp")
	if err := writeFileSync(tmp, data, doSync); err != nil {
		return err
	}
	if err := renameRetry(tmp, filepath.Join(dir, name)); err != nil {
		return fmt.Errorf("raft: storage: rename %s: %w", name, err)
	}
	if doSync {
		syncDir(dir)
	}
	return nil
}

func writeFileSync(path string, data []byte, doSync bool) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("raft: storage: create %s: %w", path, err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("raft: storage: write %s: %w", path, err)
	}
	if doSync {
		if err := f.Sync(); err != nil {
			f.Close()
			return fmt.Errorf("raft: storage: fsync %s: %w", path, err)
		}
	}
	return f.Close()
}

// renameRetry renames old over new. On Windows a rename that replaces an
// existing file fails with "Access is denied" while ANY other process
// (antivirus, search indexer, backup agent) briefly holds the target open;
// that is transient, so retry for up to ~2s before giving up.
func renameRetry(oldPath, newPath string) error {
	var err error
	for attempt := 0; attempt < 40; attempt++ {
		if err = os.Rename(oldPath, newPath); err == nil {
			return nil
		}
		if !os.IsPermission(err) && !isSharingViolation(err) {
			return err
		}
		time.Sleep(time.Duration(attempt+1) * 5 * time.Millisecond)
	}
	return err
}

func isSharingViolation(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "Access is denied") || strings.Contains(msg, "being used by another process")
}

// syncDir fsyncs a directory so a rename inside it is durable. Not supported
// on Windows (the error is ignored there; NTFS journals metadata).
func syncDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		d.Close()
	}
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

func cloneEntries(es []LogEntry) []LogEntry {
	if es == nil {
		return nil
	}
	out := make([]LogEntry, len(es))
	for i, e := range es {
		out[i] = e
		out[i].Command = cloneBytes(e.Command)
	}
	return out
}
