package store

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	walName      = "wal.log"
	snapName     = "snapshot.dat"
	snapTmpName  = "snapshot.tmp"
	maxFrameSize = 16 << 20 // 16MiB safety bound per record
)

// FileStore is a WAL-file + snapshot-file implementation of Store.
//
// On-disk layout inside dir:
//
//	wal.log       append-only frames: [u32 len][i64 sequence][u8 kind][payload]
//	snapshot.dat  [i64 sequence][raw snapshot bytes]
//
// Every Append is fsync'ed. SaveSnapshot is atomic (write tmp + rename) and
// compacts the WAL afterwards, because its data is fully covered by the
// snapshot.
type FileStore struct {
	dir string

	mu  sync.Mutex
	wal *os.File
}

// NewFileStore opens (creating if needed) the WAL under dir.
func NewFileStore(dir string) (*FileStore, error) {
	if dir == "" {
		return nil, errors.New("store: empty data dir")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("store: mkdir: %w", err)
	}
	f, err := os.OpenFile(filepath.Join(dir, walName),
		os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("store: open wal: %w", err)
	}
	return &FileStore{dir: dir, wal: f}, nil
}

// Append durably writes one accepted record.
func (s *FileStore) Append(rec Record) error {
	if len(rec.Payload) > maxFrameSize {
		return fmt.Errorf("store: record payload too large: %d", len(rec.Payload))
	}
	frame := make([]byte, 4+8+1+len(rec.Payload))
	binary.BigEndian.PutUint32(frame, uint32(9+len(rec.Payload)))
	binary.BigEndian.PutUint64(frame[4:], uint64(rec.Sequence))
	frame[12] = byte(rec.Kind)
	copy(frame[13:], rec.Payload)

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.wal.Write(frame); err != nil {
		return fmt.Errorf("store: wal write: %w", err)
	}
	if err := s.wal.Sync(); err != nil {
		return fmt.Errorf("store: wal sync: %w", err)
	}
	return nil
}

// ReadAll replays records with sequence strictly greater than snapSeq.
// A torn trailing frame (crash mid-write) is ignored rather than fatal.
func (s *FileStore) ReadAll(snapSeq int64) ([]Record, error) {
	s.mu.Lock()
	path := s.wal.Name()
	s.mu.Unlock()

	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("store: open wal for read: %w", err)
	}
	defer f.Close()

	var out []Record
	var lenBuf [4]byte
	for {
		if _, err := io.ReadFull(f, lenBuf[:]); err != nil {
			if errors.Is(err, io.EOF) {
				break // clean end
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				break // torn length prefix: stop replay here
			}
			return nil, fmt.Errorf("store: read frame len: %w", err)
		}
		bodyLen := binary.BigEndian.Uint32(lenBuf[:])
		if bodyLen < 9 || bodyLen > maxFrameSize {
			return nil, fmt.Errorf("store: corrupt frame length %d", bodyLen)
		}
		body := make([]byte, bodyLen)
		if _, err := io.ReadFull(f, body); err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
				break // torn frame: drop it and stop
			}
			return nil, fmt.Errorf("store: read frame body: %w", err)
		}
		seq := int64(binary.BigEndian.Uint64(body))
		if seq > snapSeq {
			out = append(out, Record{
				Kind:     RecordKind(body[8]),
				Sequence: seq,
				Payload:  append([]byte(nil), body[9:]...),
			})
		}
	}
	return out, nil
}

// SaveSnapshot atomically replaces the durable snapshot and compacts the WAL.
func (s *FileStore) SaveSnapshot(sn Snapshot) error {
	buf := make([]byte, 8+len(sn.Data))
	binary.BigEndian.PutUint64(buf, uint64(sn.Sequence))
	copy(buf[8:], sn.Data)

	tmp := filepath.Join(s.dir, snapTmpName)
	final := filepath.Join(s.dir, snapName)

	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("store: create snapshot tmp: %w", err)
	}
	if _, err := f.Write(buf); err != nil {
		f.Close()
		return fmt.Errorf("store: write snapshot: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("store: sync snapshot: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("store: close snapshot: %w", err)
	}
	if err := os.Rename(tmp, final); err != nil {
		return fmt.Errorf("store: rename snapshot: %w", err)
	}
	if dir, err := os.Open(s.dir); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}

	// Compact the WAL: records with seq <= snapshot sequence are durable
	// in the snapshot and are dropped; newer records are rewritten. Capture
	// the tail before taking the lock (ReadAll opens its own fd), then take
	// the lock so no append races truncate+rewrite.
	tail, err := s.ReadAll(sn.Sequence)
	if err != nil {
		return fmt.Errorf("store: read tail during compact: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.wal.Truncate(0); err != nil {
		return fmt.Errorf("store: compact wal: %w", err)
	}
	if _, err := s.wal.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("store: rewind wal: %w", err)
	}
	for _, rec := range tail {
		frame := make([]byte, 4+8+1+len(rec.Payload))
		binary.BigEndian.PutUint32(frame, uint32(9+len(rec.Payload)))
		binary.BigEndian.PutUint64(frame[4:], uint64(rec.Sequence))
		frame[12] = byte(rec.Kind)
		copy(frame[13:], rec.Payload)
		if _, err := s.wal.Write(frame); err != nil {
			return fmt.Errorf("store: rewrite wal tail: %w", err)
		}
	}
	if err := s.wal.Sync(); err != nil {
		return fmt.Errorf("store: sync compacted wal: %w", err)
	}
	return nil
}

// LoadSnapshot returns the latest snapshot, or (nil, nil) when none exists.
func (s *FileStore) LoadSnapshot() (*Snapshot, error) {
	data, err := os.ReadFile(filepath.Join(s.dir, snapName))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("store: read snapshot: %w", err)
	}
	if len(data) < 8 {
		return nil, errors.New("store: corrupt snapshot (too short)")
	}
	seq := int64(binary.BigEndian.Uint64(data))
	return &Snapshot{Sequence: seq, Data: append([]byte(nil), data[8:]...)}, nil
}

// Close flushes and closes the WAL.
func (s *FileStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.wal == nil {
		return nil
	}
	err := s.wal.Close()
	s.wal = nil
	return err
}

type storeClosedError string

func (e storeClosedError) Error() string { return string(e) }

func errClosed(msg string) error { return storeClosedError(msg) }
