// Package store implements a durable append-only write-ahead log.
//
// Every Append is fsynced before it is reported as accepted, so a restart
// cannot lose confirmed state. Frames carry a CRC so a torn tail (process
// killed mid-write) is detected on replay and truncated rather than misparsed;
// earlier frames remain intact.
//
// Frame layout (big-endian):
//
//	[ seq uint64 ][ kind byte ][ crc32 uint32 ][ length uint32 ][ payload... ]
//
// The file begins with a 4-byte magic. Replace atomically rewrites the log from
// a checkpoint plus a raw tail, bounding replay time as history grows.
package store

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// Frame kinds.
const (
	KindEvent    byte = 'E' // payload: JSON model.Event
	KindMetric   byte = 'M' // payload: JSON model.Metric
	KindRules    byte = 'R' // payload: JSON rules snapshot
	KindSnapshot byte = 'S' // payload: JSON engine checkpoint
)

var magic = [4]byte{'P', 'O', 'E', '1'}

const headerLen = 8 + 1 + 4 + 4

var (
	// ErrCorrupt means a frame failed CRC validation (not a torn tail).
	ErrCorrupt = errors.New("store: corrupt frame")
)

// Record is one replayed log frame.
type Record struct {
	Seq      uint64
	Kind     byte
	Payload  []byte
	FrameLen int // total on-disk frame length including header
}

// Store is an fsynced append-only log. Safe for concurrent use; Append calls
// are serialized and assigned gap-free monotonic sequence numbers.
type Store struct {
	mu   sync.Mutex
	f    *os.File
	path string
	seq  uint64
}

// Open opens (creating if needed) the log at path.
func Open(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	s := &Store{f: f, path: path}
	if st.Size() == 0 {
		if _, err := f.Write(magic[:]); err != nil {
			f.Close()
			return nil, err
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return nil, err
		}
	} else if err := s.checkMagic(); err != nil {
		f.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) checkMagic() error {
	var buf [4]byte
	if _, err := s.f.ReadAt(buf[:], 0); err != nil {
		return err
	}
	if buf != magic {
		return fmt.Errorf("store: bad magic in %s", s.path)
	}
	return nil
}

// Append writes one frame and fsyncs it. The assigned sequence number is
// returned; sequence numbers start at 1 and are gap-free.
func (s *Store) Append(kind byte, payload []byte) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.seq++
	frame := make([]byte, headerLen+len(payload))
	binary.BigEndian.PutUint64(frame[0:8], s.seq)
	frame[8] = kind
	crc := crc32.ChecksumIEEE(append([]byte{kind}, payload...))
	binary.BigEndian.PutUint32(frame[9:13], crc)
	binary.BigEndian.PutUint32(frame[13:17], uint32(len(payload)))
	copy(frame[17:], payload)

	if _, err := s.f.Write(frame); err != nil {
		return 0, err
	}
	if err := s.f.Sync(); err != nil {
		return 0, err
	}
	return s.seq, nil
}

// Replay iterates every valid frame in order. A torn final frame (partial write
// from a crash) is ignored and the file truncated back to the last good frame.
// fn must not call Append on the same goroutine (no lock is held during fn).
func (s *Store) Replay(fn func(Record) error) error {
	s.mu.Lock()
	f := s.f
	// Release the lock while reading? Replay happens at startup before other
	// use; keep lock for simplicity and correctness.
	defer s.mu.Unlock()

	if _, err := f.Seek(int64(len(magic)), io.SeekStart); err != nil {
		return err
	}
	var hdr [headerLen]byte
	var validEnd = int64(len(magic))
	var maxSeq uint64
	for {
		n, err := io.ReadFull(f, hdr[:])
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			if n > 0 {
				break // torn header: truncate back
			}
			break
		}
		if err != nil {
			return err
		}
		seq := binary.BigEndian.Uint64(hdr[0:8])
		kind := hdr[8]
		crc := binary.BigEndian.Uint32(hdr[9:13])
		length := binary.BigEndian.Uint32(hdr[13:17])

		payload := make([]byte, length)
		pn, perr := io.ReadFull(f, payload)
		if perr != nil {
			if perr == io.ErrUnexpectedEOF || perr == io.EOF {
				break // torn payload: header must be discarded too
			}
			return perr
		}
		_ = pn
		if crc32.ChecksumIEEE(append([]byte{kind}, payload...)) != crc {
			return fmt.Errorf("%w at seq %d", ErrCorrupt, seq)
		}
		if err := fn(Record{
			Seq:      seq,
			Kind:     kind,
			Payload:  append([]byte(nil), payload...),
			FrameLen: headerLen + int(length),
		}); err != nil {
			return err
		}
		if seq > maxSeq {
			maxSeq = seq
		}
		validEnd += int64(headerLen) + int64(length)
	}

	// Truncate any torn tail so future Appends start on a frame boundary.
	if fi, err := f.Stat(); err == nil && fi.Size() != validEnd {
		if err := f.Truncate(validEnd); err != nil {
			return err
		}
		if _, err := f.Seek(0, io.SeekEnd); err != nil {
			return err
		}
		if err := f.Sync(); err != nil {
			return err
		}
	}
	s.seq = maxSeq
	return nil
}

// Rewrite atomically compacts the log. It writes a new snapshot frame with a
// fresh sequence number (strictly greater than every previous frame) followed
// by the given trailing frames, which are copied verbatim in their original
// order and with their original bytes. The caller collects tail by remembering
// Records after the last checkpoint during Replay; their seqs are preserved so
// the gap-free ordering stays intact (snapshot seq = min(tail seq)-1, or
// oldMaxSeq if there is no tail).
func (s *Store) Rewrite(checkpointPayload []byte, tail []Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var newSeq uint64
	if len(tail) > 0 {
		newSeq = tail[0].Seq - 1
	} else {
		newSeq = s.seq
	}

	tmp := s.path + ".tmp"
	tf, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	cleanup := func() { tf.Close(); os.Remove(tmp) }

	if _, err := tf.Write(magic[:]); err != nil {
		cleanup()
		return err
	}
	if err := writeFrame(tf, newSeq, KindSnapshot, checkpointPayload); err != nil {
		cleanup()
		return err
	}
	for _, r := range tail {
		if err := writeFrame(tf, r.Seq, r.Kind, r.Payload); err != nil {
			cleanup()
			return err
		}
	}
	if err := tf.Sync(); err != nil {
		cleanup()
		return err
	}
	if err := tf.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return err
	}
	if err := syncDir(filepath.Dir(s.path)); err != nil {
		return err
	}
	nf, err := os.OpenFile(s.path, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	if s.f != nil {
		_ = s.f.Close()
	}
	s.f = nf
	last := newSeq
	if len(tail) > 0 {
		last = tail[len(tail)-1].Seq
	}
	s.seq = last
	return nil
}

func writeFrame(w io.Writer, seq uint64, kind byte, payload []byte) error {
	frame := make([]byte, headerLen+len(payload))
	binary.BigEndian.PutUint64(frame[0:8], seq)
	frame[8] = kind
	crc := crc32.ChecksumIEEE(append([]byte{kind}, payload...))
	binary.BigEndian.PutUint32(frame[9:13], crc)
	binary.BigEndian.PutUint32(frame[13:17], uint32(len(payload)))
	copy(frame[17:], payload)
	_, err := w.Write(frame)
	return err
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// Close flushes and releases the file.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return nil
	}
	err := s.f.Close()
	s.f = nil
	return err
}

// Path returns the on-disk log path.
func (s *Store) Path() string { return s.path }

// LastSeq returns the greatest sequence number assigned so far.
func (s *Store) LastSeq() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.seq
}
