package store

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestAppendReplayRoundtrip(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(filepath.Join(dir, "wal"))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		seq, err := s.Append(KindEvent, []byte{byte('a' + i)})
		if err != nil {
			t.Fatal(err)
		}
		if seq != uint64(i+1) {
			t.Fatalf("seq = %d, want %d", seq, i+1)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: state must survive.
	s2, err := Open(filepath.Join(dir, "wal"))
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	var got []Record
	if err := s2.Replay(func(r Record) error {
		got = append(got, r)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 5 {
		t.Fatalf("replayed %d records, want 5", len(got))
	}
	for i, r := range got {
		if r.Seq != uint64(i+1) || r.Kind != KindEvent || !bytes.Equal(r.Payload, []byte{byte('a' + i)}) {
			t.Fatalf("record %d mismatch: %+v", i, r)
		}
	}
	// Sequence continues after restart.
	seq, err := s2.Append(KindMetric, []byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	if seq != 6 {
		t.Fatalf("post-restart seq = %d, want 6", seq)
	}
}

func TestTornTailTruncated(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append(KindEvent, []byte("complete")); err != nil {
		t.Fatal(err)
	}
	s.Close()

	// Simulate a crash mid-write: append partial garbage bytes.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(make([]byte, 10)); err != nil {
		t.Fatal(err)
	}
	f.Close()

	s2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	n := 0
	if err := s2.Replay(func(r Record) error { n++; return nil }); err != nil {
		t.Fatalf("torn tail should be tolerated, got %v", err)
	}
	if n != 1 {
		t.Fatalf("replayed %d records, want 1 valid", n)
	}
	// Appending after truncation must land on a frame boundary and replay.
	if _, err := s2.Append(KindMetric, []byte("after")); err != nil {
		t.Fatal(err)
	}
	n = 0
	if err := s2.Replay(func(r Record) error { n++; return nil }); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("replayed %d records after append, want 2", n)
	}
}

func TestRewriteCompaction(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(filepath.Join(dir, "wal"))
	if err != nil {
		t.Fatal(err)
	}
	var recs []Record
	if err := s.Replay(func(r Record) error { return nil }); err != nil {
		t.Fatal(err)
	}
	// Write 6 records; checkpoint replaces first 4, keep last 2 verbatim.
	for i := 0; i < 6; i++ {
		if _, err := s.Append(KindEvent, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Replay(func(r Record) error {
		if r.Seq >= 5 {
			recs = append(recs, r)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Rewrite([]byte("ckpt"), recs); err != nil {
		t.Fatal(err)
	}

	// Reopen and verify contents.
	s.Close()
	s2, err := Open(filepath.Join(dir, "wal"))
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	var kinds []byte
	var seqs []uint64
	if err := s2.Replay(func(r Record) error {
		kinds = append(kinds, r.Kind)
		seqs = append(seqs, r.Seq)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(kinds) != 3 || kinds[0] != KindSnapshot || kinds[1] != KindEvent || kinds[2] != KindEvent {
		t.Fatalf("kinds after rewrite = %v", kinds)
	}
	if seqs[0] != 4 || seqs[1] != 5 || seqs[2] != 6 {
		t.Fatalf("seqs after rewrite = %v", seqs)
	}
	seq, err := s2.Append(KindMetric, []byte("z"))
	if err != nil {
		t.Fatal(err)
	}
	if seq != 7 {
		t.Fatalf("seq after rewrite = %d, want 7", seq)
	}
}
