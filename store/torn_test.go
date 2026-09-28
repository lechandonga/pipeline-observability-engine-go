package store

import (
	"os"
	"path/filepath"
	"testing"
)

func TestTornTrailingFrameIgnored(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	s, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Append(Record{Kind: KindEvent, Sequence: 1, Payload: []byte("good")}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Append a partial frame that could never have been fsync'ed whole.
	f, err := os.OpenFile(filepath.Join(dir, walName), os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0x00, 0x00, 0x00, 0x20, 0x01, 0x02}); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	recs, err := s2.ReadAll(0)
	if err != nil {
		t.Fatalf("torn frame must not fail replay: %v", err)
	}
	if len(recs) != 1 || string(recs[0].Payload) != "good" || recs[0].Sequence != 1 {
		t.Fatalf("want only the good frame, got %+v", recs)
	}
}
