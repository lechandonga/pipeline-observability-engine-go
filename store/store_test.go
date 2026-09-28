package store

import (
	"path/filepath"
	"testing"
)

func TestAppendReplayAndCompact(t *testing.T) {
	dir := t.TempDir()

	s, err := NewFileStore(filepath.Join(dir, "data"))
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	for i := int64(1); i <= 3; i++ {
		if err := s.Append(Record{Kind: KindEvent, Sequence: i, Payload: []byte{byte(i)}}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	recs, err := s.ReadAll(0)
	if err != nil {
		t.Fatalf("readall: %v", err)
	}
	if len(recs) != 3 {
		t.Fatalf("want 3 records, got %d", len(recs))
	}

	// Snapshot at sequence 2 must compact and hide seq<=2 on later replays.
	if err := s.SaveSnapshot(Snapshot{Sequence: 2, Data: []byte("state@2")}); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	recs2, err := s.ReadAll(2)
	if err != nil {
		t.Fatalf("readall after compact: %v", err)
	}
	if len(recs2) != 1 || recs2[0].Sequence != 3 {
		t.Fatalf("want only seq 3, got %+v", recs2)
	}
	snap, err := s.LoadSnapshot()
	if err != nil {
		t.Fatalf("load snapshot: %v", err)
	}
	if snap == nil || snap.Sequence != 2 || string(snap.Data) != "state@2" {
		t.Fatalf("bad snapshot: %+v", snap)
	}

	// Appends after compaction still replay correctly.
	if err := s.Append(Record{Kind: KindMetric, Sequence: 4, Payload: []byte("m4")}); err != nil {
		t.Fatalf("append 4: %v", err)
	}
	recs3, err := s.ReadAll(2)
	if err != nil {
		t.Fatalf("readall after append: %v", err)
	}
	if len(recs3) != 2 || recs3[0].Sequence != 3 || recs3[1].Sequence != 4 {
		t.Fatalf("want seq 3,4 got %+v", recs3)
	}
	if recs3[1].Kind != KindMetric || string(recs3[1].Payload) != "m4" {
		t.Fatalf("bad record 4: %+v", recs3[1])
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestReopenRecovery(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	s, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := s.Append(Record{Kind: KindEvent, Sequence: 1, Payload: []byte("e1")}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	snap, err := s2.LoadSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if snap != nil {
		t.Fatalf("unexpected snapshot: %+v", snap)
	}
	recs, err := s2.ReadAll(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 || string(recs[0].Payload) != "e1" {
		t.Fatalf("bad recovery: %+v", recs)
	}
}
