package dedup

import "testing"

func TestBoundedFIFODedup(t *testing.T) {
	s := NewSet(3)
	if !s.Add("a") || !s.Add("b") || !s.Add("c") {
		t.Fatal("first inserts must succeed")
	}
	if s.Add("b") {
		t.Fatal("duplicate add must return false")
	}
	if !s.Contains("a") || !s.Contains("c") {
		t.Fatal("contains wrong")
	}
	if s.Len() != 3 {
		t.Fatalf("len = %d", s.Len())
	}

	// Fourth insert evicts the oldest ("a").
	if !s.Add("d") {
		t.Fatal("new insert should succeed")
	}
	if s.Contains("a") {
		t.Fatal("oldest id a should have been evicted")
	}
	if !s.Contains("b") || !s.Contains("c") || !s.Contains("d") {
		t.Fatal("b,c,d must remain")
	}
	if s.Len() != 3 {
		t.Fatalf("len = %d, want 3", s.Len())
	}

	// After eviction the old id is treated as new again (documented semantics).
	if !s.Add("a") {
		t.Fatal("evicted id should be addable again")
	}
	if s.Contains("b") {
		t.Fatal("b should now be the evicted one")
	}
}
