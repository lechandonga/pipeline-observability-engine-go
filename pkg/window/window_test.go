package window

import (
	"testing"
	"time"
)

func TestLateAndFuture(t *testing.T) {
	base := time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)
	w := New(10*time.Second, 5*time.Second)

	// First accepted timestamp establishes the watermark.
	late, future := w.Check(base, base)
	if late || future {
		t.Fatalf("first event should be admissible, got late=%v future=%v", late, future)
	}
	w.Observe(base)

	// In-order event.
	late, future = w.Check(base.Add(3*time.Second), base.Add(3*time.Second))
	if late || future {
		t.Fatalf("in-order event rejected: late=%v future=%v", late, future)
	}
	w.Observe(base.Add(3 * time.Second))

	// Out-of-order but within tolerance: admissible.
	late, future = w.Check(base.Add(-5*time.Second), base.Add(3*time.Second))
	if late || future {
		t.Fatalf("slightly late event rejected: late=%v future=%v", late, future)
	}

	// Beyond the late window: rejected.
	late, future = w.Check(base.Add(-8*time.Second), base.Add(3*time.Second))
	if !late {
		t.Fatal("event 8s before watermark with tolerance 10s: want late")
	}
	// Boundary: exactly at watermark-tolerance is still admissible (Before is strict).
	late, _ = w.Check(base.Add(3*time.Second).Add(-10*time.Second), base.Add(3*time.Second))
	if late {
		t.Fatal("event exactly at the boundary must be admissible (half-open interval)")
	}

	// Too far in the future relative to wall clock.
	_, future = w.Check(base.Add(20*time.Second), base.Add(3*time.Second))
	if !future {
		t.Fatal("event 20s ahead with future tolerance 5s must be rejected")
	}

	// A rejected (late) observation must not move the watermark.
	before := w.Watermark()
	w.Observe(base.Add(-8 * time.Second))
	// Observe itself always advances; the engine must call it only for accepted
	// reports. Verify explicit contract: call Observe with a late ts would move
	// nothing since it is older.
	if !w.Watermark().Equal(before) {
		t.Fatal("older timestamp must not change watermark")
	}
}

func TestBounds(t *testing.T) {
	now := time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)
	w := New(time.Minute, 30*time.Second)
	low, high := w.Bounds(now)
	if !low.IsZero() {
		t.Fatalf("low bound should be zero before first observation, got %v", low)
	}
	if !high.Equal(now.Add(30 * time.Second)) {
		t.Fatalf("high = %v", high)
	}
	w.Observe(now.Add(2 * time.Minute))
	low, high = w.Bounds(now.Add(2 * time.Minute))
	if !low.Equal(now.Add(1 * time.Minute)) {
		t.Fatalf("low = %v, want %v", low, now.Add(time.Minute))
	}
}
