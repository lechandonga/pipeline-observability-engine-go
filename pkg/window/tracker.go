package window

import "time"

// Tracker keeps one independent watermark per key (pipeline). Events for one
// pipeline must never move the boundary of another. NOT safe for concurrent
// use; the engine serializes access.
type Tracker struct {
	lateTolerance   time.Duration
	futureTolerance time.Duration
	marks           map[string]time.Time
}

// NewTracker constructs a Tracker with the given tolerances.
func NewTracker(lateTolerance, futureTolerance time.Duration) *Tracker {
	return &Tracker{
		lateTolerance:   lateTolerance,
		futureTolerance: futureTolerance,
		marks:           make(map[string]time.Time),
	}
}

// Check reports lateness/future-ness for key at timestamp ts relative to now.
func (t *Tracker) Check(key string, ts, now time.Time) (late bool, future bool) {
	if ts.After(now.Add(t.futureTolerance)) {
		future = true
	}
	if wm, ok := t.marks[key]; ok && ts.Before(wm.Add(-t.lateTolerance)) {
		late = true
	}
	return late, future
}

// Observe advances the watermark for key. Only accepted reports must be passed.
func (t *Tracker) Observe(key string, ts time.Time) {
	if wm, ok := t.marks[key]; !ok || ts.After(wm) {
		t.marks[key] = ts
	}
}

// Watermark returns the watermark for key.
func (t *Tracker) Watermark(key string) time.Time { return t.marks[key] }

// Export serializes watermarks for checkpoints.
func (t *Tracker) Export() map[string]time.Time {
	out := make(map[string]time.Time, len(t.marks))
	for k, v := range t.marks {
		out[k] = v
	}
	return out
}

// Import restores watermarks from a checkpoint.
func (t *Tracker) Import(marks map[string]time.Time) {
	t.marks = make(map[string]time.Time, len(marks))
	for k, v := range marks {
		t.marks[k] = v
	}
}

// BoundsFor returns the admissible [low, high] interval for one key.
func (t *Tracker) BoundsFor(key string, now time.Time) (low, high time.Time) {
	high = now.Add(t.futureTolerance)
	if wm, ok := t.marks[key]; ok {
		low = wm.Add(-t.lateTolerance)
	}
	return low, high
}
