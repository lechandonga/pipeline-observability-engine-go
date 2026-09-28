// Package window implements event-time windowing.
//
// A monotonic watermark (the greatest accepted event timestamp) defines what
// "late" means: an event whose timestamp is more than LateTolerance behind the
// watermark is rejected. Events too far ahead of wall-clock time are also
// rejected so a single bogus timestamp cannot push the watermark into the far
// future and silence every later report.
package window

import "time"

// Window tracks the watermark used to reject late or too-far-future events.
//
// All methods are NOT safe for concurrent use; the engine serializes access.
type Window struct {
	lateTolerance   time.Duration
	futureTolerance time.Duration
	watermark       time.Time
	initialized     bool
}

// New constructs a Window with the given tolerances.
// A non-positive lateTolerance means "no late events at all" (watermark is
// exclusive); futureTolerance of 0 still rejects strictly-future timestamps.
func New(lateTolerance, futureTolerance time.Duration) *Window {
	return &Window{
		lateTolerance:   lateTolerance,
		futureTolerance: futureTolerance,
	}
}

// Watermark returns the current watermark (max accepted timestamp).
func (w *Window) Watermark() time.Time { return w.watermark }

// Observe advances the watermark when ts is newer. Only ACCEPTED reports must
// be observed, so rejected input can never move the boundary.
func (w *Window) Observe(ts time.Time) {
	if !w.initialized {
		w.initialized = true
		w.watermark = ts
		return
	}
	if ts.After(w.watermark) {
		w.watermark = ts
	}
}

// Check decides whether ts is admissible relative to the current watermark and
// wall-clock now. It is side-effect free.
//
//	late   = an earlier event already established a watermark more than
//	         LateTolerance ahead of ts.
//	future = ts lies more than FutureTolerance beyond now.
func (w *Window) Check(ts, now time.Time) (late bool, future bool) {
	if w.initialized && ts.Before(w.watermark.Add(-w.lateTolerance)) {
		late = true
	}
	if ts.After(now.Add(w.futureTolerance)) {
		future = true
	}
	return late, future
}

// Bounds returns the currently admissible event-time interval [low, high]
// relative to now. Low is zero when no timestamp has been observed yet.
func (w *Window) Bounds(now time.Time) (low, high time.Time) {
	if w.initialized {
		low = w.watermark.Add(-w.lateTolerance)
	}
	high = now.Add(w.futureTolerance)
	return low, high
}
