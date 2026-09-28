// Package clock provides an injectable time source so tests can be
// deterministic about ordering, lateness and restarts.
package clock

import "time"

// Clock returns the current wall-clock time in unix milliseconds.
type Clock interface {
	NowMillis() int64
}

// SystemClock reads the real wall clock.
type SystemClock struct{}

// NowMillis implements Clock.
func (SystemClock) NowMillis() int64 {
	return time.Now().UnixMilli()
}

// FakeClock is a manually advanced clock used by tests.
type FakeClock struct {
	T int64
}

// NowMillis implements Clock.
func (f *FakeClock) NowMillis() int64 { return f.T }

// Advance moves the fake clock forward by the given milliseconds.
func (f *FakeClock) Advance(ms int64) { f.T += ms }
