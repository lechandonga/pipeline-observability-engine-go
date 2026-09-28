// Package clock provides an injectable time source so event-time logic can be
// tested deterministically without sleeping.
package clock

import "time"

// Clock abstracts wall-clock access.
type Clock interface {
	Now() time.Time
}

// SystemClock reads the real wall clock.
type SystemClock struct{}

// NewSystemClock constructs a SystemClock.
func NewSystemClock() *SystemClock { return &SystemClock{} }

// Now returns the current wall-clock time in UTC.
func (SystemClock) Now() time.Time { return time.Now().UTC() }

// FakeClock is a manually controlled clock for tests.
type FakeClock struct {
	t time.Time
}

// NewFakeClock constructs a FakeClock at t.
func NewFakeClock(t time.Time) *FakeClock { return &FakeClock{t: t} }

// Now returns the controlled time.
func (c *FakeClock) Now() time.Time { return c.t }

// Set moves the clock to t.
func (c *FakeClock) Set(t time.Time) { c.t = t }

// Advance moves the clock forward by d.
func (c *FakeClock) Advance(d time.Duration) { c.t = c.t.Add(d) }
