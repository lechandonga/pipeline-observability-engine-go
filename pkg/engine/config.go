package engine

import "time"

// Config holds engine tuning parameters.
type Config struct {
	// StorePath is the location of the append-only WAL. Empty means in-memory
	// operation (state is still merged but restart recovery is unavailable).
	StorePath string

	// LateTolerance: events with timestamps older than the pipeline watermark
	// minus this duration are rejected (RejectLate).
	LateTolerance time.Duration

	// FutureTolerance: events with timestamps more than this beyond wall-clock
	// time are rejected (RejectFuture).
	FutureTolerance time.Duration

	// DedupCapacity bounds how many report IDs are remembered for duplicate
	// detection (FIFO eviction beyond it).
	DedupCapacity int

	// KeepClosedFor: closed alerts remain queryable for this long after their
	// effective close time, then are pruned.
	KeepClosedFor time.Duration

	// RunRetention bounds how long finished runs (and their counters) remain
	// queryable; running runs are retained regardless.
	RunRetention time.Duration

	// CompactionThreshold: rewrite the WAL with a fresh checkpoint once this
	// many frames have been appended since the last compaction.
	CompactionThreshold int
}

// DefaultConfig returns conservative local defaults.
func DefaultConfig(path string) Config {
	return Config{
		StorePath:           path,
		LateTolerance:       5 * time.Minute,
		FutureTolerance:     30 * time.Second,
		DedupCapacity:       100_000,
		KeepClosedFor:       24 * time.Hour,
		RunRetention:        30 * 24 * time.Hour,
		CompactionThreshold: 512,
	}
}
