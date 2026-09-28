// Package config defines the versioned rules used by the engine. Every
// config is immutable once created and carries a monotonically increasing
// Version so historical conclusions keep the config they were made under.
package config

// Threshold configures a single alerting rule. Zero disables the rule
// (except freshness/age rules where zero also means disabled).
type Threshold struct {
	Enabled    bool
	WarnAt     float64
	CriticalAt float64
}

// Config is an immutable snapshot of engine settings.
type Config struct {
	Version int

	// Lateness window: an event-time point is accepted only if
	// now - eventTime <= LateWindowMS (and eventTime <= now + FutureSkewMS).
	LateWindowMS int64
	FutureSkewMS int64

	// How long finished runs are retained for late/duplicate handling.
	RetentionMS int64

	// Alert thresholds.
	FreshnessStaleMS   int64 // fire if now - watermark exceeds this
	LatencyMS          Threshold
	ThroughputMinRPS   Threshold // records per second over ThroughputWindowMS
	ThroughputWindowMS int64
	FailuresTotal      Threshold
	RetriesTotal       Threshold
	SLABreachMS        int64 // run duration from start to success beyond this
}

// Default returns a starter configuration with version 1.
func Default() *Config {
	return &Config{
		Version:            1,
		LateWindowMS:       5 * 60 * 1000,
		FutureSkewMS:       30 * 1000,
		RetentionMS:        60 * 60 * 1000,
		FreshnessStaleMS:   2 * 60 * 1000,
		LatencyMS:          Threshold{Enabled: true, WarnAt: 1000, CriticalAt: 5000},
		ThroughputMinRPS:   Threshold{Enabled: true, WarnAt: 1, CriticalAt: 0.1},
		ThroughputWindowMS: 60 * 1000,
		FailuresTotal:      Threshold{Enabled: true, WarnAt: 1, CriticalAt: 3},
		RetriesTotal:       Threshold{Enabled: true, WarnAt: 3, CriticalAt: 5},
		SLABreachMS:        30 * 1000,
	}
}

// WithVersion returns a shallow copy with a new version number.
func (c *Config) WithVersion(v int) *Config {
	cp := *c
	cp.Version = v
	return &cp
}
