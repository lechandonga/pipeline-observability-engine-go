package alert

import "github.com/lechandonga/pipeline-observability-engine-go/pkg/model"

// MetricSnapshot is the per-pipeline derived state a rule evaluates against.
type MetricSnapshot struct {
	// FreshnessSeconds is seconds since the latest data for the pipeline.
	// A negative value means "no data yet"; freshness rules skip it.
	FreshnessSeconds float64
	// LatencySeconds is the latest run duration.
	LatencySeconds float64
	// ThroughputRPM is the latest observed records-per-minute throughput.
	ThroughputRPM float64
	// Failures is the number of failed runs within the current run window.
	Failures int
	// Retries is the number of retry attempts within the current run window.
	Retries int
	// SLACompliant is the latest SLA conclusion.
	SLACompliant bool

	Avail MetricAvailability `json:"avail"`
}

// HasData reports whether the metric the rule reads has ever been observed.
func (r ThresholdRule) HasData(s MetricSnapshot) bool {
	switch r.Metric {
	case model.MetricFreshness:
		return s.Avail.HasFreshness
	case model.MetricLatency:
		return s.Avail.HasLatency
	case model.MetricThroughput:
		return s.Avail.HasThroughput
	case model.MetricFailure:
		return s.Avail.HasFailure
	case model.MetricSLA:
		return s.Avail.HasSLA
	default:
		return false
	}
}

// Availability flags: a rule whose metric has never been observed must not
// evaluate against a zero default value (e.g. throughput 0 would alert forever
// before the first report).
type MetricAvailability struct {
	HasFreshness  bool
	HasLatency    bool
	HasThroughput bool
	HasFailure    bool // counters are considered present only when > 0
	HasSLA        bool
}
