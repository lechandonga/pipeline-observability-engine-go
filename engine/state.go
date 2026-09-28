package engine

import "github.com/lechandonga/pipeline-observability-engine-go/model"

// metricPoint is one accepted metric datum kept for windowed aggregation.
type metricPoint struct {
	Seq       int64   `json:"s"`
	Timestamp int64   `json:"t"`
	Value     float64 `json:"v"`
}

// pipelineAgg holds the merged, queryable state for one pipeline.
type pipelineAgg struct {
	Name        string `json:"name"`
	Watermark   int64  `json:"watermark"`
	LastSuccess int64  `json:"last_success"`

	// Latest accepted latency point.
	LatestLatency   float64 `json:"latest_latency"`
	LatestLatencyAt int64   `json:"latest_latency_at"`
	HasLatency      bool    `json:"has_latency"`

	// Throughput window (records-type metric points), kept pruned to the
	// configured throughput window so query time is bounded.
	Records []metricPoint `json:"records"`

	// Cumulative accepted counters (monotonic).
	FailuresTotal int `json:"failures_total"`
	RetriesTotal  int `json:"retries_total"`

	// Seen is the per-pipeline idempotency set: the union of accepted event
	// IDs and metric IDs. Any repeat with an ID already here is rejected as
	// duplicate and cannot double-count.
	Seen map[string]struct{} `json:"seen"`
}

// runKey identifies a run inside the engine.
type runKey struct {
	pipeline string
	runID    string
}

// snapshotState is the full engine state persisted atomically.
type snapshotState struct {
	Sequence int64                   `json:"sequence"`
	Runs     []model.RunState        `json:"runs"`
	Pipes    map[string]*pipelineAgg `json:"pipes"`
	Alerts   alertStateAlias         `json:"alerts"`
}
