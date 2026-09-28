// Package model holds the core value types shared across the engine.
package model

// EventType enumerates the lifecycle events a pipeline run can report.
type EventType string

const (
	EventRunStarted     EventType = "run_started"
	EventRunSucceeded   EventType = "run_succeeded"
	EventRunFailed      EventType = "run_failed"
	EventRetryScheduled EventType = "retry_scheduled"
	EventRetrySucceeded EventType = "retry_succeeded"
)

// Event is a lifecycle report. ID is the caller-provided idempotency key:
// two events with the same ID are the same event and must take effect once.
type Event struct {
	ID        string
	Pipeline  string
	RunID     string
	Type      EventType
	Timestamp int64 // unix milliseconds, event time
	Reason    string
}

// MetricName enumerates the metric series understood by the engine.
type MetricName string

const (
	MetricLatencyMS   MetricName = "latency_ms"
	MetricRecords     MetricName = "records"
	MetricFailures    MetricName = "failures"
	MetricDataArrived MetricName = "data_arrived" // used for freshness
)

// Metric is a single numeric data point at a given event time.
type Metric struct {
	ID        string
	Pipeline  string
	Name      MetricName
	Value     float64
	Timestamp int64 // unix milliseconds, event time
}

// Outcome marks whether an ingest call was applied.
type Outcome string

const (
	OutcomeAccepted Outcome = "accepted"
	OutcomeRejected Outcome = "rejected"
)

// RejectReason gives a stable, distinguishable machine code for rejections.
type RejectReason string

const (
	ReasonNone          RejectReason = ""
	ReasonInvalid       RejectReason = "invalid_data"
	ReasonDuplicate     RejectReason = "duplicate"
	ReasonLate          RejectReason = "window_expired"
	ReasonFutureSkew    RejectReason = "future_skew"
	ReasonStateConflict RejectReason = "state_conflict"
)

// IngestResult is returned for every reported event or metric.
type IngestResult struct {
	Outcome  Outcome
	Reason   RejectReason
	Message  string
	Sequence int64 // monotonic sequence number assigned on accept
}

// RunStatus is the lifecycle status of one pipeline run.
type RunStatus string

const (
	RunActive    RunStatus = "active"
	RunSucceeded RunStatus = "succeeded"
	RunFailed    RunStatus = "failed"
)

// RunState is the merged state of one run.
type RunState struct {
	Pipeline    string
	RunID       string
	Status      RunStatus
	StartedAt   int64
	EndedAt     int64
	Watermark   int64 // highest accepted event time for this run
	Retries     int
	LastEventID string
}

// AlertState is the lifecycle state of an alert.
type AlertState string

const (
	AlertFiring    AlertState = "firing"
	AlertRecovered AlertState = "recovered"
)

// Rule names.
const (
	RuleFreshness  = "freshness"
	RuleLatency    = "latency"
	RuleThroughput = "throughput"
	RuleFailures   = "failures"
	RuleRetries    = "retries"
	RuleSLA        = "sla"
)

// AlertSeverity classifies how strongly a rule is breached.
type AlertSeverity string

const (
	SeverityWarn     AlertSeverity = "warn"
	SeverityCritical AlertSeverity = "critical"
)

// Alert is an immutable-ish conclusion record. Once recovered the record is
// kept for history and never silently rewritten by later config versions.
type Alert struct {
	Fingerprint   string
	IncidentSeq   int // 1-based episode counter for this fingerprint
	Pipeline      string
	Rule          string
	Severity      AlertSeverity
	State         AlertState
	FiredAt       int64
	RecoveredAt   int64
	LastValue     float64
	Threshold     float64
	Message       string
	ConfigVersion int
	Sequence      int64
}

// PipelineStatus is a consistent query snapshot for one pipeline.
type PipelineStatus struct {
	Pipeline        string
	Watermark       int64
	LastSuccess     int64
	ActiveRuns      int
	LatestLatencyMS float64
	RetriesTotal    int
	FailuresTotal   int
	ActiveAlerts    []Alert
}
