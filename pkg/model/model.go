// Package model defines the core data types shared across the engine.
package model

import "time"

// EventType enumerates pipeline run lifecycle events.
type EventType string

const (
	EventStarted   EventType = "started"
	EventSucceeded EventType = "succeeded"
	EventFailed    EventType = "failed"
	EventRetried   EventType = "retried"
)

// Event is a lifecycle event reported for one pipeline run.
type Event struct {
	ID        string    `json:"id"`
	Pipeline  string    `json:"pipeline"`
	RunID     string    `json:"run_id"`
	Type      EventType `json:"type"`
	Timestamp time.Time `json:"ts"`
}

// MetricKind enumerates the supported metric families.
type MetricKind string

const (
	MetricFreshness  MetricKind = "freshness"
	MetricLatency    MetricKind = "latency"
	MetricThroughput MetricKind = "throughput"
	MetricFailure    MetricKind = "failure_retry"
	MetricSLA        MetricKind = "sla"
)

// Metric is a numeric observation at a point in event-time.
type Metric struct {
	ID        string     `json:"id"`
	Pipeline  string     `json:"pipeline"`
	RunID     string     `json:"run_id"`
	Kind      MetricKind `json:"kind"`
	Value     float64    `json:"value"`
	Timestamp time.Time  `json:"ts"`
}

// RejectReason explains why a report was not applied.
type RejectReason string

const (
	RejectNone          RejectReason = ""
	RejectDuplicate     RejectReason = "duplicate"
	RejectLate          RejectReason = "window_expired"
	RejectFuture        RejectReason = "future_too_far"
	RejectInvalid       RejectReason = "invalid_data"
	RejectStateConflict RejectReason = "state_conflict"
)

// IngestResult is the outcome of a single report.
type IngestResult struct {
	Accepted bool         `json:"accepted"`
	Reason   RejectReason `json:"reason,omitempty"`
	Detail   string       `json:"detail,omitempty"`
	Seq      uint64       `json:"seq,omitempty"`
}

// AlertState is the lifecycle state of an alert.
type AlertState string

const (
	AlertActive    AlertState = "active"
	AlertRecovered AlertState = "recovered"
	AlertClosed    AlertState = "closed"
)

// Alert is one deduplicated alert instance.
type Alert struct {
	Key         string     `json:"key"`
	RuleID      string     `json:"rule_id"`
	RuleVersion int        `json:"rule_version"`
	Pipeline    string     `json:"pipeline"`
	State       AlertState `json:"state"`
	Value       float64    `json:"value"`
	Message     string     `json:"message"`
	FirstSeen   time.Time  `json:"first_seen"`
	LastSeen    time.Time  `json:"last_seen"`
	RecoveredAt time.Time  `json:"recovered_at,omitempty"`
	ClosedAt    time.Time  `json:"closed_at,omitempty"`
	Count       int        `json:"count"`
}

// RunStatus is the derived lifecycle state of one run.
type RunStatus string

const (
	RunPending   RunStatus = "pending"
	RunRunning   RunStatus = "running"
	RunSucceeded RunStatus = "succeeded"
	RunFailed    RunStatus = "failed"
)

// RunState is the merged state of one pipeline run.
type RunState struct {
	Pipeline    string    `json:"pipeline"`
	RunID       string    `json:"run_id"`
	Status      RunStatus `json:"status"`
	StartedAt   time.Time `json:"started_at,omitempty"`
	EndedAt     time.Time `json:"ended_at,omitempty"`
	Attempts    int       `json:"attempts"`
	LastEventID string    `json:"last_event_id,omitempty"`
	UpdatedSeq  uint64    `json:"updated_seq"`
}
