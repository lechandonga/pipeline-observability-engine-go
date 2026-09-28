package engine

import (
	"strings"
	"time"

	"github.com/lechandonga/pipeline-observability-engine-go/pkg/model"
)

// pipelineState holds all derived state for one pipeline.
type pipelineState struct {
	Runs   map[string]*model.RunState       `json:"runs"`
	Latest map[model.MetricKind]metricValue `json:"latest"`
	// FailedRunIDs and RetriedRunIDs back failure/retry counters over the
	// retained window (deduped per run, so retries never double count).
	FailedRunIDs  map[string]struct{} `json:"failed_run_ids"`
	RetriedRunIDs map[string]struct{} `json:"retried_run_ids"`
}

type metricValue struct {
	Value     float64   `json:"v"`
	Timestamp time.Time `json:"ts"`
}

func newPipelineState() *pipelineState {
	return &pipelineState{
		Runs:          make(map[string]*model.RunState),
		Latest:        make(map[model.MetricKind]metricValue),
		FailedRunIDs:  make(map[string]struct{}),
		RetriedRunIDs: make(map[string]struct{}),
	}
}

// validateEvent checks well-formedness. Order of checks is stable so rejection
// reasons are distinguishable.
func validateEvent(ev model.Event) string {
	if strings.TrimSpace(ev.ID) == "" {
		return "event id is empty"
	}
	if strings.TrimSpace(ev.Pipeline) == "" {
		return "pipeline is empty"
	}
	if strings.TrimSpace(ev.RunID) == "" {
		return "run id is empty"
	}
	if ev.Timestamp.IsZero() {
		return "event timestamp is zero"
	}
	switch ev.Type {
	case model.EventStarted, model.EventSucceeded, model.EventFailed, model.EventRetried:
	default:
		return "unknown event type: " + string(ev.Type)
	}
	return ""
}

func validateMetric(m model.Metric) string {
	if strings.TrimSpace(m.ID) == "" {
		return "metric id is empty"
	}
	if strings.TrimSpace(m.Pipeline) == "" {
		return "pipeline is empty"
	}
	if m.Timestamp.IsZero() {
		return "metric timestamp is zero"
	}
	switch m.Kind {
	case model.MetricFreshness, model.MetricLatency, model.MetricThroughput,
		model.MetricFailure, model.MetricSLA:
	default:
		return "unknown metric kind: " + string(m.Kind)
	}
	if m.Value < 0 {
		return "metric value must be non-negative"
	}
	if m.Kind == model.MetricSLA && m.Value > 1 {
		return "sla metric value must be within [0,1]"
	}
	return ""
}

// mergeEvent applies a valid, deduped, in-window event. It returns false with a
// reason when the event conflicts with the already established run state; such
// conflicts never mutate the existing state.
func mergeEvent(ps *pipelineState, ev model.Event, seq uint64) (bool, model.RejectReason, string) {
	run := ps.Runs[ev.RunID]
	ts := ev.Timestamp

	switch ev.Type {
	case model.EventStarted:
		if run == nil {
			ps.Runs[ev.RunID] = &model.RunState{
				Pipeline: ev.Pipeline, RunID: ev.RunID, Status: model.RunRunning,
				StartedAt: ts, LastEventID: ev.ID, UpdatedSeq: seq,
			}
			return true, model.RejectNone, ""
		}
		return false, model.RejectStateConflict,
			"run already " + string(run.Status) + ": started cannot apply"

	case model.EventRetried:
		if run == nil {
			return false, model.RejectStateConflict, "retry before any started event"
		}
		if run.Status == model.RunSucceeded {
			return false, model.RejectStateConflict, "cannot retry a succeeded run"
		}
		// retried is legal while running or failed.
		run.Attempts++
		run.LastEventID = ev.ID
		run.UpdatedSeq = seq
		ps.RetriedRunIDs[ev.RunID] = struct{}{}
		return true, model.RejectNone, ""

	case model.EventFailed:
		if run == nil {
			return false, model.RejectStateConflict, "failure before any started event"
		}
		if run.Status == model.RunSucceeded {
			return false, model.RejectStateConflict, "cannot fail a succeeded run"
		}
		if run.Status == model.RunFailed && !run.EndedAt.IsZero() {
			return false, model.RejectStateConflict, "run already failed"
		}
		run.Status = model.RunFailed
		if run.EndedAt.IsZero() || ts.After(run.EndedAt) {
			run.EndedAt = ts
		}
		run.LastEventID = ev.ID
		run.UpdatedSeq = seq
		ps.FailedRunIDs[ev.RunID] = struct{}{}
		return true, model.RejectNone, ""

	case model.EventSucceeded:
		if run == nil {
			return false, model.RejectStateConflict, "success before any started event"
		}
		if run.Status == model.RunSucceeded {
			return false, model.RejectStateConflict, "run already succeeded"
		}
		run.Status = model.RunSucceeded
		if run.EndedAt.IsZero() || ts.After(run.EndedAt) {
			run.EndedAt = ts
		}
		run.LastEventID = ev.ID
		run.UpdatedSeq = seq
		// A run that ultimately succeeded is no longer counted as failed.
		delete(ps.FailedRunIDs, ev.RunID)
		return true, model.RejectNone, ""
	}
	return false, model.RejectInvalid, "unhandled event type"
}

// applyMetric stores a validated metric observation.
func applyMetric(ps *pipelineState, m model.Metric, seq uint64) {
	cur, ok := ps.Latest[m.Kind]
	if !ok || !m.Timestamp.Before(cur.Timestamp) {
		ps.Latest[m.Kind] = metricValue{Value: m.Value, Timestamp: m.Timestamp}
	}
	// seq is tracked via the outer WAL; nothing else needed per metric.
}

// checkEvent is a side-effect-free version of mergeEvent's transition rules.
// It mirrors mergeEvent exactly so a report that passes the pre-check is
// guaranteed to merge durably afterwards.
func checkEvent(ps *pipelineState, ev model.Event) (bool, model.RejectReason, string) {
	run := ps.Runs[ev.RunID]
	switch ev.Type {
	case model.EventStarted:
		if run != nil {
			return false, model.RejectStateConflict,
				"run already " + string(run.Status) + ": started cannot apply"
		}
	case model.EventRetried:
		if run == nil {
			return false, model.RejectStateConflict, "retry before any started event"
		}
		if run.Status == model.RunSucceeded {
			return false, model.RejectStateConflict, "cannot retry a succeeded run"
		}
	case model.EventFailed:
		if run == nil {
			return false, model.RejectStateConflict, "failure before any started event"
		}
		if run.Status == model.RunSucceeded {
			return false, model.RejectStateConflict, "cannot fail a succeeded run"
		}
		if run.Status == model.RunFailed && !run.EndedAt.IsZero() {
			return false, model.RejectStateConflict, "run already failed"
		}
	case model.EventSucceeded:
		if run == nil {
			return false, model.RejectStateConflict, "success before any started event"
		}
		if run.Status == model.RunSucceeded {
			return false, model.RejectStateConflict, "run already succeeded"
		}
	}
	return true, model.RejectNone, ""
}
