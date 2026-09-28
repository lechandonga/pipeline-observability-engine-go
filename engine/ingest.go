package engine

import (
	"encoding/json"
	"fmt"

	"github.com/lechandonga/pipeline-observability-engine-go/model"
	"github.com/lechandonga/pipeline-observability-engine-go/store"
)

func reject(reason model.RejectReason, format string, args ...any) model.IngestResult {
	return model.IngestResult{Outcome: model.OutcomeRejected, Reason: reason, Message: fmt.Sprintf(format, args...)}
}

func accept(seq int64) model.IngestResult {
	return model.IngestResult{Outcome: model.OutcomeAccepted, Reason: model.ReasonNone, Sequence: seq}
}

// inWindow validates the event-time lateness window. It returns a reject
// result (already populated) or nil when the timestamp is acceptable.
func (e *Engine) inWindow(ts int64, now int64) model.IngestResult {
	if ts <= 0 {
		return reject(model.ReasonInvalid, "timestamp must be positive unix millis, got %d", ts)
	}
	if now-ts > e.cfg.LateWindowMS {
		return reject(model.ReasonLate,
			"event time %d is %dms behind now %d, late window is %dms",
			ts, now-ts, now, e.cfg.LateWindowMS)
	}
	if ts-now > e.cfg.FutureSkewMS {
		return reject(model.ReasonFutureSkew,
			"event time %d is %dms ahead of now %d, future skew is %dms",
			ts, ts-now, now, e.cfg.FutureSkewMS)
	}
	return model.IngestResult{}
}

// IngestEvent applies an idempotent lifecycle event.
//
// Rejection precedence: invalid data -> window expiry/future skew ->
// duplicate -> run state conflict. Rejected input is never persisted and
// never mutates any state.
func (e *Engine) IngestEvent(ev model.Event) model.IngestResult {
	if ev.ID == "" {
		return reject(model.ReasonInvalid, "event id is required")
	}
	if ev.Pipeline == "" {
		return reject(model.ReasonInvalid, "pipeline is required")
	}
	if ev.RunID == "" {
		return reject(model.ReasonInvalid, "run id is required")
	}
	if !validEventType(ev.Type) {
		return reject(model.ReasonInvalid, "unknown event type %q", ev.Type)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	now := e.clk.NowMillis()
	if r := e.inWindow(ev.Timestamp, now); r.Outcome == model.OutcomeRejected {
		return r
	}

	p := e.pipeLocked(ev.Pipeline)
	if _, dup := p.Seen[ev.ID]; dup {
		return reject(model.ReasonDuplicate, "event id %q already accepted for pipeline %q", ev.ID, ev.Pipeline)
	}

	key := runKey{ev.Pipeline, ev.RunID}
	run := e.runs[key]
	isNewRun := run == nil
	if isNewRun {
		if ev.Type != model.EventRunStarted {
			return reject(model.ReasonStateConflict,
				"event %q for unknown run %q must be preceded by run_started", ev.Type, ev.RunID)
		}
	} else if run.Status != model.RunActive {
		// The run already reached a terminal conclusion; late lifecycle
		// events must not resurrect or overwrite it.
		return reject(model.ReasonStateConflict,
			"run %q is already %s at %d; event %q cannot change it",
			ev.RunID, run.Status, run.EndedAt, ev.Type)
	} else if ev.Type == model.EventRunStarted {
		// The run is active and already started.
		return reject(model.ReasonStateConflict, "run %q already started", ev.RunID)
	}

	// All validation passed: make the fact durable BEFORE mutating any
	// state, so a durability failure leaves the engine exactly as before.
	seq := e.nextSeq()
	payload, _ := json.Marshal(ev)
	if err := e.st.Append(store.Record{Kind: store.KindEvent, Sequence: seq, Payload: payload}); err != nil {
		e.seq--
		return reject(model.ReasonInvalid, "durability failure: %v", err)
	}

	if isNewRun {
		// ev.Type is guaranteed to be run_started here.
		run = &model.RunState{
			Pipeline:  ev.Pipeline,
			RunID:     ev.RunID,
			Status:    model.RunActive,
			StartedAt: ev.Timestamp,
		}
		e.runs[key] = run
	} else if err := e.applyEventToRun(run, ev); err != nil {
		// Unreachable: the transition was validated above.
		return reject(model.ReasonStateConflict, "%v", err)
	}

	p.Seen[ev.ID] = struct{}{}
	e.advanceWatermark(p, ev.Timestamp)
	if ev.Type == model.EventRunSucceeded {
		p.LastSuccess = max64(p.LastSuccess, ev.Timestamp)
	}
	if ev.Type == model.EventRunFailed {
		p.FailuresTotal++
	}
	if ev.Type == model.EventRetryScheduled {
		p.RetriesTotal++
	}
	run.Watermark = max64(run.Watermark, ev.Timestamp)
	run.LastEventID = ev.ID

	e.evaluateLocked(ev.Pipeline, now)
	return accept(seq)
}

func (e *Engine) applyEventToRun(run *model.RunState, ev model.Event) error {
	switch ev.Type {
	case model.EventRunSucceeded, model.EventRunFailed:
		status := model.RunSucceeded
		if ev.Type == model.EventRunFailed {
			status = model.RunFailed
		}
		run.Status = status
		run.EndedAt = ev.Timestamp
	case model.EventRetryScheduled:
		run.Retries++
	case model.EventRetrySucceeded:
		run.Status = model.RunSucceeded
		run.EndedAt = ev.Timestamp
	default:
		return fmt.Errorf("unsupported transition %q", ev.Type)
	}
	return nil
}

func validEventType(t model.EventType) bool {
	switch t {
	case model.EventRunStarted, model.EventRunSucceeded, model.EventRunFailed,
		model.EventRetryScheduled, model.EventRetrySucceeded:
		return true
	}
	return false
}

// IngestMetric applies one metric point with the same validation and
// idempotency guarantees as IngestEvent.
func (e *Engine) IngestMetric(mt model.Metric) model.IngestResult {
	if mt.ID == "" {
		return reject(model.ReasonInvalid, "metric id is required")
	}
	if mt.Pipeline == "" {
		return reject(model.ReasonInvalid, "pipeline is required")
	}
	if !validMetricName(mt.Name) {
		return reject(model.ReasonInvalid, "unknown metric %q", mt.Name)
	}
	if !metricValueOK(mt.Name, mt.Value) {
		return reject(model.ReasonInvalid, "illegal value %v for metric %q", mt.Value, mt.Name)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	now := e.clk.NowMillis()
	if r := e.inWindow(mt.Timestamp, now); r.Outcome == model.OutcomeRejected {
		return r
	}

	p := e.pipeLocked(mt.Pipeline)
	if _, dup := p.Seen[mt.ID]; dup {
		return reject(model.ReasonDuplicate, "metric id %q already accepted for pipeline %q", mt.ID, mt.Pipeline)
	}

	seq := e.nextSeq()
	payload, _ := json.Marshal(mt)
	if err := e.st.Append(store.Record{Kind: store.KindMetric, Sequence: seq, Payload: payload}); err != nil {
		e.seq--
		return reject(model.ReasonInvalid, "durability failure: %v", err)
	}

	p.Seen[mt.ID] = struct{}{}
	e.advanceWatermark(p, mt.Timestamp)
	e.applyMetricLocked(p, mt, now)
	return accept(seq)
}

// applyMetricLocked folds an accepted metric into aggregates and prunes the
// throughput window to the configured span. Caller holds e.mu.
func (e *Engine) applyMetricLocked(p *pipelineAgg, mt model.Metric, now int64) {
	switch mt.Name {
	case model.MetricLatencyMS:
		if !p.HasLatency || mt.Timestamp >= p.LatestLatencyAt {
			p.LatestLatency = mt.Value
			p.LatestLatencyAt = mt.Timestamp
			p.HasLatency = true
		}
	case model.MetricRecords:
		p.Records = append(p.Records, metricPoint{Timestamp: mt.Timestamp, Value: mt.Value})
		e.pruneRecordsLocked(p, now)
	case model.MetricFailures:
		p.FailuresTotal += int(mt.Value)
	case model.MetricDataArrived:
		// watermark already advanced above; nothing else to do.
	}
	e.evaluateLocked(p.Name, now)
}

// pruneRecordsLocked drops throughput points older than the throughput
// window relative to the current clock, keeping RPS queries O(window).
func (e *Engine) pruneRecordsLocked(p *pipelineAgg, now int64) {
	cutoff := now - e.cfg.ThroughputWindowMS
	keep := p.Records[:0]
	for _, pt := range p.Records {
		if pt.Timestamp >= cutoff {
			keep = append(keep, pt)
		}
	}
	p.Records = keep
}

func (e *Engine) advanceWatermark(p *pipelineAgg, ts int64) {
	if ts > p.Watermark {
		p.Watermark = ts
	}
}

func validMetricName(n model.MetricName) bool {
	switch n {
	case model.MetricLatencyMS, model.MetricRecords, model.MetricFailures, model.MetricDataArrived:
		return true
	}
	return false
}

func metricValueOK(n model.MetricName, v float64) bool {
	if v < 0 {
		return false
	}
	if n == model.MetricLatencyMS && v == 0 {
		return false
	}
	return true
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
