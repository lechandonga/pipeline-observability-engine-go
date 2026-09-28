package engine

import (
	"encoding/json"
	"sort"

	"github.com/lechandonga/pipeline-observability-engine-go/alert"
	"github.com/lechandonga/pipeline-observability-engine-go/model"
	"github.com/lechandonga/pipeline-observability-engine-go/store"
)

// evaluateLocked runs the rule set for one pipeline and durably records any
// alert lifecycle change. Caller holds e.mu.
func (e *Engine) evaluateLocked(pipeline string, now int64) {
	p := e.pipes[pipeline]
	e.pruneRecordsLocked(p, now)
	e.pruneFinishedRunsLocked(now)

	var longest int64
	active := 0
	for k, r := range e.runs {
		if k.pipeline != pipeline || r.Status != model.RunActive {
			continue
		}
		active++
		dur := now - r.StartedAt
		if dur > longest {
			longest = dur
		}
	}

	windowSec := e.cfg.ThroughputWindowMS / 1000
	rps := 0.0
	if windowSec > 0 {
		var total float64
		for _, pt := range p.Records {
			total += pt.Value
		}
		rps = total / float64(windowSec)
	}

	req := alert.EvaluateRequest{
		Pipeline:      pipeline,
		Now:           now,
		HasWatermark:  p.Watermark > 0,
		Watermark:     p.Watermark,
		LatestLatency: p.LatestLatency,
		HasLatency:    p.HasLatency,
		RPS:           rps,
		FailuresTotal: p.FailuresTotal,
		RetriesTotal:  p.RetriesTotal,
		LongestRunMS:  longest,
		HasActiveRuns: active > 0,
	}

	changed := e.alerts.Apply(req, e.nextSeq)
	for i := range changed {
		a := changed[i]
		payload, _ := json.Marshal(a)
		if err := e.st.Append(store.Record{Kind: store.KindAlert, Sequence: a.Sequence, Payload: payload}); err != nil {
			// We deliberately do NOT reuse this sequence number (that would
			// collide with future facts). The in-memory conclusion is kept;
			// if this process dies without the record, recovery replays facts
			// and the first post-restart evaluation re-emits the same
			// incident (fingerprint+incident dedupe keeps conclusions stable).
			continue
		}
	}
}

// pruneFinishedRunsLocked bounds memory by dropping terminal runs after the
// retention period. Active runs and alert history are never dropped.
// Caller holds e.mu.
func (e *Engine) pruneFinishedRunsLocked(now int64) {
	retention := e.cfg.RetentionMS
	if e.cfg.LateWindowMS > retention {
		retention = e.cfg.LateWindowMS
	}
	for k, r := range e.runs {
		if r.Status != model.RunActive && now-r.EndedAt > retention {
			delete(e.runs, k)
		}
	}
}

// evaluateAllLocked re-evaluates every known pipeline (used after recovery
// and after config changes). Caller holds e.mu.
func (e *Engine) evaluateAllLocked(now int64) {
	names := make([]string, 0, len(e.pipes))
	for name := range e.pipes {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		e.evaluateLocked(name, now)
	}
}

// mergeRecoveredEvent reapplies a durable event without persistence or
// validation: by definition it passed validation before it was fsync'ed.
func (e *Engine) mergeRecoveredEvent(ev model.Event, seq int64) {
	p := e.pipeLocked(ev.Pipeline)
	p.Seen[ev.ID] = struct{}{}
	e.advanceWatermark(p, ev.Timestamp)

	key := runKey{ev.Pipeline, ev.RunID}
	run := e.runs[key]
	if run == nil {
		run = &model.RunState{Pipeline: ev.Pipeline, RunID: ev.RunID, Status: model.RunActive}
		e.runs[key] = run
	}
	if ev.Type == model.EventRunStarted && run.StartedAt == 0 {
		run.StartedAt = ev.Timestamp
	}
	// Only merge lifecycle transitions that were valid when accepted.
	switch ev.Type {
	case model.EventRunSucceeded:
		if run.Status == model.RunActive {
			run.Status = model.RunSucceeded
			run.EndedAt = ev.Timestamp
		}
		p.LastSuccess = max64(p.LastSuccess, ev.Timestamp)
	case model.EventRunFailed:
		if run.Status == model.RunActive {
			run.Status = model.RunFailed
			run.EndedAt = ev.Timestamp
			p.FailuresTotal++
		}
	case model.EventRetrySucceeded:
		if run.Status == model.RunActive {
			run.Status = model.RunSucceeded
			run.EndedAt = ev.Timestamp
		}
	case model.EventRetryScheduled:
		run.Retries++
		p.RetriesTotal++
	}
	run.Watermark = max64(run.Watermark, ev.Timestamp)
	run.LastEventID = ev.ID
}

// mergeRecoveredMetric reapplies a durable metric without persistence.
func (e *Engine) mergeRecoveredMetric(mt model.Metric, seq int64) {
	p := e.pipeLocked(mt.Pipeline)
	p.Seen[mt.ID] = struct{}{}
	e.advanceWatermark(p, mt.Timestamp)
	switch mt.Name {
	case model.MetricLatencyMS:
		if !p.HasLatency || mt.Timestamp >= p.LatestLatencyAt {
			p.LatestLatency = mt.Value
			p.LatestLatencyAt = mt.Timestamp
			p.HasLatency = true
		}
	case model.MetricRecords:
		p.Records = append(p.Records, metricPoint{Timestamp: mt.Timestamp, Value: mt.Value})
	case model.MetricFailures:
		p.FailuresTotal += int(mt.Value)
	case model.MetricDataArrived:
		// watermark only
	}
}
