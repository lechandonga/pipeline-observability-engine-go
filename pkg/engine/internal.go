package engine

import (
	"encoding/json"
	"errors"
	"sort"
	"time"

	"github.com/lechandonga/pipeline-observability-engine-go/pkg/alert"
	"github.com/lechandonga/pipeline-observability-engine-go/pkg/dedup"
	"github.com/lechandonga/pipeline-observability-engine-go/pkg/model"
	"github.com/lechandonga/pipeline-observability-engine-go/pkg/store"
	"github.com/lechandonga/pipeline-observability-engine-go/pkg/window"
)

var errClosed = errors.New("engine is closed")

func sortStrings(x []string) { sort.Strings(x) }

func reject(reason model.RejectReason, detail string) model.IngestResult {
	return model.IngestResult{Accepted: false, Reason: reason, Detail: detail}
}

func (e *Engine) stateFor(p string) *pipelineState {
	ps, ok := e.states[p]
	if !ok {
		ps = newPipelineState()
		e.states[p] = ps
	}
	return ps
}

// seenRollback removes a reserved id that ultimately was not accepted, so the
// bounded dedup set is not polluted by rejected reports.
func (e *Engine) seenRollback(id string) { e.seen.Remove(id) }

// appendDurable assigns a sequence via the store. In ephemeral mode it just
// increments an in-memory counter.
func (e *Engine) appendDurable(kind byte, payload []byte) (uint64, error) {
	if e.log == nil {
		e.framesSinceCP++
		return e.nextEphemeralSeq(), nil
	}
	seq, err := e.log.Append(kind, payload)
	if err != nil {
		return 0, err
	}
	e.framesSinceCP++
	if e.cfg.CompactionThreshold > 0 && e.framesSinceCP >= e.cfg.CompactionThreshold {
		// Compaction failure must not lose the just-accepted write; the record
		// is already durable, so surface the error only as a best-effort signal
		// is wrong for acknowledgments — swallow here and retry next time.
		_ = e.compactLocked()
	}
	return seq, nil
}

func (e *Engine) nextEphemeralSeq() uint64 {
	if e.ephemeralSeq == 0 {
		e.ephemeralSeq = 1
		return 1
	}
	e.ephemeralSeq++
	return e.ephemeralSeq
}

// evaluateAlerts builds the metric snapshot for a pipeline and runs its rules
// at the given event time (deterministic; does not read wall clock).
func (e *Engine) evaluateAlerts(pipeline string, ts time.Time) {
	ps := e.states[pipeline]
	if ps == nil {
		return
	}
	snap := buildMetricSnapshot(ps, ts)
	e.am.Evaluate(pipeline, snap, e.rules.ForPipeline(pipeline), ts)
}

func buildMetricSnapshot(ps *pipelineState, now time.Time) alert.MetricSnapshot {
	s := alert.MetricSnapshot{}
	if v, ok := ps.Latest[model.MetricFreshness]; ok {
		s.FreshnessSeconds = v.Value
		s.Avail.HasFreshness = true
	}
	if v, ok := ps.Latest[model.MetricLatency]; ok {
		s.LatencySeconds = v.Value
		s.Avail.HasLatency = true
	}
	if v, ok := ps.Latest[model.MetricThroughput]; ok {
		s.ThroughputRPM = v.Value
		s.Avail.HasThroughput = true
	}
	if v, ok := ps.Latest[model.MetricSLA]; ok {
		s.SLACompliant = v.Value >= 1
		s.Avail.HasSLA = true
	}
	s.Failures = len(ps.FailedRunIDs)
	s.Retries = len(ps.RetriedRunIDs)
	// Failure/retry are derived counters, not reported metrics: they are
	// "available" as soon as this pipeline has any run evidence. A count of
	// zero is then real clearing evidence that recovers an active alert
	// immediately instead of waiting for the stale window.
	s.Avail.HasFailure = len(ps.Runs) > 0 || s.Failures > 0
	_ = now
	return s
}

// snapshotPipeline builds one consistent query row.
func (e *Engine) snapshotPipeline(name string, ps *pipelineState, now time.Time) PipelineStatus {
	row := PipelineStatus{
		Name:        name,
		FailedRuns:  len(ps.FailedRunIDs),
		RetriedRuns: len(ps.RetriedRunIDs),
		Watermark:   e.watermark.Watermark(name),
	}
	runIDs := make([]string, 0, len(ps.Runs))
	for id := range ps.Runs {
		runIDs = append(runIDs, id)
	}
	sort.Strings(runIDs)
	for _, id := range runIDs {
		row.Runs = append(row.Runs, *ps.Runs[id])
	}
	if v, ok := ps.Latest[model.MetricFreshness]; ok {
		val := v.Value
		row.Latest.FreshnessSeconds = &val
	}
	if v, ok := ps.Latest[model.MetricLatency]; ok {
		val := v.Value
		row.Latest.LatencySeconds = &val
	}
	if v, ok := ps.Latest[model.MetricThroughput]; ok {
		val := v.Value
		row.Latest.ThroughputRPM = &val
	}
	if v, ok := ps.Latest[model.MetricSLA]; ok {
		row.HasSLA = true
		row.SLACompliant = v.Value >= 1
	}
	return row
}

// maintenance bounds memory: prunes finished runs older than RunRetention
// (event-time) and keeps failure/retry counters aligned with retained runs.
func (e *Engine) maintenance(ts time.Time) {
	if e.cfg.RunRetention <= 0 {
		return
	}
	cutoff := ts.Add(-e.cfg.RunRetention)
	for name, ps := range e.states {
		for id, run := range ps.Runs {
			if run.Status == model.RunRunning {
				continue
			}
			ref := run.EndedAt
			if ref.IsZero() {
				ref = run.StartedAt
			}
			if ref.Before(cutoff) {
				delete(ps.Runs, id)
				delete(ps.FailedRunIDs, id)
				delete(ps.RetriedRunIDs, id)
			}
		}
		if len(ps.Runs) == 0 && len(ps.Latest) == 0 {
			delete(e.states, name)
		}
	}
	// Alert-episode GC runs only on the write path so concurrent readers of
	// Snapshot (read lock) never mutate shared maps. It uses the same event
	// time as state pruning, keeping conclusions deterministic under a fake
	// clock and identical before/after restart.
	e.am.Sweep(ts, e.cfg.KeepClosedFor)
}

// ---- WAL recovery / compaction --------------------------------------------

// recoverFromLog rebuilds all derived state from the latest checkpoint plus the
// trailing frames. All alert conclusions are re-derived deterministically by
// replaying reports; rule frames restore every historical rule version.
func (e *Engine) recoverFromLog() error {
	var base *checkpoint
	var tail []store.Record

	err := e.log.Replay(func(r store.Record) error {
		switch r.Kind {
		case store.KindSnapshot:
			cp, err := unmarshalCheckpoint(r.Payload)
			if err != nil {
				return err
			}
			base = cp
			tail = tail[:0]
			return nil
		case store.KindEvent, store.KindMetric, store.KindRules:
			tail = append(tail, r)
		}
		return nil
	})
	if err != nil {
		return err
	}

	if base != nil {
		e.loadCheckpoint(base)
	}
	for _, r := range tail {
		if base != nil && r.Seq <= base.Seq {
			continue // older than or equal to checkpoint: already included
		}
		if err := e.applyReplayFrame(r); err != nil {
			return err
		}
		e.framesSinceCP++
	}
	return nil
}

func (e *Engine) applyReplayFrame(r store.Record) error {
	switch r.Kind {
	case store.KindEvent:
		var ev model.Event
		if err := json.Unmarshal(r.Payload, &ev); err != nil {
			return err
		}
		ps := e.stateFor(ev.Pipeline)
		if ok, _, _ := mergeEvent(ps, ev, r.Seq); ok {
			e.watermark.Observe(ev.Pipeline, ev.Timestamp.UTC())
			e.seen.Add(ev.ID)
			e.evaluateAlerts(ev.Pipeline, ev.Timestamp.UTC())
		}
	case store.KindMetric:
		var m model.Metric
		if err := json.Unmarshal(r.Payload, &m); err != nil {
			return err
		}
		ps := e.stateFor(m.Pipeline)
		applyMetric(ps, m, r.Seq)
		e.watermark.Observe(m.Pipeline, m.Timestamp.UTC())
		e.seen.Add(m.ID)
		e.evaluateAlerts(m.Pipeline, m.Timestamp.UTC())
	case store.KindRules:
		var snap rulesSnapshot
		if err := json.Unmarshal(r.Payload, &snap); err != nil {
			return err
		}
		if e.rules.Version() >= snap.Version {
			return nil
		}
		e.rules.Import(snap.Version, append(e.rules.Export(),
			versionedFromRules(snap.Version, snap.Rules)...))
	}
	return nil
}

func versionedFromRules(version int, rules []alert.ThresholdRule) []alert.VersionedRule {
	out := make([]alert.VersionedRule, 0, len(rules))
	for _, r := range rules {
		out = append(out, alert.VersionedRule{Version: version, Rule: r})
	}
	return out
}

func (e *Engine) loadCheckpoint(cp *checkpoint) {
	e.states = cp.Pipelines
	if e.states == nil {
		e.states = make(map[string]*pipelineState)
	}
	e.watermark = window.NewTracker(e.cfg.LateTolerance, e.cfg.FutureTolerance)
	e.watermark.Import(cp.Watermark)
	e.seen = dedup.NewOrdered(cp.DedupIDs, e.cfg.DedupCapacity)
	e.rules.Import(cp.Version, cp.Rules)
	e.am = alert.NewManager(e.rules)
	e.am.RestoreEpisodes(cp.Episodes)
	e.framesSinceCP = 0
}

// buildCheckpoint collects durable state for compaction.
func (e *Engine) buildCheckpoint() *checkpoint {
	var seq uint64
	if e.log != nil {
		seq = e.log.LastSeq()
	} else {
		seq = e.ephemeralSeq
	}
	return &checkpoint{
		Seq:       seq,
		Version:   e.rules.Version(),
		Rules:     e.rules.Export(),
		Pipelines: e.states,
		Watermark: e.watermark.Export(),
		DedupIDs:  e.seen.Order(),
		Episodes:  e.am.SerializeEpisodes(),
	}
}

// compactLocked rewrites the WAL as one checkpoint plus (currently empty)
// trailing frames; the store assigns the checkpoint seq preserving the
// gap-free ordering. Called under the write lock or from Close.
func (e *Engine) compactLocked() error {
	if e.log == nil {
		return nil
	}
	cp := e.buildCheckpoint()
	payload, err := cp.marshal()
	if err != nil {
		return err
	}
	if err := e.log.Rewrite(payload, nil); err != nil {
		return err
	}
	e.framesSinceCP = 0
	return nil
}

// amSaveEpisodes returns current episodes; used to reattach after rule swaps
// (manager instances are registry-bound).
func (e *Engine) amSaveEpisodes() []alert.EpisodeView { return e.am.SerializeEpisodes() }
