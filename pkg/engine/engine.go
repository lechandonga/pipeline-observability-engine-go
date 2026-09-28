// Package engine wires windowing, deduplication, state merging, alerting and a
// durable write-ahead log into one pipeline observability engine.
//
// Concurrency: one RWMutex protects all derived state. Reports take the write
// lock, queries take the read lock and observe one consistent point-in-time
// snapshot. Every accepted report is fsynced to the WAL BEFORE the in-memory
// state is advanced, so an accepted acknowledgment cannot be lost on restart;
// conversely state is only mutated after a successful durable append.
package engine

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/lechandonga/pipeline-observability-engine-go/pkg/alert"
	"github.com/lechandonga/pipeline-observability-engine-go/pkg/clock"
	"github.com/lechandonga/pipeline-observability-engine-go/pkg/dedup"
	"github.com/lechandonga/pipeline-observability-engine-go/pkg/model"
	"github.com/lechandonga/pipeline-observability-engine-go/pkg/store"
	"github.com/lechandonga/pipeline-observability-engine-go/pkg/window"
)

// Engine merges reports, evaluates alerts and serves consistent queries.
type Engine struct {
	cfg Config
	clk clock.Clock

	mu        sync.RWMutex
	states    map[string]*pipelineState
	watermark *window.Tracker
	seen      *dedup.Set
	rules     *alert.Rules
	am        *alert.Manager

	log           *store.Store
	framesSinceCP int
	ephemeralSeq  uint64
	closed        bool
}

// New constructs an engine and replays durable state when a store path is
// configured. With an empty StorePath the engine runs ephemerally.
func New(cfg Config, clk clock.Clock) (*Engine, error) {
	if clk == nil {
		clk = clock.NewSystemClock()
	}
	e := &Engine{
		cfg:       cfg,
		clk:       clk,
		states:    make(map[string]*pipelineState),
		watermark: window.NewTracker(cfg.LateTolerance, cfg.FutureTolerance),
		seen:      dedup.NewSet(cfg.DedupCapacity),
		rules:     alert.NewRules(),
	}
	e.am = alert.NewManager(e.rules)

	if cfg.StorePath != "" {
		log, err := store.Open(cfg.StorePath)
		if err != nil {
			return nil, err
		}
		e.log = log
		if err := e.recoverFromLog(); err != nil {
			return nil, err
		}
	}
	return e, nil
}

// ReportEvent validates, deduplicates, windows and merges one event.
func (e *Engine) ReportEvent(ev model.Event) (res model.IngestResult) {
	if detail := validateEvent(ev); detail != "" {
		return reject(model.RejectInvalid, detail)
	}
	ev.Timestamp = ev.Timestamp.UTC()

	// Durable append first; the mutex is only needed to protect seq/dedup
	// state ordering, so take the write lock for the whole report.
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return reject(model.RejectInvalid, "engine is closed")
	}

	now := e.clk.Now().UTC()
	if !e.seen.Add(ev.ID) {
		return reject(model.RejectDuplicate, "report id already accepted: "+ev.ID)
	}

	late, future := e.watermark.Check(ev.Pipeline, ev.Timestamp, now)
	if future {
		e.seenRollback(ev.ID)
		return reject(model.RejectFuture, "timestamp beyond future tolerance")
	}
	if late {
		e.seenRollback(ev.ID)
		low, _ := e.watermark.BoundsFor(ev.Pipeline, now)
		return reject(model.RejectLate, "timestamp before late-window boundary "+low.Format(time.RFC3339Nano))
	}

	// Pure conflict pre-check against established state: rejected reports are
	// never persisted and never mutate state.
	ps := e.stateFor(ev.Pipeline)
	if ok, reason, detail := checkEvent(ps, ev); !ok {
		e.seenRollback(ev.ID)
		return reject(reason, detail)
	}

	payload, err := json.Marshal(ev)
	if err != nil {
		e.seenRollback(ev.ID)
		return reject(model.RejectInvalid, "encode: "+err.Error())
	}
	seq, err := e.appendDurable(store.KindEvent, payload)
	if err != nil {
		e.seenRollback(ev.ID)
		return reject(model.RejectInvalid, "persist: "+err.Error())
	}

	if ok, reason, detail := mergeEvent(ps, ev, seq); !ok {
		// Defensive: the pre-check guaranteed success.
		return reject(reason, detail)
	}
	e.watermark.Observe(ev.Pipeline, ev.Timestamp)
	e.evaluateAlerts(ev.Pipeline, ev.Timestamp)
	e.maintenance(ev.Timestamp)
	return model.IngestResult{Accepted: true, Seq: seq}
}

// ReportMetric validates, deduplicates, windows and merges one metric.
func (e *Engine) ReportMetric(m model.Metric) model.IngestResult {
	if detail := validateMetric(m); detail != "" {
		return reject(model.RejectInvalid, detail)
	}
	m.Timestamp = m.Timestamp.UTC()

	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return reject(model.RejectInvalid, "engine is closed")
	}

	now := e.clk.Now().UTC()
	if !e.seen.Add(m.ID) {
		return reject(model.RejectDuplicate, "report id already accepted: "+m.ID)
	}
	late, future := e.watermark.Check(m.Pipeline, m.Timestamp, now)
	if future {
		e.seenRollback(m.ID)
		return reject(model.RejectFuture, "timestamp beyond future tolerance")
	}
	if late {
		e.seenRollback(m.ID)
		low, _ := e.watermark.BoundsFor(m.Pipeline, now)
		return reject(model.RejectLate, "timestamp before late-window boundary "+low.Format(time.RFC3339Nano))
	}

	payload, err := json.Marshal(m)
	if err != nil {
		e.seenRollback(m.ID)
		return reject(model.RejectInvalid, "encode: "+err.Error())
	}
	seq, err := e.appendDurable(store.KindMetric, payload)
	if err != nil {
		e.seenRollback(m.ID)
		return reject(model.RejectInvalid, "persist: "+err.Error())
	}

	ps := e.stateFor(m.Pipeline)
	applyMetric(ps, m, seq)
	e.watermark.Observe(m.Pipeline, m.Timestamp)
	e.evaluateAlerts(m.Pipeline, m.Timestamp)
	e.maintenance(m.Timestamp)
	return model.IngestResult{Accepted: true, Seq: seq}
}

// UpdateRules atomically replaces the alert rule set and durably records it.
// Existing alert conclusions keep their pinned rule version.
func (e *Engine) UpdateRules(rules []alert.ThresholdRule) (int, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return e.rules.Version(), errClosed
	}
	// Validate before touching anything: a rejected config must be atomic.
	version, err := e.rules.Replace(rules)
	if err != nil {
		return e.rules.Version(), err
	}
	payload, err := json.Marshal(rulesSnapshot{Version: version, Rules: rules})
	if err != nil {
		return version, err
	}
	if _, err := e.appendDurable(store.KindRules, payload); err != nil {
		return version, err
	}
	// Both the registry and the manager are the same instances, so pinned
	// episodes keep resolving their historical rule snapshots.
	return version, nil
}

type rulesSnapshot struct {
	Version int                   `json:"version"`
	Rules   []alert.ThresholdRule `json:"rules"`
}

// View is a point-in-time consistent read model.
type View struct {
	GeneratedAt time.Time        `json:"generated_at"`
	Pipelines   []PipelineStatus `json:"pipelines"`
	Alerts      []model.Alert    `json:"alerts"`
}

// PipelineStatus is one pipeline's row in a query result.
type PipelineStatus struct {
	Name         string           `json:"name"`
	Runs         []model.RunState `json:"runs"`
	FailedRuns   int              `json:"failed_runs"`
	RetriedRuns  int              `json:"retried_runs"`
	Latest       MetricRow        `json:"latest"`
	SLACompliant bool             `json:"sla_compliant"`
	HasSLA       bool             `json:"has_sla"`
	Watermark    time.Time        `json:"watermark"`
}

// MetricRow carries the latest value of each metric family, when present.
type MetricRow struct {
	FreshnessSeconds *float64 `json:"freshness_seconds,omitempty"`
	LatencySeconds   *float64 `json:"latency_seconds,omitempty"`
	ThroughputRPM    *float64 `json:"throughput_rpm,omitempty"`
}

// Query returns a consistent snapshot of all derived state. Cost is proportional
// to retained pipelines/runs/alerts, not to total history.
func (e *Engine) Query() View {
	e.mu.RLock()
	defer e.mu.RUnlock()
	now := e.clk.Now().UTC()

	v := View{GeneratedAt: now}
	names := make([]string, 0, len(e.states))
	for name := range e.states {
		names = append(names, name)
	}
	sortStrings(names)
	for _, name := range names {
		ps := e.states[name]
		v.Pipelines = append(v.Pipelines, e.snapshotPipeline(name, ps, now))
	}
	v.Alerts = e.am.Snapshot(now, e.cfg.KeepClosedFor)
	return v
}

// Close persists a final checkpoint and releases resources.
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil
	}
	e.closed = true
	var err error
	if e.log != nil {
		if cerr := e.compactLocked(); cerr != nil && err == nil {
			err = cerr
		}
		if cerr := e.log.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}
	return err
}
