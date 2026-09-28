package engine

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/lechandonga/pipeline-observability-engine-go/config"
	"github.com/lechandonga/pipeline-observability-engine-go/model"
	"github.com/lechandonga/pipeline-observability-engine-go/store"
)

// GetRun returns the current merged state of one run. The returned value is
// a copy taken under the read lock, so concurrent writers never expose a
// torn view.
func (e *Engine) GetRun(pipeline, runID string) (model.RunState, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	r, ok := e.runs[runKey{pipeline, runID}]
	if !ok {
		return model.RunState{}, false
	}
	return *r, true
}

// GetPipeline returns one consistent status snapshot for a pipeline.
func (e *Engine) GetPipeline(pipeline string) (model.PipelineStatus, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	p, ok := e.pipes[pipeline]
	if !ok {
		return model.PipelineStatus{}, false
	}
	active := 0
	for k, r := range e.runs {
		if k.pipeline == pipeline && r.Status == model.RunActive {
			active++
		}
	}
	st := model.PipelineStatus{
		Pipeline:        pipeline,
		Watermark:       p.Watermark,
		LastSuccess:     p.LastSuccess,
		ActiveRuns:      active,
		LatestLatencyMS: p.LatestLatency,
		RetriesTotal:    p.RetriesTotal,
		FailuresTotal:   p.FailuresTotal,
		ActiveAlerts:    e.alerts.ActiveFor(pipeline),
	}
	return st, true
}

// Alerts returns currently firing alerts for a pipeline ("" = all).
func (e *Engine) Alerts(pipeline string) []model.Alert {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if pipeline == "" {
		return e.alerts.AllActive()
	}
	return e.alerts.ActiveFor(pipeline)
}

// AlertHistory returns closed alert incidents, including recovered ones.
// History records retain their original ConfigVersion forever.
func (e *Engine) AlertHistory(pipeline string) []model.Alert {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if pipeline == "" {
		return e.alerts.HistoryAll()
	}
	return e.alerts.HistoryFor(pipeline)
}

// UpdateConfig replaces thresholds going forward. Versions must be
// non-decreasing: an older config cannot arrive and rewrite conclusions.
// Open alerts are re-evaluated immediately under the new rules; closed
// historical conclusions keep the version recorded when they happened.
func (e *Engine) UpdateConfig(cfg *config.Config) error {
	if cfg == nil {
		return errors.New("engine: nil config")
	}
	if cfg.Version < e.cfg.Version {
		return fmt.Errorf("engine: config version %d is older than active %d", cfg.Version, e.cfg.Version)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.cfg = cfg
	e.alerts.SetConfig(cfg)
	e.evaluateAllLocked(e.clk.NowMillis())
	return nil
}

// Config returns a copy of the active configuration.
func (e *Engine) Config() *config.Config {
	e.mu.RLock()
	defer e.mu.RUnlock()
	cp := *e.cfg
	return &cp
}

// Snapshot forces an atomic durability snapshot and WAL compaction. After it
// returns successfully, all confirmed facts and conclusions are recoverable
// from the snapshot even if every WAL frame since is lost.
func (e *Engine) Snapshot() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.snapshotLocked()
}

func (e *Engine) snapshotLocked() error {
	ss := snapshotState{
		Sequence: e.seq,
		Pipes:    e.pipes,
		Alerts:   e.alerts.ExportState(),
	}
	ss.Runs = make([]model.RunState, 0, len(e.runs))
	for _, r := range e.runs {
		ss.Runs = append(ss.Runs, *r)
	}
	data, err := json.Marshal(&ss)
	if err != nil {
		return fmt.Errorf("encode snapshot: %w", err)
	}
	return e.st.SaveSnapshot(store.Snapshot{Sequence: e.seq, Data: data})
}

// Reevaluate re-runs all rules at the current clock time without ingesting
// data. It is intended for time-driven checks (and tests); alert
// de-duplication ensures repeated evaluation has no side effects beyond
// value refresh.
func (e *Engine) Reevaluate() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.evaluateAllLocked(e.clk.NowMillis())
}
