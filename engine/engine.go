// Package engine is the concurrency-safe facade that merges reported events
// and metrics into consistent run/pipeline/alert state, with explicit
// lateness windows, idempotent de-duplication and durable recovery.
package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/lechandonga/pipeline-observability-engine-go/alert"
	"github.com/lechandonga/pipeline-observability-engine-go/clock"
	"github.com/lechandonga/pipeline-observability-engine-go/config"
	"github.com/lechandonga/pipeline-observability-engine-go/model"
	"github.com/lechandonga/pipeline-observability-engine-go/store"
)

// Engine owns all mutable observability state.
type Engine struct {
	mu sync.RWMutex

	cfg    *config.Config
	clk    clock.Clock
	st     store.Store
	alerts *alert.Manager
	dir    string

	runs  map[runKey]*model.RunState
	pipes map[string]*pipelineAgg

	// seq is the global durable sequence. Both input facts and alert
	// conclusions share it so replay order on recovery is unambiguous.
	seq int64

	closed bool
}

// Options constructs an Engine.
type Options struct {
	DataDir string
	Clock   clock.Clock
	Config  *config.Config
	Store   store.Store // injectable; nil creates a FileStore in DataDir
}

// New builds an engine and recovers durable state from disk.
func New(opts Options) (*Engine, error) {
	cfg := opts.Config
	if cfg == nil {
		cfg = config.Default()
	}
	cl := opts.Clock
	if cl == nil {
		cl = clock.SystemClock{}
	}
	st := opts.Store
	if st == nil {
		if opts.DataDir == "" {
			return nil, errors.New("engine: either Store or DataDir must be set")
		}
		fs, err := store.NewFileStore(opts.DataDir)
		if err != nil {
			return nil, err
		}
		st = fs
	}

	e := &Engine{
		cfg:    cfg,
		clk:    cl,
		st:     st,
		dir:    opts.DataDir,
		alerts: alert.NewManager(cfg),
		runs:   map[runKey]*model.RunState{},
		pipes:  map[string]*pipelineAgg{},
	}
	if err := e.recover(); err != nil {
		return nil, fmt.Errorf("engine: recovery: %w", err)
	}
	return e, nil
}

// recover restores the last snapshot (if any) and replays subsequent WAL
// records. Confirmed facts and alert conclusions come back exactly as they
// were; rejected input was never persisted and therefore cannot reappear.
func (e *Engine) recover() error {
	snap, err := e.st.LoadSnapshot()
	if err != nil {
		return err
	}
	if snap != nil {
		var ss snapshotState
		if err := json.Unmarshal(snap.Data, &ss); err != nil {
			return fmt.Errorf("decode snapshot: %w", err)
		}
		e.seq = ss.Sequence
		for i := range ss.Runs {
			r := ss.Runs[i]
			e.runs[runKey{r.Pipeline, r.RunID}] = &r
		}
		e.pipes = ss.Pipes
		if e.pipes == nil {
			e.pipes = map[string]*pipelineAgg{}
		}
		for _, p := range e.pipes {
			if p.Seen == nil {
				p.Seen = map[string]struct{}{}
			}
		}
		e.alerts.ImportState(ss.Alerts)
	}

	recs, err := e.st.ReadAll(e.seq)
	if err != nil {
		return err
	}
	for _, rec := range recs {
		if rec.Sequence <= e.seq {
			continue // already covered by snapshot
		}
		switch rec.Kind {
		case store.KindEvent:
			var ev model.Event
			if err := json.Unmarshal(rec.Payload, &ev); err != nil {
				return fmt.Errorf("decode event seq=%d: %w", rec.Sequence, err)
			}
			e.mergeRecoveredEvent(ev, rec.Sequence)
		case store.KindMetric:
			var mt model.Metric
			if err := json.Unmarshal(rec.Payload, &mt); err != nil {
				return fmt.Errorf("decode metric seq=%d: %w", rec.Sequence, err)
			}
			e.mergeRecoveredMetric(mt, rec.Sequence)
		case store.KindAlert:
			var a model.Alert
			if err := json.Unmarshal(rec.Payload, &a); err != nil {
				return fmt.Errorf("decode alert seq=%d: %w", rec.Sequence, err)
			}
			e.alerts.UpsertFromWAL(a)
		default:
			return fmt.Errorf("unknown wal record kind %d at seq=%d", rec.Kind, rec.Sequence)
		}
		e.seq = rec.Sequence
	}

	// Reconcile in-memory alerts against recovered facts under the active
	// (newest) config. Confirmed firing alerts are already in WAL/snapshot
	// and survive as-is; this mainly self-heals a conclusion whose durable
	// alert record was the only thing lost in a crash window.
	if len(e.pipes) > 0 {
		e.evaluateAllLocked(e.clk.NowMillis())
	}
	return nil
}

// nextSeq must be called with e.mu held.
func (e *Engine) nextSeq() int64 {
	e.seq++
	return e.seq
}

// pipeLocked must be called with e.mu held.
func (e *Engine) pipeLocked(name string) *pipelineAgg {
	p := e.pipes[name]
	if p == nil {
		p = &pipelineAgg{Name: name, Seen: map[string]struct{}{}}
		e.pipes[name] = p
	}
	return p
}

// Close releases the underlying store. Durable facts are already fsync'ed
// on each ingest; callers may Snapshot() first to compact the WAL.
func (e *Engine) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	e.mu.Unlock()
	return e.st.Close()
}
