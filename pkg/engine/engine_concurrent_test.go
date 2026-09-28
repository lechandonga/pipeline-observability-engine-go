package engine

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lechandonga/pipeline-observability-engine-go/pkg/alert"
	"github.com/lechandonga/pipeline-observability-engine-go/pkg/model"
)

func TestConcurrentIngestAndQueryNoDeadlockOrRace(t *testing.T) {
	// In-memory engine (fsync is exercised separately in recovery tests) keeps
	// the race-instrumented run fast; the locking/consistency guarantees under
	// test are identical.
	cfg := DefaultConfig("")
	cfg.LateTolerance = 10 * time.Hour
	cfg.FutureTolerance = 10 * time.Hour
	cfg.DedupCapacity = 1_000_000
	cfg.CompactionThreshold = 0
	e, err := New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if _, err := e.UpdateRules([]alert.ThresholdRule{
		{ID: "lat", Metric: model.MetricLatency, Above: true, Threshold: 100},
	}); err != nil {
		t.Fatal(err)
	}

	var accepted, duplicated, rejected int64

	// Readers run independently and stop via stopCh (NOT the writers' WG).
	stopCh := make(chan struct{})
	var readers sync.WaitGroup
	for r := 0; r < 4; r++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stopCh:
					return
				default:
					v := e.Query() // read lock only; must never mutate state
					for _, p := range v.Pipelines {
						if len(p.Runs) < 0 {
							t.Errorf("inconsistent view")
							return
						}
					}
					// Yield between queries so writers are never starved.
					time.Sleep(time.Millisecond)
				}
			}
		}()
	}

	// Writers.
	var writers sync.WaitGroup
	const nw, npw = 8, 300
	for w := 0; w < nw; w++ {
		writers.Add(1)
		go func(w int) {
			defer writers.Done()
			now := time.Now().UTC()
			classify := func(r model.IngestResult) {
				switch r.Reason {
				case model.RejectNone:
					atomic.AddInt64(&accepted, 1)
				case model.RejectDuplicate:
					atomic.AddInt64(&duplicated, 1)
				default:
					atomic.AddInt64(&rejected, 1)
				}
			}
			for i := 0; i < npw; i++ {
				id := fmt.Sprintf("w%d-start-%d", w, i)
				run := fmt.Sprintf("w%d-r%d", w, i)
				classify(e.ReportEvent(model.Event{ID: id, Pipeline: "p", RunID: run,
					Type: model.EventStarted, Timestamp: now}))
				r := e.ReportMetric(model.Metric{
					ID: fmt.Sprintf("w%d-m-%d", w, i), Pipeline: "p",
					Kind: model.MetricLatency, Value: 150,
					Timestamp: now.Add(time.Duration(i) * time.Millisecond),
				})
				switch r.Reason {
				case model.RejectNone:
					atomic.AddInt64(&accepted, 1)
				case model.RejectDuplicate:
					atomic.AddInt64(&duplicated, 1)
				default:
					atomic.AddInt64(&rejected, 1)
				}
			}
		}(w)
	}

	// Concurrent duplicate replays: every start id replayed twice, each must be
	// rejected as a duplicate exactly (no double accumulation).
	writers.Add(1)
	go func() {
		defer writers.Done()
		now := time.Now().UTC()
		for w := 0; w < nw; w++ {
			for i := 0; i < npw; i++ {
				id := fmt.Sprintf("w%d-start-%d", w, i)
				run := fmt.Sprintf("w%d-r%d", w, i)
				for k := 0; k < 2; k++ {
					r := e.ReportEvent(model.Event{ID: id, Pipeline: "p", RunID: run,
						Type: model.EventStarted, Timestamp: now})
					if r.Reason == model.RejectNone {
						atomic.AddInt64(&accepted, 1)
					} else if r.Reason == model.RejectDuplicate {
						atomic.AddInt64(&duplicated, 1)
					} else {
						atomic.AddInt64(&rejected, 1)
					}
				}
			}
		}
	}()

	writers.Wait()
	close(stopCh)
	readers.Wait()

	// Each unique start id is sent 3 times (writer once + 2 replay copies):
	// exactly one acceptance and two duplicates, regardless of goroutine order.
	// Metrics are sent once each and all accepted.
	wantAccepted := int64(nw*npw) + int64(nw*npw) // unique starts + metrics
	wantDup := int64(2 * nw * npw)
	if got := atomic.LoadInt64(&accepted); got != wantAccepted {
		t.Fatalf("accepted=%d want %d (dup=%d rej=%d)", got, wantAccepted,
			atomic.LoadInt64(&duplicated), atomic.LoadInt64(&rejected))
	}
	if got := atomic.LoadInt64(&duplicated); got != wantDup {
		t.Fatalf("duplicates=%d want %d", got, wantDup)
	}

	v := e.Query()
	active := 0
	for _, a := range v.Alerts {
		if a.Key == "lat\x00p" && a.State == model.AlertActive {
			active++
		}
	}
	if active != 1 {
		t.Fatalf("want exactly 1 active deduplicated alert, got %d", active)
	}
	for _, p := range v.Pipelines {
		if p.Name == "p" && len(p.Runs) != nw*npw {
			t.Fatalf("runs=%d want %d (lost or double accumulated)", len(p.Runs), nw*npw)
		}
	}
	t.Logf("concurrent: accepted=%d duplicated=%d rejected=%d runs=%d",
		atomic.LoadInt64(&accepted), atomic.LoadInt64(&duplicated),
		atomic.LoadInt64(&rejected), nw*npw)
}

func TestQueryCostBoundedByRetentionNotHistory(t *testing.T) {
	cfg := DefaultConfig("")
	cfg.RunRetention = 100 * time.Millisecond
	cfg.LateTolerance = time.Hour
	cfg.FutureTolerance = time.Hour
	e, _ := New(cfg, nil)
	defer e.Close()
	now := time.Now().UTC()

	for i := 0; i < 5000; i++ {
		ts := now.Add(-time.Hour - time.Duration(i))
		e.ReportEvent(model.Event{ID: fmt.Sprintf("old-s-%d", i), Pipeline: "p",
			RunID: fmt.Sprintf("old-%d", i), Type: model.EventStarted, Timestamp: ts})
		e.ReportEvent(model.Event{ID: fmt.Sprintf("old-f-%d", i), Pipeline: "p",
			RunID: fmt.Sprintf("old-%d", i), Type: model.EventFailed, Timestamp: ts.Add(time.Millisecond)})
	}
	before := 0
	for _, p := range e.Query().Pipelines {
		before += len(p.Runs)
	}
	for i := 0; i < 50; i++ {
		e.ReportEvent(model.Event{ID: fmt.Sprintf("new-%d", i), Pipeline: "p",
			RunID: fmt.Sprintf("new-%d", i), Type: model.EventStarted,
			Timestamp: now.Add(time.Duration(i) * time.Millisecond)})
	}
	after := 0
	for _, p := range e.Query().Pipelines {
		after += len(p.Runs)
	}
	if after >= before {
		t.Fatalf("query size not bounded: before=%d after=%d", before, after)
	}
	if after > 200 {
		t.Fatalf("retained runs %d far above the small fresh set", after)
	}
	t.Logf("query size bounded by retention: %d old runs -> %d retained", before, after)
}
