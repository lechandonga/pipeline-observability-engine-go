package engine

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/lechandonga/pipeline-observability-engine-go/clock"
	"github.com/lechandonga/pipeline-observability-engine-go/model"
)

func TestQueryTimeBoundedByRetention(t *testing.T) {
	fc := &clock.FakeClock{T: 1_000_000}
	cfg := recoveryCfg()
	cfg.RetentionMS = 10_000
	cfg.LateWindowMS = 5_000
	e, err := New(Options{DataDir: filepath.Join(t.TempDir(), "poe"), Clock: fc, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	// 2000 short-lived finished runs with unique dedup IDs (simulates long
	// history) plus one long-lived active run that must survive GC.
	for i := 0; i < 2000; i++ {
		ts := int64(1_000_000)
		rid := "r" + itoaTest(int64(i))
		if r := e.IngestEvent(model.Event{ID: rid + "-s", Pipeline: "p", RunID: rid, Type: model.EventRunStarted, Timestamp: ts}); r.Outcome != model.OutcomeAccepted {
			t.Fatalf("start %d: %+v", i, r)
		}
		if r := e.IngestEvent(model.Event{ID: rid + "-e", Pipeline: "p", RunID: rid, Type: model.EventRunSucceeded, Timestamp: ts + 10}); r.Outcome != model.OutcomeAccepted {
			t.Fatalf("end %d: %+v", i, r)
		}
	}
	if r := e.IngestEvent(model.Event{ID: "keep", Pipeline: "p", RunID: "active-keeper", Type: model.EventRunStarted, Timestamp: 1_000_000}); r.Outcome != model.OutcomeAccepted {
		t.Fatal(r)
	}

	// Advance beyond retention and force evaluation to trigger lazy GC.
	fc.Advance(60_000)
	e.Reevaluate()

	// Finished runs are pruned; the active run remains.
	if _, ok := e.GetRun("p", "r0"); ok {
		t.Fatal("finished run past retention should have been pruned")
	}
	if run, ok := e.GetRun("p", "active-keeper"); !ok || run.Status != model.RunActive {
		t.Fatalf("active run must never be pruned: %+v", run)
	}

	// Query latency must be small and independent of the 4000 historical
	// facts (which are dedup-set entries retained intentionally).
	start := time.Now()
	for i := 0; i < 100; i++ {
		if _, ok := e.GetPipeline("p"); !ok {
			t.Fatal("pipeline missing")
		}
	}
	avg := time.Since(start) / 100
	if avg > 5*time.Millisecond {
		t.Fatalf("query too slow after long history: %v per query", avg)
	}
	t.Logf("avg GetPipeline latency after 4000 historical facts: %v", avg)
}
