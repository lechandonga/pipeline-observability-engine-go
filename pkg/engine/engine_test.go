package engine

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/lechandonga/pipeline-observability-engine-go/pkg/clock"
	"github.com/lechandonga/pipeline-observability-engine-go/pkg/model"
)

func newTestEngine(t *testing.T, path string) (*Engine, *clock.FakeClock) {
	t.Helper()
	cfg := DefaultConfig(path)
	cfg.LateTolerance = time.Minute
	cfg.FutureTolerance = 10 * time.Second
	cfg.DedupCapacity = 1000
	cfg.CompactionThreshold = 0 // deterministic tests: compact only on Close
	clk := clock.NewFakeClock(time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC))
	e, err := New(cfg, clk)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close() })
	return e, clk
}

func logResult(t *testing.T, tag string, in any, r model.IngestResult) {
	t.Helper()
	if r.Accepted {
		t.Logf("%s input=%v -> ACCEPTED seq=%d", tag, in, r.Seq)
	} else {
		t.Logf("%s input=%v -> REJECTED reason=%q detail=%q", tag, in, r.Reason, r.Detail)
	}
}

func ev(id, run string, typ model.EventType, ts time.Time) model.Event {
	return model.Event{ID: id, Pipeline: "p1", RunID: run, Type: typ, Timestamp: ts}
}

func TestOutOfOrderMerge(t *testing.T) {
	e, clk := newTestEngine(t, "")
	base := clk.Now()

	// Arrive out of order: failed first, then the started.
	clk.Advance(20 * time.Second)
	r1 := e.ReportEvent(ev("e-fail", "run1", model.EventFailed, base.Add(20*time.Second)))
	logResult(t, "early-failure", "failed@+20s", r1)
	if r1.Accepted {
		t.Fatal("failure without started is a state conflict, must be rejected without polluting state")
	}
	if r1.Reason != model.RejectStateConflict {
		t.Fatalf("want state_conflict, got %s", r1.Reason)
	}

	r2 := e.ReportEvent(ev("e-start", "run1", model.EventStarted, base))
	logResult(t, "start", "started@0", r2)
	if !r2.Accepted {
		t.Fatal("started should accept")
	}
	// Out of order within the window: failure earlier than start event but
	// logically after the run started (ts 20s > 0) is fine.
	r3 := e.ReportEvent(ev("e-fail2", "run1", model.EventFailed, base.Add(20*time.Second)))
	logResult(t, "late-failure", "failed@+20s(reordered)", r3)
	if !r3.Accepted {
		t.Fatalf("reordered failure within window must accept: %s %s", r3.Reason, r3.Detail)
	}

	v := e.Query()
	run := v.Pipelines[0].Runs[0]
	if run.Status != model.RunFailed || !run.EndedAt.Equal(base.Add(20*time.Second)) {
		t.Fatalf("merged run wrong: %+v", run)
	}
}

func TestDuplicateAppliesOnce(t *testing.T) {
	e, clk := newTestEngine(t, "")
	base := clk.Now()
	r1 := e.ReportEvent(ev("dup1", "run1", model.EventRetried, base))
	// retried without started -> conflict; reserve must be rolled back so the
	// same id on a valid event later is not seen as a duplicate.
	if r1.Reason != model.RejectStateConflict {
		t.Fatalf("want conflict, got %s", r1.Reason)
	}
	if !e.ReportEvent(ev("start1", "run1", model.EventStarted, base)).Accepted {
		t.Fatal("start failed")
	}

	a := e.ReportEvent(ev("dup1", "run1", model.EventRetried, base.Add(time.Second)))
	b := e.ReportEvent(ev("dup1", "run1", model.EventRetried, base.Add(2*time.Second)))
	c := e.ReportEvent(ev("dup1", "run1", model.EventRetried, base.Add(3*time.Second)))
	logResult(t, "retry-1", "dup1", a)
	logResult(t, "retry-2", "dup1", b)
	logResult(t, "retry-3", "dup1", c)
	if !a.Accepted || b.Reason != model.RejectDuplicate || c.Reason != model.RejectDuplicate {
		t.Fatalf("duplicate handling wrong: %+v %+v %+v", a, b, c)
	}
	v := e.Query()
	if attempts := v.Pipelines[0].Runs[0].Attempts; attempts != 1 {
		t.Fatalf("attempts = %d, duplicate retries must not accumulate", attempts)
	}
	// Rejected-conflict reservation rolled back => id usable after start.
	if v.Pipelines[0].RetriedRuns != 1 {
		t.Fatalf("retried runs = %d", v.Pipelines[0].RetriedRuns)
	}
}

func TestLateAndFutureRejected(t *testing.T) {
	e, clk := newTestEngine(t, "")
	base := clk.Now()

	// Establish watermark at +2m (wall clock moves to the same instant).
	clk.Advance(2 * time.Minute)
	r := e.ReportEvent(ev("w1", "r1", model.EventStarted, base.Add(2*time.Minute)))
	if !r.Accepted {
		t.Fatal(r.Detail)
	}

	// 90s behind the watermark, tolerance 60s -> late.
	late := e.ReportEvent(ev("late1", "r2", model.EventStarted, base.Add(30*time.Second)))
	logResult(t, "late", "started@+30s vs wm@+2m", late)
	if late.Reason != model.RejectLate {
		t.Fatalf("want window_expired, got %s", late.Reason)
	}

	// Exactly at boundary (wm - tolerance = +1m) -> admitted.
	boundary := e.ReportEvent(ev("b1", "r3", model.EventStarted, base.Add(time.Minute)))
	logResult(t, "boundary", "started@+1m", boundary)
	if !boundary.Accepted {
		t.Fatalf("boundary event must be admitted, got %s", boundary.Reason)
	}

	// 30s into the future relative to wall clock, tolerance 10s.
	future := e.ReportEvent(ev("f1", "r4", model.EventStarted, clk.Now().Add(30*time.Second)))
	logResult(t, "future", "started@wall+30s", future)
	if future.Reason != model.RejectFuture {
		t.Fatalf("want future rejection, got %s", future.Reason)
	}

	// Rejected reports must not have consumed dedup ids nor moved watermark.
	if r := e.ReportEvent(ev("late1", "r2", model.EventStarted, base.Add(30*time.Second))); r.Reason != model.RejectLate {
		t.Fatalf("rejected id reusable; still late, got reason=%s detail=%s", r.Reason, r.Detail)
	}
}

func TestInvalidInput(t *testing.T) {
	e, clk := newTestEngine(t, "")
	ts := clk.Now()
	cases := []struct {
		name string
		ev   model.Event
	}{
		{"empty id", model.Event{Pipeline: "p", RunID: "r", Type: model.EventStarted, Timestamp: ts}},
		{"empty pipeline", model.Event{ID: "i", RunID: "r", Type: model.EventStarted, Timestamp: ts}},
		{"empty run", model.Event{ID: "i", Pipeline: "p", Type: model.EventStarted, Timestamp: ts}},
		{"zero ts", model.Event{ID: "i", Pipeline: "p", RunID: "r", Type: model.EventStarted}},
		{"bad type", model.Event{ID: "i", Pipeline: "p", RunID: "r", Type: "bogus", Timestamp: ts}},
	}
	for _, c := range cases {
		r := e.ReportEvent(c.ev)
		logResult(t, "invalid/"+c.name, c.ev, r)
		if r.Reason != model.RejectInvalid {
			t.Fatalf("%s: want invalid_data, got %s", c.name, r.Reason)
		}
	}
	// Invalid metric: negative and sla>1.
	mr := e.ReportMetric(model.Metric{ID: "m1", Pipeline: "p", Kind: model.MetricLatency, Value: -1, Timestamp: ts})
	if mr.Reason != model.RejectInvalid {
		t.Fatalf("negative metric: %s", mr.Reason)
	}
	mr = e.ReportMetric(model.Metric{ID: "m2", Pipeline: "p", Kind: model.MetricSLA, Value: 1.5, Timestamp: ts})
	if mr.Reason != model.RejectInvalid {
		t.Fatalf("sla>1: %s", mr.Reason)
	}
}

func TestRunStateTransitionsAndConflicts(t *testing.T) {
	e, clk := newTestEngine(t, "")
	base := clk.Now()

	if !e.ReportEvent(ev("s1", "r1", model.EventStarted, base)).Accepted {
		t.Fatal("start")
	}
	// Duplicate start.
	if r := e.ReportEvent(ev("s1b", "r1", model.EventStarted, base)); r.Reason != model.RejectStateConflict {
		t.Fatalf("duplicate start: %s", r.Reason)
	}
	if !e.ReportEvent(ev("rt1", "r1", model.EventRetried, base.Add(time.Second))).Accepted {
		t.Fatal("retry running")
	}
	if !e.ReportEvent(ev("f1", "r1", model.EventFailed, base.Add(2*time.Second))).Accepted {
		t.Fatal("fail")
	}
	// Retry after failure allowed (retry storm), run still retained.
	if !e.ReportEvent(ev("rt2", "r1", model.EventRetried, base.Add(3*time.Second))).Accepted {
		t.Fatal("retry after fail")
	}
	// Succeed after failure -> run ends succeeded; failure counter drops.
	if !e.ReportEvent(ev("ok1", "r1", model.EventSucceeded, base.Add(4*time.Second))).Accepted {
		t.Fatal("success after retry")
	}
	if r := e.ReportEvent(ev("f2", "r1", model.EventFailed, base.Add(5*time.Second))); r.Reason != model.RejectStateConflict {
		t.Fatalf("fail after success must conflict, got %s", r.Reason)
	}
	v := e.Query()
	row := v.Pipelines[0]
	if row.Runs[0].Status != model.RunSucceeded || row.Runs[0].Attempts != 2 {
		t.Fatalf("run = %+v", row.Runs[0])
	}
	if row.FailedRuns != 0 {
		t.Fatalf("successful run must not remain counted as failed, got %d", row.FailedRuns)
	}
	if row.RetriedRuns != 1 {
		t.Fatalf("retried runs = %d", row.RetriedRuns)
	}
}

func TestWatermarksPerPipeline(t *testing.T) {
	e, clk := newTestEngine(t, "")
	base := clk.Now()
	// p-a advances far; p-b must have an independent boundary.
	clk.Advance(time.Hour)
	if !e.ReportEvent(model.Event{ID: "a1", Pipeline: "p-a", RunID: "r", Type: model.EventStarted, Timestamp: base.Add(time.Hour)}).Accepted {
		t.Fatal("a1")
	}
	r := e.ReportEvent(model.Event{ID: "b1", Pipeline: "p-b", RunID: "r", Type: model.EventStarted, Timestamp: base})
	logResult(t, "independent-watermark", "p-b at base while p-a at +1h", r)
	if !r.Accepted {
		t.Fatalf("per-pipeline watermark violated: %s %s", r.Reason, r.Detail)
	}
}

// silence unused helper lint in case of edits
var _ = filepath.Join
