package engine

import (
	"path/filepath"
	"testing"

	"github.com/lechandonga/pipeline-observability-engine-go/clock"
	"github.com/lechandonga/pipeline-observability-engine-go/config"
	"github.com/lechandonga/pipeline-observability-engine-go/model"
)

func newTestEngine(t *testing.T, t0 int64, lateWindow int64) (*Engine, *clock.FakeClock) {
	t.Helper()
	fc := &clock.FakeClock{T: t0}
	cfg := config.Default()
	cfg.LateWindowMS = lateWindow
	cfg.FutureSkewMS = 5_000
	cfg.FreshnessStaleMS = 0 // disabled to keep this test focused
	cfg.LatencyMS = config.Threshold{Enabled: false}
	cfg.ThroughputMinRPS = config.Threshold{Enabled: false}
	cfg.FailuresTotal = config.Threshold{Enabled: false}
	cfg.RetriesTotal = config.Threshold{Enabled: false}
	cfg.SLABreachMS = 0
	dir := filepath.Join(t.TempDir(), "poe")
	e, err := New(Options{DataDir: dir, Clock: fc, Config: cfg})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e, fc
}

func logEvent(t *testing.T, ev model.Event, r model.IngestResult) {
	t.Helper()
	t.Logf("INPUT  event pipeline=%s run=%s type=%-15s ts=%d id=%s", ev.Pipeline, ev.RunID, ev.Type, ev.Timestamp, ev.ID)
	t.Logf("RESULT outcome=%-8s reason=%-15s seq=%d msg=%s", r.Outcome, r.Reason, r.Sequence, r.Message)
}

func TestHappyLifecycle(t *testing.T) {
	e, fc := newTestEngine(t, 100_000, 60_000)

	ev := func(id string, typ model.EventType, ts int64) model.Event {
		return model.Event{ID: id, Pipeline: "p", RunID: "r1", Type: typ, Timestamp: ts}
	}
	r := e.IngestEvent(ev("e1", model.EventRunStarted, 100_000))
	logEvent(t, ev("e1", model.EventRunStarted, 100_000), r)
	if r.Outcome != model.OutcomeAccepted {
		t.Fatalf("start rejected: %+v", r)
	}
	r = e.IngestEvent(ev("e2", model.EventRunSucceeded, 100_500))
	logEvent(t, ev("e2", model.EventRunSucceeded, 100_500), r)
	if r.Outcome != model.OutcomeAccepted {
		t.Fatalf("success rejected: %+v", r)
	}

	run, ok := e.GetRun("p", "r1")
	if !ok || run.Status != model.RunSucceeded || run.StartedAt != 100_000 || run.EndedAt != 100_500 {
		t.Fatalf("bad run state: %+v ok=%v", run, ok)
	}
	if run.Watermark != 100_500 {
		t.Fatalf("watermark want 100500, got %d", run.Watermark)
	}

	// Time travel does not alter established state.
	fc.Advance(1_000_000)
	run, _ = e.GetRun("p", "r1")
	if run.Status != model.RunSucceeded || run.EndedAt != 100_500 {
		t.Fatalf("historical conclusion changed over time: %+v", run)
	}
}

func TestInvalidInputs(t *testing.T) {
	e, _ := newTestEngine(t, 100_000, 60_000)
	cases := []model.Event{
		{ID: "", Pipeline: "p", RunID: "r", Type: model.EventRunStarted, Timestamp: 100_000},
		{ID: "x", Pipeline: "", RunID: "r", Type: model.EventRunStarted, Timestamp: 100_000},
		{ID: "x", Pipeline: "p", RunID: "", Type: model.EventRunStarted, Timestamp: 100_000},
		{ID: "x", Pipeline: "p", RunID: "r", Type: "bogus", Timestamp: 100_000},
		{ID: "x", Pipeline: "p", RunID: "r", Type: model.EventRunStarted, Timestamp: 0},
	}
	for _, ev := range cases {
		r := e.IngestEvent(ev)
		logEvent(t, ev, r)
		if r.Outcome != model.OutcomeRejected || r.Reason != model.ReasonInvalid {
			t.Fatalf("want invalid_data rejection, got %+v", r)
		}
	}
	// Invalid metric: negative value, zero latency, unknown name.
	for _, mt := range []model.Metric{
		{ID: "m", Pipeline: "p", Name: "latency_ms", Value: -1, Timestamp: 100_000},
		{ID: "m2", Pipeline: "p", Name: "latency_ms", Value: 0, Timestamp: 100_000},
		{ID: "m3", Pipeline: "p", Name: "nope", Value: 1, Timestamp: 100_000},
	} {
		r := e.IngestMetric(mt)
		t.Logf("INPUT metric %+v -> %+v", mt, r)
		if r.Outcome != model.OutcomeRejected || r.Reason != model.ReasonInvalid {
			t.Fatalf("want invalid metric rejection, got %+v", r)
		}
	}
	if _, ok := e.GetPipeline("p"); ok {
		t.Fatal("rejected input must not create pipeline state")
	}
}

func TestDuplicateTakesEffectOnce(t *testing.T) {
	e, _ := newTestEngine(t, 100_000, 60_000)
	ev := model.Event{ID: "d1", Pipeline: "p", RunID: "r", Type: model.EventRunStarted, Timestamp: 100_000}
	r1 := e.IngestEvent(ev)
	logEvent(t, ev, r1)
	if r1.Outcome != model.OutcomeAccepted {
		t.Fatal(r1)
	}
	// Exact resend.
	r2 := e.IngestEvent(ev)
	logEvent(t, ev, r2)
	if r2.Outcome != model.OutcomeRejected || r2.Reason != model.ReasonDuplicate {
		t.Fatalf("exact duplicate must be rejected, got %+v", r2)
	}
	// Same ID but different payload: still a duplicate, must not re-apply.
	tampered := ev
	tampered.Timestamp = 100_100
	r3 := e.IngestEvent(tampered)
	logEvent(t, tampered, r3)
	if r3.Outcome != model.OutcomeRejected || r3.Reason != model.ReasonDuplicate {
		t.Fatalf("reused id must be duplicate regardless of payload, got %+v", r3)
	}
	run, _ := e.GetRun("p", "r")
	if run.StartedAt != 100_000 {
		t.Fatalf("duplicate changed state: %+v", run)
	}
}

func TestOutOfOrderWithinWindowAndLateRejection(t *testing.T) {
	e, fc := newTestEngine(t, 100_000, 10_000)

	start := model.Event{ID: "s", Pipeline: "p", RunID: "r", Type: model.EventRunStarted, Timestamp: 100_000}
	ok := e.IngestEvent(start)
	logEvent(t, start, ok)

	// A slightly late success (out of order relative to wall clock) inside
	// the 10s window must still be accepted and produce a stable result.
	fc.Advance(5_000)
	late := model.Event{ID: "l", Pipeline: "p", RunID: "r", Type: model.EventRunSucceeded, Timestamp: 100_300}
	r := e.IngestEvent(late)
	logEvent(t, late, r)
	if r.Outcome != model.OutcomeAccepted {
		t.Fatalf("in-window late event must be accepted, got %+v", r)
	}
	run, _ := e.GetRun("p", "r")
	if run.Status != model.RunSucceeded || run.EndedAt != 100_300 {
		t.Fatalf("late event not merged: %+v", run)
	}

	// Beyond the window: rejected as window_expired and state untouched.
	fc.Advance(10_000) // now 115_000
	expired := model.Event{ID: "x", Pipeline: "p", RunID: "r2", Type: model.EventRunStarted, Timestamp: 102_000}
	r = e.IngestEvent(expired)
	logEvent(t, expired, r)
	if r.Outcome != model.OutcomeRejected || r.Reason != model.ReasonLate {
		t.Fatalf("want window_expired, got %+v", r)
	}
	if _, exists := e.GetRun("p", "r2"); exists {
		t.Fatal("rejected late event created a run")
	}

	// Too far in the future is a distinct rejection reason.
	future := model.Event{ID: "f", Pipeline: "p", RunID: "r3", Type: model.EventRunStarted, Timestamp: fc.T + 1_000_000}
	r = e.IngestEvent(future)
	logEvent(t, future, r)
	if r.Outcome != model.OutcomeRejected || r.Reason != model.ReasonFutureSkew {
		t.Fatalf("want future_skew, got %+v", r)
	}
}

func TestStateConflictOnTerminalRun(t *testing.T) {
	e, _ := newTestEngine(t, 100_000, 60_000)
	mk := func(id string, typ model.EventType) model.Event {
		return model.Event{ID: id, Pipeline: "p", RunID: "r", Type: typ, Timestamp: 100_000}
	}
	e.IngestEvent(mk("s", model.EventRunStarted))
	e.IngestEvent(model.Event{ID: "ok", Pipeline: "p", RunID: "r", Type: model.EventRunSucceeded, Timestamp: 100_100})
	// After success, a failure with a new ID must not overwrite.
	conflict := model.Event{ID: "fail", Pipeline: "p", RunID: "r", Type: model.EventRunFailed, Timestamp: 100_200}
	r := e.IngestEvent(conflict)
	logEvent(t, conflict, r)
	if r.Outcome != model.OutcomeRejected || r.Reason != model.ReasonStateConflict {
		t.Fatalf("want state_conflict, got %+v", r)
	}
	run, _ := e.GetRun("p", "r")
	if run.Status != model.RunSucceeded {
		t.Fatalf("terminal state overwritten: %+v", run)
	}
	// Event for an unknown run (no start) is also a state conflict.
	orphan := model.Event{ID: "o", Pipeline: "p", RunID: "ghost", Type: model.EventRunFailed, Timestamp: 100_000}
	r = e.IngestEvent(orphan)
	logEvent(t, orphan, r)
	if r.Reason != model.ReasonStateConflict {
		t.Fatalf("want state_conflict for orphan event, got %+v", r)
	}
}
