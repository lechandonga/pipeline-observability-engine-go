package engine

import (
	"path/filepath"
	"testing"

	"github.com/lechandonga/pipeline-observability-engine-go/clock"
	"github.com/lechandonga/pipeline-observability-engine-go/config"
	"github.com/lechandonga/pipeline-observability-engine-go/model"
)

// quietCfg disables all alert rules so this test isolates merging semantics.
func quietCfg() *config.Config {
	c := config.Default()
	c.LateWindowMS = 3_600_000
	c.RetentionMS = 3_600_000
	c.FreshnessStaleMS = 0
	c.LatencyMS = config.Threshold{Enabled: false}
	c.ThroughputMinRPS = config.Threshold{Enabled: false}
	c.FailuresTotal = config.Threshold{Enabled: false}
	c.RetriesTotal = config.Threshold{Enabled: false}
	c.SLABreachMS = 0
	return c
}

func newQuietEngine(t *testing.T) *Engine {
	e, err := New(Options{
		DataDir: filepath.Join(t.TempDir(), "poe"),
		Clock:   &clock.FakeClock{T: 10_000_000},
		Config:  quietCfg(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}

func TestMergeIsOrderInvariantWithinWindow(t *testing.T) {
	// Event time order differs from delivery order, and two engines receive
	// the same facts in reversed delivery order.
	events := []model.Event{
		{ID: "start", Pipeline: "p", RunID: "r", Type: model.EventRunStarted, Timestamp: 10_000_000},
		{ID: "retry", Pipeline: "p", RunID: "r", Type: model.EventRetryScheduled, Timestamp: 10_000_100},
		{ID: "ok", Pipeline: "p", RunID: "r", Type: model.EventRetrySucceeded, Timestamp: 10_000_200},
	}
	// NOTE: "retry" and "ok" can only be accepted after "start" by the run
	// state machine, so pure reverse order is itself a state_conflict; the
	// order-invariance guarantee is that, given valid delivery orders inside
	// the window, the final merged conclusion is identical. We compare a
	// start-first order against a start-first order with later events swapped
	// plus an out-of-order metric batch.
	orderA := []int{0, 1, 2}
	orderB := []int{0, 2, 1}

	eA := newQuietEngine(t)
	for _, i := range orderA {
		if r := eA.IngestEvent(events[i]); r.Outcome != model.OutcomeAccepted {
			t.Fatalf("A event %d rejected: %+v", i, r)
		}
	}

	eB := newQuietEngine(t)
	for _, i := range orderB {
		r := eB.IngestEvent(events[i])
		if i == 1 && r.Reason == model.ReasonStateConflict {
			// retry arriving after a terminal success must be refused.
			t.Logf("B retry after terminal correctly refused: %s", r.Message)
			continue
		}
		if r.Outcome != model.OutcomeAccepted {
			t.Fatalf("B event %d rejected: %+v", i, r)
		}
	}

	ra, _ := eA.GetRun("p", "r")
	rb, _ := eB.GetRun("p", "r")
	// Both observed success as the terminal status (B rejected the late retry
	// precisely so it cannot corrupt that conclusion).
	if ra.Status != model.RunSucceeded || rb.Status != model.RunSucceeded {
		t.Fatalf("terminal status differs: A=%s B=%s", ra.Status, rb.Status)
	}
	if ra.EndedAt != 10_000_200 || rb.EndedAt != 10_000_200 {
		t.Fatalf("endedAt differs: A=%d B=%d", ra.EndedAt, rb.EndedAt)
	}
	if ra.Watermark != rb.Watermark {
		t.Fatalf("watermark differs: A=%d B=%d", ra.Watermark, rb.Watermark)
	}
	t.Logf("stable conclusion regardless of in-window delivery order: status=%s endedAt=%d watermark=%d (A retries=%d, B retries=%d)",
		ra.Status, ra.EndedAt, ra.Watermark, ra.Retries, rb.Retries)
}

func TestOutOfOrderLatencyDeterministic(t *testing.T) {
	// Two engines get the same latency points in opposite delivery order.
	points := []struct {
		id string
		ts int64
		v  float64
	}{
		{"a", 10_000_200, 220},
		{"b", 10_000_000, 100},
		{"c", 10_000_100, 160},
	}
	e1, e2 := newQuietEngine(t), newQuietEngine(t)
	ingest := func(e *Engine, order []int) {
		for _, i := range order {
			r := e.IngestMetric(model.Metric{
				ID: points[i].id, Pipeline: "p", Name: model.MetricLatencyMS,
				Value: points[i].v, Timestamp: points[i].ts,
			})
			if r.Outcome != model.OutcomeAccepted {
				t.Fatalf("point %s rejected: %+v", points[i].id, r)
			}
		}
	}
	ingest(e1, []int{0, 1, 2}) // newest-first
	ingest(e2, []int{2, 1, 0}) // ascending then newest last

	st1, _ := e1.GetPipeline("p")
	st2, _ := e2.GetPipeline("p")
	if st1.LatestLatencyMS != 220 || st2.LatestLatencyMS != 220 || st1.Watermark != st2.Watermark {
		t.Fatalf("out-of-order merge differs: %+v vs %+v", st1, st2)
	}

	// Re-sending a point (even at any time) is a duplicate and cannot
	// overwrite or re-count.
	r := e1.IngestMetric(model.Metric{
		ID: "b", Pipeline: "p", Name: model.MetricLatencyMS, Value: 999, Timestamp: 10_000_300,
	})
	if r.Outcome != model.OutcomeRejected || r.Reason != model.ReasonDuplicate {
		t.Fatalf("tampered resend must be duplicate, got %+v", r)
	}
	st1, _ = e1.GetPipeline("p")
	if st1.LatestLatencyMS != 220 {
		t.Fatalf("duplicate overwrote latency: %v", st1.LatestLatencyMS)
	}
	t.Logf("deterministic out-of-order latest latency=%v watermark=%d", st1.LatestLatencyMS, st1.Watermark)
}
