package engine

import (
	"testing"
	"time"

	"github.com/lechandonga/pipeline-observability-engine-go/pkg/alert"
	"github.com/lechandonga/pipeline-observability-engine-go/pkg/model"
)

func activeKeys(v View) map[string]model.Alert {
	out := map[string]model.Alert{}
	for _, a := range v.Alerts {
		if a.State == model.AlertActive {
			out[a.Key] = a
		}
	}
	return out
}

func TestLatencyAlertLifecycle(t *testing.T) {
	e, clk := newTestEngine(t, "")
	v, err := e.UpdateRules([]alert.ThresholdRule{{
		ID: "latency", Pipeline: "p1", Metric: model.MetricLatency,
		Above: true, Threshold: 100, RecoverBelow: 50,
		RecoveryWindow: time.Minute, CloseAfter: time.Minute,
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("rules installed at version %d", v)

	base := clk.Now()
	mk := func(id string, val float64, at time.Time) model.IngestResult {
		return e.ReportMetric(model.Metric{
			ID: id, Pipeline: "p1", Kind: model.MetricLatency, Value: val, Timestamp: at,
		})
	}
	if r := mk("m1", 150, base); !r.Accepted {
		t.Fatal(r.Detail)
	}
	// Repeated violations -> still a single active alert.
	mk("m2", 160, base.Add(time.Second))
	mk("m3", 170, base.Add(2*time.Second))
	q := e.Query()
	if len(activeKeys(q)) != 1 {
		t.Fatalf("want 1 active alert, got %+v", q.Alerts)
	}
	t.Logf("fired alert: %+v", activeKeys(q)["latency\x00p1"])

	// Recover below the hysteresis threshold.
	mk("m4", 40, base.Add(3*time.Second))
	q = e.Query()
	if len(activeKeys(q)) != 0 || len(q.Alerts) != 1 || q.Alerts[0].State != model.AlertRecovered {
		t.Fatalf("want recovered, got %+v", q.Alerts)
	}

	// Close after the configured interval.
	clk.Advance(64 * time.Second)
	q = e.Query()
	if len(q.Alerts) != 1 || q.Alerts[0].State != model.AlertClosed {
		t.Fatalf("want closed, got %+v", q.Alerts)
	}
	t.Logf("closed alert: %+v", q.Alerts[0])
}

func TestFailureAndSLARules(t *testing.T) {
	e, clk := newTestEngine(t, "")
	if _, err := e.UpdateRules([]alert.ThresholdRule{
		{ID: "failures", Pipeline: "p1", Metric: model.MetricFailure, Above: true, Threshold: 1},
		{ID: "sla", Pipeline: "p1", Metric: model.MetricSLA, Above: false, Threshold: 1},
	}); err != nil {
		t.Fatal(err)
	}
	base := clk.Now()
	// start -> failed -> retried -> succeeded.
	steps := []model.Event{
		ev("s", "r1", model.EventStarted, base),
		ev("f", "r1", model.EventFailed, base.Add(time.Second)),
		ev("rt", "r1", model.EventRetried, base.Add(2*time.Second)),
	}
	for _, e2 := range steps {
		if r := e.ReportEvent(e2); !r.Accepted {
			t.Fatalf("%s: %s", e2.ID, r.Detail)
		}
	}
	q := e.Query()
	if a, ok := activeKeys(q)["failures\x00p1"]; !ok {
		t.Fatalf("failure alert expected, got %+v", q.Alerts)
	} else {
		t.Logf("failure alert active value=%v count=%d", a.Value, a.Count)
	}

	// SLA non-compliance reported explicitly.
	r := e.ReportMetric(model.Metric{ID: "sla1", Pipeline: "p1", Kind: model.MetricSLA, Value: 0, Timestamp: base.Add(3 * time.Second)})
	if !r.Accepted {
		t.Fatal(r.Detail)
	}
	q = e.Query()
	if _, ok := activeKeys(q)["sla\x00p1"]; !ok {
		t.Fatalf("sla alert expected, got %+v", q.Alerts)
	}

	// Event-driven recovery: the run ultimately succeeds; failure counter drops.
	if r := e.ReportEvent(ev("ok", "r1", model.EventSucceeded, base.Add(4*time.Second))); !r.Accepted {
		t.Fatal(r.Detail)
	}
	clk.Advance(4 * time.Second)
	q = e.Query()
	for _, a := range q.Alerts {
		if a.Key == "failures\x00p1" && a.State == model.AlertActive {
			t.Fatalf("failure alert should clear once run succeeds, got %+v", a)
		}
	}
}

func TestConfigChangeDoesNotRewriteHistory(t *testing.T) {
	e, clk := newTestEngine(t, "")
	v1, err := e.UpdateRules([]alert.ThresholdRule{{
		ID: "lat", Pipeline: "p1", Metric: model.MetricLatency,
		Above: true, Threshold: 100, RecoveryWindow: time.Hour,
	}})
	if err != nil {
		t.Fatal(err)
	}
	base := clk.Now()
	e.ReportMetric(model.Metric{ID: "m1", Pipeline: "p1", Kind: model.MetricLatency, Value: 150, Timestamp: base})
	q := e.Query()
	if len(activeKeys(q)) != 1 {
		t.Fatalf("v1 alert expected: %+v", q.Alerts)
	}

	// Change threshold to 200 (rule id stays "lat"): the v1 alert stays pinned.
	v2, err := e.UpdateRules([]alert.ThresholdRule{{
		ID: "lat", Pipeline: "p1", Metric: model.MetricLatency,
		Above: true, Threshold: 200, RecoveryWindow: time.Hour,
	}})
	if err != nil {
		t.Fatal(err)
	}
	clk.Advance(time.Second)
	e.ReportMetric(model.Metric{ID: "m2", Pipeline: "p1", Kind: model.MetricLatency, Value: 150, Timestamp: clk.Now()})
	q = e.Query()
	if len(activeKeys(q)) != 1 {
		t.Fatalf("pinned alert count wrong: %+v", q.Alerts)
	}
	a := q.Alerts[0]
	if a.RuleVersion != v1 {
		t.Fatalf("historical alert rewritten: version %d, want %d", a.RuleVersion, v1)
	}
	t.Logf("history preserved: alert RuleVersion=%d (current=%d) state=%s", a.RuleVersion, v2, a.State)

	// Invalid new configuration must be rejected atomically (version unchanged).
	badV, err := e.UpdateRules([]alert.ThresholdRule{{ID: "x", Metric: "bogus", Above: true}})
	if err == nil || badV != v2 {
		t.Fatalf("invalid rule set must be rejected atomically, got v=%d err=%v", badV, err)
	}
}

func TestQueryConsistencySnapshot(t *testing.T) {
	e, _ := newTestEngine(t, "")
	// Empty query is well-formed.
	q := e.Query()
	if len(q.Pipelines) != 0 || len(q.Alerts) != 0 || q.GeneratedAt.IsZero() {
		t.Fatalf("empty view wrong: %+v", q)
	}
}
