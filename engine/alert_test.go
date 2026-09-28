package engine

import (
	"path/filepath"
	"testing"

	"github.com/lechandonga/pipeline-observability-engine-go/clock"
	"github.com/lechandonga/pipeline-observability-engine-go/config"
	"github.com/lechandonga/pipeline-observability-engine-go/model"
)

func alertCfg(v int) *config.Config {
	c := config.Default()
	c.Version = v
	c.LateWindowMS = 3_600_000
	c.RetentionMS = 3_600_000
	c.FutureSkewMS = 5_000
	c.FreshnessStaleMS = 2_000 // stale if watermark older than 2s
	c.LatencyMS = config.Threshold{Enabled: true, WarnAt: 100, CriticalAt: 500}
	c.ThroughputMinRPS = config.Threshold{Enabled: false}
	c.ThroughputWindowMS = 60_000
	c.FailuresTotal = config.Threshold{Enabled: true, WarnAt: 1, CriticalAt: 3}
	c.RetriesTotal = config.Threshold{Enabled: true, WarnAt: 2, CriticalAt: 4}
	c.SLABreachMS = 5_000
	return c
}

func newAlertEngine(t *testing.T, t0 int64, cfg *config.Config) (*Engine, *clock.FakeClock) {
	t.Helper()
	fc := &clock.FakeClock{T: t0}
	e, err := New(Options{DataDir: filepath.Join(t.TempDir(), "poe"), Clock: fc, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e, fc
}

func findAlert(list []model.Alert, rule string) *model.Alert {
	for i := range list {
		if list[i].Rule == rule {
			return &list[i]
		}
	}
	return nil
}

func TestFreshnessFireAndRecover(t *testing.T) {
	e, fc := newAlertEngine(t, 10_000, alertCfg(1))

	// First arrival establishes the watermark; no staleness yet.
	r := e.IngestMetric(model.Metric{ID: "m1", Pipeline: "p", Name: model.MetricDataArrived, Value: 1, Timestamp: 10_000})
	t.Logf("data arrived -> %s/%s", r.Outcome, r.Reason)
	if len(e.Alerts("p")) != 0 {
		t.Fatalf("no freshness alert expected immediately, got %+v", e.Alerts("p"))
	}

	// Silence beyond 2s fires a critical freshness alert.
	fc.Advance(2_001)
	e.Reevaluate()
	firing := e.Alerts("p")
	t.Logf("after silence active alerts=%d", len(firing))
	a := findAlert(firing, model.RuleFreshness)
	if a == nil || a.State != model.AlertFiring || a.Severity != model.SeverityCritical || a.ConfigVersion != 1 {
		t.Fatalf("want firing critical freshness pinned v1: %+v", firing)
	}

	// Staying silent must not duplicate the alert.
	fc.Advance(1_000)
	e.Reevaluate()
	if got := e.Alerts("p"); len(got) != 1 {
		t.Fatalf("alert duplicated while firing: %d", len(got))
	}

	// New data inside the window recovers and closes the incident.
	rr := e.IngestMetric(model.Metric{ID: "m2", Pipeline: "p", Name: model.MetricDataArrived, Value: 1, Timestamp: fc.T})
	t.Logf("new data -> %s/%s", rr.Outcome, rr.Reason)
	if len(e.Alerts("p")) != 0 {
		t.Fatalf("freshness alert should recover, got %+v", e.Alerts("p"))
	}
	hist := e.AlertHistory("p")
	if a := findAlert(hist, model.RuleFreshness); a == nil || a.State != model.AlertRecovered {
		t.Fatalf("recovered alert missing in history: %+v", hist)
	}
}

func TestFailuresAlertAndRestartPersistence(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "poe")
	cfg := alertCfg(3)
	fc := &clock.FakeClock{T: 50_000}
	e, err := New(Options{DataDir: dir, Clock: fc, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	// failures_total cumulative metric reaches warn then critical.
	for _, v := range []float64{1, 1, 1} {
		r := e.IngestMetric(model.Metric{
			ID: id(), Pipeline: "p", Name: model.MetricFailures, Value: v, Timestamp: fc.T,
		})
		if r.Outcome != model.OutcomeAccepted {
			t.Fatalf("failure metric: %+v", r)
		}
	}
	firing := e.Alerts("p")
	a := findAlert(firing, model.RuleFailures)
	if a == nil || a.Severity != model.SeverityCritical || a.ConfigVersion != 3 {
		t.Fatalf("want critical failures alert v3, got %+v", firing)
	}
	firedSeq := a.Sequence
	t.Logf("fired failures alert incident=%d seq=%d", a.IncidentSeq, a.Sequence)
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	// Restart: the firing conclusion must still be present and not re-fired.
	e2, err := New(Options{DataDir: dir, Clock: fc, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	after := e2.Alerts("p")
	a2 := findAlert(after, model.RuleFailures)
	if a2 == nil || a2.State != model.AlertFiring {
		t.Fatalf("firing alert lost on restart: %+v", after)
	}
	if a2.Sequence != firedSeq || a2.IncidentSeq != a.IncidentSeq {
		t.Fatalf("alert identity changed across restart: before=%+v after=%+v", a, a2)
	}
}

func TestSLABreachOnlyWhileRunning(t *testing.T) {
	e, fc := newAlertEngine(t, 80_000, alertCfg(1))
	e.IngestEvent(model.Event{ID: "s", Pipeline: "p", RunID: "r", Type: model.EventRunStarted, Timestamp: 80_000})
	fc.Advance(6_000)
	e.Reevaluate()
	if a := findAlert(e.Alerts("p"), model.RuleSLA); a == nil {
		t.Fatalf("expected SLA breach for long active run: %+v", e.Alerts("p"))
	}
	// Run succeeds late; SLA alert must recover (no active run anymore).
	e.IngestEvent(model.Event{ID: "ok", Pipeline: "p", RunID: "r", Type: model.EventRunSucceeded, Timestamp: fc.T})
	if a := findAlert(e.Alerts("p"), model.RuleSLA); a != nil {
		t.Fatalf("SLA alert should recover after run ended, got %+v", a)
	}
}

func TestLatencyAndRetryLifecycle(t *testing.T) {
	e, fc := newAlertEngine(t, 90_000, alertCfg(1))
	r := e.IngestMetric(model.Metric{ID: "l1", Pipeline: "p", Name: model.MetricLatencyMS, Value: 200, Timestamp: 90_000})
	t.Logf("latency 200 -> %s/%s", r.Outcome, r.Reason)
	if a := findAlert(e.Alerts("p"), model.RuleLatency); a == nil || a.Severity != model.SeverityWarn {
		t.Fatalf("want warn latency: %+v", e.Alerts("p"))
	}
	r = e.IngestMetric(model.Metric{ID: "l2", Pipeline: "p", Name: model.MetricLatencyMS, Value: 600, Timestamp: 90_100})
	if a := findAlert(e.Alerts("p"), model.RuleLatency); a == nil || a.Severity != model.SeverityCritical {
		t.Fatalf("want critical latency: %+v", e.Alerts("p"))
	}
	// Retries cumulative crossing warn.
	e.IngestEvent(model.Event{ID: "rs1", Pipeline: "p", RunID: "x", Type: model.EventRunStarted, Timestamp: 90_000})
	e.IngestEvent(model.Event{ID: "rt1", Pipeline: "p", RunID: "x", Type: model.EventRetryScheduled, Timestamp: 90_100})
	// second run needed because first retry run stays active; use same run is fine
	e.IngestEvent(model.Event{ID: "rt2", Pipeline: "p", RunID: "x", Type: model.EventRetryScheduled, Timestamp: 90_200})
	if a := findAlert(e.Alerts("p"), model.RuleRetries); a == nil {
		t.Fatalf("want retries alert: %+v", e.Alerts("p"))
	}
	_ = fc
}
