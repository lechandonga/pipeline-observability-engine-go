package engine

import (
	"testing"
	"time"

	"github.com/lechandonga/pipeline-observability-engine-go/pkg/alert"
	"github.com/lechandonga/pipeline-observability-engine-go/pkg/model"
)

func TestFreshnessAndThroughputAlerts(t *testing.T) {
	e, clk := newTestEngine(t, "")
	// Freshness bad when age >= 120s; throughput bad when rpm <= 10.
	if _, err := e.UpdateRules([]alert.ThresholdRule{
		{ID: "stale", Pipeline: "p1", Metric: model.MetricFreshness, Above: true, Threshold: 120},
		{ID: "slowpipe", Pipeline: "p1", Metric: model.MetricThroughput, Above: false, Threshold: 10},
	}); err != nil {
		t.Fatal(err)
	}
	base := clk.Now()

	mk := func(id string, kind model.MetricKind, v float64, at time.Time) model.IngestResult {
		return e.ReportMetric(model.Metric{ID: id, Pipeline: "p1", Kind: kind, Value: v, Timestamp: at})
	}

	// Healthy first: fresh age 5s, throughput 100 rpm -> no alerts.
	if r := mk("f1", model.MetricFreshness, 5, base); !r.Accepted {
		t.Fatal(r.Detail)
	}
	if r := mk("t1", model.MetricThroughput, 100, base); !r.Accepted {
		t.Fatal(r.Detail)
	}
	if q := e.Query(); len(q.Alerts) != 0 {
		t.Fatalf("healthy state must have no alerts, got %+v", q.Alerts)
	}

	// Throughput drops to 4 -> alert immediately.
	if r := mk("t2", model.MetricThroughput, 4, base.Add(time.Second)); !r.Accepted {
		t.Fatal(r.Detail)
	}
	q := e.Query()
	if _, ok := activeKeys(q)["slowpipe\x00p1"]; !ok {
		t.Fatalf("throughput alert expected: %+v", q.Alerts)
	}

	// Freshness ages to 300s -> second alert.
	if r := mk("f2", model.MetricFreshness, 300, base.Add(2*time.Second)); !r.Accepted {
		t.Fatal(r.Detail)
	}
	q = e.Query()
	if len(activeKeys(q)) != 2 {
		t.Fatalf("want 2 active alerts, got %+v", q.Alerts)
	}
	t.Logf("two metric-family alerts active: keys=%d", len(activeKeys(q)))

	// Throughput recovers above threshold while freshness stays bad.
	if r := mk("t3", model.MetricThroughput, 50, base.Add(3*time.Second)); !r.Accepted {
		t.Fatal(r.Detail)
	}
	q = e.Query()
	states := map[string]model.AlertState{}
	for _, a := range q.Alerts {
		states[a.RuleID] = a.State
	}
	if states["slowpipe"] != model.AlertRecovered || states["stale"] != model.AlertActive {
		t.Fatalf("independent recovery wrong: %+v", states)
	}
}

func TestRejectedLateMetricDoesNotMoveAlertOrState(t *testing.T) {
	e, clk := newTestEngine(t, "")
	if _, err := e.UpdateRules([]alert.ThresholdRule{
		{ID: "lat", Pipeline: "p1", Metric: model.MetricLatency, Above: true, Threshold: 100},
	}); err != nil {
		t.Fatal(err)
	}
	base := clk.Now()
	// Establish watermark far ahead then a late bad metric: must be rejected and
	// must neither start an alert nor overwrite the latest value.
	clk.Advance(10 * time.Minute)
	if r := e.ReportMetric(model.Metric{ID: "good", Pipeline: "p1", Kind: model.MetricLatency, Value: 10, Timestamp: clk.Now()}); !r.Accepted {
		t.Fatal(r.Detail)
	}
	r := e.ReportMetric(model.Metric{ID: "evil", Pipeline: "p1", Kind: model.MetricLatency, Value: 999, Timestamp: base})
	if r.Reason != model.RejectLate {
		t.Fatalf("want late rejection, got %s", r.Reason)
	}
	q := e.Query()
	if len(q.Alerts) != 0 {
		t.Fatalf("late metric must not raise an alert: %+v", q.Alerts)
	}
	if v := q.Pipelines[0].Latest.LatencySeconds; v == nil || *v != 10 {
		t.Fatalf("late metric polluted latest value: %v", v)
	}
	t.Logf("late metric rejected=%s, latest latency stays 10, no alert", r.Reason)

	// Global rule (empty pipeline) applies to every pipeline independently.
	if _, err := e.UpdateRules([]alert.ThresholdRule{
		{ID: "globallat", Metric: model.MetricLatency, Above: true, Threshold: 100},
	}); err != nil {
		t.Fatal(err)
	}
	if r := e.ReportMetric(model.Metric{ID: "b1", Pipeline: "p2", Kind: model.MetricLatency, Value: 150, Timestamp: clk.Now()}); !r.Accepted {
		t.Fatal(r.Detail)
	}
	q = e.Query()
	var keys []string
	for _, a := range q.Alerts {
		if a.State == model.AlertActive {
			keys = append(keys, a.Key)
		}
	}
	if len(keys) != 1 || keys[0] != "globallat\x00p2" {
		t.Fatalf("global rule must alert only on the violating pipeline, got %v", keys)
	}
}
