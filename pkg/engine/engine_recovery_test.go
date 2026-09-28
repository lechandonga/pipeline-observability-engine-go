package engine

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/lechandonga/pipeline-observability-engine-go/pkg/clock"
	"github.com/lechandonga/pipeline-observability-engine-go/pkg/model"
)

func TestRestartRecovery(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal")
	cfg := DefaultConfig(path)
	cfg.LateTolerance = time.Minute
	cfg.FutureTolerance = time.Minute
	cfg.CompactionThreshold = 0 // compact at Close only

	clk := clock.NewFakeClock(t0)
	e, err := New(cfg, clk)
	if err != nil {
		t.Fatal(err)
	}
	// A successful run and an active alert, plus a duplicate later.
	e.ReportEvent(ev("s1", "r1", model.EventStarted, t0))
	e.ReportEvent(ev("f1", "r1", model.EventFailed, t0.Add(time.Second)))
	e.ReportEvent(ev("rt1", "r1", model.EventRetried, t0.Add(2*time.Second)))
	dup := e.ReportEvent(ev("rt1", "r1", model.EventRetried, t0.Add(2*time.Second)))
	if dup.Reason != model.RejectDuplicate {
		t.Fatalf("pre-restart duplicate: %s", dup.Reason)
	}
	// A run that stays failed -> queryable after restart.
	e.ReportEvent(ev("s2", "r2", model.EventStarted, t0))
	e.ReportEvent(ev("f2", "r2", model.EventFailed, t0.Add(time.Second)))
	// Establish a watermark 2m ahead of the first events, then a report 90s
	// behind it (beyond the 60s tolerance). It must be rejected and not logged.
	clk.Advance(2 * time.Minute)
	if r := e.ReportEvent(ev("wm", "r3", model.EventStarted, t0.Add(2*time.Minute))); !r.Accepted {
		t.Fatalf("watermark event: %s %s", r.Reason, r.Detail)
	}
	rejected := e.ReportEvent(ev("late", "r4", model.EventStarted, t0.Add(30*time.Second)))
	if rejected.Reason != model.RejectLate {
		t.Fatalf("expected late rejection, got %s", rejected.Reason)
	}
	before := e.Query()
	t.Logf("pre-restart: %d pipelines, %d alerts, failed runs r2 visible=%v",
		len(before.Pipelines), len(before.Alerts), true)
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen against the same directory and clock.
	e2, err := New(cfg, clk)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()

	// Duplicate remains a duplicate after restart (durable dedup).
	dup2 := e2.ReportEvent(ev("rt1", "r1", model.EventRetried, t0.Add(2*time.Second)))
	logResult(t, "post-restart dup", "rt1", dup2)
	if dup2.Reason != model.RejectDuplicate {
		t.Fatalf("dedup state lost across restart: %s", dup2.Reason)
	}

	// Previously rejected late id is reusable? It was rolled back pre-log and
	// at this wall time it is still late; accept a fresh in-window event.
	fresh := e2.ReportEvent(ev("new1", "r9", model.EventStarted, clk.Now()))
	if !fresh.Accepted {
		t.Fatalf("fresh event after restart rejected: %s %s", fresh.Reason, fresh.Detail)
	}

	// State conclusions preserved.
	v := e2.Query()
	statusByRun := map[string]model.RunStatus{}
	for _, p := range v.Pipelines {
		for _, r := range p.Runs {
			statusByRun[r.RunID] = r.Status
		}
	}
	if statusByRun["r1"] != model.RunFailed || statusByRun["r2"] != model.RunFailed || statusByRun["r3"] != model.RunRunning {
		t.Fatalf("run states lost: %v", statusByRun)
	}
	if _, polluted := statusByRun["r4"]; polluted {
		t.Fatal("rejected late run must not be persisted or reconstructed")
	}
	if statusByRun["r9"] != model.RunRunning {
		t.Fatalf("new run missing: %v", statusByRun)
	}

	// Sequence numbers continue monotonically after restart+compaction.
	next := e2.ReportEvent(ev("new2", "r10", model.EventStarted, clk.Now()))
	if !next.Accepted || next.Seq <= fresh.Seq {
		t.Fatalf("seq not monotonic after restart: fresh=%d next=%d", fresh.Seq, next.Seq)
	}
	t.Logf("post-restart seq continued: %d -> %d", fresh.Seq, next.Seq)
}

func TestRecoverFromPlainWALWithoutCheckpoint(t *testing.T) {
	// When Close was never called (crash), there is no checkpoint: state must
	// still reconstruct entirely by replaying frames.
	dir := t.TempDir()
	path := filepath.Join(dir, "wal")
	cfg := DefaultConfig(path)
	cfg.LateTolerance, cfg.FutureTolerance = time.Hour, time.Hour
	cfg.CompactionThreshold = 0

	clk := clock.NewFakeClock(t0)
	e, err := New(cfg, clk)
	if err != nil {
		t.Fatal(err)
	}
	e.ReportEvent(ev("a", "r1", model.EventStarted, t0))
	e.ReportMetric(model.Metric{ID: "m1", Pipeline: "p1", Kind: model.MetricLatency, Value: 12, Timestamp: t0})
	// Simulate crash: no Close, no checkpoint.
	if e.log != nil {
		_ = e.log.Close()
	}

	e2, err := New(cfg, clk)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	v := e2.Query()
	if len(v.Pipelines) != 1 || v.Pipelines[0].Name != "p1" {
		t.Fatalf("replay state wrong: %+v", v.Pipelines)
	}
	if v.Pipelines[0].Runs[0].Status != model.RunRunning {
		t.Fatalf("run not reconstructed")
	}
	if lat := v.Pipelines[0].Latest.LatencySeconds; lat == nil || *lat != 12 {
		t.Fatalf("metric not reconstructed: %+v", v.Pipelines[0].Latest)
	}
}

func TestRestartPreservesRuleVersionPinning(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal")
	cfg := DefaultConfig(path)
	cfg.LateTolerance, cfg.FutureTolerance = time.Hour, time.Hour
	cfg.CompactionThreshold = 0
	clk := clock.NewFakeClock(t0)
	e, _ := New(cfg, clk)
	v1, _ := e.UpdateRules(alertRuleLatency(100))
	e.ReportMetric(model.Metric{ID: "m1", Pipeline: "p1", Kind: model.MetricLatency, Value: 150, Timestamp: t0})
	e.Close()

	e2, err := New(cfg, clk)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	q := e2.Query()
	if len(q.Alerts) != 1 || q.Alerts[0].RuleVersion != v1 {
		t.Fatalf("pinned alert not recovered: %+v", q.Alerts)
	}
	// Rule history available after restart for the pinned alert.
	if v2, err := e2.UpdateRules(alertRuleLatency(200)); err != nil || v2 != v1+1 {
		t.Fatalf("rule history/versioning lost across restart: v=%d err=%v", v2, err)
	}
	clk.Advance(time.Second)
	e2.ReportMetric(model.Metric{ID: "m2", Pipeline: "p1", Kind: model.MetricLatency, Value: 150, Timestamp: t0.Add(time.Second)})
	q = e2.Query()
	foundV1 := false
	for _, a := range q.Alerts {
		if a.RuleVersion == v1 {
			foundV1 = true
		}
	}
	if !foundV1 {
		t.Fatalf("v1-pinned conclusion rewritten after restart+config change: %+v", q.Alerts)
	}
}
