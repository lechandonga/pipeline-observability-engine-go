package alert

import (
	"testing"
	"time"

	"github.com/lechandonga/pipeline-observability-engine-go/pkg/model"
)

var t0 = time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)

func mkRule(id string, threshold, recoverBelow float64, forDur, recovery, close time.Duration) ThresholdRule {
	return ThresholdRule{
		ID: id, Pipeline: "p1", Metric: model.MetricLatency, Above: true,
		Threshold: threshold, RecoverBelow: recoverBelow,
		For: forDur, RecoveryWindow: recovery, CloseAfter: close,
	}
}

func rulesOf(rs *Rules, p string) []VersionedRule { return rs.ForPipeline(p) }

func activeAlerts(as []model.Alert) map[string]model.Alert {
	m := map[string]model.Alert{}
	for _, a := range as {
		if a.State == model.AlertActive {
			m[a.RuleID] = a
		}
	}
	return m
}

func TestFireAndDedup(t *testing.T) {
	rs := NewRules()
	v, err := rs.Replace([]ThresholdRule{mkRule("r-high", 100, 0, 0, 0, 0)})
	if err != nil {
		t.Fatal(err)
	}
	m := NewManager(rs)

	snap := latSnap(150)
	f1 := m.Evaluate("p1", snap, rulesOf(rs, "p1"), t0)
	f2 := m.Evaluate("p1", snap, rulesOf(rs, "p1"), t0.Add(time.Second))
	f3 := m.Evaluate("p1", snap, rulesOf(rs, "p1"), t0.Add(2*time.Second))
	if len(f1) != 1 || len(f2) != 0 || len(f3) != 0 {
		t.Fatalf("fired counts %d,%d,%d: repeated violations must dedup to 1", len(f1), len(f2), len(f3))
	}
	got := activeAlerts(m.Snapshot(t0.Add(2*time.Second), time.Minute))
	if a, ok := got["r-high"]; !ok || a.RuleVersion != v || a.Count != 3 {
		t.Fatalf("deduplicated active alert wrong: %+v", a)
	}
}

func TestForDuration(t *testing.T) {
	rs := NewRules()
	if _, err := rs.Replace([]ThresholdRule{mkRule("r-for", 100, 0, 30*time.Second, 0, 0)}); err != nil {
		t.Fatal(err)
	}
	m := NewManager(rs)
	bad := latSnap(150)
	rules := rulesOf(rs, "p1")

	if f := m.Evaluate("p1", bad, rules, t0); len(f) != 0 {
		t.Fatal("must not fire before For elapses")
	}
	if f := m.Evaluate("p1", bad, rules, t0.Add(10*time.Second)); len(f) != 0 {
		t.Fatal("must not fire at 10s")
	}
	// Pending episodes are invisible before firing.
	if len(m.Snapshot(t0.Add(20*time.Second), time.Minute)) != 0 {
		t.Fatal("pending episode must not be queryable")
	}
	if f := m.Evaluate("p1", bad, rules, t0.Add(30*time.Second)); len(f) != 1 {
		t.Fatal("must fire at 30s boundary")
	}
	// Continued bad evidence never re-fires (dedup).
	if f := m.Evaluate("p1", bad, rules, t0.Add(time.Minute)); len(f) != 0 {
		t.Fatalf("already fired, got %d", len(f))
	}
}

func TestHysteresisRecoveryAndFlap(t *testing.T) {
	rs := NewRules()
	if _, err := rs.Replace([]ThresholdRule{mkRule("r-hyst", 100, 80, 0, 0, 0)}); err != nil {
		t.Fatal(err)
	}
	m := NewManager(rs)
	rules := rulesOf(rs, "p1")

	if f := m.Evaluate("p1", latSnap(150), rules, t0); len(f) != 1 {
		t.Fatal("fire at 150")
	}
	// 90 is below 100 but above RecoverBelow 80 -> still active (hysteresis).
	m.Evaluate("p1", latSnap(90), rules, t0.Add(time.Second))
	got := m.Snapshot(t0.Add(time.Second), time.Minute)
	if len(got) != 1 || got[0].State != model.AlertActive {
		t.Fatalf("hysteresis: still active expected, got %+v", got)
	}
	// 70 clears.
	m.Evaluate("p1", latSnap(70), rules, t0.Add(2*time.Second))
	got = m.Snapshot(t0.Add(2*time.Second), time.Minute)
	if len(got) != 1 || got[0].State != model.AlertRecovered {
		t.Fatalf("want recovered, got %+v", got)
	}
	// Flap while recovered (before close): same episode reopens, not a duplicate.
	m.Evaluate("p1", latSnap(150), rules, t0.Add(3*time.Second))
	got = m.Snapshot(t0.Add(3*time.Second), time.Minute)
	if len(got) != 1 || got[0].State != model.AlertActive {
		t.Fatalf("flap must reopen one alert, got %+v", got)
	}
}

func TestStaleRecoveryAndClose(t *testing.T) {
	rs := NewRules()
	if _, err := rs.Replace([]ThresholdRule{mkRule("r-stale", 100, 0, 0, time.Minute, time.Minute)}); err != nil {
		t.Fatal(err)
	}
	m := NewManager(rs)
	rules := rulesOf(rs, "p1")
	m.Evaluate("p1", latSnap(150), rules, t0)

	// At +61s with no evidence: virtually recovered.
	got := m.Snapshot(t0.Add(61*time.Second), 0)
	if len(got) != 1 || got[0].State != model.AlertRecovered {
		t.Fatalf("want stale recovered, got %+v", got)
	}
	// RecoveryWindow + CloseAfter elapsed: closed.
	got = m.Snapshot(t0.Add(121*time.Second), 0)
	if len(got) != 1 || got[0].State != model.AlertClosed {
		t.Fatalf("want closed, got %+v", got)
	}
	// Within keepClosed the closed episode is still queryable.
	got = m.Snapshot(t0.Add(121*time.Second), 30*time.Second)
	if len(got) != 1 || got[0].State != model.AlertClosed {
		t.Fatalf("closed episode retained within keepClosed window, got %+v", got)
	}
	// Once close+keepClosed has elapsed it is pruned (closedAt=120s + 30s).
	got = m.Snapshot(t0.Add(150*time.Second), 30*time.Second)
	if len(got) != 0 {
		t.Fatalf("want pruned after keepClosed, got %+v", got)
	}
}

func TestRuleVersionPinning(t *testing.T) {
	rs := NewRules()
	v1, err := rs.Replace([]ThresholdRule{mkRule("r", 100, 0, 0, time.Minute, 0)})
	if err != nil {
		t.Fatal(err)
	}
	m := NewManager(rs)
	m.Evaluate("p1", latSnap(150), rs.ForPipeline("p1"), t0)
	got := m.Snapshot(t0, time.Minute)
	if len(got) != 1 || got[0].RuleVersion != v1 {
		t.Fatalf("alert must pin v%d, got %+v", v1, got)
	}

	// Config change: threshold raised to 200. The old alert must remain active
	// (pinned), and no new episode starts because it is a different generation
	// whose current value does not violate.
	v2, err := rs.Replace([]ThresholdRule{mkRule("r", 200, 0, 0, time.Minute, 0)})
	if err != nil {
		t.Fatal(err)
	}
	m.Evaluate("p1", latSnap(150), rs.ForPipeline("p1"), t0.Add(time.Second))
	got = m.Snapshot(t0.Add(time.Second), time.Minute)
	if len(got) != 1 || got[0].RuleVersion != v1 || got[0].State != model.AlertActive {
		t.Fatalf("pinned v%d alert must survive config change to v%d, got %+v", v1, v2, got)
	}
	// The v1 alert goes stale only by its own pinned recovery window.
	got = m.Snapshot(t0.Add(2*time.Minute), time.Minute)
	if len(got) != 1 || got[0].State != model.AlertRecovered || got[0].RuleVersion != v1 {
		t.Fatalf("pinned alert stale-recovers under v1 rule, got %+v", got)
	}

	// A value violating the NEW threshold starts a separate v2 episode without
	// rewriting the v1 conclusion.
	m.Evaluate("p1", latSnap(250), rs.ForPipeline("p1"), t0.Add(3*time.Minute))
	got = m.Snapshot(t0.Add(3*time.Minute), time.Minute)
	if len(got) != 2 {
		t.Fatalf("want two generation-pinned episodes, got %+v", got)
	}
	versions := map[int]bool{}
	for _, a := range got {
		versions[a.RuleVersion] = true
	}
	if !versions[v1] || !versions[v2] {
		t.Fatalf("want episodes for v1 and v2, got %v", versions)
	}
}

func latSnap(v float64) MetricSnapshot {
	return MetricSnapshot{LatencySeconds: v, Avail: MetricAvailability{HasLatency: true}}
}
