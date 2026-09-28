package engine

import (
	"path/filepath"
	"testing"

	"github.com/lechandonga/pipeline-observability-engine-go/clock"
	"github.com/lechandonga/pipeline-observability-engine-go/model"
)

func TestAlertRecoveredAfterRestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "poe")
	cfg := alertCfg(7)
	fc := &clock.FakeClock{T: 100_000}

	e, err := New(Options{DataDir: dir, Clock: fc, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	// Fire a failures alert before the crash (critical threshold 3).
	for i := 0; i < 3; i++ {
		r := e.IngestMetric(model.Metric{
			ID: "fail" + itoaTest(int64(i)), Pipeline: "p",
			Name: model.MetricFailures, Value: 1, Timestamp: 100_000,
		})
		if r.Outcome != model.OutcomeAccepted {
			t.Fatal(r)
		}
	}
	before := e.Alerts("p")
	a := findAlert(before, model.RuleFailures)
	if a == nil || a.State != model.AlertFiring || a.IncidentSeq != 1 {
		t.Fatalf("want single firing incident, got %+v", before)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	// Restart under the SAME config first: the firing conclusion survives.
	cfg7 := alertCfg(7)
	e2, err := New(Options{DataDir: dir, Clock: fc, Config: cfg7})
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()

	after := e2.Alerts("p")
	a2 := findAlert(after, model.RuleFailures)
	if a2 == nil {
		t.Fatalf("firing alert must survive restart")
	}
	t.Logf("post-restart alert: incident=%d firedAt=%d configVersion=%d", a2.IncidentSeq, a2.FiredAt, a2.ConfigVersion)
	if a2.IncidentSeq != 1 || a2.FiredAt != a.FiredAt || a2.ConfigVersion != 7 {
		t.Fatalf("alert identity drifted across restart: %+v vs %+v", a, a2)
	}

	// Now move to v8 which disables the rule: closes the incident once.
	cfg8 := alertCfg(8)
	cfg8.FailuresTotal.Enabled = false
	if err := e2.UpdateConfig(cfg8); err != nil {
		t.Fatal(err)
	}
	if firing := e2.Alerts("p"); findAlert(firing, model.RuleFailures) != nil {
		t.Fatalf("alert should be closed after rule disabled: %+v", firing)
	}
	hist := e2.AlertHistory("p")
	closed := findAlert(hist, model.RuleFailures)
	if closed == nil || closed.State != model.AlertRecovered || closed.IncidentSeq != 1 {
		t.Fatalf("want one closed incident #1, got %+v", hist)
	}
	// No duplicate closed incidents for the same fingerprint.
	count := 0
	for _, x := range hist {
		if x.Rule == model.RuleFailures {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 historical failures alert, got %d", count)
	}

	// Restart once more; history remains exactly one closed conclusion and
	// nothing resurrects.
	if err := e2.Close(); err != nil {
		t.Fatal(err)
	}
	e3, err := New(Options{DataDir: dir, Clock: fc, Config: cfg8})
	if err != nil {
		t.Fatal(err)
	}
	defer e3.Close()
	if len(e3.Alerts("p")) != 0 {
		t.Fatalf("nothing should fire after final restart, got %+v", e3.Alerts("p"))
	}
	if hist3 := e3.AlertHistory("p"); len(hist3) != 1 || hist3[0].ConfigVersion != 7 {
		t.Fatalf("history corrupted on restart: %+v", hist3)
	}
}
