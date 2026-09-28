package engine

import (
	"path/filepath"
	"testing"

	"github.com/lechandonga/pipeline-observability-engine-go/clock"
	"github.com/lechandonga/pipeline-observability-engine-go/model"
)

func TestAlertsSurviveSnapshotThenRestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "poe")
	cfg := alertCfg(4)
	fc := &clock.FakeClock{T: 600_000}

	e, err := New(Options{DataDir: dir, Clock: fc, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	// Fire then recover a latency alert -> one closed incident in history.
	e.IngestMetric(model.Metric{ID: "l-hot", Pipeline: "p", Name: model.MetricLatencyMS, Value: 900, Timestamp: 600_000})
	e.IngestMetric(model.Metric{ID: "l-ok", Pipeline: "p", Name: model.MetricLatencyMS, Value: 5, Timestamp: 600_100})
	// Keep a failures alert firing (cumulative failures = 4 -> critical).
	e.IngestMetric(model.Metric{ID: "f1", Pipeline: "p", Name: model.MetricFailures, Value: 4, Timestamp: 600_000})

	if len(e.Alerts("p")) != 1 || e.Alerts("p")[0].Rule != model.RuleFailures {
		t.Fatalf("setup: want 1 firing failures alert, got %+v", e.Alerts("p"))
	}
	if len(e.AlertHistory("p")) != 1 {
		t.Fatalf("setup: want 1 closed latency incident, got %+v", e.AlertHistory("p"))
	}
	if err := e.Snapshot(); err != nil { // compacts WAL; state now snapshot-only
		t.Fatal(err)
	}
	// A fact after the snapshot lives in the WAL tail and must also survive.
	r := e.IngestMetric(model.Metric{ID: "tail", Pipeline: "p", Name: model.MetricDataArrived, Value: 1, Timestamp: 600_200})
	if r.Outcome != model.OutcomeAccepted {
		t.Fatal(r)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	e2, err := New(Options{DataDir: dir, Clock: fc, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()

	firing := e2.Alerts("p")
	if len(firing) != 1 || firing[0].Rule != model.RuleFailures ||
		firing[0].State != model.AlertFiring || firing[0].ConfigVersion != 4 {
		t.Fatalf("firing alert lost via snapshot path: %+v", firing)
	}
	hist := e2.AlertHistory("p")
	if len(hist) != 1 || hist[0].Rule != model.RuleLatency || hist[0].State != model.AlertRecovered {
		t.Fatalf("closed alert history lost via snapshot path: %+v", hist)
	}
	// WAL-tail fact survived too.
	if rr := e2.IngestMetric(model.Metric{ID: "tail", Pipeline: "p", Name: model.MetricDataArrived, Value: 1, Timestamp: 600_200}); rr.Reason != model.ReasonDuplicate {
		t.Fatalf("post-snapshot WAL tail fact lost: %+v", rr)
	}
	t.Logf("snapshot recovery OK: firing=%s(incident %d), history=%s recoveredAt=%d v%d",
		firing[0].Rule, firing[0].IncidentSeq, hist[0].Rule, hist[0].RecoveredAt, hist[0].ConfigVersion)
}
