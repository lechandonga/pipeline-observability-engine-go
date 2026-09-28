package engine

import (
	"path/filepath"
	"testing"

	"github.com/lechandonga/pipeline-observability-engine-go/clock"
	"github.com/lechandonga/pipeline-observability-engine-go/config"
	"github.com/lechandonga/pipeline-observability-engine-go/model"
)

func recoveryCfg() *config.Config {
	cfg := config.Default()
	cfg.LateWindowMS = 3_600_000
	cfg.RetentionMS = 3_600_000
	cfg.FreshnessStaleMS = 0
	cfg.LatencyMS = config.Threshold{Enabled: false}
	cfg.ThroughputMinRPS = config.Threshold{Enabled: false}
	cfg.FailuresTotal = config.Threshold{Enabled: false}
	cfg.RetriesTotal = config.Threshold{Enabled: false}
	cfg.SLABreachMS = 0
	return cfg
}

func TestRestartRecoveryFactsAndDedup(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "poe")
	fc := &clock.FakeClock{T: 1_000_000}

	build := func() *Engine {
		e, err := New(Options{DataDir: dir, Clock: fc, Config: recoveryCfg()})
		if err != nil {
			t.Fatalf("new engine: %v", err)
		}
		return e
	}

	e := build()
	events := []model.Event{
		{ID: "a", Pipeline: "p", RunID: "r", Type: model.EventRunStarted, Timestamp: 1_000_000},
		{ID: "b", Pipeline: "p", RunID: "r", Type: model.EventRetryScheduled, Timestamp: 1_000_100},
		{ID: "c", Pipeline: "p", RunID: "r", Type: model.EventRetrySucceeded, Timestamp: 1_000_200},
	}
	for _, ev := range events {
		r := e.IngestEvent(ev)
		t.Logf("BEFORE-RESTART input=%s ts=%d -> %s/%s", ev.ID, ev.Timestamp, r.Outcome, r.Reason)
		if r.Outcome != model.OutcomeAccepted {
			t.Fatalf("accept %s: %+v", ev.ID, r)
		}
	}
	mr := e.IngestMetric(model.Metric{ID: "m1", Pipeline: "p", Name: model.MetricRecords, Value: 42, Timestamp: 1_000_150})
	t.Logf("BEFORE-RESTART metric=m1 -> %s/%s", mr.Outcome, mr.Reason)
	if mr.Outcome != model.OutcomeAccepted {
		t.Fatal(mr)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	// Restart: everything confirmed must be back.
	e2 := build()
	defer e2.Close()

	run, ok := e2.GetRun("p", "r")
	if !ok {
		t.Fatal("run lost on restart")
	}
	if run.Status != model.RunSucceeded || run.Retries != 1 ||
		run.StartedAt != 1_000_000 || run.EndedAt != 1_000_200 || run.Watermark != 1_000_200 {
		t.Fatalf("recovered run differs: %+v", run)
	}
	ps, ok := e2.GetPipeline("p")
	if !ok || ps.Watermark != 1_000_200 {
		t.Fatalf("recovered pipeline differs: %+v ok=%v", ps, ok)
	}

	// Duplicates are still rejected after restart: the dedup set is durable.
	for _, ev := range events {
		r := e2.IngestEvent(ev)
		t.Logf("AFTER-RESTART resend=%s -> %s/%s", ev.ID, r.Outcome, r.Reason)
		if r.Outcome != model.OutcomeRejected || r.Reason != model.ReasonDuplicate {
			t.Fatalf("resend %s must be duplicate, got %+v", ev.ID, r)
		}
	}
	if r := e2.IngestMetric(model.Metric{ID: "m1", Pipeline: "p", Name: model.MetricRecords, Value: 42, Timestamp: 1_000_150}); r.Reason != model.ReasonDuplicate {
		t.Fatalf("metric resend must be duplicate, got %+v", r)
	}

	// Sequence must continue strictly after the pre-restart value.
	r := e2.IngestEvent(model.Event{ID: "new", Pipeline: "p2", RunID: "q", Type: model.EventRunStarted, Timestamp: 1_000_300})
	if r.Outcome != model.OutcomeAccepted || r.Sequence <= mr.Sequence {
		t.Fatalf("sequence did not continue across restart: %+v", r)
	}
	t.Logf("post-restart new seq=%d (previous max metric seq=%d)", r.Sequence, mr.Sequence)
}

func TestRecoveryAfterSnapshotCompaction(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "poe")
	fc := &clock.FakeClock{T: 2_000_000}
	cfg := recoveryCfg()

	e, err := New(Options{DataDir: dir, Clock: fc, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	e.IngestEvent(model.Event{ID: "1", Pipeline: "p", RunID: "r", Type: model.EventRunStarted, Timestamp: 2_000_000})
	e.IngestEvent(model.Event{ID: "2", Pipeline: "p", RunID: "r", Type: model.EventRunSucceeded, Timestamp: 2_000_100})
	if err := e.Snapshot(); err != nil {
		t.Fatal(err)
	}
	// New fact after compaction lives only in the (rewritten) WAL tail.
	r := e.IngestMetric(model.Metric{ID: "3", Pipeline: "p", Name: model.MetricRecords, Value: 7, Timestamp: 2_000_200})
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
	run, ok := e2.GetRun("p", "r")
	if !ok || run.Status != model.RunSucceeded || run.EndedAt != 2_000_100 {
		t.Fatalf("snapshot state lost: %+v", run)
	}
	// Post-snapshot fact survives via WAL tail replay.
	if rr := e2.IngestMetric(model.Metric{ID: "3", Pipeline: "p", Name: model.MetricRecords, Value: 7, Timestamp: 2_000_200}); rr.Reason != model.ReasonDuplicate {
		t.Fatalf("wal-tail fact lost: %+v", rr)
	}
}
