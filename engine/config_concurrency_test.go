package engine

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/lechandonga/pipeline-observability-engine-go/clock"
	"github.com/lechandonga/pipeline-observability-engine-go/config"
	"github.com/lechandonga/pipeline-observability-engine-go/model"
)

func TestConfigChangeKeepsHistory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "poe")
	fc := &clock.FakeClock{T: 10_000}
	cfg := alertCfg(1)
	e, err := New(Options{DataDir: dir, Clock: fc, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	// Fire latency under v1, then recover under v1 -> history pinned v1.
	e.IngestMetric(model.Metric{ID: "l1", Pipeline: "p", Name: model.MetricLatencyMS, Value: 200, Timestamp: 10_000})
	if a := findAlert(e.Alerts("p"), model.RuleLatency); a == nil || a.ConfigVersion != 1 {
		t.Fatalf("want v1 firing: %+v", e.Alerts("p"))
	}

	// Move to v2 with much higher thresholds: the same value is healthy and
	// the open alert recovers under v2 semantics but incident history stays.
	cfg2 := alertCfg(2)
	cfg2.LatencyMS = config.Threshold{Enabled: true, WarnAt: 10_000, CriticalAt: 50_000}
	if err := e.UpdateConfig(cfg2); err != nil {
		t.Fatal(err)
	}
	if len(e.Alerts("p")) != 0 {
		t.Fatalf("v2 should recover alert, got %+v", e.Alerts("p"))
	}
	hist := e.AlertHistory("p")
	a := findAlert(hist, model.RuleLatency)
	if a == nil || a.ConfigVersion != 1 {
		t.Fatalf("history must keep v1: %+v", hist)
	}

	// Older configs are rejected.
	old := alertCfg(1)
	if err := e.UpdateConfig(old); err == nil {
		t.Fatal("older config version must be rejected")
	}

	// A new incident under v2 carries v2.
	fc.Advance(20_000)
	e.IngestMetric(model.Metric{ID: "l2", Pipeline: "p", Name: model.MetricLatencyMS, Value: 20_000, Timestamp: fc.T})
	if a := findAlert(e.Alerts("p"), model.RuleLatency); a == nil || a.ConfigVersion != 2 {
		t.Fatalf("new incident should be v2: %+v", e.Alerts("p"))
	}
}

func TestThroughputRule(t *testing.T) {
	e, fc := newAlertEngine(t, 30_000, func() *config.Config {
		c := alertCfg(1)
		c.ThroughputMinRPS = config.Threshold{Enabled: true, WarnAt: 2, CriticalAt: 0.5}
		c.ThroughputWindowMS = 10_000
		return c
	}())
	// 30 records over 10s = 3 rps: healthy.
	e.IngestMetric(model.Metric{ID: "t1", Pipeline: "p", Name: model.MetricRecords, Value: 30, Timestamp: 30_000})
	if a := findAlert(e.Alerts("p"), model.RuleThroughput); a != nil {
		t.Fatalf("3 rps should be healthy, got %+v", a)
	}
	// Time passes so the window no longer contains the old bulk; add 1
	// record over 10s = 0.1 rps: critical.
	fc.Advance(10_001)
	e.IngestMetric(model.Metric{ID: "t2", Pipeline: "p", Name: model.MetricRecords, Value: 1, Timestamp: fc.T})
	if a := findAlert(e.Alerts("p"), model.RuleThroughput); a == nil || a.Severity != model.SeverityCritical {
		t.Fatalf("want critical throughput: %+v", e.Alerts("p"))
	}
}

func TestConcurrentWritersNoDoubleCountAndReadersConsistent(t *testing.T) {
	fc := &clock.FakeClock{T: 1_000_000}
	cfg := alertCfg(1)
	cfg.FreshnessStaleMS = 0
	cfg.LatencyMS.Enabled = false
	cfg.RetriesTotal.Enabled = false
	cfg.SLABreachMS = 0
	e, err := New(Options{DataDir: filepath.Join(t.TempDir(), "poe"), Clock: fc, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	const writers = 8
	const perWriter = 100
	var wg sync.WaitGroup
	// Each writer has UNIQUE ids: every metric must count exactly once.
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				mt := model.Metric{
					ID:        fmt.Sprintf("w%d-n%d", w, i),
					Pipeline:  "p",
					Name:      model.MetricFailures,
					Value:     1,
					Timestamp: 1_000_000,
				}
				r := e.IngestMetric(mt)
				if r.Outcome != model.OutcomeAccepted {
					t.Errorf("unique metric rejected: %+v", r)
					return
				}
			}
		}(w)
	}

	// Concurrent readers must only ever observe consistent snapshots.
	stop := make(chan struct{})
	var readers sync.WaitGroup
	for r := 0; r < 4; r++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				st, ok := e.GetPipeline("p")
				if ok {
					sum := 0
					for _, a := range st.ActiveAlerts {
						if a.Pipeline != "p" || a.State != model.AlertFiring {
							t.Errorf("torn/illegal alert in snapshot: %+v", a)
							return
						}
						sum++
					}
					if st.FailuresTotal < 0 || st.FailuresTotal > writers*perWriter {
						t.Errorf("impossible failure total %d", st.FailuresTotal)
						return
					}
				}
			}
		}()
	}

	wg.Wait()
	close(stop)
	readers.Wait()

	ps, _ := e.GetPipeline("p")
	if ps.FailuresTotal != writers*perWriter {
		t.Fatalf("want exactly %d failures, got %d (double count or loss)", writers*perWriter, ps.FailuresTotal)
	}
	// Failures threshold (warn 1 / critical 3) yields exactly ONE alert.
	if got := e.Alerts("p"); len(got) != 1 || got[0].Rule != model.RuleFailures {
		t.Fatalf("want exactly 1 de-duplicated failures alert, got %+v", got)
	}
}

func TestConcurrentDuplicateResends(t *testing.T) {
	fc := &clock.FakeClock{T: 2_000_000}
	cfg := alertCfg(1)
	e, err := New(Options{DataDir: filepath.Join(t.TempDir(), "poe"), Clock: fc, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	// Same set of IDs hammered concurrently: each accepted exactly once.
	const n = 50
	const resend = 10
	var wg sync.WaitGroup
	for k := 0; k < resend; k++ {
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				ev := model.Event{ID: fmt.Sprintf("e%d", i), Pipeline: "p", RunID: fmt.Sprintf("r%d", i),
					Type: model.EventRunStarted, Timestamp: 2_000_000}
				r := e.IngestEvent(ev)
				if r.Outcome != model.OutcomeAccepted && r.Reason != model.ReasonDuplicate {
					t.Errorf("unexpected result for %d: %+v", i, r)
				}
			}(i)
		}
	}
	wg.Wait()
	// Exactly n runs must exist; no run double-started (which would conflict).
	for i := 0; i < n; i++ {
		run, ok := e.GetRun("p", fmt.Sprintf("r%d", i))
		if !ok || run.Status != model.RunActive || run.StartedAt != 2_000_000 {
			t.Fatalf("run r%d bad state: %+v ok=%v", i, run, ok)
		}
	}
}
