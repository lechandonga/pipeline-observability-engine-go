// Command poe is a tiny local smoke program for the pipeline observability
// engine. It runs a deterministic script (fake clock) that exercises out of
// order delivery, duplicates, late rejection, alert fire/recover and a
// durability snapshot, printing every decision with its reason.
package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/lechandonga/pipeline-observability-engine-go/clock"
	"github.com/lechandonga/pipeline-observability-engine-go/config"
	"github.com/lechandonga/pipeline-observability-engine-go/engine"
	"github.com/lechandonga/pipeline-observability-engine-go/model"
)

func main() {
	dir := filepath.Join(os.TempDir(), "poe-demo-data")
	// The demo is a deterministic script; start from a clean dir so repeated
	// runs print the same decisions (real applications keep the directory).
	if err := os.RemoveAll(dir); err != nil {
		fmt.Fprintln(os.Stderr, "cleanup error:", err)
		os.Exit(1)
	}
	fc := &clock.FakeClock{T: 1_000_000}
	cfg := config.Default()
	cfg.LateWindowMS = 10_000
	cfg.FutureSkewMS = 1_000
	cfg.FreshnessStaleMS = 2_000
	cfg.LatencyMS = config.Threshold{Enabled: true, WarnAt: 100, CriticalAt: 500}
	cfg.SLABreachMS = 0

	e, err := engine.New(engine.Options{DataDir: dir, Clock: fc, Config: cfg})
	if err != nil {
		fmt.Fprintln(os.Stderr, "engine error:", err)
		os.Exit(1)
	}
	defer e.Close()

	ev := func(id, run string, t model.EventType, ts int64) {
		r := e.IngestEvent(model.Event{ID: id, Pipeline: "orders", RunID: run, Type: t, Timestamp: ts})
		fmt.Printf("event  %-4s %-16s ts=%d -> %-8s %-15s %s\n", id, t, ts, r.Outcome, r.Reason, r.Message)
	}
	mt := func(id string, name model.MetricName, v float64, ts int64) {
		r := e.IngestMetric(model.Metric{ID: id, Pipeline: "orders", Name: name, Value: v, Timestamp: ts})
		fmt.Printf("metric %-4s %-16s v=%-4g ts=%d -> %-8s %-15s %s\n", id, name, v, ts, r.Outcome, r.Reason, r.Message)
	}

	// In-order start, then an out-of-order-but-in-window success.
	ev("e1", "run-1", model.EventRunStarted, 1_000_000)
	fc.Advance(3_000)
	ev("e2", "run-1", model.EventRunSucceeded, 1_000_500)

	// Duplicate resend and a tampered copy sharing the id: both rejected once.
	ev("e2", "run-1", model.EventRunSucceeded, 1_000_500)
	ev("e2", "run-1", model.EventRunSucceeded, 1_000_600)

	// A slow latency point fires a warn alert.
	mt("m1", model.MetricLatencyMS, 250, fc.T)

	// Healthy latency recovers it.
	mt("m2", model.MetricLatencyMS, 40, fc.T)

	// Late event beyond the 10s window: rejected without state change.
	fc.Advance(20_000)
	ev("late", "run-2", model.EventRunStarted, 1_000_000)

	// Freshness: silence then new data.
	fc.T = 1_050_000
	mt("fresh", model.MetricDataArrived, 1, fc.T)
	fc.Advance(3_000)
	e.Reevaluate()
	printAlerts := func(tag string) {
		for _, a := range e.Alerts("orders") {
			fmt.Printf("%s: firing rule=%s severity=%s\n", tag, a.Rule, a.Severity)
		}
	}
	printAlerts("after silence")
	mt("fresh2", model.MetricDataArrived, 1, fc.T)
	fmt.Printf("after new data: %d firing\n", len(e.Alerts("orders")))

	if err := e.Snapshot(); err != nil {
		fmt.Fprintln(os.Stderr, "snapshot error:", err)
		os.Exit(1)
	}
	if st, ok := e.GetPipeline("orders"); ok {
		fmt.Printf("pipeline: watermark=%d lastSuccess=%d failures=%d\n",
			st.Watermark, st.LastSuccess, st.FailuresTotal)
	}
	fmt.Printf("history: %d closed alert incident(s); data dir=%s\n",
		len(e.AlertHistory("orders")), dir)
}
