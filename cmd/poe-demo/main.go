// Command poe-demo exercises the pipeline observability engine end to end on
// the local machine with no external dependencies: it reports out-of-order and
// duplicate events, shows distinguishable rejections, fires/dedups/recovers an
// alert, then closes and reopens the engine to demonstrate restart recovery.
//
// Run:
//
//	go run ./cmd/poe-demo -dir ./demo-data
//
// Inspect the resulting WAL file in -dir and re-run to see recovery.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"path/filepath"
	"time"

	"github.com/lechandonga/pipeline-observability-engine-go/pkg/alert"
	"github.com/lechandonga/pipeline-observability-engine-go/pkg/clock"
	"github.com/lechandonga/pipeline-observability-engine-go/pkg/engine"
	"github.com/lechandonga/pipeline-observability-engine-go/pkg/model"
)

func main() {
	dir := flag.String("dir", "./demo-data", "directory for the WAL file")
	flag.Parse()

	cfg := engine.DefaultConfig(filepath.Join(*dir, "wal"))
	cfg.LateTolerance = 30 * time.Second
	cfg.FutureTolerance = 5 * time.Second
	// Compaction only at Close for a readable on-disk trail; production-like
	// periodic compaction is enabled by default (CompactionThreshold).

	clk := clock.NewSystemClock()
	e, err := engine.New(cfg, clk)
	must(err)

	_, err = e.UpdateRules([]alert.ThresholdRule{{
		ID: "latency-high", Pipeline: "orders",
		Metric: model.MetricLatency, Above: true,
		Threshold: 500, RecoverBelow: 300,
		RecoveryWindow: 2 * time.Minute, CloseAfter: time.Minute,
	}})
	must(err)

	now := clk.Now()
	report := func(tag string, r model.IngestResult) {
		if r.Accepted {
			fmt.Printf("[%-22s] accepted seq=%d\n", tag, r.Seq)
		} else {
			fmt.Printf("[%-22s] REJECTED reason=%s detail=%s\n", tag, r.Reason, r.Detail)
		}
	}

	// 1. Ordered lifecycle.
	report("started", e.ReportEvent(model.Event{
		ID: "evt-1", Pipeline: "orders", RunID: "run-1",
		Type: model.EventStarted, Timestamp: now,
	}))
	// 2. Out-of-order (failure a few seconds later, reported after a retry).
	report("retried", e.ReportEvent(model.Event{
		ID: "evt-2", Pipeline: "orders", RunID: "run-1",
		Type: model.EventRetried, Timestamp: now.Add(2 * time.Second),
	}))
	report("failed", e.ReportEvent(model.Event{
		ID: "evt-3", Pipeline: "orders", RunID: "run-1",
		Type: model.EventFailed, Timestamp: now.Add(1 * time.Second),
	}))
	// 3. Exact duplicate of evt-2: applies once.
	report("duplicate-retry", e.ReportEvent(model.Event{
		ID: "evt-2", Pipeline: "orders", RunID: "run-1",
		Type: model.EventRetried, Timestamp: now.Add(2 * time.Second),
	}))
	// 4. Invalid payload.
	report("invalid", e.ReportEvent(model.Event{
		Pipeline: "orders", RunID: "run-1", Type: "weird", Timestamp: now,
	}))
	// 5. State conflict: success before start for an unknown run.
	report("state-conflict", e.ReportEvent(model.Event{
		ID: "evt-x", Pipeline: "orders", RunID: "ghost",
		Type: model.EventSucceeded, Timestamp: now,
	}))
	// 6. Bad latency fires the alert; duplicates dedup.
	report("latency-900", e.ReportMetric(model.Metric{
		ID: "m-1", Pipeline: "orders", Kind: model.MetricLatency,
		Value: 900, Timestamp: now.Add(3 * time.Second),
	}))
	report("latency-950", e.ReportMetric(model.Metric{
		ID: "m-2", Pipeline: "orders", Kind: model.MetricLatency,
		Value: 950, Timestamp: now.Add(4 * time.Second),
	}))
	// 7. Late event beyond the 30s window: rejected, state untouched.
	report("late-event", e.ReportEvent(model.Event{
		ID: "evt-late", Pipeline: "orders", RunID: "ancient",
		Type: model.EventStarted, Timestamp: now.Add(-10 * time.Minute),
	}))

	printView("state after reports", e.Query())

	// 8. Recovery below hysteresis.
	report("latency-250", e.ReportMetric(model.Metric{
		ID: "m-3", Pipeline: "orders", Kind: model.MetricLatency,
		Value: 250, Timestamp: now.Add(5 * time.Second),
	}))
	printView("state after recovery", e.Query())

	// Close: fsync + checkpoint compaction.
	must(e.Close())
	fmt.Println("engine closed (WAL fsynced and checkpointed)")

	// 9. Restart recovery.
	e2, err := engine.New(cfg, clk)
	must(err)
	defer e2.Close()
	report("duplicate-after-restart", e2.ReportEvent(model.Event{
		ID: "evt-1", Pipeline: "orders", RunID: "run-1",
		Type: model.EventStarted, Timestamp: now,
	}))
	printView("state after restart", e2.Query())
}

func printView(tag string, v engine.View) {
	b, _ := json.MarshalIndent(v, "", "  ")
	fmt.Printf("--- %s ---\n%s\n\n", tag, b)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
