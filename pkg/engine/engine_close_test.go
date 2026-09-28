package engine

import (
	"testing"

	"github.com/lechandonga/pipeline-observability-engine-go/pkg/model"
)

func TestReportAfterCloseRejected(t *testing.T) {
	e, clk := newTestEngine(t, "")
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	r := e.ReportEvent(ev("x", "r1", model.EventStarted, clk.Now()))
	if r.Accepted || r.Reason == "" {
		t.Fatalf("report after close must be rejected, got %+v", r)
	}
	// Queries remain safe after close (read-only snapshot).
	if q := e.Query(); q.GeneratedAt.IsZero() {
		t.Fatal("query after close must still return a view")
	}
}
