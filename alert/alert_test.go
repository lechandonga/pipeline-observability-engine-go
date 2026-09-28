package alert

import (
	"testing"

	"github.com/lechandonga/pipeline-observability-engine-go/config"
	"github.com/lechandonga/pipeline-observability-engine-go/model"
)

func testCfg(v int) *config.Config {
	c := config.Default()
	c.Version = v
	c.FreshnessStaleMS = 1000
	c.LatencyMS = config.Threshold{Enabled: true, WarnAt: 100, CriticalAt: 500}
	c.ThroughputMinRPS = config.Threshold{Enabled: false}
	c.FailuresTotal = config.Threshold{Enabled: true, WarnAt: 1, CriticalAt: 3}
	c.RetriesTotal = config.Threshold{Enabled: false}
	c.SLABreachMS = 0
	return c
}

func TestFireDedupeRecover(t *testing.T) {
	m := NewManager(testCfg(7))
	seq := int64(0)
	next := func() int64 { seq++; return seq }

	// First breach fires exactly one freshness alert carrying cfg v7.
	ch := m.Apply(EvaluateRequest{
		Pipeline: "p", Now: 10000, HasWatermark: true, Watermark: 8000,
	}, next)
	if len(ch) != 1 || ch[0].Rule != model.RuleFreshness || ch[0].State != model.AlertFiring {
		t.Fatalf("want 1 firing freshness, got %+v", ch)
	}
	if ch[0].ConfigVersion != 7 {
		t.Fatalf("want config v7, got %d", ch[0].ConfigVersion)
	}
	incident := ch[0].IncidentSeq

	// Continued breach: no new alert (de-dup), no changes returned.
	ch = m.Apply(EvaluateRequest{
		Pipeline: "p", Now: 11000, HasWatermark: true, Watermark: 8000,
	}, next)
	if len(ch) != 0 {
		t.Fatalf("want no change on continued breach, got %+v", ch)
	}
	if len(m.ActiveFor("p")) != 1 {
		t.Fatalf("want exactly 1 active alert")
	}

	// Recovery closes the incident but keeps history.
	ch = m.Apply(EvaluateRequest{
		Pipeline: "p", Now: 12000, HasWatermark: true, Watermark: 11500,
	}, next)
	if len(ch) != 1 || ch[0].State != model.AlertRecovered || ch[0].RecoveredAt != 12000 {
		t.Fatalf("want recovery, got %+v", ch)
	}
	if len(m.ActiveFor("p")) != 0 || len(m.HistoryFor("p")) != 1 {
		t.Fatalf("want 0 active / 1 history")
	}

	// A later breach starts a NEW incident (new fire), old history intact.
	ch = m.Apply(EvaluateRequest{
		Pipeline: "p", Now: 20000, HasWatermark: true, Watermark: 18000,
	}, next)
	if len(ch) != 1 || ch[0].IncidentSeq != incident+1 {
		t.Fatalf("want new incident %d, got %+v", incident+1, ch)
	}
	if len(m.HistoryFor("p")) != 1 {
		t.Fatalf("old conclusion must remain in history")
	}
}

func TestSeverityEscalationAndConfigFreeze(t *testing.T) {
	cfg := testCfg(1)
	m := NewManager(cfg)
	seq := int64(0)
	next := func() int64 { seq++; return seq }

	ch := m.Apply(EvaluateRequest{Pipeline: "p", Now: 1, HasLatency: true, LatestLatency: 200}, next)
	if len(ch) != 1 || ch[0].Severity != model.SeverityWarn || ch[0].ConfigVersion != 1 {
		t.Fatalf("want warn at v1, got %+v", ch)
	}

	// Escalation produces an updated alert but same incident.
	ch = m.Apply(EvaluateRequest{Pipeline: "p", Now: 2, HasLatency: true, LatestLatency: 600}, next)
	if len(ch) != 1 || ch[0].Severity != model.SeverityCritical || ch[0].IncidentSeq != 1 {
		t.Fatalf("want critical escalation same incident, got %+v", ch)
	}

	// Config changes to v2. A firing alert keeps the version it fired under
	// until it recovers; lastValue drift at same severity is not durable.
	m.SetConfig(testCfg(2))
	ch = m.Apply(EvaluateRequest{Pipeline: "p", Now: 3, HasLatency: true, LatestLatency: 250}, next)
	if len(ch) != 1 || ch[0].ConfigVersion != 1 || ch[0].Severity != model.SeverityWarn {
		t.Fatalf("want de-escalated alert still pinned to v1, got %+v", ch)
	}
	ch = m.Apply(EvaluateRequest{Pipeline: "p", Now: 4, HasLatency: true, LatestLatency: 10}, next)
	if len(ch) != 1 || ch[0].State != model.AlertRecovered {
		t.Fatalf("want recovery, got %+v", ch)
	}
	if m.HistoryFor("p")[0].ConfigVersion != 1 {
		t.Fatalf("historical conclusion must keep v1")
	}

	// A fresh incident after config change is v2.
	ch = m.Apply(EvaluateRequest{Pipeline: "p", Now: 5, HasLatency: true, LatestLatency: 200}, next)
	if len(ch) != 1 || ch[0].ConfigVersion != 2 {
		t.Fatalf("want v2 incident, got %+v", ch)
	}
}

func TestDisabledRuleRecovers(t *testing.T) {
	cfg := testCfg(1)
	m := NewManager(cfg)
	seq2 := int64(0)
	next := func() int64 { seq2++; return seq2 }
	ch := m.Apply(EvaluateRequest{Pipeline: "p", Now: 1, HasLatency: true, LatestLatency: 200}, next)
	if len(ch) != 1 {
		t.Fatalf("want fire, got %+v", ch)
	}
	cfg2 := testCfg(2)
	cfg2.LatencyMS.Enabled = false
	m.SetConfig(cfg2)
	ch = m.Apply(EvaluateRequest{Pipeline: "p", Now: 2, HasLatency: true, LatestLatency: 200}, next)
	if len(ch) != 1 || ch[0].State != model.AlertRecovered {
		t.Fatalf("disabling rule must close alert, got %+v", ch)
	}
}
