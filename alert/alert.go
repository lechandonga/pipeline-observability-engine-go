// Package alert evaluates versioned rules and manages the alert lifecycle
// (fire -> escalate -> recover -> close) with fingerprint based
// de-duplication. Historical conclusions are kept and always remember the
// config version they were made under.
package alert

import (
	"sort"
	"sync"

	"github.com/lechandonga/pipeline-observability-engine-go/config"
	"github.com/lechandonga/pipeline-observability-engine-go/model"
)

// maxHistoryPerKey bounds recovered history so memory/query time do not grow
// without limit; older closed incidents are pruned (firing ones never are).
const maxHistoryPerKey = 50

// Manager owns alert conclusions. It has its own lock because evaluation is
// invoked from the engine's single critical section while read APIs may also
// be called directly in tests.
type Manager struct {
	mu sync.RWMutex

	cfg *config.Config

	// active maps pipeline|rule -> currently firing alert.
	active map[string]*model.Alert
	// history maps pipeline|rule -> closed incidents, newest last.
	history map[string][]model.Alert
	// incidentSeq maps pipeline|rule -> number of episodes seen.
	incidentSeq map[string]int
	// lastSeq is the highest sequence assigned by the engine.
	lastSeq int64
}

// NewManager builds an empty Manager.
func NewManager(cfg *config.Config) *Manager {
	if cfg == nil {
		cfg = config.Default()
	}
	return &Manager{
		cfg:         cfg,
		active:      map[string]*model.Alert{},
		history:     map[string][]model.Alert{},
		incidentSeq: map[string]int{},
	}
}

// SetConfig swaps the rule set. The manager does not immediately recover
// alerts; the engine drives evaluation under the new config.
func (m *Manager) SetConfig(cfg *config.Config) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if cfg != nil {
		m.cfg = cfg
	}
}

// ConfigVersion returns the version of the active rule set.
func (m *Manager) ConfigVersion() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.cfg.Version
}

// Fingerprint computes the stable identity of an alert.
func Fingerprint(pipeline, rule string) string {
	return pipeline + "|" + rule
}

// breach describes a rule evaluation result.
type breach struct {
	rule      string
	breached  bool
	severity  model.AlertSeverity
	value     float64
	threshold float64
	message   string
	enabled   bool
}

// EvaluateRequest is the evaluation input assembled by the engine.
type EvaluateRequest struct {
	Pipeline      string
	Now           int64
	HasWatermark  bool
	Watermark     int64
	LatestLatency float64
	HasLatency    bool
	RPS           float64
	FailuresTotal int
	RetriesTotal  int
	LongestRunMS  int64
	HasActiveRuns bool
}

func classifyThreshold(v, warn, crit float64) (model.AlertSeverity, bool) {
	// crit may be zero meaning unset; when set, crit >= warn by convention.
	if crit > 0 && v >= crit {
		return model.SeverityCritical, true
	}
	if warn > 0 && v >= warn {
		return model.SeverityWarn, true
	}
	return "", false
}

func (m *Manager) evaluate(req EvaluateRequest) []breach {
	cfg := m.cfg // read under engine lock in practice; see Apply
	rs := make([]breach, 0, 6)

	// Freshness: stale watermark relative to now. Only meaningful once the
	// pipeline has reported anything (HasWatermark).
	fresh := breach{rule: model.RuleFreshness, enabled: cfg.FreshnessStaleMS > 0}
	if fresh.enabled && req.HasWatermark {
		age := float64(req.Now - req.Watermark)
		if age >= float64(cfg.FreshnessStaleMS) {
			fresh.breached = true
			fresh.severity = model.SeverityCritical
			fresh.value = age
			fresh.threshold = float64(cfg.FreshnessStaleMS)
			fresh.message = "no data within freshness window"
		}
	}
	rs = append(rs, fresh)

	// Latency: most recent accepted latency point.
	lat := breach{rule: model.RuleLatency, enabled: cfg.LatencyMS.Enabled}
	if lat.enabled && req.HasLatency {
		if sev, bad := classifyThreshold(req.LatestLatency, cfg.LatencyMS.WarnAt, cfg.LatencyMS.CriticalAt); bad {
			lat.breached, lat.severity = true, sev
			lat.value = req.LatestLatency
			if sev == model.SeverityCritical {
				lat.threshold = cfg.LatencyMS.CriticalAt
			} else {
				lat.threshold = cfg.LatencyMS.WarnAt
			}
			lat.message = "latency above threshold"
		}
	}
	rs = append(rs, lat)

	// Throughput: records per second over the configured window.
	tp := breach{rule: model.RuleThroughput, enabled: cfg.ThroughputMinRPS.Enabled && cfg.ThroughputWindowMS > 0}
	if tp.enabled {
		if sev, bad := classifyBelow(req.RPS, cfg.ThroughputMinRPS.WarnAt, cfg.ThroughputMinRPS.CriticalAt); bad {
			tp.breached, tp.severity = true, sev
			tp.value = req.RPS
			if sev == model.SeverityCritical {
				tp.threshold = cfg.ThroughputMinRPS.CriticalAt
			} else {
				tp.threshold = cfg.ThroughputMinRPS.WarnAt
			}
			tp.message = "throughput below minimum"
		}
	}
	rs = append(rs, tp)

	// Failures: cumulative accepted failure count for the pipeline.
	fail := breach{rule: model.RuleFailures, enabled: cfg.FailuresTotal.Enabled}
	if fail.enabled {
		v := float64(req.FailuresTotal)
		if sev, bad := classifyThreshold(v, cfg.FailuresTotal.WarnAt, cfg.FailuresTotal.CriticalAt); bad {
			fail.breached, fail.severity = true, sev
			fail.value = v
			if sev == model.SeverityCritical {
				fail.threshold = cfg.FailuresTotal.CriticalAt
			} else {
				fail.threshold = cfg.FailuresTotal.WarnAt
			}
			fail.message = "failure count above threshold"
		}
	}
	rs = append(rs, fail)

	// Retries: cumulative accepted retry count for the pipeline.
	retry := breach{rule: model.RuleRetries, enabled: cfg.RetriesTotal.Enabled}
	if retry.enabled {
		v := float64(req.RetriesTotal)
		if sev, bad := classifyThreshold(v, cfg.RetriesTotal.WarnAt, cfg.RetriesTotal.CriticalAt); bad {
			retry.breached, retry.severity = true, sev
			retry.value = v
			if sev == model.SeverityCritical {
				retry.threshold = cfg.RetriesTotal.CriticalAt
			} else {
				retry.threshold = cfg.RetriesTotal.WarnAt
			}
			retry.message = "retry count above threshold"
		}
	}
	rs = append(rs, retry)

	// SLA: any active run running longer than the SLA budget.
	sla := breach{rule: model.RuleSLA, enabled: cfg.SLABreachMS > 0 && req.HasActiveRuns}
	if sla.enabled && req.LongestRunMS >= cfg.SLABreachMS {
		sla.breached = true
		sla.severity = model.SeverityCritical
		sla.value = float64(req.LongestRunMS)
		sla.threshold = float64(cfg.SLABreachMS)
		sla.message = "run exceeds SLA budget"
	}
	rs = append(rs, sla)

	return rs
}

// classifyBelow is the inverse-direction variant of classifyThreshold:
// values at or below warn are a warn breach, at or below crit critical.
func classifyBelow(v, warn, crit float64) (model.AlertSeverity, bool) {
	if crit > 0 && v <= crit {
		return model.SeverityCritical, true
	}
	if warn > 0 && v <= warn {
		return model.SeverityCritical, true
	}
	return "", false
}

// Apply reconciles alert state for one pipeline against the given facts.
// It returns the alerts whose lifecycle changed (new firing, severity
// escalation/de-escalation, or recovery); the engine persists these.
// nextSeq is invoked (under the manager lock) to durable-stamp each change.
func (m *Manager) Apply(req EvaluateRequest, nextSeq func() int64) []model.Alert {
	m.mu.Lock()
	defer m.mu.Unlock()

	var changed []model.Alert
	for _, b := range m.evaluate(req) {
		fp := Fingerprint(req.Pipeline, b.rule)
		cur, firing := m.active[fp]

		switch {
		case !b.enabled:
			// Rule disabled by current config: close the incident if any.
			if firing {
				closed := *cur
				closed.State = model.AlertRecovered
				closed.RecoveredAt = req.Now
				closed.Message = "rule disabled by config v" + itoa(m.cfg.Version)
				closed.Sequence = nextSeq()
				m.archive(fp, closed)
				m.lastSeq = closed.Sequence
				changed = append(changed, closed)
			}
		case b.breached:
			if !firing {
				m.incidentSeq[fp]++
				a := model.Alert{
					Fingerprint:   fp,
					IncidentSeq:   m.incidentSeq[fp],
					Pipeline:      req.Pipeline,
					Rule:          b.rule,
					Severity:      b.severity,
					State:         model.AlertFiring,
					FiredAt:       req.Now,
					LastValue:     b.value,
					Threshold:     b.threshold,
					Message:       b.message,
					ConfigVersion: m.cfg.Version,
					Sequence:      nextSeq(),
				}
				m.lastSeq = a.Sequence
				m.active[fp] = &a
				changed = append(changed, a)
			} else {
				upd := *cur
				upd.LastValue = b.value
				if b.severity != cur.Severity {
					upd.Severity = b.severity
					upd.Threshold = b.threshold
					upd.Message = b.message
					upd.Sequence = nextSeq()
					m.lastSeq = upd.Sequence
					m.active[fp] = &upd
					changed = append(changed, upd)
				} else {
					// value-only updates need no durability write.
					m.active[fp] = &upd
				}
			}
		default:
			// Rule healthy: recover any firing incident. The conclusion is
			// closed under the CURRENT config version that observed recovery.
			if firing {
				closed := *cur
				closed.State = model.AlertRecovered
				closed.RecoveredAt = req.Now
				closed.LastValue = b.value
				closed.Message = "recovered"
				closed.Sequence = nextSeq()
				m.lastSeq = closed.Sequence
				m.archive(fp, closed)
				changed = append(changed, closed)
			}
		}
	}
	return changed
}

// archive moves a firing alert into history with bounded retention.
// Caller holds m.mu.
func (m *Manager) archive(fp string, a model.Alert) {
	delete(m.active, fp)
	h := m.history[fp]
	h = append(h, a)
	if len(h) > maxHistoryPerKey {
		h = h[len(h)-maxHistoryPerKey:]
	}
	m.history[fp] = h
}

// UpsertFromWAL installs a durable alert record during recovery. It
// reconstructs exactly the post-crash conclusions: firing alerts remain
// firing, recovered ones land in history, incident counters and lastSeq are
// restored.
func (m *Manager) UpsertFromWAL(a model.Alert) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if a.Sequence > m.lastSeq {
		m.lastSeq = a.Sequence
	}
	if a.IncidentSeq > m.incidentSeq[a.Fingerprint] {
		m.incidentSeq[a.Fingerprint] = a.IncidentSeq
	}
	if a.State == model.AlertFiring {
		cp := a
		m.active[a.Fingerprint] = &cp
		// Remove any stale history copy of the same incident.
		m.history[a.Fingerprint] = filterIncident(m.history[a.Fingerprint], a.IncidentSeq)
	} else {
		delete(m.active, a.Fingerprint)
		h := m.history[a.Fingerprint]
		h = replaceOrAppendIncident(h, a)
		if len(h) > maxHistoryPerKey {
			h = h[len(h)-maxHistoryPerKey:]
		}
		m.history[a.Fingerprint] = h
	}
}

func filterIncident(h []model.Alert, incident int) []model.Alert {
	out := h[:0]
	for _, a := range h {
		if a.IncidentSeq != incident {
			out = append(out, a)
		}
	}
	return out
}

func replaceOrAppendIncident(h []model.Alert, a model.Alert) []model.Alert {
	for i := range h {
		if h[i].Fingerprint == a.Fingerprint && h[i].IncidentSeq == a.IncidentSeq {
			h[i] = a
			return h
		}
	}
	return append(h, a)
}

// ActiveFor returns currently firing alerts for one pipeline (sorted).
func (m *Manager) ActiveFor(pipeline string) []model.Alert {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []model.Alert
	for _, a := range m.active {
		if a.Pipeline == pipeline {
			out = append(out, *a)
		}
	}
	sortAlerts(out)
	return out
}

// HistoryFor returns closed incidents for one pipeline, oldest first.
func (m *Manager) HistoryFor(pipeline string) []model.Alert {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []model.Alert
	for _, h := range m.history {
		for _, a := range h {
			if a.Pipeline == pipeline {
				out = append(out, a)
			}
		}
	}
	sortAlerts(out)
	return out
}

// AllActive returns every firing alert (sorted).
func (m *Manager) AllActive() []model.Alert {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]model.Alert, 0, len(m.active))
	for _, a := range m.active {
		out = append(out, *a)
	}
	sortAlerts(out)
	return out
}

func sortAlerts(out []model.Alert) {
	sort.Slice(out, func(i, j int) bool {
		if out[i].FiredAt != out[j].FiredAt {
			return out[i].FiredAt < out[j].FiredAt
		}
		if out[i].Fingerprint != out[j].Fingerprint {
			return out[i].Fingerprint < out[j].Fingerprint
		}
		return out[i].IncidentSeq < out[j].IncidentSeq
	})
}

// LastSequence returns the highest assigned alert sequence.
func (m *Manager) LastSequence() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.lastSeq
}

// State is the serializable manager state used in engine snapshots.
type State struct {
	Active      []model.Alert  `json:"active"`
	History     []model.Alert  `json:"history"`
	IncidentSeq map[string]int `json:"incident_seq"`
	LastSeq     int64          `json:"last_seq"`
}

// ExportState returns a point-in-time copy for snapshotting.
func (m *Manager) ExportState() State {
	m.mu.RLock()
	defer m.mu.RUnlock()
	st := State{IncidentSeq: map[string]int{}, LastSeq: m.lastSeq}
	for _, a := range m.active {
		st.Active = append(st.Active, *a)
	}
	for _, h := range m.history {
		st.History = append(st.History, h...)
	}
	for k, v := range m.incidentSeq {
		st.IncidentSeq[k] = v
	}
	return st
}

// ImportState replaces all manager state during recovery.
func (m *Manager) ImportState(st State) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.active = map[string]*model.Alert{}
	m.history = map[string][]model.Alert{}
	m.incidentSeq = map[string]int{}
	m.lastSeq = st.LastSeq
	for i := range st.Active {
		a := st.Active[i]
		m.active[a.Fingerprint] = &a
	}
	for _, a := range st.History {
		m.history[a.Fingerprint] = append(m.history[a.Fingerprint], a)
	}
	for k, v := range st.IncidentSeq {
		m.incidentSeq[k] = v
	}
}

func itoa(v int) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// HistoryAll returns every closed incident across pipelines, oldest first.
func (m *Manager) HistoryAll() []model.Alert {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []model.Alert
	for _, h := range m.history {
		out = append(out, h...)
	}
	sortAlerts(out)
	return out
}
