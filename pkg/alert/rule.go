// Package alert implements alert rules and per-pipeline alert state machines.
//
// Historical conclusions are immutable with respect to configuration changes:
// when an alert episode starts it pins the exact (rule ID, rule-set version);
// every later evaluation of that episode uses that pinned snapshot. Replacing
// the rule set only affects episodes that start afterwards. All past versions
// are retained so a pinned rule can always be found, including after restart
// (the registry history is part of engine checkpoints).
package alert

import (
	"time"

	"github.com/lechandonga/pipeline-observability-engine-go/pkg/model"
)

// ThresholdRule raises an alert when the selected metric violates a threshold.
type ThresholdRule struct {
	ID       string
	Pipeline string // exact pipeline name; empty means every pipeline
	Metric   model.MetricKind

	// Threshold is the firing boundary.
	Threshold float64

	// Above selects direction of badness:
	//   true  -> bad when value >= Threshold (freshness, latency, failures)
	//   false -> bad when value <= Threshold (throughput, SLA compliance)
	Above bool

	// RecoverBelow adds hysteresis. Ignored when zero, in which case recovery
	// happens as soon as the threshold is strictly un-crossed:
	//   Above=true : recover once value <= RecoverBelow
	//   Above=false: recover once value >= RecoverBelow
	RecoverBelow float64

	// For requires the violation to persist this long (event-time) before an
	// alert fires. Zero fires immediately.
	For time.Duration

	// RecoveryWindow auto-recovers an active alert whose last violating
	// evidence is older than this at query/evaluation time.
	RecoveryWindow time.Duration

	// CloseAfter moves a recovered alert to closed after this duration.
	CloseAfter time.Duration

	Message string
}

// VersionedRule pairs a rule snapshot with the rule-set version that introduced
// it. Alerts pin this pair.
type VersionedRule struct {
	Version int           `json:"version"`
	Rule    ThresholdRule `json:"rule"`
}

// Select reads the metric value this rule evaluates.
func (r ThresholdRule) Select(snap MetricSnapshot) float64 {
	switch r.Metric {
	case model.MetricFreshness:
		return snap.FreshnessSeconds
	case model.MetricLatency:
		return snap.LatencySeconds
	case model.MetricThroughput:
		return snap.ThroughputRPM
	case model.MetricFailure:
		return float64(snap.Failures)
	case model.MetricSLA:
		if snap.SLACompliant {
			return 1
		}
		return 0
	default:
		return 0
	}
}

// Entering reports whether v crosses the firing boundary for the first time.
func (r ThresholdRule) Entering(v float64) bool {
	if r.Above {
		return v >= r.Threshold
	}
	return v <= r.Threshold
}

// Sustained reports whether v stays in the violating region given hysteresis:
// once active, the bad region only ends strictly beyond RecoverBelow. Without a
// configured RecoverBelow it is identical to Entering.
func (r ThresholdRule) Sustained(v float64) bool {
	if r.RecoverBelow == 0 {
		return r.Entering(v)
	}
	if r.Above {
		return v > r.RecoverBelow
	}
	return v < r.RecoverBelow
}

// Violating chooses the enter or the hysteresis boundary depending on whether
// an episode is currently live (cur=true means "already violating").
func (r ThresholdRule) Violating(v float64, cur bool) bool {
	if cur {
		return r.Sustained(v)
	}
	return r.Entering(v)
}

// Match reports whether the rule applies to pipeline p.
func (r ThresholdRule) Match(p string) bool {
	return r.Pipeline == "" || r.Pipeline == p
}

// Validate checks rule well-formedness.
func (r ThresholdRule) Validate() error {
	if r.ID == "" {
		return errRule("rule id is empty")
	}
	switch r.Metric {
	case model.MetricFreshness, model.MetricLatency, model.MetricThroughput,
		model.MetricFailure, model.MetricSLA:
	default:
		return errRule("rule " + r.ID + ": unknown metric kind")
	}
	if r.For < 0 || r.RecoveryWindow < 0 || r.CloseAfter < 0 {
		return errRule("rule " + r.ID + ": negative duration")
	}
	if r.RecoverBelow != 0 {
		if r.Above && r.RecoverBelow >= r.Threshold {
			return errRule("rule " + r.ID + ": RecoverBelow must be < Threshold")
		}
		if !r.Above && r.RecoverBelow <= r.Threshold {
			return errRule("rule " + r.ID + ": RecoverBelow must be > Threshold")
		}
	}
	return nil
}

type ruleError string

func (e ruleError) Error() string { return string(e) }
func errRule(s string) error      { return ruleError(s) }

// Rules is the versioned rule registry with full version history.
// NOT safe for concurrent use; the engine serializes access.
type Rules struct {
	version int
	current []VersionedRule
	// history: rule ID -> version -> pinned rule
	history map[string]map[int]ThresholdRule
}

// NewRules creates an empty rule registry.
func NewRules() *Rules {
	return &Rules{history: make(map[string]map[int]ThresholdRule)}
}

// Replace atomically swaps the rule set, validates it and bumps the version.
// Every supplied rule is stored under the new version; previously stored
// versions remain reachable for pinned alerts. It returns the new version.
func (rs *Rules) Replace(rules []ThresholdRule) (int, error) {
	ids := make(map[string]struct{}, len(rules))
	for _, r := range rules {
		if err := r.Validate(); err != nil {
			return rs.version, err
		}
		if _, dup := ids[r.ID]; dup {
			return rs.version, errRule("duplicate rule id: " + r.ID)
		}
		ids[r.ID] = struct{}{}
	}
	rs.version++
	rs.current = rs.current[:0]
	for _, r := range rules {
		if rs.history[r.ID] == nil {
			rs.history[r.ID] = make(map[int]ThresholdRule)
		}
		rs.history[r.ID][rs.version] = r
		rs.current = append(rs.current, VersionedRule{Version: rs.version, Rule: r})
	}
	return rs.version, nil
}

// Version returns the current rule-set version.
func (rs *Rules) Version() int { return rs.version }

// ForPipeline returns the current rules that apply to pipeline p.
func (rs *Rules) ForPipeline(p string) []VersionedRule {
	var out []VersionedRule
	for _, vr := range rs.current {
		if vr.Rule.Match(p) {
			out = append(out, vr)
		}
	}
	return out
}

// Get returns the pinned rule snapshot for an alert.
func (rs *Rules) Get(id string, version int) (ThresholdRule, bool) {
	if vs, ok := rs.history[id]; ok {
		r, ok := vs[version]
		return r, ok
	}
	return ThresholdRule{}, false
}

// Export returns the full version history (for checkpoints).
func (rs *Rules) Export() []VersionedRule {
	var out []VersionedRule
	type kv struct {
		id string
		v  int
	}
	seen := make(map[kv]bool)
	for id, vs := range rs.history {
		for v, r := range vs {
			k := kv{id, v}
			if seen[k] {
				continue
			}
			seen[k] = true
			out = append(out, VersionedRule{Version: v, Rule: r})
		}
	}
	return out
}

// Import restores full history from a checkpoint.
func (rs *Rules) Import(version int, entries []VersionedRule) {
	rs.version = version
	rs.current = rs.current[:0]
	rs.history = make(map[string]map[int]ThresholdRule)
	for _, e := range entries {
		r := e.Rule
		if rs.history[r.ID] == nil {
			rs.history[r.ID] = make(map[int]ThresholdRule)
		}
		rs.history[r.ID][e.Version] = r
		if e.Version == version {
			rs.current = append(rs.current, VersionedRule{Version: e.Version, Rule: r})
		}
	}
}
