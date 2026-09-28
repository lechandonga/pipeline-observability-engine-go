package alert

import (
	"sort"
	"time"

	"github.com/lechandonga/pipeline-observability-engine-go/pkg/model"
)

// episode is one alert lifecycle instance keyed by (rule, pipeline). Its
// timestamped facts are the durable conclusion; view-time timeouts (no recent
// evidence, close-after elapsed) are computed virtually in Snapshot, so the
// same facts yield the same answer before and after restart.
type episode struct {
	RuleID      string  `json:"rule_id"`
	RuleVersion int     `json:"rule_version"`
	Pipeline    string  `json:"pipeline"`
	Value       float64 `json:"value"`
	Message     string  `json:"message"`

	FirstViolation time.Time `json:"first_violation"` // first violating evidence of this episode
	LastBad        time.Time `json:"last_bad"`        // most recent violating evidence
	LastGood       time.Time `json:"last_good"`       // most recent clearing evidence
	FiredAt        time.Time `json:"fired_at"`        // when the For-duration was satisfied
	RecoveredAt    time.Time `json:"recovered_at"`    // recovery conclusion (event-time)
	ClosedAt       time.Time `json:"closed_at"`       // close conclusion (event-time)
	Count          int       `json:"count"`           // violating evidence points seen
}

// Key builds the external dedup key of an alert (one logical alert per
// rule+pipeline, regardless of generation). Internal storage additionally
// qualifies by RuleVersion so a configuration change can never overwrite the
// conclusion of a previous generation.
func Key(ruleID, pipeline string) string { return ruleID + "\x00" + pipeline }

func epKey(ruleID, pipeline string, version int) string {
	return ruleID + "\x00" + pipeline + "\x00v" + itoa(version)
}

func itoa(v int) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	var b [24]byte
	i := len(b)
	u := v
	if neg {
		u = -v
	}
	for u > 0 {
		i--
		b[i] = byte('0' + u%10)
		u /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

// Manager owns alert episodes per pipeline, keyed by (rule, pipeline).
// NOT safe for concurrent use; the engine holds a single state lock.
type Manager struct {
	registry *Rules
	episodes map[string]*episode
}

// NewManager constructs an empty Manager bound to registry (used to resolve the
// pinned rule snapshot of historical episodes and on restart).
func NewManager(registry *Rules) *Manager {
	return &Manager{registry: registry, episodes: make(map[string]*episode)}
}

// Evaluate advances the alert episodes of one pipeline at event-time ts using a
// snapshot derived AFTER the triggering report was merged. It returns the alert
// views that transition into firing during this call (key for rule dedup: a
// live episode never yields a second firing; recovery/re-fire needs close).
//
// Episodes pinned to rule versions no longer current receive no new evidence:
// they recover/close via the stale-evidence timeout in Snapshot. New evidence
// under a newer generation starts a separate episode, so old and new
// conclusions never overwrite each other.
func (m *Manager) Evaluate(pipeline string, snap MetricSnapshot, current []VersionedRule, ts time.Time) []model.Alert {
	var fired []model.Alert

	for _, vr := range current {
		r := vr.Rule
		key := epKey(r.ID, pipeline, vr.Version)
		ep := m.episodes[key]

		// Live episode only exists under the same rule generation.
		if ep != nil && ep.RuleVersion != vr.Version {
			ep = nil
		}

		// Without any observation of the rule's metric, neither start nor clear
		// an episode: a pre-start zero default must not trigger, and absence of
		// data is handled by the stale-recovery window instead.
		if !r.HasData(snap) {
			continue
		}
		v := r.Select(snap)
		// A brand-new episode uses the enter boundary; a live episode uses the
		// hysteresis (sustained) boundary. Closed episodes start fresh.
		live := ep != nil && ep.ClosedAt.IsZero() && ep.RecoveredAt.IsZero()
		var violating bool
		if live {
			violating = r.Sustained(v)
		} else {
			violating = r.Entering(v)
		}

		switch {
		case ep == nil || !ep.ClosedAt.IsZero():
			if !violating {
				continue
			}
			// Brand-new episode (a closed one always starts a fresh conclusion).
			fresh := &episode{
				RuleID: r.ID, RuleVersion: vr.Version, Pipeline: pipeline,
				FirstViolation: ts, LastBad: ts, Count: 1,
				Value: v, Message: r.Message,
			}
			if r.For <= 0 {
				fresh.FiredAt = ts
				fired = append(fired, fresh.view(model.AlertActive))
			}
			m.episodes[key] = fresh

		case !ep.RecoveredAt.IsZero():
			// Recovered episode re-violating before close: reopen (dedup keeps
			// one alert; flapping does not spawn duplicates).
			if violating {
				ep.RecoveredAt = time.Time{}
				ep.ClosedAt = time.Time{}
				ep.LastBad = ts
				ep.Count++
				ep.Value = v
				if r.For <= 0 {
					fired = append(fired, ep.view(model.AlertActive))
				}
			} else {
				ep.LastGood = ts
				// Close may already have been concluded event-time-wise.
				if r.CloseAfter > 0 && !ts.Before(ep.RecoveredAt.Add(r.CloseAfter)) {
					ep.ClosedAt = ep.RecoveredAt.Add(r.CloseAfter)
				}
			}

		default: // live, pending or active, not recovered
			if violating {
				ep.LastBad = ts
				ep.Count++
				ep.Value = v
				if ep.FiredAt.IsZero() {
					if r.For <= 0 {
						ep.FiredAt = ts
						fired = append(fired, ep.view(model.AlertActive))
					} else if !ts.Before(ep.FirstViolation.Add(r.For)) {
						ep.FiredAt = ep.FirstViolation.Add(r.For)
						fired = append(fired, ep.view(model.AlertActive))
					}
				}
			} else {
				ep.LastGood = ts
				if ep.FiredAt.IsZero() {
					// Cleared before ever firing: the pending episode dissolves.
					delete(m.episodes, key)
					continue
				}
				ep.RecoveredAt = ts
				if r.CloseAfter > 0 && !ts.Before(ep.RecoveredAt.Add(r.CloseAfter)) {
					ep.ClosedAt = ep.RecoveredAt.Add(r.CloseAfter)
				}
			}
		}
	}
	return fired
}

// isViolating reflects the durable "currently in bad state" condition: a
// violation has been observed and no recovery has been concluded.
func (ep *episode) isViolating() bool {
	return ep.RecoveredAt.IsZero() && !ep.FirstViolation.IsZero() &&
		!ep.LastBad.Before(ep.LastGood)
}

// view projects durable facts into a model.Alert.
func (ep *episode) view(state model.AlertState) model.Alert {
	return model.Alert{
		Key:         Key(ep.RuleID, ep.Pipeline),
		RuleID:      ep.RuleID,
		RuleVersion: ep.RuleVersion,
		Pipeline:    ep.Pipeline,
		State:       state,
		Value:       ep.Value,
		Message:     ep.Message,
		FirstSeen:   ep.FiredAt,
		LastSeen:    ep.LastBad,
		RecoveredAt: ep.RecoveredAt,
		ClosedAt:    ep.ClosedAt,
		Count:       ep.Count,
	}
}

// Snapshot returns a consistent, time-adjusted read model of every episode.
// Pending (never fired) episodes are internal and excluded. Closed episodes
// older than keepClosed beyond their effective close time are pruned.
//
// Recovery/close produced purely by the passage of observation time are
// projections and are not written back to the durable facts here; they are
// concluded durably whenever new evidence or a later evaluation reaches the
// same timestamps.
func (m *Manager) Snapshot(now time.Time, keepClosed time.Duration) []model.Alert {
	out := make([]model.Alert, 0, len(m.episodes))
	for _, ep := range m.episodes {
		rule, ok := m.registry.Get(ep.RuleID, ep.RuleVersion)
		if !ok {
			continue
		}

		// Pending episodes are internal; a stale pending one is reported as
		// absent here and physically removed later by Sweep under the write lock.
		if ep.FiredAt.IsZero() {
			continue
		}

		state := model.AlertActive
		effectiveRecovery := ep.RecoveredAt
		effectiveClose := ep.ClosedAt

		// Virtual stale recovery: no violating evidence within RecoveryWindow.
		if effectiveRecovery.IsZero() && rule.RecoveryWindow > 0 &&
			now.After(ep.LastBad.Add(rule.RecoveryWindow)) {
			effectiveRecovery = ep.LastBad.Add(rule.RecoveryWindow)
		}
		if !effectiveRecovery.IsZero() {
			state = model.AlertRecovered
			if effectiveClose.IsZero() && rule.CloseAfter > 0 {
				if c := effectiveRecovery.Add(rule.CloseAfter); !now.Before(c) {
					effectiveClose = c
				}
			}
			if !effectiveClose.IsZero() {
				state = model.AlertClosed
			}
		}

		if state == model.AlertClosed && keepClosed > 0 &&
			!now.Before(effectiveClose.Add(keepClosed)) {
			// Retention elapsed: hidden from reads now; Sweep reclaims memory.
			continue
		}

		a := ep.view(state)
		a.RecoveredAt = effectiveRecovery
		a.ClosedAt = effectiveClose
		out = append(out, a)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Pipeline != out[j].Pipeline {
			return out[i].Pipeline < out[j].Pipeline
		}
		if out[i].RuleID != out[j].RuleID {
			return out[i].RuleID < out[j].RuleID
		}
		return out[i].FirstSeen.Before(out[j].FirstSeen)
	})
	return out
}

// Sweep physically removes episodes that no longer need to be retained:
// pending episodes whose last bad evidence is older than the recovery window,
// and closed episodes beyond the keep-closed retention. It MUST be called only
// while holding the engine's write lock; Snapshot itself is read-only so any
// number of concurrent queries is safe.
func (m *Manager) Sweep(now time.Time, keepClosed time.Duration) {
	for key, ep := range m.episodes {
		rule, ok := m.registry.Get(ep.RuleID, ep.RuleVersion)
		if !ok {
			continue
		}
		if ep.FiredAt.IsZero() {
			if rule.RecoveryWindow > 0 && now.After(ep.LastBad.Add(rule.RecoveryWindow)) {
				delete(m.episodes, key)
			}
			continue
		}
		recovery := ep.RecoveredAt
		closed := ep.ClosedAt
		if recovery.IsZero() && rule.RecoveryWindow > 0 &&
			now.After(ep.LastBad.Add(rule.RecoveryWindow)) {
			recovery = ep.LastBad.Add(rule.RecoveryWindow)
		}
		if !recovery.IsZero() && closed.IsZero() && rule.CloseAfter > 0 {
			c := recovery.Add(rule.CloseAfter)
			if !now.Before(c) {
				closed = c
			}
		}
		if !closed.IsZero() && keepClosed > 0 &&
			!now.Before(closed.Add(keepClosed)) {
			delete(m.episodes, key)
		}
	}
}

// Restore replaces internal episode state with persisted episodes (restart).
func (m *Manager) Restore(eps []episode) {
	m.episodes = make(map[string]*episode, len(eps))
	for i := range eps {
		ep := eps[i]
		m.episodes[epKey(ep.RuleID, ep.Pipeline, ep.RuleVersion)] = &ep
	}
}

// Episodes exposes persisted episodes for checkpoints.
func (m *Manager) Episodes() []episode {
	out := make([]episode, 0, len(m.episodes))
	for _, ep := range m.episodes {
		out = append(out, *ep)
	}
	return out
}

// EpisodeView is the persistable projection of an episode.
type EpisodeView = episode

// SerializeEpisodes returns episodes for checkpoint persistence.
func (m *Manager) SerializeEpisodes() []EpisodeView { return m.Episodes() }

// RestoreEpisodes loads episodes from a checkpoint.
func (m *Manager) RestoreEpisodes(eps []EpisodeView) { m.Restore(eps) }
