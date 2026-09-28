package engine

import (
	"github.com/lechandonga/pipeline-observability-engine-go/pkg/alert"
	"github.com/lechandonga/pipeline-observability-engine-go/pkg/model"
	"time"
)

// alertRuleLatency builds a single latency rule for tests.
func alertRuleLatency(threshold float64) []alert.ThresholdRule {
	return []alert.ThresholdRule{{
		ID: "lat", Pipeline: "p1", Metric: model.MetricLatency,
		Above: true, Threshold: threshold,
	}}
}

func modelMetricLatency() model.MetricKind { return model.MetricLatency }

// t0 is the shared event-time base for recovery tests.
var t0 = time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)
