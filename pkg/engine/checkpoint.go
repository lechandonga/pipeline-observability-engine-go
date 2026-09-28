package engine

import (
	"encoding/json"
	"time"

	"github.com/lechandonga/pipeline-observability-engine-go/pkg/alert"
)

// checkpoint is the durable compact snapshot used for WAL compaction and fast
// restart. Everything needed to answer queries without replaying the whole
// history (except the trailing frames, which carry strictly newer state) is
// included.
type checkpoint struct {
	Version   int                       `json:"version"`
	Seq       uint64                    `json:"seq"`
	Rules     []alert.VersionedRule     `json:"rules"`
	Pipelines map[string]*pipelineState `json:"pipelines"`
	Watermark map[string]time.Time      `json:"watermarks"`
	DedupIDs  []string                  `json:"dedup_ids"` // FIFO order, oldest first
	Episodes  []alert.EpisodeView       `json:"episodes"`
}

// Episode is the persistable form of an alert episode; it is an alias exposed
// by the alert package via json tags on its unexported type. Encoding goes
// through alert.Manager so the shape stays in one place.

func (c *checkpoint) marshal() ([]byte, error) { return json.Marshal(c) }

func unmarshalCheckpoint(b []byte) (*checkpoint, error) {
	var c checkpoint
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, err
	}
	return &c, nil
}
