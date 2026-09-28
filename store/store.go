// Package store implements durable local recovery via an append-only write
// ahead log plus periodic snapshots. Only facts that have been accepted are
// persisted; rejected input never reaches disk and therefore can never
// contaminate recovered state.
package store

import "io"

// RecordKind discriminates WAL records.
type RecordKind byte

const (
	KindEvent  RecordKind = 1
	KindMetric RecordKind = 2
	KindAlert  RecordKind = 3
)

// Record is one persisted accepted fact (opaque payload: JSON-encoded
// model.Event or model.Metric).
type Record struct {
	Kind     RecordKind
	Sequence int64
	Payload  []byte
}

// Snapshot is the opaque engine state written to disk for fast recovery.
type Snapshot struct {
	Sequence int64
	Data     []byte
}

// Store is the durability interface used by the engine.
type Store interface {
	// Append durably writes one accepted record.
	Append(rec Record) error
	// ReadAll replays records newer than the snapshot's sequence.
	ReadAll(snapSeq int64) ([]Record, error)
	// SaveSnapshot atomically replaces the durable snapshot.
	SaveSnapshot(s Snapshot) error
	// LoadSnapshot returns the latest snapshot (nil data if none exists).
	LoadSnapshot() (*Snapshot, error)
	io.Closer
}
