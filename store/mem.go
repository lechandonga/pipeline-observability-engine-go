package store

import "sync"

// MemStore is an in-process Store implementation. It honors sequence
// filtering like FileStore but never touches disk, so it is useful for
// ephemeral/injected use and tests that do not model restart.
type MemStore struct {
	mu      sync.Mutex
	records []Record
	snap    *Snapshot
	closed  bool
}

// NewMemStore creates an empty in-memory store.
func NewMemStore() *MemStore { return &MemStore{} }

// Append implements Store.
func (m *MemStore) Append(rec Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return errClosed("mem store closed")
	}
	cp := Record{Kind: rec.Kind, Sequence: rec.Sequence, Payload: append([]byte(nil), rec.Payload...)}
	m.records = append(m.records, cp)
	return nil
}

// ReadAll implements Store.
func (m *MemStore) ReadAll(snapSeq int64) ([]Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Record, 0)
	for _, r := range m.records {
		if r.Sequence > snapSeq {
			out = append(out, Record{Kind: r.Kind, Sequence: r.Sequence, Payload: append([]byte(nil), r.Payload...)})
		}
	}
	return out, nil
}

// SaveSnapshot implements Store (also compacts covered records in memory).
func (m *MemStore) SaveSnapshot(s Snapshot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.snap = &Snapshot{Sequence: s.Sequence, Data: append([]byte(nil), s.Data...)}
	kept := m.records[:0]
	for _, r := range m.records {
		if r.Sequence > s.Sequence {
			kept = append(kept, r)
		}
	}
	m.records = kept
	return nil
}

// LoadSnapshot implements Store.
func (m *MemStore) LoadSnapshot() (*Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.snap == nil {
		return nil, nil
	}
	return &Snapshot{Sequence: m.snap.Sequence, Data: append([]byte(nil), m.snap.Data...)}, nil
}

// Close implements Store.
func (m *MemStore) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}
