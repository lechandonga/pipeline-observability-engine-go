// Package dedup provides a bounded FIFO set of report IDs.
//
// Duplicate detection must not grow without bound on a long-running engine, so
// IDs are retained in insertion order and the oldest is evicted once capacity
// is exceeded. A practical consequence: a report whose ID was evicted and is
// then re-sent will be treated as new again; callers should size the capacity
// well beyond the number of reports that can arrive within the late window.
package dedup

import "container/list"

// Set is a bounded FIFO dedup set. NOT safe for concurrent use.
type Set struct {
	capacity int
	seen     map[string]*list.Element
	order    *list.List // front = oldest
}

// entry is the payload stored in the linked list.
type entry struct{ id string }

// NewSet constructs a Set with the given capacity. Capacity < 1 is clamped to 1.
func NewSet(capacity int) *Set {
	if capacity < 1 {
		capacity = 1
	}
	return &Set{
		capacity: capacity,
		seen:     make(map[string]*list.Element),
		order:    list.New(),
	}
}

// Add records id. It returns false (without changing order) when id is already
// retained; otherwise it inserts id and evicts the oldest ID at capacity.
func (s *Set) Add(id string) bool {
	if _, ok := s.seen[id]; ok {
		return false
	}
	el := s.order.PushBack(&entry{id: id})
	s.seen[id] = el
	if s.order.Len() > s.capacity {
		oldest := s.order.Front()
		if oldest != nil {
			s.order.Remove(oldest)
			delete(s.seen, oldest.Value.(*entry).id)
		}
	}
	return true
}

// Contains reports whether id is currently retained.
func (s *Set) Contains(id string) bool {
	_, ok := s.seen[id]
	return ok
}

// Len returns the number of retained IDs.
func (s *Set) Len() int { return s.order.Len() }

// Remove deletes id if present. Used to release a reservation made for a
// report that is ultimately rejected (late/future/invalid/conflict).
func (s *Set) Remove(id string) {
	if el, ok := s.seen[id]; ok {
		s.order.Remove(el)
		delete(s.seen, id)
	}
}

// Order returns retained IDs from oldest to newest (checkpoint support).
func (s *Set) Order() []string {
	out := make([]string, 0, s.order.Len())
	for el := s.order.Front(); el != nil; el = el.Next() {
		out = append(out, el.Value.(*entry).id)
	}
	return out
}

// NewOrdered reconstructs a set from an ordered list, applying the capacity
// bound by dropping the oldest excess entries.
func NewOrdered(ids []string, capacity int) *Set {
	s := NewSet(capacity)
	for _, id := range ids {
		s.Add(id)
	}
	return s
}
