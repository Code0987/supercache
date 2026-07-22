package warmup

import (
	"sort"
	"sync"
)

// DefaultTopK is the max keys tracked per keyspace (PLAN §14).
const DefaultTopK = 1024

// Tracker is a bounded approximate hot-key counter (local to a node).
type Tracker struct {
	mu      sync.Mutex
	counts  map[string]uint64
	maxKeys int
}

// NewTracker creates a tracker with a max cardinality bound.
func NewTracker(maxKeys int) *Tracker {
	if maxKeys <= 0 {
		maxKeys = DefaultTopK
	}
	return &Tracker{
		counts:  make(map[string]uint64),
		maxKeys: maxKeys,
	}
}

// Hit records an access for key.
func (t *Tracker) Hit(key string) {
	if t == nil || key == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.counts[key]; !ok && len(t.counts) >= t.maxKeys {
		// Evict a random-ish cold key: remove one with minimal count.
		t.evictOneLocked()
	}
	t.counts[key]++
}

// Top returns up to n keys sorted by hit count descending.
func (t *Tracker) Top(n int) []string {
	if t == nil || n <= 0 {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	type kv struct {
		k string
		c uint64
	}
	items := make([]kv, 0, len(t.counts))
	for k, c := range t.counts {
		items = append(items, kv{k, c})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].c == items[j].c {
			return items[i].k < items[j].k
		}
		return items[i].c > items[j].c
	})
	if n > len(items) {
		n = len(items)
	}
	out := make([]string, n)
	for i := 0; i < n; i++ {
		out[i] = items[i].k
	}
	return out
}

// Snapshot returns a copy of counts (for admin).
func (t *Tracker) Snapshot() map[string]uint64 {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make(map[string]uint64, len(t.counts))
	for k, c := range t.counts {
		out[k] = c
	}
	return out
}

func (t *Tracker) evictOneLocked() {
	var (
		minK string
		minC uint64
		first = true
	)
	for k, c := range t.counts {
		if first || c < minC {
			minK, minC = k, c
			first = false
		}
	}
	if minK != "" {
		delete(t.counts, minK)
	}
}
