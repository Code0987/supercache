package store

import (
	"container/list"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultTombstoneTTL bounds how long delete markers block stale ApplyPut.
const DefaultTombstoneTTL = 5 * time.Minute

// Memory is a mutex-protected LRU store with MaxBytes.
type Memory struct {
	mu           sync.Mutex
	maxBytes     int64
	bytes        int64
	items        map[string]*list.Element
	order        *list.List // front = most recent
	now          func() time.Time
	tombstoneTTL time.Duration

	hits      atomic.Uint64
	misses    atomic.Uint64
	evictions atomic.Uint64
	staleSkip atomic.Uint64
}

type lruItem struct {
	key   string
	entry Entry
	cost  int64
}

// MemoryOption configures Memory.
type MemoryOption func(*Memory)

// WithClock sets the clock used for TTL expiry checks.
func WithClock(now func() time.Time) MemoryOption {
	return func(m *Memory) { m.now = now }
}

// WithTombstoneTTL sets how long delete tombstones are retained (0 = no expiry).
func WithTombstoneTTL(d time.Duration) MemoryOption {
	return func(m *Memory) { m.tombstoneTTL = d }
}

// NewMemory creates a store. maxBytes <= 0 means unbounded (still LRU-ordered).
func NewMemory(maxBytes int64, opts ...MemoryOption) *Memory {
	m := &Memory{
		maxBytes:     maxBytes,
		items:        make(map[string]*list.Element),
		order:        list.New(),
		now:          time.Now,
		tombstoneTTL: DefaultTombstoneTTL,
	}
	for _, o := range opts {
		o(m)
	}
	if m.now == nil {
		m.now = time.Now
	}
	return m
}

func (m *Memory) Get(key string) (Entry, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	el, ok := m.items[key]
	if !ok {
		m.misses.Add(1)
		return Entry{}, false
	}
	it := el.Value.(*lruItem)
	if it.entry.Expired(m.now()) {
		m.removeElement(el)
		m.misses.Add(1)
		return Entry{}, false
	}
	// Tombstones are not readable values.
	if it.entry.IsTombstone() {
		m.misses.Add(1)
		return Entry{}, false
	}
	m.order.MoveToFront(el)
	m.hits.Add(1)
	out := it.entry
	out.Value = it.entry.CloneValue()
	return out, true
}

func (m *Memory) Peek(key string) (Entry, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	el, ok := m.items[key]
	if !ok {
		return Entry{}, false
	}
	it := el.Value.(*lruItem)
	if it.entry.Expired(m.now()) {
		m.removeElement(el)
		return Entry{}, false
	}
	// Peek returns tombstones so version allocation can seed from delete version.
	out := it.entry
	out.Value = it.entry.CloneValue()
	return out, true
}

func (m *Memory) Set(key string, e Entry) bool {
	cost := entryCost(key, e)
	if m.maxBytes > 0 && cost > m.maxBytes {
		return false
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if el, ok := m.items[key]; ok {
		it := el.Value.(*lruItem)
		m.bytes -= it.cost
		it.entry = copyEntry(e)
		it.cost = cost
		m.bytes += cost
		m.order.MoveToFront(el)
	} else {
		it := &lruItem{key: key, entry: copyEntry(e), cost: cost}
		el := m.order.PushFront(it)
		m.items[key] = el
		m.bytes += cost
	}
	m.evictLocked()
	return true
}

func (m *Memory) AcceptIfNewer(key string, e Entry) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Never install a plain put over a still-valid higher/equal tombstone or live entry.
	if el, ok := m.items[key]; ok {
		it := el.Value.(*lruItem)
		if !it.entry.Expired(m.now()) && e.Version <= it.entry.Version {
			m.staleSkip.Add(1)
			return false
		}
		// replace (including superseding an expired or lower-version tombstone)
		cost := entryCost(key, e)
		if m.maxBytes > 0 && cost > m.maxBytes {
			return false
		}
		m.bytes -= it.cost
		it.entry = copyEntry(e)
		it.cost = cost
		m.bytes += cost
		m.order.MoveToFront(el)
		m.evictLocked()
		return true
	}

	cost := entryCost(key, e)
	if m.maxBytes > 0 && cost > m.maxBytes {
		return false
	}
	it := &lruItem{key: key, entry: copyEntry(e), cost: cost}
	el := m.order.PushFront(it)
	m.items[key] = el
	m.bytes += cost
	m.evictLocked()
	return true
}

func (m *Memory) AcceptNegative(key string, e Entry) bool {
	cost := entryCost(key, e)
	if m.maxBytes > 0 && cost > m.maxBytes {
		return false
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if el, ok := m.items[key]; ok {
		it := el.Value.(*lruItem)
		if !it.entry.Expired(m.now()) {
			if it.entry.IsTombstone() {
				// Negatives must not resurrect over a delete tombstone with higher/equal version.
				if e.Version <= it.entry.Version {
					m.staleSkip.Add(1)
					return false
				}
			} else if !it.entry.IsNegative() {
				// Never clobber a live positive entry.
				return false
			} else if e.Version <= it.entry.Version {
				m.staleSkip.Add(1)
				return false
			}
		}
		m.bytes -= it.cost
		it.entry = copyEntry(e)
		it.cost = cost
		m.bytes += cost
		m.order.MoveToFront(el)
		m.evictLocked()
		return true
	}

	it := &lruItem{key: key, entry: copyEntry(e), cost: cost}
	el := m.order.PushFront(it)
	m.items[key] = el
	m.bytes += cost
	m.evictLocked()
	return true
}

func (m *Memory) Delete(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	el, ok := m.items[key]
	if !ok {
		return false
	}
	m.removeElement(el)
	return true
}

func (m *Memory) DeleteIfVersion(key string, deleteVersion uint64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	tomb := Entry{
		Version:  deleteVersion,
		Flags:    FlagTombstone,
		ExpireAt: m.tombstoneExpireAt(),
	}
	cost := entryCost(key, tomb)

	if el, ok := m.items[key]; ok {
		it := el.Value.(*lruItem)
		if !it.entry.Expired(m.now()) && deleteVersion < it.entry.Version {
			m.staleSkip.Add(1)
			return false
		}
		m.bytes -= it.cost
		it.entry = copyEntry(tomb)
		it.cost = cost
		m.bytes += cost
		m.order.MoveToFront(el)
		m.evictLocked()
		return true
	}

	it := &lruItem{key: key, entry: copyEntry(tomb), cost: cost}
	el := m.order.PushFront(it)
	m.items[key] = el
	m.bytes += cost
	m.evictLocked()
	return true
}

func (m *Memory) tombstoneExpireAt() int64 {
	if m.tombstoneTTL <= 0 {
		return 0
	}
	return m.now().Add(m.tombstoneTTL).UnixNano()
}

func (m *Memory) Stats() Stats {
	m.mu.Lock()
	items := int64(len(m.items))
	bytes := m.bytes
	m.mu.Unlock()
	return Stats{
		Hits:      m.hits.Load(),
		Misses:    m.misses.Load(),
		Evictions: m.evictions.Load(),
		Items:     items,
		Bytes:     bytes,
		StaleSkip: m.staleSkip.Load(),
	}
}

// Range visits live non-tombstone entries (positives and negatives).
// Snapshot keys under lock, then re-Peek each so the callback never runs under
// the store mutex (avoids re-entrancy if fn later calls Get/Set).
func (m *Memory) Range(fn func(key string, e Entry) bool) {
	if m == nil || fn == nil {
		return
	}
	m.mu.Lock()
	keys := make([]string, 0, len(m.items))
	for k := range m.items {
		keys = append(keys, k)
	}
	m.mu.Unlock()

	for _, k := range keys {
		ent, ok := m.Peek(k)
		if !ok || ent.IsTombstone() {
			continue
		}
		// Peek already drops expired; tombstones are not useful for handoff fills.
		if !fn(k, ent) {
			return
		}
	}
}

func (m *Memory) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.items = make(map[string]*list.Element)
	m.order = list.New()
	m.bytes = 0
}

func (m *Memory) evictLocked() {
	if m.maxBytes <= 0 {
		return
	}
	for m.bytes > m.maxBytes && m.order.Len() > 0 {
		el := m.lruVictim()
		if el == nil {
			// Only live tombstones remain; allow overshoot so deletes stay gated.
			return
		}
		m.removeElement(el)
		m.evictions.Add(1)
	}
}

// lruVictim is the least-recent live/expired entry. Unexpired tombstones are
// not evicted — that would let a delayed ApplyPut resurrect the key.
func (m *Memory) lruVictim() *list.Element {
	for el := m.order.Back(); el != nil; el = el.Prev() {
		it := el.Value.(*lruItem)
		if it.entry.IsTombstone() && !it.entry.Expired(m.now()) {
			continue
		}
		return el
	}
	return nil
}

func (m *Memory) removeElement(el *list.Element) {
	it := el.Value.(*lruItem)
	delete(m.items, it.key)
	m.order.Remove(el)
	m.bytes -= it.cost
	if m.bytes < 0 {
		m.bytes = 0
	}
}

func entryCost(key string, e Entry) int64 {
	return int64(len(key)) + e.Cost()
}

func copyEntry(e Entry) Entry {
	return Entry{
		Value:    e.CloneValue(),
		Version:  e.Version,
		ExpireAt: e.ExpireAt,
		Flags:    e.Flags,
	}
}
