package store

import (
	"container/list"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Code0987/supercache/pkg/bloom"
	"github.com/Code0987/supercache/pkg/geo"
	"github.com/Code0987/supercache/pkg/hashx"
	"github.com/Code0987/supercache/pkg/jsonx"
	"github.com/Code0987/supercache/pkg/listx"
	"github.com/Code0987/supercache/pkg/set"
	"github.com/Code0987/supercache/pkg/zset"
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
	// setCache is the live decoded membership for FlagSet entries (avoids
	// re-decode on every Contains/Card). Rebuilt from entry.Value if nil.
	setCache *set.Set
	// setDirty means entry.Value may lag setCache; flushed on Peek/Get.
	setDirty bool
	// zCache / zDirty mirror setCache for FlagZSet.
	zCache *zset.ZSet
	zDirty bool
	// gCache / gDirty mirror setCache for FlagGeo.
	gCache *geo.Index
	gDirty bool
	lCache *listx.List
	lDirty bool
	hCache *hashx.Hash
	hDirty bool
	// jCache is the live decoded JSON tree. After Decode("null") / commit of
	// JSON null, jCache == nil is the document (not "missing").
	jCache any
	jDirty bool
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
	m.flushSetValueLocked(it)
	m.flushZSetValueLocked(it)
	m.flushGeoValueLocked(it)
	m.flushListValueLocked(it)
	m.flushHashValueLocked(it)
	m.flushJSONValueLocked(it)
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
	m.flushSetValueLocked(it)
	m.flushZSetValueLocked(it)
	m.flushGeoValueLocked(it)
	m.flushListValueLocked(it)
	m.flushHashValueLocked(it)
	m.flushJSONValueLocked(it)
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
		it.setCache = nil
		it.setDirty = false
		it.zCache = nil
		it.zDirty = false
		it.gCache = nil
		it.gDirty = false
		it.lCache = nil
		it.lDirty = false
		it.hCache = nil
		it.hDirty = false
		it.jCache = nil
		it.jDirty = false
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
		it.setCache = nil
		it.setDirty = false
		it.zCache = nil
		it.zDirty = false
		it.gCache = nil
		it.gDirty = false
		it.lCache = nil
		it.lDirty = false
		it.hCache = nil
		it.hDirty = false
		it.jCache = nil
		it.jDirty = false
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

// RangeAll visits non-expired entries including tombstones.
func (m *Memory) RangeAll(fn func(key string, e Entry) bool) {
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
		if !ok {
			continue
		}
		if !fn(k, ent) {
			return
		}
	}
}


// PeekVersion returns the stored version without flushing dirty set blobs or cloning Value.
func (m *Memory) PeekVersion(key string) (uint64, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	el, ok := m.items[key]
	if !ok {
		return 0, false
	}
	it := el.Value.(*lruItem)
	if it.entry.Expired(m.now()) {
		m.removeElement(el)
		return 0, false
	}
	return it.entry.Version, true
}


func (m *Memory) GeoAdd(key string, member []byte, lon, lat float64, version uint64, expireAt int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.gMutateLocked(key, version, expireAt, func(g *geo.Index) error {
		return g.Add(member, lon, lat)
	})
}

func (m *Memory) GeoRem(key string, member []byte, version uint64, expireAt int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	el, ok := m.items[key]
	if !ok {
		return true
	}
	it := el.Value.(*lruItem)
	if it.entry.Expired(m.now()) {
		m.removeElement(el)
		return true
	}
	if it.entry.IsTombstone() {
		if version <= it.entry.Version {
			m.staleSkip.Add(1)
			return false
		}
		return true
	}
	if !it.entry.IsGeo() {
		return false
	}
	return m.gMutateLocked(key, version, expireAt, func(g *geo.Index) error {
		g.Rem(member)
		return nil
	})
}

func (m *Memory) GeoPos(key string, member []byte) (float64, float64, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	g, ok := m.gPeekLocked(key)
	if !ok {
		return 0, 0, false
	}
	p, ok := g.Pos(member)
	return p.Lon, p.Lat, ok
}

func (m *Memory) GeoCard(key string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	g, ok := m.gPeekLocked(key)
	if !ok {
		return 0
	}
	return g.Card()
}

func (m *Memory) HasGeo(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.gPeekLocked(key)
	return ok
}

func (m *Memory) GeoDist(key string, a, b []byte) (float64, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	g, ok := m.gPeekLocked(key)
	if !ok {
		return 0, false
	}
	return g.Dist(a, b)
}

func (m *Memory) GeoRadius(key string, lon, lat, radiusM float64, limit int) []GeoMember {
	m.mu.Lock()
	defer m.mu.Unlock()
	g, ok := m.gPeekLocked(key)
	if !ok {
		return nil
	}
	return toStoreGeoMembers(g.Radius(lon, lat, radiusM, limit))
}

func (m *Memory) GeoInstall(key string, blob []byte, version uint64, expireAt int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if el, ok := m.items[key]; ok {
		it := el.Value.(*lruItem)
		if !it.entry.Expired(m.now()) {
			if it.entry.IsTombstone() {
				if version <= it.entry.Version {
					m.staleSkip.Add(1)
					return false
				}
			} else if it.entry.IsGeo() {
				if version <= it.entry.Version {
					m.staleSkip.Add(1)
					return false
				}
			} else if version <= it.entry.Version {
				return false
			}
		}
		m.removeElement(el)
	}
	g, err := geo.Decode(blob)
	if err != nil {
		return false
	}
	ent := Entry{Value: append([]byte(nil), blob...), Version: version, ExpireAt: expireAt, Flags: FlagGeo}
	return m.insertGeoLocked(key, ent, g, false)
}

func toStoreGeoMembers(in []geo.Member) []GeoMember {
	if len(in) == 0 {
		return nil
	}
	out := make([]GeoMember, len(in))
	for i, m := range in {
		out[i] = GeoMember{Member: m.Member, Lon: m.Lon, Lat: m.Lat, Dist: m.Dist}
	}
	return out
}

func (m *Memory) gPeekLocked(key string) (*geo.Index, bool) {
	el, ok := m.items[key]
	if !ok {
		return nil, false
	}
	it := el.Value.(*lruItem)
	if it.entry.Expired(m.now()) {
		m.removeElement(el)
		return nil, false
	}
	if it.entry.IsTombstone() || !it.entry.IsGeo() {
		return nil, false
	}
	if it.gCache != nil {
		return it.gCache, true
	}
	g, err := geo.Decode(it.entry.Value)
	if err != nil {
		return nil, false
	}
	it.gCache = g
	return g, true
}

func (m *Memory) gMutateLocked(key string, version uint64, expireAt int64, mut func(*geo.Index) error) bool {
	var g *geo.Index
	if el, ok := m.items[key]; ok {
		it := el.Value.(*lruItem)
		if !it.entry.Expired(m.now()) {
			if it.entry.IsTombstone() {
				if version <= it.entry.Version {
					m.staleSkip.Add(1)
					return false
				}
				m.removeElement(el)
				g = geo.New()
			} else if it.entry.IsGeo() {
				g = it.gCache
				if g == nil {
					var err error
					g, err = geo.Decode(it.entry.Value)
					if err != nil {
						return false
					}
				}
				if err := mut(g); err != nil {
					return false
				}
				oldCost := it.cost
				it.entry.Version = version
				if expireAt != 0 {
					it.entry.ExpireAt = expireAt
				}
				it.entry.Flags = FlagGeo
				it.gCache = g
				it.gDirty = true
				it.cost = int64(len(key)) + 64 + g.ApproxWireBytes()
				m.bytes += it.cost - oldCost
				m.order.MoveToFront(el)
				m.evictLocked()
				_, still := m.items[key]
				return still
			} else {
				return false
			}
		} else {
			m.removeElement(el)
			g = geo.New()
		}
	} else {
		g = geo.New()
	}
	if err := mut(g); err != nil {
		return false
	}
	ent := Entry{Value: g.Encode(), Version: version, ExpireAt: expireAt, Flags: FlagGeo}
	return m.insertGeoLocked(key, ent, g, false)
}

func (m *Memory) flushGeoValueLocked(it *lruItem) {
	if it == nil || !it.gDirty || it.gCache == nil || !it.entry.IsGeo() {
		return
	}
	blob := it.gCache.Encode()
	oldCost := it.cost
	it.entry.Value = blob
	it.gDirty = false
	it.cost = entryCost(it.key, it.entry)
	m.bytes += it.cost - oldCost
	if m.bytes < 0 {
		m.bytes = 0
	}
}

func (m *Memory) insertGeoLocked(key string, ent Entry, cache *geo.Index, dirty bool) bool {
	cost := entryCost(key, ent)
	if dirty && cache != nil {
		cost = int64(len(key)) + 64 + cache.ApproxWireBytes()
	}
	it := &lruItem{key: key, entry: copyEntry(ent), cost: cost, gCache: cache, gDirty: dirty}
	el := m.order.PushFront(it)
	m.items[key] = el
	m.bytes += cost
	m.evictLocked()
	_, ok := m.items[key]
	return ok
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
		if (it.entry.IsTombstone() || it.entry.IsBloom() || it.entry.IsSet() || it.entry.IsZSet() || it.entry.IsGeo() || it.entry.IsList() || it.entry.IsHash() || it.entry.IsCounter() || it.entry.IsJSON() || it.entry.IsBitmap() || it.entry.IsHLL() || it.entry.IsTopK() || it.entry.IsCMS()) && !it.entry.Expired(m.now()) {
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
