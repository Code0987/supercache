package store

import (
	"github.com/Code0987/supercache/pkg/geo"
)

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
