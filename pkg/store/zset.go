package store

import (
	"github.com/Code0987/supercache/pkg/zset"
)

// ZAdd upserts member/score.
func (m *Memory) ZAdd(key string, member []byte, score float64, version uint64, expireAt int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.zMutateLocked(key, version, expireAt, func(z *zset.ZSet) error {
		return z.Add(member, score)
	})
}

// ZRem removes a member (no-op if zset missing).
func (m *Memory) ZRem(key string, member []byte, version uint64, expireAt int64) bool {
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
	if !it.entry.IsZSet() {
		return false
	}
	return m.zMutateLocked(key, version, expireAt, func(z *zset.ZSet) error {
		z.Rem(member)
		return nil
	})
}

// ZScore returns score if present.
func (m *Memory) ZScore(key string, member []byte) (float64, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	z, ok := m.zPeekLocked(key)
	if !ok {
		return 0, false
	}
	return z.Score(member)
}

// ZCard returns member count.
func (m *Memory) ZCard(key string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	z, ok := m.zPeekLocked(key)
	if !ok {
		return 0
	}
	return z.Card()
}

// HasZSet reports a live FlagZSet entry.
func (m *Memory) HasZSet(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.zPeekLocked(key)
	return ok
}

// ZRange returns members by rank.
func (m *Memory) ZRange(key string, start, stop int) []ZMember {
	m.mu.Lock()
	defer m.mu.Unlock()
	z, ok := m.zPeekLocked(key)
	if !ok {
		return nil
	}
	return toStoreZMembers(z.Range(start, stop))
}

// ZRangeByScore returns members in score window.
func (m *Memory) ZRangeByScore(key string, min, max float64) []ZMember {
	m.mu.Lock()
	defer m.mu.Unlock()
	z, ok := m.zPeekLocked(key)
	if !ok {
		return nil
	}
	return toStoreZMembers(z.RangeByScore(min, max))
}

// ZInstall installs a versioned full snapshot.
func (m *Memory) ZInstall(key string, blob []byte, version uint64, expireAt int64) bool {
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
			} else if it.entry.IsZSet() {
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
	z, err := zset.Decode(blob)
	if err != nil {
		return false
	}
	ent := Entry{Value: append([]byte(nil), blob...), Version: version, ExpireAt: expireAt, Flags: FlagZSet}
	return m.insertZSetLocked(key, ent, z, false)
}

func toStoreZMembers(in []zset.Member) []ZMember {
	if len(in) == 0 {
		return nil
	}
	out := make([]ZMember, len(in))
	for i, m := range in {
		out[i] = ZMember{Member: m.Member, Score: m.Score}
	}
	return out
}

func (m *Memory) zPeekLocked(key string) (*zset.ZSet, bool) {
	el, ok := m.items[key]
	if !ok {
		return nil, false
	}
	it := el.Value.(*lruItem)
	if it.entry.Expired(m.now()) {
		m.removeElement(el)
		return nil, false
	}
	if it.entry.IsTombstone() || !it.entry.IsZSet() {
		return nil, false
	}
	if it.zCache != nil {
		return it.zCache, true
	}
	z, err := zset.Decode(it.entry.Value)
	if err != nil {
		return nil, false
	}
	it.zCache = z
	return z, true
}

func (m *Memory) zMutateLocked(key string, version uint64, expireAt int64, mut func(*zset.ZSet) error) bool {
	var z *zset.ZSet
	if el, ok := m.items[key]; ok {
		it := el.Value.(*lruItem)
		if !it.entry.Expired(m.now()) {
			if it.entry.IsTombstone() {
				if version <= it.entry.Version {
					m.staleSkip.Add(1)
					return false
				}
				m.removeElement(el)
				z = zset.New()
			} else if it.entry.IsZSet() {
				z = it.zCache
				if z == nil {
					var err error
					z, err = zset.Decode(it.entry.Value)
					if err != nil {
						return false
					}
				}
				if err := mut(z); err != nil {
					return false
				}
				oldCost := it.cost
				it.entry.Version = version
				if expireAt != 0 {
					it.entry.ExpireAt = expireAt
				}
				it.entry.Flags = FlagZSet
				it.zCache = z
				it.zDirty = true
				it.cost = int64(len(key)) + 64 + z.ApproxWireBytes()
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
			z = zset.New()
		}
	} else {
		z = zset.New()
	}
	if err := mut(z); err != nil {
		return false
	}
	ent := Entry{Value: z.Encode(), Version: version, ExpireAt: expireAt, Flags: FlagZSet}
	return m.insertZSetLocked(key, ent, z, false)
}

func (m *Memory) flushZSetValueLocked(it *lruItem) {
	if it == nil || !it.zDirty || it.zCache == nil || !it.entry.IsZSet() {
		return
	}
	blob := it.zCache.Encode()
	oldCost := it.cost
	it.entry.Value = blob
	it.zDirty = false
	it.cost = entryCost(it.key, it.entry)
	m.bytes += it.cost - oldCost
	if m.bytes < 0 {
		m.bytes = 0
	}
}

func (m *Memory) insertZSetLocked(key string, ent Entry, cache *zset.ZSet, dirty bool) bool {
	cost := entryCost(key, ent)
	if dirty && cache != nil {
		cost = int64(len(key)) + 64 + cache.ApproxWireBytes()
	}
	it := &lruItem{key: key, entry: copyEntry(ent), cost: cost, zCache: cache, zDirty: dirty}
	el := m.order.PushFront(it)
	m.items[key] = el
	m.bytes += cost
	m.evictLocked()
	_, ok := m.items[key]
	return ok
}
