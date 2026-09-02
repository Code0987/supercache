package store

import (
	"github.com/Code0987/supercache/pkg/set"
)

// SetAdd inserts item into the named set (creates if missing). Version gates tombstones.
func (m *Memory) SetAdd(key string, item []byte, version uint64, expireAt int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.setMutateLocked(key, version, expireAt, func(s *set.Set) {
		s.Add(item)
	})
}

// SetRemove removes item from the named set. No-op if set missing (does not create).
// Returns false only when blocked by a higher-version tombstone or non-set entry.
func (m *Memory) SetRemove(key string, item []byte, version uint64, expireAt int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	el, ok := m.items[key]
	if !ok {
		return true // missing set: success no-op
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
		// Higher than tombstone with remove only: still no set to remove from.
		return true
	}
	if !it.entry.IsSet() {
		return false
	}
	return m.setMutateLocked(key, version, expireAt, func(s *set.Set) {
		s.Remove(item)
	})
}

// SetContains reports exact membership.
func (m *Memory) SetContains(key string, item []byte) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.setPeekLocked(key)
	if !ok {
		return false
	}
	return s.Contains(item)
}

// HasSet reports a live FlagSet entry without cloning the blob.
func (m *Memory) HasSet(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.setPeekLocked(key)
	return ok
}

// SetCard returns the number of elements (0 if missing).
func (m *Memory) SetCard(key string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.setPeekLocked(key)
	if !ok {
		return 0
	}
	return s.Len()
}

// SetMembers returns defensive copies of all members (nil if missing/empty).
func (m *Memory) SetMembers(key string) [][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.setPeekLocked(key)
	if !ok {
		return nil
	}
	return s.Members()
}

// SetInstall installs a versioned full-set snapshot (handoff). Higher version wins.
func (m *Memory) SetInstall(key string, blob []byte, version uint64, expireAt int64) bool {
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
			} else if it.entry.IsSet() {
				if version < it.entry.Version {
					m.staleSkip.Add(1)
					return false
				}
				if version == it.entry.Version {
					// Equal version: ignore (idempotent handoff).
					return false
				}
			} else if !it.entry.IsTombstone() {
				// Non-set live entry: only replace if version wins via remove path.
				if version <= it.entry.Version {
					return false
				}
			}
		}
		m.removeElement(el)
	}
	s, err := set.DecodeSet(blob)
	if err != nil {
		return false
	}
	ent := Entry{Value: append([]byte(nil), blob...), Version: version, ExpireAt: expireAt, Flags: FlagSet}
	return m.insertSetLocked(key, ent, s, false)
}

func (m *Memory) setPeekLocked(key string) (*set.Set, bool) {
	el, ok := m.items[key]
	if !ok {
		return nil, false
	}
	it := el.Value.(*lruItem)
	if it.entry.Expired(m.now()) {
		m.removeElement(el)
		return nil, false
	}
	if it.entry.IsTombstone() || !it.entry.IsSet() {
		return nil, false
	}
	if it.setCache != nil {
		return it.setCache, true
	}
	s, err := set.DecodeSet(it.entry.Value)
	if err != nil {
		return nil, false
	}
	it.setCache = s
	return s, true
}

func (m *Memory) setMutateLocked(key string, version uint64, expireAt int64, mut func(*set.Set)) bool {
	var s *set.Set
	if el, ok := m.items[key]; ok {
		it := el.Value.(*lruItem)
		if !it.entry.Expired(m.now()) {
			if it.entry.IsTombstone() {
				if version <= it.entry.Version {
					m.staleSkip.Add(1)
					return false
				}
				// Higher than tombstone: recreate empty then mut.
				m.removeElement(el)
				s = set.New()
			} else if it.entry.IsSet() {
				s = it.setCache
				if s == nil {
					var err error
					s, err = set.DecodeSet(it.entry.Value)
					if err != nil {
						return false
					}
				}
				mut(s)
				oldCost := it.cost
				it.entry.Version = version
				if expireAt != 0 {
					it.entry.ExpireAt = expireAt
				}
				it.entry.Flags = FlagSet
				it.setCache = s
				it.setDirty = true
				// Defer Encode until Peek/Get; approximate cost for MaxBytes.
				it.cost = int64(len(key)) + 64 + s.ApproxWireBytes()
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
			s = set.New()
		}
	} else {
		s = set.New()
	}
	mut(s)
	// New set: encode once so Value is defined; later mutates stay dirty.
	ent := Entry{Value: s.Encode(), Version: version, ExpireAt: expireAt, Flags: FlagSet}
	return m.insertSetLocked(key, ent, s, false)
}

// flushSetValueLocked materializes entry.Value from setCache when dirty.
func (m *Memory) flushSetValueLocked(it *lruItem) {
	if it == nil || !it.setDirty || it.setCache == nil || !it.entry.IsSet() {
		return
	}
	blob := it.setCache.Encode()
	oldCost := it.cost
	it.entry.Value = blob
	it.setDirty = false
	it.cost = entryCost(it.key, it.entry)
	m.bytes += it.cost - oldCost
	if m.bytes < 0 {
		m.bytes = 0
	}
}

func (m *Memory) insertSetLocked(key string, ent Entry, cache *set.Set, dirty bool) bool {
	cost := entryCost(key, ent)
	if dirty && cache != nil {
		cost = int64(len(key)) + 64 + cache.ApproxWireBytes()
	}
	it := &lruItem{key: key, entry: copyEntry(ent), cost: cost, setCache: cache, setDirty: dirty}
	el := m.order.PushFront(it)
	m.items[key] = el
	m.bytes += cost
	m.evictLocked()
	_, ok := m.items[key]
	return ok
}
