package store

import (
	"github.com/Code0987/supercache/pkg/listx"
)

func (m *Memory) LPush(key string, item []byte, version uint64, expireAt int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lMutateLocked(key, version, expireAt, func(l *listx.List) {
		l.LPush(item)
	})
}

func (m *Memory) RPush(key string, item []byte, version uint64, expireAt int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lMutateLocked(key, version, expireAt, func(l *listx.List) {
		l.RPush(item)
	})
}

func (m *Memory) LPop(key string, version uint64, expireAt int64) (item []byte, popped, applied bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lPopLocked(key, version, expireAt, true)
}

func (m *Memory) RPop(key string, version uint64, expireAt int64) (item []byte, popped, applied bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lPopLocked(key, version, expireAt, false)
}

func (m *Memory) LLen(key string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	l, ok := m.lPeekLocked(key)
	if !ok {
		return 0
	}
	return l.Len()
}

func (m *Memory) HasList(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.lPeekLocked(key)
	return ok
}

func (m *Memory) LIndex(key string, idx int) ([]byte, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	l, ok := m.lPeekLocked(key)
	if !ok {
		return nil, false
	}
	return l.Index(idx)
}

func (m *Memory) LRange(key string, start, stop int) [][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	l, ok := m.lPeekLocked(key)
	if !ok {
		return nil
	}
	return l.Range(start, stop)
}

func (m *Memory) LInstall(key string, blob []byte, version uint64, expireAt int64) bool {
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
			} else if it.entry.IsList() {
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
	l, err := listx.Decode(blob)
	if err != nil {
		return false
	}
	ent := Entry{Value: append([]byte(nil), blob...), Version: version, ExpireAt: expireAt, Flags: FlagList}
	return m.insertListLocked(key, ent, l, false)
}

func (m *Memory) lPeekLocked(key string) (*listx.List, bool) {
	el, ok := m.items[key]
	if !ok {
		return nil, false
	}
	it := el.Value.(*lruItem)
	if it.entry.Expired(m.now()) {
		m.removeElement(el)
		return nil, false
	}
	if it.entry.IsTombstone() || !it.entry.IsList() {
		return nil, false
	}
	if it.lCache != nil {
		return it.lCache, true
	}
	l, err := listx.Decode(it.entry.Value)
	if err != nil {
		return nil, false
	}
	it.lCache = l
	return l, true
}

func (m *Memory) lMutateLocked(key string, version uint64, expireAt int64, mut func(*listx.List)) bool {
	var l *listx.List
	if el, ok := m.items[key]; ok {
		it := el.Value.(*lruItem)
		if !it.entry.Expired(m.now()) {
			if it.entry.IsTombstone() {
				if version <= it.entry.Version {
					m.staleSkip.Add(1)
					return false
				}
				stored := it.entry.Version + 1
				m.removeElement(el)
				l = listx.New()
				mut(l)
				ent := Entry{Value: l.Encode(), Version: stored, ExpireAt: expireAt, Flags: FlagList}
				return m.insertListLocked(key, ent, l, false)
			} else if it.entry.IsList() {
				l = it.lCache
				if l == nil {
					var err error
					l, err = listx.Decode(it.entry.Value)
					if err != nil {
						return false
					}
				}
				mut(l)
				oldCost := it.cost
				it.entry.Version = it.entry.Version + 1
				if expireAt != 0 {
					it.entry.ExpireAt = expireAt
				}
				it.entry.Flags = FlagList
				it.lCache = l
				it.lDirty = true
				it.cost = int64(len(key)) + 64 + l.ApproxWireBytes()
				m.bytes += it.cost - oldCost
				m.order.MoveToFront(el)
				m.evictLocked()
				_, still := m.items[key]
				return still
			} else {
				return false
			}
		}
		m.removeElement(el)
	}
	l = listx.New()
	mut(l)
	ent := Entry{Value: l.Encode(), Version: 1, ExpireAt: expireAt, Flags: FlagList}
	return m.insertListLocked(key, ent, l, false)
}

func (m *Memory) lPopLocked(key string, version uint64, expireAt int64, left bool) (item []byte, popped, applied bool) {
	el, ok := m.items[key]
	if !ok {
		return nil, false, true
	}
	it := el.Value.(*lruItem)
	if it.entry.Expired(m.now()) {
		m.removeElement(el)
		return nil, false, true
	}
	if it.entry.IsTombstone() {
		if version <= it.entry.Version {
			m.staleSkip.Add(1)
			return nil, false, false
		}
		return nil, false, true
	}
	if !it.entry.IsList() {
		return nil, false, false
	}
	l := it.lCache
	if l == nil {
		var err error
		l, err = listx.Decode(it.entry.Value)
		if err != nil {
			return nil, false, false
		}
	}
	if left {
		item, popped = l.LPop()
	} else {
		item, popped = l.RPop()
	}
	if !popped {
		return nil, false, true
	}
	oldCost := it.cost
	it.entry.Version = it.entry.Version + 1
	if expireAt != 0 {
		it.entry.ExpireAt = expireAt
	}
	it.entry.Flags = FlagList
	it.lCache = l
	it.lDirty = true
	it.cost = int64(len(key)) + 64 + l.ApproxWireBytes()
	m.bytes += it.cost - oldCost
	m.order.MoveToFront(el)
	m.evictLocked()
	return item, true, true
}

func (m *Memory) flushListValueLocked(it *lruItem) {
	if it == nil || !it.lDirty || it.lCache == nil || !it.entry.IsList() {
		return
	}
	blob := it.lCache.Encode()
	oldCost := it.cost
	it.entry.Value = blob
	it.lDirty = false
	it.cost = entryCost(it.key, it.entry)
	m.bytes += it.cost - oldCost
	if m.bytes < 0 {
		m.bytes = 0
	}
}

func (m *Memory) insertListLocked(key string, ent Entry, cache *listx.List, dirty bool) bool {
	cost := entryCost(key, ent)
	if dirty && cache != nil {
		cost = int64(len(key)) + 64 + cache.ApproxWireBytes()
	}
	it := &lruItem{key: key, entry: copyEntry(ent), cost: cost, lCache: cache, lDirty: dirty}
	el := m.order.PushFront(it)
	m.items[key] = el
	m.bytes += cost
	m.evictLocked()
	_, ok := m.items[key]
	return ok
}
