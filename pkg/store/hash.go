package store

import (
	"github.com/Code0987/supercache/pkg/hashx"
)

func (m *Memory) HSet(key string, field, value []byte, version uint64, expireAt int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.hMutateLocked(key, version, expireAt, func(h *hashx.Hash) {
		h.Set(field, value)
	})
}

func (m *Memory) HDel(key string, field []byte, version uint64, expireAt int64) bool {
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
	if !it.entry.IsHash() {
		return false
	}
	return m.hMutateLocked(key, version, expireAt, func(h *hashx.Hash) {
		h.Del(field)
	})
}

func (m *Memory) HGet(key string, field []byte) ([]byte, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	h, ok := m.hPeekLocked(key)
	if !ok {
		return nil, false
	}
	return h.Get(field)
}

func (m *Memory) HExists(key string, field []byte) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	h, ok := m.hPeekLocked(key)
	if !ok {
		return false
	}
	return h.Exists(field)
}

func (m *Memory) HLen(key string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	h, ok := m.hPeekLocked(key)
	if !ok {
		return 0
	}
	return h.Len()
}

func (m *Memory) HasHash(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.hPeekLocked(key)
	return ok
}

func (m *Memory) HGetAll(key string) []HashField {
	m.mu.Lock()
	defer m.mu.Unlock()
	h, ok := m.hPeekLocked(key)
	if !ok {
		return nil
	}
	all := h.All()
	if len(all) == 0 {
		return nil
	}
	out := make([]HashField, len(all))
	for i, f := range all {
		out[i] = HashField{Field: f.Field, Value: f.Value}
	}
	return out
}

func (m *Memory) HInstall(key string, blob []byte, version uint64, expireAt int64) bool {
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
			} else if it.entry.IsHash() {
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
	h, err := hashx.Decode(blob)
	if err != nil {
		return false
	}
	ent := Entry{Value: append([]byte(nil), blob...), Version: version, ExpireAt: expireAt, Flags: FlagHash}
	return m.insertHashLocked(key, ent, h, false)
}

func (m *Memory) hPeekLocked(key string) (*hashx.Hash, bool) {
	el, ok := m.items[key]
	if !ok {
		return nil, false
	}
	it := el.Value.(*lruItem)
	if it.entry.Expired(m.now()) {
		m.removeElement(el)
		return nil, false
	}
	if it.entry.IsTombstone() || !it.entry.IsHash() {
		return nil, false
	}
	if it.hCache != nil {
		return it.hCache, true
	}
	h, err := hashx.Decode(it.entry.Value)
	if err != nil {
		return nil, false
	}
	it.hCache = h
	return h, true
}

func (m *Memory) hMutateLocked(key string, version uint64, expireAt int64, mut func(*hashx.Hash)) bool {
	var h *hashx.Hash
	if el, ok := m.items[key]; ok {
		it := el.Value.(*lruItem)
		if !it.entry.Expired(m.now()) {
			if it.entry.IsTombstone() {
				if version <= it.entry.Version {
					m.staleSkip.Add(1)
					return false
				}
				m.removeElement(el)
				h = hashx.New()
			} else if it.entry.IsHash() {
				h = it.hCache
				if h == nil {
					var err error
					h, err = hashx.Decode(it.entry.Value)
					if err != nil {
						return false
					}
				}
				mut(h)
				oldCost := it.cost
				it.entry.Version = version
				if expireAt != 0 {
					it.entry.ExpireAt = expireAt
				}
				it.entry.Flags = FlagHash
				it.hCache = h
				it.hDirty = true
				it.cost = int64(len(key)) + 64 + h.ApproxWireBytes()
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
			h = hashx.New()
		}
	} else {
		h = hashx.New()
	}
	mut(h)
	ent := Entry{Value: h.Encode(), Version: version, ExpireAt: expireAt, Flags: FlagHash}
	return m.insertHashLocked(key, ent, h, false)
}

func (m *Memory) flushHashValueLocked(it *lruItem) {
	if it == nil || !it.hDirty || it.hCache == nil || !it.entry.IsHash() {
		return
	}
	blob := it.hCache.Encode()
	oldCost := it.cost
	it.entry.Value = blob
	it.hDirty = false
	it.cost = entryCost(it.key, it.entry)
	m.bytes += it.cost - oldCost
	if m.bytes < 0 {
		m.bytes = 0
	}
}

func (m *Memory) insertHashLocked(key string, ent Entry, cache *hashx.Hash, dirty bool) bool {
	cost := entryCost(key, ent)
	if dirty && cache != nil {
		cost = int64(len(key)) + 64 + cache.ApproxWireBytes()
	}
	it := &lruItem{key: key, entry: copyEntry(ent), cost: cost, hCache: cache, hDirty: dirty}
	el := m.order.PushFront(it)
	m.items[key] = el
	m.bytes += cost
	m.evictLocked()
	_, ok := m.items[key]
	return ok
}
