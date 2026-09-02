package store

import (
	"container/list"

	"github.com/Code0987/supercache/pkg/jsonx"
)

func (m *Memory) JSet(key, path string, raw []byte, version uint64, expireAt int64, maxValue int) (applied, tooLarge bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	val, err := jsonx.Decode(raw)
	if err != nil {
		return false, false
	}
	p, err := jsonx.ParsePath(path)
	if err != nil {
		return false, false
	}

	if el, ok := m.items[key]; ok {
		it := el.Value.(*lruItem)
		if !it.entry.Expired(m.now()) {
			if it.entry.IsTombstone() {
				if version <= it.entry.Version {
					m.staleSkip.Add(1)
					return false, false
				}
				newDoc, err := m.jSetCreate(p, val)
				if err != nil {
					return false, false
				}
				return m.jCommitReplaceLocked(el, it, key, newDoc, it.entry.Version+1, expireAt, maxValue)
			}
			if !it.entry.IsJSON() {
				return false, false
			}
			doc, ok := m.jDocLocked(it)
			if !ok {
				return false, false
			}
			newDoc, err := jsonx.Set(doc, p, val)
			if err != nil {
				return false, false
			}
			return m.jCommitReplaceLocked(el, it, key, newDoc, it.entry.Version+1, expireAt, maxValue)
		}
		m.removeElement(el)
	}
	newDoc, err := m.jSetCreate(p, val)
	if err != nil {
		return false, false
	}
	return m.jCommitInsertLocked(key, newDoc, 1, expireAt, maxValue)
}

func (m *Memory) jSetCreate(p jsonx.Path, val any) (any, error) {
	if len(p) == 0 {
		return val, nil
	}
	return jsonx.Set(map[string]any{}, p, val)
}

func (m *Memory) JDel(key, path string, version uint64, expireAt int64) (ok, mutated bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	el, exists := m.items[key]
	if !exists {
		return true, false
	}
	it := el.Value.(*lruItem)
	if it.entry.Expired(m.now()) {
		m.removeElement(el)
		return true, false
	}
	if it.entry.IsTombstone() {
		if version <= it.entry.Version {
			m.staleSkip.Add(1)
			return false, false
		}
		return false, false
	}
	if !it.entry.IsJSON() {
		return false, false
	}
	p, err := jsonx.ParsePath(path)
	if err != nil {
		return false, false
	}
	doc, ok := m.jDocLocked(it)
	if !ok {
		return false, false
	}
	newDoc, removed, err := jsonx.Del(doc, p)
	if err != nil {
		return false, false
	}
	if !removed {
		return true, false
	}
	blob, err := jsonx.Encode(newDoc)
	if err != nil {
		return false, false
	}
	oldCost := it.cost
	it.entry.Version = it.entry.Version + 1
	it.entry.Flags = FlagJSON
	it.entry.Value = blob
	if expireAt != 0 {
		it.entry.ExpireAt = expireAt
	}
	it.jCache = newDoc
	it.jDirty = false
	it.cost = entryCost(key, it.entry)
	m.bytes += it.cost - oldCost
	if m.bytes < 0 {
		m.bytes = 0
	}
	m.order.MoveToFront(el)
	m.evictLocked()
	return true, true
}

func (m *Memory) JGet(key, path string) ([]byte, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.hasJSONLocked(key) {
		return nil, false
	}
	p, err := jsonx.ParsePath(path)
	if err != nil {
		return nil, false
	}
	el := m.items[key]
	it := el.Value.(*lruItem)
	if len(p) == 0 && !it.jDirty {
		return append([]byte(nil), it.entry.Value...), true
	}
	doc, ok := m.jDocLocked(it)
	if !ok {
		return nil, false
	}
	node, ok := jsonx.Get(doc, p)
	if !ok {
		return nil, false
	}
	blob, err := jsonx.Encode(node)
	if err != nil {
		return nil, false
	}
	return blob, true
}

func (m *Memory) HasJSON(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.hasJSONLocked(key)
}

func (m *Memory) hasJSONLocked(key string) bool {
	el, ok := m.items[key]
	if !ok {
		return false
	}
	it := el.Value.(*lruItem)
	if it.entry.Expired(m.now()) {
		m.removeElement(el)
		return false
	}
	return !it.entry.IsTombstone() && it.entry.IsJSON()
}

func (m *Memory) JInstall(key string, blob []byte, version uint64, expireAt int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	doc, err := jsonx.Decode(blob)
	if err != nil {
		return false
	}
	if el, ok := m.items[key]; ok {
		it := el.Value.(*lruItem)
		if !it.entry.Expired(m.now()) {
			if it.entry.IsTombstone() {
				if version <= it.entry.Version {
					m.staleSkip.Add(1)
					return false
				}
			} else if it.entry.IsJSON() {
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
	ent := Entry{Value: append([]byte(nil), blob...), Version: version, ExpireAt: expireAt, Flags: FlagJSON}
	return m.insertJSONLocked(key, ent, doc, false)
}

func (m *Memory) jDocLocked(it *lruItem) (any, bool) {
	if it.jDirty {
		return it.jCache, true
	}
	if it.jCache != nil {
		return it.jCache, true
	}
	v, err := jsonx.Decode(it.entry.Value)
	if err != nil {
		return nil, false
	}
	it.jCache = v
	return v, true
}

func (m *Memory) jCommitReplaceLocked(el *list.Element, it *lruItem, key string, newDoc any, stored uint64, expireAt int64, maxValue int) (applied, tooLarge bool) {
	blob, err := jsonx.Encode(newDoc)
	if err != nil {
		return false, false
	}
	if maxValue > 0 && len(blob) > maxValue {
		return false, true
	}
	oldCost := it.cost
	it.entry.Version = stored
	it.entry.Flags = FlagJSON
	it.entry.Value = blob
	if expireAt != 0 {
		it.entry.ExpireAt = expireAt
	}
	it.jCache = newDoc
	it.jDirty = false
	it.cost = entryCost(key, it.entry)
	m.bytes += it.cost - oldCost
	if m.bytes < 0 {
		m.bytes = 0
	}
	m.order.MoveToFront(el)
	m.evictLocked()
	_, still := m.items[key]
	return still, false
}

func (m *Memory) jCommitInsertLocked(key string, newDoc any, stored uint64, expireAt int64, maxValue int) (applied, tooLarge bool) {
	blob, err := jsonx.Encode(newDoc)
	if err != nil {
		return false, false
	}
	if maxValue > 0 && len(blob) > maxValue {
		return false, true
	}
	ent := Entry{Value: blob, Version: stored, ExpireAt: expireAt, Flags: FlagJSON}
	return m.insertJSONLocked(key, ent, newDoc, false), false
}

func (m *Memory) flushJSONValueLocked(it *lruItem) {
	if it == nil || !it.jDirty || !it.entry.IsJSON() {
		return
	}
	blob, err := jsonx.Encode(it.jCache)
	if err != nil {
		return
	}
	oldCost := it.cost
	it.entry.Value = blob
	it.jDirty = false
	it.cost = entryCost(it.key, it.entry)
	m.bytes += it.cost - oldCost
	if m.bytes < 0 {
		m.bytes = 0
	}
}

func (m *Memory) insertJSONLocked(key string, ent Entry, cache any, dirty bool) bool {
	cost := entryCost(key, ent)
	if dirty {
		cost = int64(len(key)) + 64 + jsonx.ApproxWireBytes(cache)
	}
	it := &lruItem{key: key, entry: copyEntry(ent), cost: cost, jCache: cache, jDirty: dirty}
	el := m.order.PushFront(it)
	m.items[key] = el
	m.bytes += cost
	m.evictLocked()
	_, ok := m.items[key]
	return ok
}
