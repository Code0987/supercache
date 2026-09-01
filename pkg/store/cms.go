package store

import (
	"container/list"

	"github.com/Code0987/supercache/pkg/cmsx"
)

// CMSIncr adds n to the named sketch. n==0 means 1.
//
// version is only the tombstone-gate floor. The stored version is 1 on create
// or local+1 on a live/tombstone replace — never the inbound number.
func (m *Memory) CMSIncr(key string, item []byte, n uint64, version uint64, expireAt int64, maxValue int) (applied, tooLarge bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(item) == 0 {
		return false, false
	}
	if maxValue > 0 && cmsx.DenseSize > maxValue {
		return false, true
	}

	if el, exists := m.items[key]; exists {
		it := el.Value.(*lruItem)
		if !it.entry.Expired(m.now()) {
			if it.entry.IsTombstone() {
				if version <= it.entry.Version {
					m.staleSkip.Add(1)
					return false, false
				}
				return m.cmsInsertOverLocked(el, it, key, item, n, it.entry.Version+1, expireAt)
			}
			if !it.entry.IsCMS() {
				return false, false
			}
			return m.cmsCommitLocked(el, it, key, item, n, it.entry.Version+1, expireAt)
		}
		m.removeElement(el)
	}
	return m.cmsInsertLocked(key, item, n, 1, expireAt)
}

func (m *Memory) cmsCommitLocked(el *list.Element, it *lruItem, key string, item []byte, n uint64, stored uint64, expireAt int64) (applied, tooLarge bool) {
	if len(it.entry.Value) != cmsx.DenseSize {
		return false, false
	}
	if err := cmsx.Incr(it.entry.Value, item, n); err != nil {
		return false, false
	}
	oldCost := it.cost
	it.entry.Version = stored
	it.entry.Flags = FlagCMS
	if expireAt != 0 {
		it.entry.ExpireAt = expireAt
	}
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

func (m *Memory) cmsInsertOverLocked(el *list.Element, it *lruItem, key string, item []byte, n uint64, stored uint64, expireAt int64) (applied, tooLarge bool) {
	blob := cmsx.New()
	if err := cmsx.Incr(blob, item, n); err != nil {
		return false, false
	}
	oldCost := it.cost
	it.entry.Version = stored
	it.entry.Flags = FlagCMS
	it.entry.Value = blob
	if expireAt != 0 {
		it.entry.ExpireAt = expireAt
	}
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

func (m *Memory) cmsInsertLocked(key string, item []byte, n uint64, stored uint64, expireAt int64) (applied, tooLarge bool) {
	blob := cmsx.New()
	if err := cmsx.Incr(blob, item, n); err != nil {
		return false, false
	}
	ent := Entry{Value: blob, Version: stored, ExpireAt: expireAt, Flags: FlagCMS}
	return m.insertCMSLocked(key, ent), false
}

// CMSQuery is min-of-d for item on a live FlagCMS name.
func (m *Memory) CMSQuery(key string, item []byte) (uint64, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.hasCMSLocked(key) {
		return 0, false
	}
	el := m.items[key]
	it := el.Value.(*lruItem)
	return cmsx.Query(it.entry.Value, item), true
}

// HasCMS is the present-bit: live, unexpired, FlagCMS. Does not query.
func (m *Memory) HasCMS(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.hasCMSLocked(key)
}

func (m *Memory) hasCMSLocked(key string) bool {
	el, ok := m.items[key]
	if !ok {
		return false
	}
	it := el.Value.(*lruItem)
	if it.entry.Expired(m.now()) {
		m.removeElement(el)
		return false
	}
	return !it.entry.IsTombstone() && it.entry.IsCMS()
}

// CMSInstall is LWW snapshot handoff: keep blob if version > local.
func (m *Memory) CMSInstall(key string, blob []byte, version uint64, expireAt int64) bool {
	if len(blob) != cmsx.DenseSize {
		return false
	}
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
			} else if it.entry.IsCMS() {
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
	ent := Entry{Value: append([]byte(nil), blob...), Version: version, ExpireAt: expireAt, Flags: FlagCMS}
	return m.insertCMSLocked(key, ent)
}

func (m *Memory) insertCMSLocked(key string, ent Entry) bool {
	cost := entryCost(key, ent)
	it := &lruItem{key: key, entry: copyEntry(ent), cost: cost}
	el := m.order.PushFront(it)
	m.items[key] = el
	m.bytes += cost
	m.evictLocked()
	_, ok := m.items[key]
	return ok
}
