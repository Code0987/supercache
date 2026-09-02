package store

import (
	"container/list"

	"github.com/Code0987/supercache/pkg/hllx"
)

// HLLAdd hashes item into the named sketch (creates if missing).
//
// version is only the tombstone-gate floor. The stored version is 1 on create
// or local+1 on a live/tombstone replace — never the inbound number.
func (m *Memory) HLLAdd(key string, item []byte, version uint64, expireAt int64, maxValue int) (applied, tooLarge bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if maxValue > 0 && hllx.DenseSize > maxValue {
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
				return m.hllCommitLocked(el, it, key, nil, item, it.entry.Version+1, expireAt)
			}
			if !it.entry.IsHLL() {
				return false, false
			}
			return m.hllCommitLocked(el, it, key, it.entry.Value, item, it.entry.Version+1, expireAt)
		}
		m.removeElement(el)
	}
	return m.hllInsertLocked(key, item, 1, expireAt)
}

func (m *Memory) hllCommitLocked(el *list.Element, it *lruItem, key string, cur, item []byte, stored uint64, expireAt int64) (applied, tooLarge bool) {
	var work []byte
	if len(cur) == hllx.DenseSize {
		hllx.Add(cur, item)
		work = cur
	} else {
		work = hllx.New()
		hllx.Add(work, item)
	}
	oldCost := it.cost
	it.entry.Version = stored
	it.entry.Flags = FlagHLL
	it.entry.Value = work
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

func (m *Memory) hllInsertLocked(key string, item []byte, stored uint64, expireAt int64) (applied, tooLarge bool) {
	regs := hllx.New()
	hllx.Add(regs, item)
	ent := Entry{Value: regs, Version: stored, ExpireAt: expireAt, Flags: FlagHLL}
	return m.insertHLLLocked(key, ent), false
}

// HLLCount is the sketch estimate. Missing → ok=false.
func (m *Memory) HLLCount(key string) (uint64, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.hasHLLLocked(key) {
		return 0, false
	}
	el := m.items[key]
	it := el.Value.(*lruItem)
	return hllx.Count(it.entry.Value), true
}

// HasHLL is the present-bit: live, unexpired, FlagHLL.
func (m *Memory) HasHLL(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.hasHLLLocked(key)
}

func (m *Memory) hasHLLLocked(key string) bool {
	el, ok := m.items[key]
	if !ok {
		return false
	}
	it := el.Value.(*lruItem)
	if it.entry.Expired(m.now()) {
		m.removeElement(el)
		return false
	}
	return !it.entry.IsTombstone() && it.entry.IsHLL()
}

// HLLInstall is LWW snapshot handoff: keep blob if version > local.
func (m *Memory) HLLInstall(key string, blob []byte, version uint64, expireAt int64) bool {
	if len(blob) != hllx.DenseSize {
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
			} else if it.entry.IsHLL() {
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
	ent := Entry{Value: append([]byte(nil), blob...), Version: version, ExpireAt: expireAt, Flags: FlagHLL}
	return m.insertHLLLocked(key, ent)
}

func (m *Memory) insertHLLLocked(key string, ent Entry) bool {
	cost := entryCost(key, ent)
	it := &lruItem{key: key, entry: copyEntry(ent), cost: cost}
	el := m.order.PushFront(it)
	m.items[key] = el
	m.bytes += cost
	m.evictLocked()
	_, ok := m.items[key]
	return ok
}
