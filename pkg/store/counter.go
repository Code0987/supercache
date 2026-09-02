package store

import (
	"github.com/Code0987/supercache/pkg/counter"
)

// CIncr adds delta to the named counter (creates if missing).
//
// version is only the tombstone-gate floor. The stored version is 1 on create
// or local+1 on a live/tombstone replace — never the inbound number.
// Returns (value, applied, overflow).
func (m *Memory) CIncr(key string, delta int64, version uint64, expireAt int64) (int64, bool, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if el, ok := m.items[key]; ok {
		it := el.Value.(*lruItem)
		if !it.entry.Expired(m.now()) {
			if it.entry.IsTombstone() {
				if version <= it.entry.Version {
					m.staleSkip.Add(1)
					return 0, false, false
				}
				stored := it.entry.Version + 1
				m.removeElement(el)
				return m.cInsertLocked(key, delta, stored, expireAt)
			}
			if !it.entry.IsCounter() {
				return 0, false, false
			}
			cur, err := counter.Decode(it.entry.Value)
			if err != nil {
				return 0, false, false
			}
			next, err := counter.Add(cur, delta)
			if err != nil {
				return cur, false, true
			}
			it.entry.Version = it.entry.Version + 1
			it.entry.Flags = FlagCounter
			it.entry.Value = counter.Encode(next)
			if expireAt != 0 {
				it.entry.ExpireAt = expireAt
			}
			oldCost := it.cost
			it.cost = entryCost(key, it.entry)
			m.bytes += it.cost - oldCost
			m.order.MoveToFront(el)
			m.evictLocked()
			return next, true, false
		}
		m.removeElement(el)
	}
	return m.cInsertLocked(key, delta, 1, expireAt)
}

func (m *Memory) cInsertLocked(key string, val int64, version uint64, expireAt int64) (int64, bool, bool) {
	ent := Entry{Value: counter.Encode(val), Version: version, ExpireAt: expireAt, Flags: FlagCounter}
	if !m.insertCounterLocked(key, ent) {
		return 0, false, false
	}
	return val, true, false
}

// CGet returns the counter value. Missing → 0, ok=false.
func (m *Memory) CGet(key string) (int64, bool) {
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
	if it.entry.IsTombstone() || !it.entry.IsCounter() {
		return 0, false
	}
	v, err := counter.Decode(it.entry.Value)
	if err != nil {
		return 0, false
	}
	return v, true
}

// HasCounter is the present-bit: live, unexpired, FlagCounter, decodable.
func (m *Memory) HasCounter(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	el, ok := m.items[key]
	if !ok {
		return false
	}
	it := el.Value.(*lruItem)
	if it.entry.Expired(m.now()) {
		m.removeElement(el)
		return false
	}
	if it.entry.IsTombstone() || !it.entry.IsCounter() {
		return false
	}
	_, err := counter.Decode(it.entry.Value)
	return err == nil
}

// CInstall is LWW snapshot handoff: keep blob if version > local.
func (m *Memory) CInstall(key string, blob []byte, version uint64, expireAt int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, err := counter.Decode(blob); err != nil {
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
			} else if it.entry.IsCounter() {
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
	ent := Entry{Value: append([]byte(nil), blob...), Version: version, ExpireAt: expireAt, Flags: FlagCounter}
	return m.insertCounterLocked(key, ent)
}

func (m *Memory) insertCounterLocked(key string, ent Entry) bool {
	cost := entryCost(key, ent)
	it := &lruItem{key: key, entry: copyEntry(ent), cost: cost}
	el := m.order.PushFront(it)
	m.items[key] = el
	m.bytes += cost
	m.evictLocked()
	_, ok := m.items[key]
	return ok
}
