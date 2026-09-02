package store

import (
	"container/list"

	"github.com/Code0987/supercache/pkg/bitmapx"
)

// BSet writes a bit on the named bitmap (creates if missing).
//
// version is only the tombstone-gate floor. The stored version is 1 on create
// or local+1 on a live/tombstone replace — never the inbound number.
func (m *Memory) BSet(key string, offset uint64, bit bool, version uint64, expireAt int64, maxValue int) (applied, tooLarge bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	need, ok := bitmapx.EncodedLen(offset)
	if !ok {
		return false, true
	}
	if maxValue > 0 && need > maxValue {
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
				return m.bCommitLocked(el, it, key, nil, offset, bit, it.entry.Version+1, expireAt, maxValue)
			}
			if !it.entry.IsBitmap() {
				return false, false
			}
			return m.bCommitLocked(el, it, key, it.entry.Value, offset, bit, it.entry.Version+1, expireAt, maxValue)
		}
		m.removeElement(el)
	}
	return m.bInsertLocked(key, offset, bit, 1, expireAt, maxValue)
}

func (m *Memory) bCommitLocked(el *list.Element, it *lruItem, key string, cur []byte, offset uint64, bit bool, stored uint64, expireAt int64, maxValue int) (applied, tooLarge bool) {
	var work []byte
	if cur != nil {
		work = append([]byte(nil), cur...)
	}
	next := bitmapx.Set(work, offset, bit)
	if maxValue > 0 && len(next) > maxValue {
		return false, true
	}
	oldCost := it.cost
	it.entry.Version = stored
	it.entry.Flags = FlagBitmap
	it.entry.Value = next
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

func (m *Memory) bInsertLocked(key string, offset uint64, bit bool, stored uint64, expireAt int64, maxValue int) (applied, tooLarge bool) {
	next := bitmapx.Set(nil, offset, bit)
	if maxValue > 0 && len(next) > maxValue {
		return false, true
	}
	ent := Entry{Value: next, Version: stored, ExpireAt: expireAt, Flags: FlagBitmap}
	return m.insertBitmapLocked(key, ent), false
}

// BGet returns the bit at offset. Missing → ok=false.
func (m *Memory) BGet(key string, offset uint64) (bool, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.hasBitmapLocked(key) {
		return false, false
	}
	el := m.items[key]
	it := el.Value.(*lruItem)
	return bitmapx.Get(it.entry.Value, offset), true
}

// BCount is BITCOUNT over a byte window. Missing → ok=false.
func (m *Memory) BCount(key string, start, end int) (int64, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.hasBitmapLocked(key) {
		return 0, false
	}
	el := m.items[key]
	it := el.Value.(*lruItem)
	return bitmapx.Count(it.entry.Value, start, end), true
}

// BPos is BITPOS over a byte window. Missing → ok=false.
func (m *Memory) BPos(key string, bit bool, start, end int) (int64, bool, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.hasBitmapLocked(key) {
		return 0, false, false
	}
	el := m.items[key]
	it := el.Value.(*lruItem)
	pos, found := bitmapx.Pos(it.entry.Value, bit, start, end)
	return pos, found, true
}

// HasBitmap is the present-bit: live, unexpired, FlagBitmap.
func (m *Memory) HasBitmap(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.hasBitmapLocked(key)
}

func (m *Memory) hasBitmapLocked(key string) bool {
	el, ok := m.items[key]
	if !ok {
		return false
	}
	it := el.Value.(*lruItem)
	if it.entry.Expired(m.now()) {
		m.removeElement(el)
		return false
	}
	return !it.entry.IsTombstone() && it.entry.IsBitmap()
}

// BInstall is LWW snapshot handoff: keep blob if version > local.
func (m *Memory) BInstall(key string, blob []byte, version uint64, expireAt int64) bool {
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
			} else if it.entry.IsBitmap() {
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
	ent := Entry{Value: append([]byte(nil), blob...), Version: version, ExpireAt: expireAt, Flags: FlagBitmap}
	return m.insertBitmapLocked(key, ent)
}

func (m *Memory) insertBitmapLocked(key string, ent Entry) bool {
	cost := entryCost(key, ent)
	it := &lruItem{key: key, entry: copyEntry(ent), cost: cost}
	el := m.order.PushFront(it)
	m.items[key] = el
	m.bytes += cost
	m.evictLocked()
	_, ok := m.items[key]
	return ok
}
