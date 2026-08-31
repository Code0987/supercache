package store

import (
	"container/list"

	"github.com/Code0987/supercache/pkg/topkx"
)

// TopKAdd records one observation of item on the named table.
//
// version is only the tombstone-gate floor. The stored version is 1 on create
// or local+1 on a live/tombstone replace — never the inbound number.
// k is the current TopKSize: a live blob with occupied > k is a no-mutate reject
// (operator must Delete the name before shrinking K).
func (m *Memory) TopKAdd(key string, item []byte, version uint64, expireAt int64, k int, maxValue int) (applied, tooLarge bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if k < 1 {
		return false, true
	}
	if len(item) == 0 {
		return false, false
	}

	if el, exists := m.items[key]; exists {
		it := el.Value.(*lruItem)
		if !it.entry.Expired(m.now()) {
			if it.entry.IsTombstone() {
				if version <= it.entry.Version {
					m.staleSkip.Add(1)
					return false, false
				}
				return m.topkReplaceLocked(el, it, key, nil, item, it.entry.Version+1, expireAt, k, maxValue)
			}
			if !it.entry.IsTopK() {
				return false, false
			}
			return m.topkReplaceLocked(el, it, key, it.entry.Value, item, it.entry.Version+1, expireAt, k, maxValue)
		}
		m.removeElement(el)
	}
	return m.topkInsertLocked(key, item, 1, expireAt, k, maxValue)
}

// topkReplaceLocked writes a new snapshot over an existing LRU item (live Top-K
// or a superseded tombstone). Decode/Add failure leaves bytes and cost unchanged.
func (m *Memory) topkReplaceLocked(el *list.Element, it *lruItem, key string, cur, item []byte, stored uint64, expireAt int64, k, maxValue int) (applied, tooLarge bool) {
	tab, blob, too, ok := topkApply(cur, item, k, maxValue)
	if too {
		return false, true
	}
	if !ok || tab == nil {
		return false, false
	}
	oldCost := it.cost
	it.entry.Version = stored
	it.entry.Flags = FlagTopK
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

// topkInsertLocked creates a new FlagTopK entry (missing or expired name).
func (m *Memory) topkInsertLocked(key string, item []byte, stored uint64, expireAt int64, k, maxValue int) (applied, tooLarge bool) {
	tab, blob, too, ok := topkApply(nil, item, k, maxValue)
	if too {
		return false, true
	}
	if !ok || tab == nil {
		return false, false
	}
	ent := Entry{Value: blob, Version: stored, ExpireAt: expireAt, Flags: FlagTopK}
	return m.insertTopKLocked(key, ent), false
}

// topkApply decodes cur (empty = new table), adds item, and re-encodes.
// Decode/Add errors → ok=false (no mutate). Oversize encode → too=true.
func topkApply(cur, item []byte, k, maxValue int) (tab *topkx.Table, blob []byte, too, ok bool) {
	var err error
	if cur == nil {
		tab = topkx.New(k)
	} else {
		tab, err = topkx.Decode(cur, k)
		if err != nil {
			return nil, nil, false, false
		}
	}
	if err = tab.Add(item); err != nil {
		return nil, nil, false, false
	}
	blob = tab.Encode()
	if maxValue > 0 && len(blob) > maxValue {
		return nil, nil, true, false
	}
	return tab, blob, false, true
}

// TopKList returns the chart for a live FlagTopK name.
// Missing, expired, tombstone, or decode failure (including shrink-K) → ok=false.
func (m *Memory) TopKList(key string, k int) ([]TopKEntry, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.hasTopKLocked(key) {
		return nil, false
	}
	el := m.items[key]
	it := el.Value.(*lruItem)
	tab, err := topkx.Decode(it.entry.Value, k)
	if err != nil {
		return nil, false
	}
	rows := tab.List()
	out := make([]TopKEntry, len(rows))
	for i, e := range rows {
		out[i] = TopKEntry{Item: e.Item, Count: e.Count}
	}
	return out, true
}

// HasTopK is the present-bit: live, unexpired, FlagTopK. Does not decode.
// An empty install blob is still present.
func (m *Memory) HasTopK(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.hasTopKLocked(key)
}

func (m *Memory) hasTopKLocked(key string) bool {
	el, ok := m.items[key]
	if !ok {
		return false
	}
	it := el.Value.(*lruItem)
	if it.entry.Expired(m.now()) {
		m.removeElement(el)
		return false
	}
	return !it.entry.IsTombstone() && it.entry.IsTopK()
}

// TopKInstall is LWW snapshot handoff: keep blob as-is if version > local.
// Equal/lower versions and tombstone-gated deletes are ignored. Bad blobs
// (Decode fail, occupied > k) are rejected before the lock.
func (m *Memory) TopKInstall(key string, blob []byte, version uint64, expireAt int64, k int) bool {
	if _, err := topkx.Decode(blob, k); err != nil {
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
			} else if it.entry.IsTopK() {
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
	ent := Entry{Value: append([]byte(nil), blob...), Version: version, ExpireAt: expireAt, Flags: FlagTopK}
	return m.insertTopKLocked(key, ent)
}

// insertTopKLocked links a new FlagTopK item at the LRU front and may evict others.
func (m *Memory) insertTopKLocked(key string, ent Entry) bool {
	cost := entryCost(key, ent)
	it := &lruItem{key: key, entry: copyEntry(ent), cost: cost}
	el := m.order.PushFront(it)
	m.items[key] = el
	m.bytes += cost
	m.evictLocked()
	_, ok := m.items[key]
	return ok
}
