package store

import (
	"github.com/Code0987/supercache/pkg/bloom"
)

func (m *Memory) BloomAdd(key string, item []byte, mBits, k int, version uint64, expireAt int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.bloomMutateLocked(key, mBits, k, version, expireAt, func(f *bloom.Filter) {
		f.Add(item)
	})
}

func (m *Memory) BloomTest(key string, item []byte, mBits, k int) bool {
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
	if it.entry.IsTombstone() || !it.entry.IsBloom() {
		return false
	}
	f, err := bloom.Open(mBits, k, it.entry.Value)
	if err != nil {
		return false
	}
	return f.Test(item)
}

func (m *Memory) BloomMerge(key string, bits []byte, mBits, k int, version uint64, expireAt int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	remote, err := bloom.Open(mBits, k, bits)
	if err != nil {
		return false
	}
	return m.bloomMutateLocked(key, mBits, k, version, expireAt, func(f *bloom.Filter) {
		_ = f.Merge(remote)
	})
}

func (m *Memory) bloomMutateLocked(key string, mBits, k int, version uint64, expireAt int64, mut func(*bloom.Filter)) bool {
	need := (mBits + 7) / 8
	if el, ok := m.items[key]; ok {
		it := el.Value.(*lruItem)
		if !it.entry.Expired(m.now()) {
			if it.entry.IsTombstone() {
				if version <= it.entry.Version {
					m.staleSkip.Add(1)
					return false
				}
			} else if it.entry.IsBloom() {
				f, err := bloom.Open(mBits, k, it.entry.Value)
				if err != nil {
					return false
				}
				mut(f)
				m.order.MoveToFront(el)
				return true
			} else {
				return false
			}
		}
		m.removeElement(el)
	}
	bits := make([]byte, need)
	f, err := bloom.Open(mBits, k, bits)
	if err != nil {
		return false
	}
	mut(f)
	ent := Entry{Value: bits, Version: version, ExpireAt: expireAt, Flags: FlagBloom}
	cost := entryCost(key, ent)
	if m.maxBytes > 0 && cost > m.maxBytes && m.order.Len() == 0 {
		// Single filter larger than budget: still keep it (same as tombstone overshoot).
	}
	it := &lruItem{key: key, entry: copyEntry(ent), cost: cost}
	el := m.order.PushFront(it)
	m.items[key] = el
	m.bytes += cost
	m.evictLocked()
	// evictLocked must not drop this live bloom if it is the only item.
	if _, ok := m.items[key]; !ok {
		return false
	}
	return true
}
