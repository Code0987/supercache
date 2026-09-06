package store

import (
	"container/list"

	"github.com/Code0987/supercache/pkg/vecset"
)

// VAdd upserts member/vec. version is a tombstone-gate floor; stored = 1 or local+1.
func (m *Memory) VAdd(key string, member []byte, vec []float32, version uint64, expireAt int64, maxValue int) (applied, tooLarge bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(member) < 1 || len(member) > vecset.MaxMemberLen || !vecset.ValidDim(len(vec)) || !vecset.Finite(vec) {
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
				return m.vsInsertOverLocked(el, it, key, member, vec, it.entry.Version+1, expireAt, maxValue)
			}
			if !it.entry.IsVectorSet() {
				return false, false
			}
			return m.vsCommitLocked(el, it, key, member, vec, it.entry.Version+1, expireAt, maxValue)
		}
		m.removeElement(el)
	}
	return m.vsInsertLocked(key, member, vec, 1, expireAt, maxValue)
}

func (m *Memory) vsCommitLocked(el *list.Element, it *lruItem, key string, member []byte, vec []float32, stored uint64, expireAt int64, maxValue int) (applied, tooLarge bool) {
	s, err := vecset.DecodeSnapshot(it.entry.Value)
	if err != nil {
		return false, false
	}
	if s.Dim != len(vec) {
		return false, false
	}
	if err := s.Add(member, vec); err != nil {
		return false, false
	}
	blob := s.Encode()
	if maxValue > 0 && len(blob) > maxValue {
		return false, true
	}
	oldCost := it.cost
	it.entry.Value = blob
	it.entry.Version = stored
	it.entry.Flags = FlagVectorSet
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

func (m *Memory) vsInsertOverLocked(el *list.Element, it *lruItem, key string, member []byte, vec []float32, stored uint64, expireAt int64, maxValue int) (applied, tooLarge bool) {
	s := vecset.NewSet(len(vec))
	if err := s.Add(member, vec); err != nil {
		return false, false
	}
	blob := s.Encode()
	if maxValue > 0 && len(blob) > maxValue {
		return false, true
	}
	oldCost := it.cost
	it.entry.Value = blob
	it.entry.Version = stored
	it.entry.Flags = FlagVectorSet
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

func (m *Memory) vsInsertLocked(key string, member []byte, vec []float32, stored uint64, expireAt int64, maxValue int) (applied, tooLarge bool) {
	s := vecset.NewSet(len(vec))
	if err := s.Add(member, vec); err != nil {
		return false, false
	}
	blob := s.Encode()
	if maxValue > 0 && len(blob) > maxValue {
		return false, true
	}
	ent := Entry{Value: blob, Version: stored, ExpireAt: expireAt, Flags: FlagVectorSet}
	return m.insertVSLocked(key, ent), false
}

func (m *Memory) insertVSLocked(key string, ent Entry) bool {
	cost := entryCost(key, ent)
	it := &lruItem{key: key, entry: copyEntry(ent), cost: cost}
	el := m.order.PushFront(it)
	m.items[key] = el
	m.bytes += cost
	m.evictLocked()
	_, ok := m.items[key]
	return ok
}

// VRem removes member. Missing name is a successful no-op (true).
func (m *Memory) VRem(key string, member []byte, version uint64, expireAt int64) bool {
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
	if !it.entry.IsVectorSet() {
		return false
	}
	s, err := vecset.DecodeSnapshot(it.entry.Value)
	if err != nil {
		return false
	}
	s.Rem(member)
	blob := s.Encode()
	oldCost := it.cost
	it.entry.Value = blob
	it.entry.Version = it.entry.Version + 1
	it.entry.Flags = FlagVectorSet
	if expireAt != 0 {
		it.entry.ExpireAt = expireAt
	}
	it.cost = entryCost(key, it.entry)
	m.bytes += it.cost - oldCost
	if m.bytes < 0 {
		m.bytes = 0
	}
	m.order.MoveToFront(el)
	return true
}

func (m *Memory) liveVSLocked(key string) (*vecset.Set, bool) {
	if !m.hasVSLocked(key) {
		return nil, false
	}
	it := m.items[key].Value.(*lruItem)
	s, err := vecset.DecodeSnapshot(it.entry.Value)
	if err != nil {
		return nil, false
	}
	return s, true
}

func (m *Memory) VSim(key string, vec []float32, k int, metric int) ([]VSimHit, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.liveVSLocked(key)
	if !ok {
		return nil, false
	}
	hits, err := s.Sim(vec, k, vecset.Metric(metric))
	if err != nil {
		return nil, false
	}
	out := make([]VSimHit, len(hits))
	for i, h := range hits {
		out[i] = VSimHit{Member: h.Member, Score: h.Score}
	}
	return out, true
}

func (m *Memory) VCard(key string) (int, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.liveVSLocked(key)
	if !ok {
		return 0, false
	}
	return s.Card(), true
}

func (m *Memory) VDim(key string) (int, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.liveVSLocked(key)
	if !ok {
		return 0, false
	}
	return s.Dim, true
}

func (m *Memory) VEmb(key string, member []byte) ([]float32, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.liveVSLocked(key)
	if !ok {
		return nil, false
	}
	return s.Emb(member)
}

func (m *Memory) HasVectorSet(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.hasVSLocked(key)
}

func (m *Memory) hasVSLocked(key string) bool {
	el, ok := m.items[key]
	if !ok {
		return false
	}
	it := el.Value.(*lruItem)
	if it.entry.Expired(m.now()) {
		m.removeElement(el)
		return false
	}
	return !it.entry.IsTombstone() && it.entry.IsVectorSet()
}

// VSInstall is LWW snapshot handoff.
func (m *Memory) VSInstall(key string, blob []byte, version uint64, expireAt int64) bool {
	if _, err := vecset.DecodeSnapshot(blob); err != nil {
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
			} else if it.entry.IsVectorSet() {
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
	ent := Entry{Value: append([]byte(nil), blob...), Version: version, ExpireAt: expireAt, Flags: FlagVectorSet}
	return m.insertVSLocked(key, ent)
}
