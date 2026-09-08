package store

import (
	"container/list"
	"errors"

	"github.com/Code0987/supercache/pkg/stream"
)

// StreamEntry is one XRange row.
type StreamEntry struct {
	ID      string
	Payload []byte
}

func (m *Memory) XAdd(key string, payload []byte, version uint64, expireAt int64, maxValue, autoTrim int) (id string, applied, tooLarge, full bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := stream.NowMilli()
	if el, ok := m.items[key]; ok {
		it := el.Value.(*lruItem)
		if !it.entry.Expired(m.now()) {
			if it.entry.IsTombstone() {
				if version <= it.entry.Version {
					m.staleSkip.Add(1)
					return "", false, false, false
				}
				return m.sxCreateLocked(el, it, key, payload, it.entry.Version+1, expireAt, maxValue, autoTrim, now)
			}
			if !it.entry.IsStream() {
				return "", false, false, false
			}
			return m.sxCommitLocked(el, it, key, payload, it.entry.Version+1, expireAt, maxValue, autoTrim, now)
		}
		m.removeElement(el)
	}
	return m.sxInsertLocked(key, payload, 1, expireAt, maxValue, autoTrim, now)
}

func (m *Memory) sxCommitLocked(el *list.Element, it *lruItem, key string, payload []byte, stored uint64, expireAt int64, maxValue, autoTrim int, now uint64) (string, bool, bool, bool) {
	lg, err := stream.DecodeSnapshot(it.entry.Value)
	if err != nil {
		return "", false, false, false
	}
	id, err := lg.Append(payload, now, autoTrim)
	if errors.Is(err, stream.ErrFull) {
		return "", false, false, true
	}
	if err != nil {
		return "", false, false, false
	}
	blob := lg.Encode()
	if maxValue > 0 && len(blob) > maxValue {
		return "", false, true, false
	}
	oldCost := it.cost
	it.entry.Value = blob
	it.entry.Version = stored
	it.entry.Flags = FlagStream
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
	return id, still, false, false
}

func (m *Memory) sxCreateLocked(el *list.Element, it *lruItem, key string, payload []byte, stored uint64, expireAt int64, maxValue, autoTrim int, now uint64) (string, bool, bool, bool) {
	lg := stream.New()
	id, err := lg.Append(payload, now, autoTrim)
	if errors.Is(err, stream.ErrFull) {
		return "", false, false, true
	}
	if err != nil {
		return "", false, false, false
	}
	blob := lg.Encode()
	if maxValue > 0 && len(blob) > maxValue {
		return "", false, true, false
	}
	oldCost := it.cost
	it.entry.Value = blob
	it.entry.Version = stored
	it.entry.Flags = FlagStream
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
	return id, still, false, false
}

func (m *Memory) sxInsertLocked(key string, payload []byte, stored uint64, expireAt int64, maxValue, autoTrim int, now uint64) (string, bool, bool, bool) {
	lg := stream.New()
	id, err := lg.Append(payload, now, autoTrim)
	if errors.Is(err, stream.ErrFull) {
		return "", false, false, true
	}
	if err != nil {
		return "", false, false, false
	}
	blob := lg.Encode()
	if maxValue > 0 && len(blob) > maxValue {
		return "", false, true, false
	}
	ent := Entry{Value: blob, Version: stored, ExpireAt: expireAt, Flags: FlagStream}
	cost := entryCost(key, ent)
	it := &lruItem{key: key, entry: copyEntry(ent), cost: cost}
	el := m.order.PushFront(it)
	m.items[key] = el
	m.bytes += cost
	m.evictLocked()
	_, ok := m.items[key]
	return id, ok, false, false
}

func (m *Memory) XDel(key, id string, version uint64, expireAt int64) bool {
	ms, seq, err := stream.ParseID(id)
	if err != nil {
		return false
	}
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
	if !it.entry.IsStream() {
		return false
	}
	lg, err := stream.DecodeSnapshot(it.entry.Value)
	if err != nil {
		return false
	}
	lg.Del(ms, seq)
	return m.sxWriteLocked(el, it, key, lg, it.entry.Version+1, expireAt)
}

func (m *Memory) XTrim(key string, maxLen int, version uint64, expireAt int64) bool {
	if maxLen < 0 {
		return false
	}
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
	if !it.entry.IsStream() {
		return false
	}
	lg, err := stream.DecodeSnapshot(it.entry.Value)
	if err != nil {
		return false
	}
	lg.Trim(maxLen)
	return m.sxWriteLocked(el, it, key, lg, it.entry.Version+1, expireAt)
}

func (m *Memory) sxWriteLocked(el *list.Element, it *lruItem, key string, lg *stream.Log, stored uint64, expireAt int64) bool {
	blob := lg.Encode()
	oldCost := it.cost
	it.entry.Value = blob
	it.entry.Version = stored
	it.entry.Flags = FlagStream
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

func (m *Memory) liveStreamLocked(key string) (*stream.Log, bool) {
	el, ok := m.items[key]
	if !ok {
		return nil, false
	}
	it := el.Value.(*lruItem)
	if it.entry.Expired(m.now()) {
		m.removeElement(el)
		return nil, false
	}
	if it.entry.IsTombstone() || !it.entry.IsStream() {
		return nil, false
	}
	lg, err := stream.DecodeSnapshot(it.entry.Value)
	if err != nil {
		return nil, false
	}
	return lg, true
}

func (m *Memory) XRange(key, start, end string, count int, rev bool) ([]StreamEntry, bool, error) {
	sb, err := stream.ParseBound(start, true)
	if err != nil {
		return nil, false, err
	}
	eb, err := stream.ParseBound(end, false)
	if err != nil {
		return nil, false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	lg, ok := m.liveStreamLocked(key)
	if !ok {
		return nil, false, nil
	}
	raw := lg.Range(sb, eb, count, rev)
	out := make([]StreamEntry, len(raw))
	for i, e := range raw {
		out[i] = StreamEntry{ID: e.ID(), Payload: e.Payload}
	}
	return out, true, nil
}

func (m *Memory) XLen(key string) (int, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	lg, ok := m.liveStreamLocked(key)
	if !ok {
		return 0, false
	}
	return len(lg.Entries), true
}

func (m *Memory) HasStream(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.liveStreamLocked(key)
	return ok
}

func (m *Memory) SXInstall(key string, blob []byte, version uint64, expireAt int64) bool {
	if _, err := stream.DecodeSnapshot(blob); err != nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if el, ok := m.items[key]; ok {
		it := el.Value.(*lruItem)
		if !it.entry.Expired(m.now()) {
			if version <= it.entry.Version {
				m.staleSkip.Add(1)
				return false
			}
		}
		m.removeElement(el)
	}
	ent := Entry{Value: append([]byte(nil), blob...), Version: version, ExpireAt: expireAt, Flags: FlagStream}
	cost := entryCost(key, ent)
	it := &lruItem{key: key, entry: copyEntry(ent), cost: cost}
	el := m.order.PushFront(it)
	m.items[key] = el
	m.bytes += cost
	m.evictLocked()
	_, ok := m.items[key]
	return ok
}
