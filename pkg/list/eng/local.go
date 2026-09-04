package eng

import "errors"

// ErrRejected means the store refused the mutate (wrong type / tombstone gate).
var ErrRejected = errors.New("rejected")

// Push writes one item on the owner / single-node path.
func Push(h Host, name string, item []byte, left, fanout bool) error {
	expire := h.ExpireAt()
	cur, _ := h.Store().PeekVersion(name)
	gate := cur + 1
	ok := false
	if left {
		ok = h.Store().LPush(name, item, gate, expire)
	} else {
		ok = h.Store().RPush(name, item, gate, expire)
	}
	if !ok {
		return ErrRejected
	}
	ver, _ := h.Store().PeekVersion(name)
	h.ObserveVersion(name, ver)
	if fanout {
		ReplicateSnapshot(h, name, ver, expire)
	}
	return nil
}

// Pop removes one end on the owner / single-node path.
func Pop(h Host, name string, left, fanout bool) ([]byte, bool, error) {
	expire := h.ExpireAt()
	cur, _ := h.Store().PeekVersion(name)
	gate := cur + 1
	var item []byte
	var popped, applied bool
	if left {
		item, popped, applied = h.Store().LPop(name, gate, expire)
	} else {
		item, popped, applied = h.Store().RPop(name, gate, expire)
	}
	if !applied {
		if !h.HasList(name) {
			return nil, false, nil
		}
		return nil, false, ErrRejected
	}
	if popped && fanout {
		ver, _ := h.Store().PeekVersion(name)
		h.ObserveVersion(name, ver)
		ReplicateSnapshot(h, name, ver, expire)
	}
	return item, popped, nil
}

// ApplyLPush is inbox FlagListLPush on the owner.
func ApplyLPush(h Host, name string, item []byte, expireAt int64) bool {
	if expireAt == 0 {
		expireAt = h.ExpireAt()
	}
	cur, _ := h.Store().PeekVersion(name)
	gate := cur + 1
	if !h.Store().LPush(name, item, gate, expireAt) {
		return false
	}
	ver, _ := h.Store().PeekVersion(name)
	h.ObserveVersion(name, ver)
	ReplicateSnapshot(h, name, ver, expireAt)
	return true
}

// ApplyRPush is inbox FlagListRPush on the owner.
func ApplyRPush(h Host, name string, item []byte, expireAt int64) bool {
	if expireAt == 0 {
		expireAt = h.ExpireAt()
	}
	cur, _ := h.Store().PeekVersion(name)
	gate := cur + 1
	if !h.Store().RPush(name, item, gate, expireAt) {
		return false
	}
	ver, _ := h.Store().PeekVersion(name)
	h.ObserveVersion(name, ver)
	ReplicateSnapshot(h, name, ver, expireAt)
	return true
}

// ApplyInstall is replica / handoff FlagList snapshot.
func ApplyInstall(h Host, name string, blob []byte, version uint64, expireAt int64) bool {
	if expireAt == 0 {
		expireAt = h.ExpireAt()
	}
	return h.Store().LInstall(name, blob, version, expireAt)
}
