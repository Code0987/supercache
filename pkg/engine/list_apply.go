package engine

import "fmt"

func (e *Engine) lPushLocal(ks *ksRuntime, name string, item []byte, left, fanout bool) error {
	expire := e.expireAt(ks.cfg.TTL)
	cur, _ := ks.store.PeekVersion(name)
	gate := cur + 1
	ok := false
	if left {
		ok = ks.store.LPush(name, item, gate, expire)
	} else {
		ok = ks.store.RPush(name, item, gate, expire)
	}
	if !ok {
		return fmt.Errorf(errListPushRejected, ErrInvalidArgument)
	}
	ver, _ := ks.store.PeekVersion(name)
	ks.observeVersion(name, ver)
	if fanout {
		e.lReplicateSnapshot(ks, name, ver, expire)
	}
	return nil
}

func (e *Engine) lPopLocal(ks *ksRuntime, name string, left, fanout bool) ([]byte, bool, error) {
	expire := e.expireAt(ks.cfg.TTL)
	cur, _ := ks.store.PeekVersion(name)
	gate := cur + 1
	var item []byte
	var popped, applied bool
	if left {
		item, popped, applied = ks.store.LPop(name, gate, expire)
	} else {
		item, popped, applied = ks.store.RPop(name, gate, expire)
	}
	if !applied {
		if !e.hasListLocal(ks, name) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf(errListPopRejected, ErrInvalidArgument)
	}
	if popped && fanout {
		ver, _ := ks.store.PeekVersion(name)
		ks.observeVersion(name, ver)
		e.lReplicateSnapshot(ks, name, ver, expire)
	}
	return item, popped, nil
}

func (e *Engine) applyListLPush(ks *ksRuntime, name string, item []byte, _ uint64, expireAt int64) bool {
	if expireAt == 0 {
		expireAt = e.expireAt(ks.cfg.TTL)
	}
	cur, _ := ks.store.PeekVersion(name)
	gate := cur + 1
	if !ks.store.LPush(name, item, gate, expireAt) {
		return false
	}
	ver, _ := ks.store.PeekVersion(name)
	ks.observeVersion(name, ver)
	e.lReplicateSnapshot(ks, name, ver, expireAt)
	return true
}

func (e *Engine) applyListRPush(ks *ksRuntime, name string, item []byte, _ uint64, expireAt int64) bool {
	if expireAt == 0 {
		expireAt = e.expireAt(ks.cfg.TTL)
	}
	cur, _ := ks.store.PeekVersion(name)
	gate := cur + 1
	if !ks.store.RPush(name, item, gate, expireAt) {
		return false
	}
	ver, _ := ks.store.PeekVersion(name)
	ks.observeVersion(name, ver)
	e.lReplicateSnapshot(ks, name, ver, expireAt)
	return true
}

func (e *Engine) applyListInstall(ks *ksRuntime, name string, blob []byte, version uint64, expireAt int64) bool {
	if expireAt == 0 {
		expireAt = e.expireAt(ks.cfg.TTL)
	}
	return ks.store.LInstall(name, blob, version, expireAt)
}
