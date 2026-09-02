package engine

import "fmt"

// cIncrLocal is the owner / single-node write path.
// The store assigns the stored version; we fan PeekVersion after the write.
func (e *Engine) cIncrLocal(ks *ksRuntime, name string, delta int64, fanout bool) (int64, error) {
	expire := e.expireAt(ks.cfg.TTL)
	cur, _ := ks.store.PeekVersion(name)
	gate := cur + 1
	n, applied, overflow := ks.store.CIncr(name, delta, gate, expire)
	if overflow {
		return 0, fmt.Errorf("%w: counter overflow", ErrInvalidArgument)
	}
	if !applied {
		return 0, fmt.Errorf("%w: incr rejected", ErrInvalidArgument)
	}
	ver, _ := ks.store.PeekVersion(name)
	ks.observeVersion(name, ver)
	if fanout {
		e.cReplicateSnapshot(ks, name, n, ver, expire)
	}
	return n, nil
}

// applyCounterInstall is replica / handoff ApplyPut of FlagCounter (LWW replace).
func (e *Engine) applyCounterInstall(ks *ksRuntime, name string, blob []byte, version uint64, expireAt int64) bool {
	if expireAt == 0 {
		expireAt = e.expireAt(ks.cfg.TTL)
	}
	return ks.store.CInstall(name, blob, version, expireAt)
}
