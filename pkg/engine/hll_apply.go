package engine

import "fmt"

// hllAddLocal is the owner / single-node write path.
// The store assigns the stored version; we fan PeekVersion after the write.
func (e *Engine) hllAddLocal(ks *ksRuntime, name string, item []byte) error {
	expire := e.expireAt(ks.cfg.TTL)
	max := e.maxValueSize
	if ks.cfg.MaxValueSize > 0 {
		max = ks.cfg.MaxValueSize
	}
	cur, _ := ks.store.PeekVersion(name)
	gate := cur + 1
	applied, tooLarge := ks.store.HLLAdd(name, item, gate, expire, max)
	if tooLarge {
		return ErrValueTooLarge
	}
	if !applied {
		return fmt.Errorf("%w: hll add rejected", ErrInvalidArgument)
	}
	ver, _ := ks.store.PeekVersion(name)
	ks.observeVersion(name, ver)
	e.hllReplicateSnapshot(ks, name, ver, expire)
	return nil
}

// applyHLLAdd is the owner-inbox ApplyPut of FlagHLLAdd (raw item).
// Non-owners must not reach here (ApplyPut returns applied=false first).
func (e *Engine) applyHLLAdd(ks *ksRuntime, name string, item []byte, expireAt int64) bool {
	if len(item) == 0 {
		return false
	}
	if expireAt == 0 {
		expireAt = e.expireAt(ks.cfg.TTL)
	}
	max := e.maxValueSize
	if ks.cfg.MaxValueSize > 0 {
		max = ks.cfg.MaxValueSize
	}
	cur, _ := ks.store.PeekVersion(name)
	gate := cur + 1
	ok, tooLarge := ks.store.HLLAdd(name, item, gate, expireAt, max)
	if !ok || tooLarge {
		return false
	}
	ver, _ := ks.store.PeekVersion(name)
	ks.observeVersion(name, ver)
	e.hllReplicateSnapshot(ks, name, ver, expireAt)
	return true
}

// applyHLLInstall is replica / handoff ApplyPut of FlagHLL (LWW replace).
func (e *Engine) applyHLLInstall(ks *ksRuntime, name string, blob []byte, version uint64, expireAt int64) bool {
	if expireAt == 0 {
		expireAt = e.expireAt(ks.cfg.TTL)
	}
	return ks.store.HLLInstall(name, blob, version, expireAt)
}
