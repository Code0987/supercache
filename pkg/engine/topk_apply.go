package engine

import "fmt"

// topkAddLocal is the owner / single-node write path.
// The store assigns the stored version; we fan PeekVersion after the write.
func (e *Engine) topkAddLocal(ks *ksRuntime, name string, item []byte) error {
	expire := e.expireAt(ks.cfg.TTL)
	max := e.maxValueSize
	if ks.cfg.MaxValueSize > 0 {
		max = ks.cfg.MaxValueSize
	}
	k := ks.cfg.EffectiveTopKSize()
	cur, _ := ks.store.PeekVersion(name)
	gate := cur + 1 // tombstone floor only
	applied, tooLarge := ks.store.TopKAdd(name, item, gate, expire, k, max)
	if tooLarge {
		return ErrValueTooLarge
	}
	if !applied {
		return fmt.Errorf("%w: topk add rejected", ErrInvalidArgument)
	}
	ver, _ := ks.store.PeekVersion(name)
	ks.observeVersion(name, ver)
	e.topkReplicateSnapshot(ks, name, ver, expire)
	return nil
}

// applyTopKAdd is the owner-inbox ApplyPut of FlagTopKAdd (raw item).
// Non-owners must not reach here (ApplyPut returns applied=false first).
func (e *Engine) applyTopKAdd(ks *ksRuntime, name string, item []byte, expireAt int64) bool {
	if len(item) == 0 || len(item) > e.itemMax(ks) {
		return false
	}
	if expireAt == 0 {
		expireAt = e.expireAt(ks.cfg.TTL)
	}
	max := e.maxValueSize
	if ks.cfg.MaxValueSize > 0 {
		max = ks.cfg.MaxValueSize
	}
	k := ks.cfg.EffectiveTopKSize()
	cur, _ := ks.store.PeekVersion(name)
	gate := cur + 1
	ok, tooLarge := ks.store.TopKAdd(name, item, gate, expireAt, k, max)
	if !ok || tooLarge {
		return false
	}
	ver, _ := ks.store.PeekVersion(name)
	ks.observeVersion(name, ver)
	e.topkReplicateSnapshot(ks, name, ver, expireAt)
	return true
}

// applyTopKInstall is replica snapshot handoff (incoming > local).
func (e *Engine) applyTopKInstall(ks *ksRuntime, name string, blob []byte, version uint64, expireAt int64) bool {
	if expireAt == 0 {
		expireAt = e.expireAt(ks.cfg.TTL)
	}
	return ks.store.TopKInstall(name, blob, version, expireAt, ks.cfg.EffectiveTopKSize())
}
