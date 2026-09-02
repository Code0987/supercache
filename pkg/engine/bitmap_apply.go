package engine

import (
	"fmt"

	"github.com/Code0987/supercache/pkg/bitmapx"
)

// bSetLocal is the owner / single-node write path.
// The store assigns the stored version; we fan PeekVersion after the write.
func (e *Engine) bSetLocal(ks *ksRuntime, name string, offset uint64, bit bool) error {
	expire := e.expireAt(ks.cfg.TTL)
	max := e.maxValueSize
	if ks.cfg.MaxValueSize > 0 {
		max = ks.cfg.MaxValueSize
	}
	cur, _ := ks.store.PeekVersion(name)
	gate := cur + 1
	applied, tooLarge := ks.store.BSet(name, offset, bit, gate, expire, max)
	if tooLarge {
		return ErrValueTooLarge
	}
	if !applied {
		return fmt.Errorf("%w: bitmap set rejected", ErrInvalidArgument)
	}
	ver, _ := ks.store.PeekVersion(name)
	ks.observeVersion(name, ver)
	e.bReplicateSnapshot(ks, name, ver, expire)
	return nil
}

// applyBitmapSet is the owner-inbox ApplyPut of FlagBitmapSet.
// Non-owners must not reach here (ApplyPut returns applied=false first).
func (e *Engine) applyBitmapSet(ks *ksRuntime, name string, inbox []byte, expireAt int64) bool {
	offset, bit, err := bitmapx.DecodeSet(inbox)
	if err != nil {
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
	ok, tooLarge := ks.store.BSet(name, offset, bit, gate, expireAt, max)
	if !ok || tooLarge {
		return false
	}
	ver, _ := ks.store.PeekVersion(name)
	ks.observeVersion(name, ver)
	e.bReplicateSnapshot(ks, name, ver, expireAt)
	return true
}

// applyBitmapInstall is replica / handoff ApplyPut of FlagBitmap (LWW replace).
func (e *Engine) applyBitmapInstall(ks *ksRuntime, name string, blob []byte, version uint64, expireAt int64) bool {
	if expireAt == 0 {
		expireAt = e.expireAt(ks.cfg.TTL)
	}
	return ks.store.BInstall(name, blob, version, expireAt)
}
