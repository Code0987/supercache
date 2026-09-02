package engine

import (
	"fmt"

	"github.com/Code0987/supercache/pkg/store"
)

func (e *Engine) setAddLocal(ks *ksRuntime, name string, item []byte, fanout bool) error {
	ver := e.setNextVersion(ks, name)
	expire := e.expireAt(ks.cfg.TTL)
	if !ks.store.SetAdd(name, item, ver, expire) {
		return fmt.Errorf(errSetAddRejected, ErrInvalidArgument)
	}
	// Re-read version after mutate (store may keep same ver on in-place for bloom; we set ver).
	if fanout {
		e.replicate(ks.cfg.Name, name, store.Entry{
			Value:    append([]byte(nil), item...),
			Version:  ver,
			ExpireAt: expire,
			Flags:    store.FlagSetAdd,
		}, false)
	}
	return nil
}

func (e *Engine) setRemoveLocal(ks *ksRuntime, name string, item []byte, fanout bool) error {
	ver := e.setNextVersion(ks, name)
	expire := e.expireAt(ks.cfg.TTL)
	if !ks.store.SetRemove(name, item, ver, expire) {
		// Missing set or tombstone reject — remove of missing is success no-op if no entry.
		if !e.hasSetLocal(ks, name) {
			// If still no set, treat as success (design: no-op if missing).
			return nil
		}
		return fmt.Errorf(errSetRemoveRejected, ErrInvalidArgument)
	}
	if fanout {
		e.replicate(ks.cfg.Name, name, store.Entry{
			Value:    append([]byte(nil), item...),
			Version:  ver,
			ExpireAt: expire,
			Flags:    store.FlagSetRemove,
		}, false)
	}
	return nil
}

func (e *Engine) setNextVersion(ks *ksRuntime, name string) uint64 {
	// PeekVersion avoids flushing a dirty set blob on every Add/Remove.
	if ver, ok := ks.store.PeekVersion(name); ok {
		return ks.nextVersion(name, ver)
	}
	return ks.nextVersion(name, 0)
}

func (e *Engine) applySetAdd(ks *ksRuntime, name string, item []byte, version uint64, expireAt int64) bool {
	if expireAt == 0 {
		expireAt = e.expireAt(ks.cfg.TTL)
	}
	return ks.store.SetAdd(name, item, version, expireAt)
}

func (e *Engine) applySetRemove(ks *ksRuntime, name string, item []byte, version uint64, expireAt int64) bool {
	if expireAt == 0 {
		expireAt = e.expireAt(ks.cfg.TTL)
	}
	return ks.store.SetRemove(name, item, version, expireAt)
}

func (e *Engine) applySetInstall(ks *ksRuntime, name string, blob []byte, version uint64, expireAt int64) bool {
	if expireAt == 0 {
		expireAt = e.expireAt(ks.cfg.TTL)
	}
	return ks.store.SetInstall(name, blob, version, expireAt)
}
