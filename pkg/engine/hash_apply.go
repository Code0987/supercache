package engine

import (
	"fmt"

	"github.com/Code0987/supercache/pkg/hashx"
	"github.com/Code0987/supercache/pkg/store"
)

func (e *Engine) hSetLocal(ks *ksRuntime, name string, field, value []byte, fanout bool) error {
	ver := e.hNextVersion(ks, name)
	expire := e.expireAt(ks.cfg.TTL)
	if !ks.store.HSet(name, field, value, ver, expire) {
		return fmt.Errorf(errHSetRejected, ErrInvalidArgument)
	}
	if fanout {
		e.replicate(ks.cfg.Name, name, store.Entry{
			Value:    hashx.EncodeSet(field, value),
			Version:  ver,
			ExpireAt: expire,
			Flags:    store.FlagHashSet,
		}, false)
	}
	return nil
}

func (e *Engine) hDelLocal(ks *ksRuntime, name string, field []byte, fanout bool) error {
	ver := e.hNextVersion(ks, name)
	expire := e.expireAt(ks.cfg.TTL)
	if !ks.store.HDel(name, field, ver, expire) {
		if !e.hasHashLocal(ks, name) {
			return nil
		}
		return fmt.Errorf(errHDelRejected, ErrInvalidArgument)
	}
	if fanout {
		e.replicate(ks.cfg.Name, name, store.Entry{
			Value:    append([]byte(nil), field...),
			Version:  ver,
			ExpireAt: expire,
			Flags:    store.FlagHashDel,
		}, false)
	}
	return nil
}

func (e *Engine) hNextVersion(ks *ksRuntime, name string) uint64 {
	if ver, ok := ks.store.PeekVersion(name); ok {
		return ks.nextVersion(name, ver)
	}
	return ks.nextVersion(name, 0)
}

func (e *Engine) applyHashSet(ks *ksRuntime, name string, value []byte, version uint64, expireAt int64) bool {
	field, val, err := hashx.DecodeSet(value)
	if err != nil {
		return false
	}
	if expireAt == 0 {
		expireAt = e.expireAt(ks.cfg.TTL)
	}
	return ks.store.HSet(name, field, val, version, expireAt)
}

func (e *Engine) applyHashDel(ks *ksRuntime, name string, field []byte, version uint64, expireAt int64) bool {
	if expireAt == 0 {
		expireAt = e.expireAt(ks.cfg.TTL)
	}
	return ks.store.HDel(name, field, version, expireAt)
}

func (e *Engine) applyHashInstall(ks *ksRuntime, name string, blob []byte, version uint64, expireAt int64) bool {
	if expireAt == 0 {
		expireAt = e.expireAt(ks.cfg.TTL)
	}
	return ks.store.HInstall(name, blob, version, expireAt)
}
