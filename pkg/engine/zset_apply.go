package engine

import (
	"fmt"

	"github.com/Code0987/supercache/pkg/store"
	"github.com/Code0987/supercache/pkg/zset"
)

func (e *Engine) zAddLocal(ks *ksRuntime, name string, member []byte, score float64, fanout bool) error {
	ver := e.zNextVersion(ks, name)
	expire := e.expireAt(ks.cfg.TTL)
	if !ks.store.ZAdd(name, member, score, ver, expire) {
		return fmt.Errorf(errZAddRejected, ErrInvalidArgument)
	}
	if fanout {
		e.replicate(ks.cfg.Name, name, store.Entry{
			Value:    zset.EncodeAdd(member, score),
			Version:  ver,
			ExpireAt: expire,
			Flags:    store.FlagZSetAdd,
		}, false)
	}
	return nil
}

func (e *Engine) zRemLocal(ks *ksRuntime, name string, member []byte, fanout bool) error {
	ver := e.zNextVersion(ks, name)
	expire := e.expireAt(ks.cfg.TTL)
	if !ks.store.ZRem(name, member, ver, expire) {
		if !e.hasZSetLocal(ks, name) {
			return nil
		}
		return fmt.Errorf(errZRemRejected, ErrInvalidArgument)
	}
	if fanout {
		e.replicate(ks.cfg.Name, name, store.Entry{
			Value:    append([]byte(nil), member...),
			Version:  ver,
			ExpireAt: expire,
			Flags:    store.FlagZSetRem,
		}, false)
	}
	return nil
}

func (e *Engine) zNextVersion(ks *ksRuntime, name string) uint64 {
	if ver, ok := ks.store.PeekVersion(name); ok {
		return ks.nextVersion(name, ver)
	}
	return ks.nextVersion(name, 0)
}

func (e *Engine) applyZSetAdd(ks *ksRuntime, name string, value []byte, version uint64, expireAt int64) bool {
	member, score, err := zset.DecodeAdd(value)
	if err != nil {
		return false
	}
	if expireAt == 0 {
		expireAt = e.expireAt(ks.cfg.TTL)
	}
	return ks.store.ZAdd(name, member, score, version, expireAt)
}

func (e *Engine) applyZSetRem(ks *ksRuntime, name string, member []byte, version uint64, expireAt int64) bool {
	if expireAt == 0 {
		expireAt = e.expireAt(ks.cfg.TTL)
	}
	return ks.store.ZRem(name, member, version, expireAt)
}

func (e *Engine) applyZSetInstall(ks *ksRuntime, name string, blob []byte, version uint64, expireAt int64) bool {
	if expireAt == 0 {
		expireAt = e.expireAt(ks.cfg.TTL)
	}
	return ks.store.ZInstall(name, blob, version, expireAt)
}
