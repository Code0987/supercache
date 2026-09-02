package engine

import (
	"fmt"

	"github.com/Code0987/supercache/pkg/jsonx"
)

func (e *Engine) jSetLocal(ks *ksRuntime, name, path string, value []byte) error {
	expire := e.expireAt(ks.cfg.TTL)
	max := e.maxValueSize
	if ks.cfg.MaxValueSize > 0 {
		max = ks.cfg.MaxValueSize
	}
	cur, _ := ks.store.PeekVersion(name)
	gate := cur + 1
	applied, tooLarge := ks.store.JSet(name, path, value, gate, expire, max)
	if tooLarge {
		return ErrValueTooLarge
	}
	if !applied {
		return fmt.Errorf(errJSONSetRejected, ErrInvalidArgument)
	}
	ver, _ := ks.store.PeekVersion(name)
	ks.observeVersion(name, ver)
	e.jReplicateSnapshot(ks, name, ver, expire)
	return nil
}

func (e *Engine) jDelLocal(ks *ksRuntime, name, path string) error {
	expire := e.expireAt(ks.cfg.TTL)
	cur, _ := ks.store.PeekVersion(name)
	gate := cur + 1
	ok, mutated := ks.store.JDel(name, path, gate, expire)
	if !ok {
		return fmt.Errorf(errJSONDelRejected, ErrInvalidArgument)
	}
	if !mutated {
		return nil
	}
	ver, _ := ks.store.PeekVersion(name)
	ks.observeVersion(name, ver)
	e.jReplicateSnapshot(ks, name, ver, expire)
	return nil
}

func (e *Engine) applyJSONSet(ks *ksRuntime, name string, inbox []byte, expireAt int64) bool {
	path, raw, err := jsonx.DecodeSet(inbox)
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
	ok, tooLarge := ks.store.JSet(name, path, raw, gate, expireAt, max)
	if !ok || tooLarge {
		return false
	}
	ver, _ := ks.store.PeekVersion(name)
	ks.observeVersion(name, ver)
	e.jReplicateSnapshot(ks, name, ver, expireAt)
	return true
}

func (e *Engine) applyJSONDel(ks *ksRuntime, name string, path []byte, expireAt int64) bool {
	if expireAt == 0 {
		expireAt = e.expireAt(ks.cfg.TTL)
	}
	cur, _ := ks.store.PeekVersion(name)
	gate := cur + 1
	ok, mutated := ks.store.JDel(name, string(path), gate, expireAt)
	if !ok {
		return false
	}
	if !mutated {
		return true
	}
	ver, _ := ks.store.PeekVersion(name)
	ks.observeVersion(name, ver)
	e.jReplicateSnapshot(ks, name, ver, expireAt)
	return true
}

func (e *Engine) applyJSONInstall(ks *ksRuntime, name string, blob []byte, version uint64, expireAt int64) bool {
	if expireAt == 0 {
		expireAt = e.expireAt(ks.cfg.TTL)
	}
	return ks.store.JInstall(name, blob, version, expireAt)
}
