package engine

import (
	"fmt"

	"github.com/Code0987/supercache/pkg/store"
)

func (e *Engine) bloomAddLocal(ks *ksRuntime, name string, item []byte, fanout bool) error {
	ver := uint64(1)
	if cur, ok := ks.store.Peek(name); ok {
		ver = ks.nextVersion(name, cur.Version)
		if cur.IsBloom() && !cur.IsTombstone() {
			ver = cur.Version
		}
	}
	expire := e.expireAt(ks.cfg.TTL)
	m, k := ks.cfg.EffectiveBloomBits(), ks.cfg.EffectiveBloomHashes()
	if !ks.store.BloomAdd(name, item, m, k, ver, expire) {
		return fmt.Errorf(errBloomAddRejected, ErrInvalidArgument)
	}
	if fanout {
		e.replicate(ks.cfg.Name, name, store.Entry{
			Value:    append([]byte(nil), item...),
			Version:  ver,
			ExpireAt: expire,
			Flags:    store.FlagBloomAdd,
		}, false)
	}
	return nil
}

func (e *Engine) applyBloomAdd(ks *ksRuntime, name string, item []byte, version uint64, expireAt int64) bool {
	m, k := ks.cfg.EffectiveBloomBits(), ks.cfg.EffectiveBloomHashes()
	if expireAt == 0 {
		expireAt = e.expireAt(ks.cfg.TTL)
	}
	return ks.store.BloomAdd(name, item, m, k, version, expireAt)
}

func (e *Engine) applyBloomMerge(ks *ksRuntime, name string, bits []byte, version uint64, expireAt int64) bool {
	m, k := ks.cfg.EffectiveBloomBits(), ks.cfg.EffectiveBloomHashes()
	if expireAt == 0 {
		expireAt = e.expireAt(ks.cfg.TTL)
	}
	return ks.store.BloomMerge(name, bits, m, k, version, expireAt)
}
