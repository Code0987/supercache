package engine

import (
	"fmt"

	"github.com/Code0987/supercache/pkg/geo"
	"github.com/Code0987/supercache/pkg/store"
)

func (e *Engine) gAddLocal(ks *ksRuntime, name string, member []byte, lon, lat float64, fanout bool) error {
	ver := e.gNextVersion(ks, name)
	expire := e.expireAt(ks.cfg.TTL)
	if !ks.store.GeoAdd(name, member, lon, lat, ver, expire) {
		return fmt.Errorf(errGeoAddRejected, ErrInvalidArgument)
	}
	if fanout {
		e.replicate(ks.cfg.Name, name, store.Entry{
			Value:    geo.EncodeAdd(member, lon, lat),
			Version:  ver,
			ExpireAt: expire,
			Flags:    store.FlagGeoAdd,
		}, false)
	}
	return nil
}

func (e *Engine) gRemLocal(ks *ksRuntime, name string, member []byte, fanout bool) error {
	ver := e.gNextVersion(ks, name)
	expire := e.expireAt(ks.cfg.TTL)
	if !ks.store.GeoRem(name, member, ver, expire) {
		if !e.hasGeoLocal(ks, name) {
			return nil
		}
		return fmt.Errorf(errGeoRemRejected, ErrInvalidArgument)
	}
	if fanout {
		e.replicate(ks.cfg.Name, name, store.Entry{
			Value:    append([]byte(nil), member...),
			Version:  ver,
			ExpireAt: expire,
			Flags:    store.FlagGeoRem,
		}, false)
	}
	return nil
}

func (e *Engine) gNextVersion(ks *ksRuntime, name string) uint64 {
	if ver, ok := ks.store.PeekVersion(name); ok {
		return ks.nextVersion(name, ver)
	}
	return ks.nextVersion(name, 0)
}

func (e *Engine) applyGeoAdd(ks *ksRuntime, name string, value []byte, version uint64, expireAt int64) bool {
	member, lon, lat, err := geo.DecodeAdd(value)
	if err != nil {
		return false
	}
	if expireAt == 0 {
		expireAt = e.expireAt(ks.cfg.TTL)
	}
	return ks.store.GeoAdd(name, member, lon, lat, version, expireAt)
}

func (e *Engine) applyGeoRem(ks *ksRuntime, name string, member []byte, version uint64, expireAt int64) bool {
	if expireAt == 0 {
		expireAt = e.expireAt(ks.cfg.TTL)
	}
	return ks.store.GeoRem(name, member, version, expireAt)
}

func (e *Engine) applyGeoInstall(ks *ksRuntime, name string, blob []byte, version uint64, expireAt int64) bool {
	if expireAt == 0 {
		expireAt = e.expireAt(ks.cfg.TTL)
	}
	return ks.store.GeoInstall(name, blob, version, expireAt)
}
