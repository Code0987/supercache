package eng

import (
	"context"
	"errors"

	"github.com/Code0987/supercache/pkg/geo"
	"github.com/Code0987/supercache/pkg/store"
)

var ErrRejected = errors.New("rejected")

func AddLocal(h Host, name string, member []byte, lon, lat float64, fanout bool) error {
	ver := h.NextVersion(name)
	expire := h.ExpireAt()
	if !h.Store().GeoAdd(name, member, lon, lat, ver, expire) {
		return ErrRejected
	}
	if fanout {
		h.Replicate(name, store.Entry{
			Value:    geo.EncodeAdd(member, lon, lat),
			Version:  ver,
			ExpireAt: expire,
			Flags:    store.FlagGeoAdd,
		})
	}
	return nil
}

func RemLocal(h Host, name string, member []byte, fanout bool) error {
	ver := h.NextVersion(name)
	expire := h.ExpireAt()
	if !h.Store().GeoRem(name, member, ver, expire) {
		if !h.HasGeo(name) {
			return nil
		}
		return ErrRejected
	}
	if fanout {
		h.Replicate(name, store.Entry{
			Value:    append([]byte(nil), member...),
			Version:  ver,
			ExpireAt: expire,
			Flags:    store.FlagGeoRem,
		})
	}
	return nil
}

func ApplyAdd(h Host, name string, value []byte, version uint64, expireAt int64) bool {
	member, lon, lat, err := geo.DecodeAdd(value)
	if err != nil {
		return false
	}
	if expireAt == 0 {
		expireAt = h.ExpireAt()
	}
	return h.Store().GeoAdd(name, member, lon, lat, version, expireAt)
}

func ApplyRem(h Host, name string, member []byte, version uint64, expireAt int64) bool {
	if expireAt == 0 {
		expireAt = h.ExpireAt()
	}
	return h.Store().GeoRem(name, member, version, expireAt)
}

func ApplyInstall(h Host, name string, blob []byte, version uint64, expireAt int64) bool {
	if expireAt == 0 {
		expireAt = h.ExpireAt()
	}
	return h.Store().GeoInstall(name, blob, version, expireAt)
}

func FetchOwner(ctx context.Context, h Host, name string) (store.Entry, bool, error) {
	_, addr, isSelf, ok := h.Owner(name)
	if !ok || isSelf || addr == "" {
		return store.Entry{}, false, nil
	}
	pctx, cancel := h.PeerCtx(ctx)
	defer cancel()
	ent, found, err := h.GetOrLoad(pctx, addr, h.KeyspaceName(), name)
	if err != nil || !found || !ent.IsGeo() {
		return store.Entry{}, false, nil
	}
	if h.HoldsReplica(name) {
		_ = h.Store().GeoInstall(name, ent.Value, ent.Version, ent.ExpireAt)
	}
	return ent, true, nil
}
