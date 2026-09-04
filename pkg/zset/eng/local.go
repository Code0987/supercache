package eng

import (
	"context"
	"errors"

	"github.com/Code0987/supercache/pkg/store"
	"github.com/Code0987/supercache/pkg/zset"
)

type Host interface {
	Store() store.Store
	KeyspaceName() string
	ExpireAt() int64
	HasZSet(name string) bool
	NextVersion(name string) uint64
	Replicate(name string, ent store.Entry)
	PeerCtx(ctx context.Context) (context.Context, context.CancelFunc)
	Owner(name string) (id, addr string, isSelf bool, ok bool)
	GetOrLoad(ctx context.Context, addr, keyspace, name string) (store.Entry, bool, error)
	HoldsReplica(name string) bool
}

var ErrRejected = errors.New("rejected")

func AddLocal(h Host, name string, member []byte, score float64, fanout bool) error {
	ver := h.NextVersion(name)
	expire := h.ExpireAt()
	if !h.Store().ZAdd(name, member, score, ver, expire) {
		return ErrRejected
	}
	if fanout {
		h.Replicate(name, store.Entry{
			Value:    zset.EncodeAdd(member, score),
			Version:  ver,
			ExpireAt: expire,
			Flags:    store.FlagZSetAdd,
		})
	}
	return nil
}

func RemLocal(h Host, name string, member []byte, fanout bool) error {
	ver := h.NextVersion(name)
	expire := h.ExpireAt()
	if !h.Store().ZRem(name, member, ver, expire) {
		if !h.HasZSet(name) {
			return nil
		}
		return ErrRejected
	}
	if fanout {
		h.Replicate(name, store.Entry{
			Value:    append([]byte(nil), member...),
			Version:  ver,
			ExpireAt: expire,
			Flags:    store.FlagZSetRem,
		})
	}
	return nil
}

func ApplyAdd(h Host, name string, value []byte, version uint64, expireAt int64) bool {
	member, score, err := zset.DecodeAdd(value)
	if err != nil {
		return false
	}
	if expireAt == 0 {
		expireAt = h.ExpireAt()
	}
	return h.Store().ZAdd(name, member, score, version, expireAt)
}

func ApplyRem(h Host, name string, member []byte, version uint64, expireAt int64) bool {
	if expireAt == 0 {
		expireAt = h.ExpireAt()
	}
	return h.Store().ZRem(name, member, version, expireAt)
}

func ApplyInstall(h Host, name string, blob []byte, version uint64, expireAt int64) bool {
	if expireAt == 0 {
		expireAt = h.ExpireAt()
	}
	return h.Store().ZInstall(name, blob, version, expireAt)
}

func FetchOwner(ctx context.Context, h Host, name string) (store.Entry, bool, error) {
	_, addr, isSelf, ok := h.Owner(name)
	if !ok || isSelf || addr == "" {
		return store.Entry{}, false, nil
	}
	pctx, cancel := h.PeerCtx(ctx)
	defer cancel()
	ent, found, err := h.GetOrLoad(pctx, addr, h.KeyspaceName(), name)
	if err != nil || !found || !ent.IsZSet() {
		return store.Entry{}, false, nil
	}
	if h.HoldsReplica(name) {
		_ = h.Store().ZInstall(name, ent.Value, ent.Version, ent.ExpireAt)
	}
	return ent, true, nil
}
