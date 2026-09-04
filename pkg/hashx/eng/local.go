package eng

import (
	"context"
	"errors"

	"github.com/Code0987/supercache/pkg/hashx"
	"github.com/Code0987/supercache/pkg/store"
)

type Host interface {
	Store() store.Store
	KeyspaceName() string
	ExpireAt() int64
	HasHash(name string) bool
	NextVersion(name string) uint64
	Replicate(name string, ent store.Entry)
	PeerCtx(ctx context.Context) (context.Context, context.CancelFunc)
	Owner(name string) (id, addr string, isSelf bool, ok bool)
	GetOrLoad(ctx context.Context, addr, keyspace, name string) (store.Entry, bool, error)
	HoldsReplica(name string) bool
}

var ErrRejected = errors.New("rejected")

func SetLocal(h Host, name string, field, value []byte, fanout bool) error {
	ver := h.NextVersion(name)
	expire := h.ExpireAt()
	if !h.Store().HSet(name, field, value, ver, expire) {
		return ErrRejected
	}
	if fanout {
		h.Replicate(name, store.Entry{
			Value:    hashx.EncodeSet(field, value),
			Version:  ver,
			ExpireAt: expire,
			Flags:    store.FlagHashSet,
		})
	}
	return nil
}

func DelLocal(h Host, name string, field []byte, fanout bool) error {
	ver := h.NextVersion(name)
	expire := h.ExpireAt()
	if !h.Store().HDel(name, field, ver, expire) {
		if !h.HasHash(name) {
			return nil
		}
		return ErrRejected
	}
	if fanout {
		h.Replicate(name, store.Entry{
			Value:    append([]byte(nil), field...),
			Version:  ver,
			ExpireAt: expire,
			Flags:    store.FlagHashDel,
		})
	}
	return nil
}

func ApplySet(h Host, name string, value []byte, version uint64, expireAt int64) bool {
	field, val, err := hashx.DecodeSet(value)
	if err != nil {
		return false
	}
	if expireAt == 0 {
		expireAt = h.ExpireAt()
	}
	return h.Store().HSet(name, field, val, version, expireAt)
}

func ApplyDel(h Host, name string, field []byte, version uint64, expireAt int64) bool {
	if expireAt == 0 {
		expireAt = h.ExpireAt()
	}
	return h.Store().HDel(name, field, version, expireAt)
}

func ApplyInstall(h Host, name string, blob []byte, version uint64, expireAt int64) bool {
	if expireAt == 0 {
		expireAt = h.ExpireAt()
	}
	return h.Store().HInstall(name, blob, version, expireAt)
}

func FetchOwner(ctx context.Context, h Host, name string) (store.Entry, bool, error) {
	_, addr, isSelf, ok := h.Owner(name)
	if !ok || isSelf || addr == "" {
		return store.Entry{}, false, nil
	}
	pctx, cancel := h.PeerCtx(ctx)
	defer cancel()
	ent, found, err := h.GetOrLoad(pctx, addr, h.KeyspaceName(), name)
	if err != nil || !found || !ent.IsHash() {
		return store.Entry{}, false, nil
	}
	if h.HoldsReplica(name) {
		_ = h.Store().HInstall(name, ent.Value, ent.Version, ent.ExpireAt)
	}
	return ent, true, nil
}
