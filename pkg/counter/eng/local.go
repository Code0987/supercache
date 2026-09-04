package eng

import (
	"context"
	"errors"

	"github.com/Code0987/supercache/pkg/counter"
	"github.com/Code0987/supercache/pkg/store"
)

var (
	ErrRejected = errors.New("rejected")
	ErrOverflow = errors.New("overflow")
)

func IncrLocal(h Host, name string, delta int64, fanout bool) (int64, error) {
	expire := h.ExpireAt()
	cur, _ := h.Store().PeekVersion(name)
	gate := cur + 1
	n, applied, overflow := h.Store().CIncr(name, delta, gate, expire)
	if overflow {
		return 0, ErrOverflow
	}
	if !applied {
		return 0, ErrRejected
	}
	ver, _ := h.Store().PeekVersion(name)
	h.ObserveVersion(name, ver)
	if fanout {
		ReplicateSnapshot(h, name, n, ver, expire)
	}
	return n, nil
}

func ApplyInstall(h Host, name string, blob []byte, version uint64, expireAt int64) bool {
	if expireAt == 0 {
		expireAt = h.ExpireAt()
	}
	return h.Store().CInstall(name, blob, version, expireAt)
}

func ReplicateSnapshot(h Host, name string, n int64, ver uint64, expire int64) {
	h.Replicate(name, store.Entry{
		Value:    counter.Encode(n),
		Version:  ver,
		ExpireAt: expire,
		Flags:    store.FlagCounter,
	})
}

func FetchOwner(ctx context.Context, h Host, name string) (store.Entry, bool, error) {
	_, addr, isSelf, ok := h.Owner(name)
	if !ok || isSelf || addr == "" {
		return store.Entry{}, false, nil
	}
	pctx, cancel := h.PeerCtx(ctx)
	defer cancel()
	ent, found, err := h.GetOrLoad(pctx, addr, h.KeyspaceName(), name)
	if err != nil || !found || !ent.IsCounter() {
		return store.Entry{}, false, nil
	}
	if _, decErr := counter.Decode(ent.Value); decErr != nil {
		return store.Entry{}, false, nil
	}
	if h.HoldsReplica(name) {
		_ = h.Store().CInstall(name, ent.Value, ent.Version, ent.ExpireAt)
	}
	return ent, true, nil
}
