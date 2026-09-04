package eng

import (
	"context"
	"errors"

	"github.com/Code0987/supercache/pkg/jsonx"
	"github.com/Code0987/supercache/pkg/store"
)

type Host interface {
	Store() store.Store
	KeyspaceName() string
	ExpireAt() int64
	MaxValue() int
	ObserveVersion(name string, ver uint64)
	Replicate(name string, ent store.Entry)
	PeerCtx(ctx context.Context) (context.Context, context.CancelFunc)
	Owner(name string) (id, addr string, isSelf bool, ok bool)
	GetOrLoad(ctx context.Context, addr, keyspace, name string) (store.Entry, bool, error)
	HoldsReplica(name string) bool
}

var (
	ErrRejected = errors.New("rejected")
	ErrTooLarge = errors.New("too large")
)

func ReplicateSnapshot(h Host, name string, ver uint64, expire int64) {
	ent, ok := h.Store().Peek(name)
	if !ok || !ent.IsJSON() {
		return
	}
	h.Replicate(name, store.Entry{
		Value:    ent.Value,
		Version:  ver,
		ExpireAt: expire,
		Flags:    store.FlagJSON,
	})
}

func SetLocal(h Host, name, path string, value []byte) error {
	expire := h.ExpireAt()
	cur, _ := h.Store().PeekVersion(name)
	gate := cur + 1
	applied, tooLarge := h.Store().JSet(name, path, value, gate, expire, h.MaxValue())
	if tooLarge {
		return ErrTooLarge
	}
	if !applied {
		return ErrRejected
	}
	ver, _ := h.Store().PeekVersion(name)
	h.ObserveVersion(name, ver)
	ReplicateSnapshot(h, name, ver, expire)
	return nil
}

func DelLocal(h Host, name, path string) error {
	expire := h.ExpireAt()
	cur, _ := h.Store().PeekVersion(name)
	gate := cur + 1
	ok, mutated := h.Store().JDel(name, path, gate, expire)
	if !ok {
		return ErrRejected
	}
	if !mutated {
		return nil
	}
	ver, _ := h.Store().PeekVersion(name)
	h.ObserveVersion(name, ver)
	ReplicateSnapshot(h, name, ver, expire)
	return nil
}

func ApplySet(h Host, name string, inbox []byte, expireAt int64) bool {
	path, raw, err := jsonx.DecodeSet(inbox)
	if err != nil {
		return false
	}
	if expireAt == 0 {
		expireAt = h.ExpireAt()
	}
	cur, _ := h.Store().PeekVersion(name)
	gate := cur + 1
	ok, tooLarge := h.Store().JSet(name, path, raw, gate, expireAt, h.MaxValue())
	if !ok || tooLarge {
		return false
	}
	ver, _ := h.Store().PeekVersion(name)
	h.ObserveVersion(name, ver)
	ReplicateSnapshot(h, name, ver, expireAt)
	return true
}

func ApplyDel(h Host, name string, path []byte, expireAt int64) bool {
	if expireAt == 0 {
		expireAt = h.ExpireAt()
	}
	cur, _ := h.Store().PeekVersion(name)
	gate := cur + 1
	ok, mutated := h.Store().JDel(name, string(path), gate, expireAt)
	if !ok {
		return false
	}
	if !mutated {
		return true
	}
	ver, _ := h.Store().PeekVersion(name)
	h.ObserveVersion(name, ver)
	ReplicateSnapshot(h, name, ver, expireAt)
	return true
}

func ApplyInstall(h Host, name string, blob []byte, version uint64, expireAt int64) bool {
	if expireAt == 0 {
		expireAt = h.ExpireAt()
	}
	return h.Store().JInstall(name, blob, version, expireAt)
}

func FetchOwner(ctx context.Context, h Host, name string) (store.Entry, bool, error) {
	_, addr, isSelf, ok := h.Owner(name)
	if !ok || isSelf || addr == "" {
		return store.Entry{}, false, nil
	}
	pctx, cancel := h.PeerCtx(ctx)
	defer cancel()
	ent, found, err := h.GetOrLoad(pctx, addr, h.KeyspaceName(), name)
	if err != nil || !found || !ent.IsJSON() {
		return store.Entry{}, false, nil
	}
	if _, decErr := jsonx.Decode(ent.Value); decErr != nil {
		return store.Entry{}, false, nil
	}
	if h.HoldsReplica(name) {
		_ = h.Store().JInstall(name, ent.Value, ent.Version, ent.ExpireAt)
	}
	return ent, true, nil
}
