package eng

import (
	"context"

	"github.com/Code0987/supercache/pkg/list"
	"github.com/Code0987/supercache/pkg/store"
)

// ReplicateSnapshot fans the post-write FlagList blob to RF−1 replicas.
func ReplicateSnapshot(h Host, name string, ver uint64, expire int64) {
	ent, ok := h.Store().Peek(name)
	if !ok || !ent.IsList() {
		return
	}
	h.Replicate(name, store.Entry{
		Value:    ent.Value,
		Version:  ver,
		ExpireAt: expire,
		Flags:    store.FlagList,
	})
}

// FetchOwner loads a missing local name from the owner (GetOrLoad).
// RPC / !Found / wrong type → miss + nil error.
func FetchOwner(ctx context.Context, h Host, name string) (store.Entry, bool, error) {
	_, addr, isSelf, ok := h.Owner(name)
	if !ok || isSelf || addr == "" {
		return store.Entry{}, false, nil
	}
	pctx, cancel := h.PeerCtx(ctx)
	defer cancel()
	ent, found, err := h.GetOrLoad(pctx, addr, h.KeyspaceName(), name)
	if err != nil || !found || !ent.IsList() {
		return store.Entry{}, false, nil
	}
	if h.HoldsReplica(name) {
		_ = h.Store().LInstall(name, ent.Value, ent.Version, ent.ExpireAt)
	}
	return ent, true, nil
}

// LenFromBlob decodes a snapshot. Bad blob → 0, false.
func LenFromBlob(blob []byte) (int, bool) {
	l, err := list.Decode(blob)
	if err != nil {
		return 0, false
	}
	return l.Len(), true
}

// IndexFromBlob decodes one element. Bad blob → miss.
func IndexFromBlob(blob []byte, idx int) ([]byte, bool) {
	l, err := list.Decode(blob)
	if err != nil {
		return nil, false
	}
	return l.Index(idx)
}

// RangeFromBlob decodes a window. Bad blob → nil.
func RangeFromBlob(blob []byte, start, stop int) [][]byte {
	l, err := list.Decode(blob)
	if err != nil {
		return nil
	}
	return l.Range(start, stop)
}
