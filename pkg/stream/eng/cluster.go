package eng

import (
	"context"

	"github.com/Code0987/supercache/pkg/store"
	"github.com/Code0987/supercache/pkg/stream"
)

func FetchOwner(ctx context.Context, h Host, name string) (store.Entry, bool, error) {
	_, addr, isSelf, ok := h.Owner(name)
	if !ok || isSelf || addr == "" {
		return store.Entry{}, false, nil
	}
	pctx, cancel := h.PeerCtx(ctx)
	defer cancel()
	ent, found, err := h.GetOrLoad(pctx, addr, h.KeyspaceName(), name)
	if err != nil || !found || !ent.IsStream() {
		return store.Entry{}, false, nil
	}
	if _, decErr := stream.DecodeSnapshot(ent.Value); decErr != nil {
		return store.Entry{}, false, nil
	}
	if h.HoldsReplica(name) {
		_ = h.Store().SXInstall(name, ent.Value, ent.Version, ent.ExpireAt)
	}
	return ent, true, nil
}
