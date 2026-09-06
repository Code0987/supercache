package eng

import (
	"context"

	"github.com/Code0987/supercache/pkg/store"
)

// FetchOwner GetOrLoads a snapshot. Installs only if this node holds a replica.
func FetchOwner(ctx context.Context, h Host, name string) (store.Entry, bool, error) {
	_, addr, isSelf, ok := h.Owner(name)
	if !ok || isSelf || addr == "" {
		return store.Entry{}, false, nil
	}
	pctx, cancel := h.PeerCtx(ctx)
	defer cancel()
	ent, found, err := h.GetOrLoad(pctx, addr, h.KeyspaceName(), name)
	if err != nil || !found || !ent.IsVectorSet() {
		return store.Entry{}, false, nil
	}
	if h.HoldsReplica(name) {
		_ = h.Store().VSInstall(name, ent.Value, ent.Version, ent.ExpireAt)
	}
	return ent, true, nil
}
