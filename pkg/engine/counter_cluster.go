package engine

import (
	"context"

	"github.com/Code0987/supercache/pkg/counter"
	"github.com/Code0987/supercache/pkg/store"
)

// cReplicateSnapshot fans the post-write FlagCounter blob to RF−1 replicas.
func (e *Engine) cReplicateSnapshot(ks *ksRuntime, name string, n int64, ver uint64, expire int64) {
	e.replicate(ks.cfg.Name, name, store.Entry{
		Value:    counter.Encode(n),
		Version:  ver,
		ExpireAt: expire,
		Flags:    store.FlagCounter,
	}, false)
}

// cFetchOwner loads a missing local name from the owner (GetOrLoad).
// RPC / !Found / wrong type / bad blob → miss + nil error (do not return Unavailable).
func (e *Engine) cFetchOwner(ctx context.Context, ks *ksRuntime, name string) (store.Entry, bool, error) {
	c := e.clusterSnapshot()
	if c == nil || c.Ring == nil || c.Transport == nil {
		return store.Entry{}, false, nil
	}
	owner, ok := c.Ring.Owner(name)
	if !ok || owner.ID == "" || owner.ID == c.SelfID || owner.Addr == "" {
		return store.Entry{}, false, nil
	}
	pctx, cancel := e.peerCtx(ctx, ks)
	defer cancel()
	res, err := c.Transport.GetOrLoad(pctx, owner.Addr, ks.cfg.Name, name)
	if err != nil || !res.Found || !res.Entry.IsCounter() {
		return store.Entry{}, false, nil
	}
	if _, decErr := counter.Decode(res.Entry.Value); decErr != nil {
		return store.Entry{}, false, nil
	}
	if e.holdsReplica(c, ks, name) {
		_ = ks.store.CInstall(name, res.Entry.Value, res.Entry.Version, res.Entry.ExpireAt)
	}
	return res.Entry, true, nil
}
