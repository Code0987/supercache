package engine

import (
	"context"
	"fmt"

	"github.com/Code0987/supercache/pkg/store"
)

// bMutViaOwner sends an inbox FlagBitmapSet to the ring owner.
// ACK-only: a rejected apply surfaces as InvalidArgument (no return payload).
func (e *Engine) bMutViaOwner(ctx context.Context, ks *ksRuntime, name string, value []byte) error {
	c := e.clusterSnapshot()
	owner, _ := c.Ring.Owner(name)
	ent := store.Entry{Value: value, Flags: store.FlagBitmapSet, Version: 1}
	pctx, cancel := e.peerCtx(ctx, ks)
	defer cancel()
	applied, err := c.Transport.ApplyPut(pctx, owner.Addr, ks.cfg.Name, name, ent, c.Ring.Generation())
	if err != nil {
		return err
	}
	if !applied {
		return fmt.Errorf("%w: bitmap set rejected", ErrInvalidArgument)
	}
	return nil
}

// bReplicateSnapshot fans the post-write FlagBitmap blob to RF−1 replicas.
// hintID is (ks, name), so a later snapshot replaces a pending hint — both bits stay.
func (e *Engine) bReplicateSnapshot(ks *ksRuntime, name string, ver uint64, expire int64) {
	ent, ok := ks.store.Peek(name)
	if !ok || !ent.IsBitmap() {
		return
	}
	e.replicate(ks.cfg.Name, name, store.Entry{
		Value:    ent.Value,
		Version:  ver,
		ExpireAt: expire,
		Flags:    store.FlagBitmap,
	}, false)
}

// bFetchOwner loads a missing local name from the owner (GetOrLoad).
// RPC / !Found / wrong type → miss + nil error (do not return Unavailable).
// Replicas may install the snapshot; non-replicas do not.
func (e *Engine) bFetchOwner(ctx context.Context, ks *ksRuntime, name string) (store.Entry, bool, error) {
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
	if err != nil || !res.Found || !res.Entry.IsBitmap() {
		return store.Entry{}, false, nil
	}
	if e.holdsReplica(c, ks, name) {
		_ = ks.store.BInstall(name, res.Entry.Value, res.Entry.Version, res.Entry.ExpireAt)
	}
	return res.Entry, true, nil
}
