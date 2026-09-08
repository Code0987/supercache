package engine

import (
	"context"

	"github.com/Code0987/supercache/pkg/store"
)

func (e *Engine) setMutViaOwner(ctx context.Context, ks *ksRuntime, name string, item []byte, flag uint64) error {
	c := e.clusterSnapshot()
	owner, _ := c.Ring.Owner(name)
	ent := store.Entry{Value: append([]byte(nil), item...), Flags: flag, Version: 1}
	pctx, cancel := e.peerCtx(ctx, ks)
	defer cancel()
	_, err := c.Transport.ApplyPut(pctx, owner.Addr, ks.cfg.Name, name, ent, c.Ring.Generation())
	return err
}
