package engine

import (
	"context"
	"fmt"

	"github.com/Code0987/supercache/pkg/counter"
	"github.com/Code0987/supercache/pkg/keyspace"
)

// Incr adds delta to a ModeCounter and returns the new value.
// Non-owners use peer CounterIncr (the return payload needs a Peer RPC).
func (e *Engine) Incr(ctx context.Context, keyspaceName, name string, delta int64) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := e.validateKey(keyspaceName, name); err != nil {
		return 0, err
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return 0, err
	}
	if ks.cfg.Mode != keyspace.ModeCounter {
		return 0, fmt.Errorf("%w: Incr requires ModeCounter", ErrInvalidArgument)
	}
	if err := e.validateKeyLen(ks, name); err != nil {
		return 0, err
	}
	c := e.clusterSnapshot()
	if c != nil && c.Ring != nil {
		if owner, ok := c.Ring.Owner(name); ok && owner.ID != "" && owner.ID != c.SelfID {
			if c.Transport == nil || owner.Addr == "" {
				return 0, fmt.Errorf("%w: owner %s has no address", ErrUnavailable, owner.ID)
			}
			pctx, cancel := e.peerCtx(ctx, ks)
			defer cancel()
			return c.Transport.CounterIncr(pctx, owner.Addr, ks.cfg.Name, name, delta)
		}
	}
	return e.cIncrLocal(ks, name, delta, true)
}

// CounterGet returns the counter value. Missing → 0, ok=false.
func (e *Engine) CounterGet(ctx context.Context, keyspaceName, name string) (int64, bool, error) {
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	if err := e.validateKey(keyspaceName, name); err != nil {
		return 0, false, err
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return 0, false, err
	}
	if ks.cfg.Mode != keyspace.ModeCounter {
		return 0, false, fmt.Errorf("%w: CounterGet requires ModeCounter", ErrInvalidArgument)
	}
	if err := e.validateKeyLen(ks, name); err != nil {
		return 0, false, err
	}
	if ks.store.HasCounter(name) {
		v, ok := ks.store.CGet(name)
		return v, ok, nil
	}
	ent, found, err := e.cFetchOwner(ctx, ks, name)
	if err != nil || !found {
		return 0, false, err
	}
	v, decErr := counter.Decode(ent.Value)
	if decErr != nil {
		return 0, false, nil
	}
	return v, true, nil
}
