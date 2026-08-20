package engine

import (
	"context"
	"fmt"

	"github.com/Code0987/supercache/pkg/counter"
	"github.com/Code0987/supercache/pkg/keyspace"
	"github.com/Code0987/supercache/pkg/store"
)

// Incr adds delta to a ModeCounter and returns the new value.
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
	if e.hasCounterLocal(ks, name) {
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

func (e *Engine) cIncrLocal(ks *ksRuntime, name string, delta int64, fanout bool) (int64, error) {
	ver := e.cNextVersion(ks, name)
	expire := e.expireAt(ks.cfg.TTL)
	n, applied, overflow := ks.store.CIncr(name, delta, ver, expire)
	if overflow {
		return 0, fmt.Errorf("%w: counter overflow", ErrInvalidArgument)
	}
	if !applied {
		return 0, fmt.Errorf("%w: incr rejected", ErrInvalidArgument)
	}
	if fanout {
		e.replicate(ks.cfg.Name, name, store.Entry{
			Value:    counter.Encode(n),
			Version:  ver,
			ExpireAt: expire,
			Flags:    store.FlagCounter,
		}, false)
	}
	return n, nil
}

func (e *Engine) cNextVersion(ks *ksRuntime, name string) uint64 {
	if ver, ok := ks.store.PeekVersion(name); ok {
		return ks.nextVersion(name, ver)
	}
	return ks.nextVersion(name, 0)
}

func (e *Engine) hasCounterLocal(ks *ksRuntime, name string) bool {
	return ks.store.HasCounter(name)
}

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

func (e *Engine) applyCounterInstall(ks *ksRuntime, name string, blob []byte, version uint64, expireAt int64) bool {
	if expireAt == 0 {
		expireAt = e.expireAt(ks.cfg.TTL)
	}
	return ks.store.CInstall(name, blob, version, expireAt)
}
