package engine

import (
	"context"
	"fmt"

	"github.com/Code0987/supercache/pkg/cmsx"
	cmseng "github.com/Code0987/supercache/pkg/cmsx/eng"
	"github.com/Code0987/supercache/pkg/keyspace"
	"github.com/Code0987/supercache/pkg/store"
)

// CMSIncr adds n to a ModeCMS sketch. n==0 means 1. ACK-only.
func (e *Engine) CMSIncr(ctx context.Context, keyspaceName, name string, item []byte, n uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(item) == 0 {
		return fmt.Errorf("%w: empty cms item", ErrInvalidArgument)
	}
	if err := e.validateKey(keyspaceName, name); err != nil {
		return err
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return err
	}
	if ks.cfg.Mode != keyspace.ModeCMS {
		return fmt.Errorf("%w: CMSIncr requires ModeCMS", ErrInvalidArgument)
	}
	if err := e.validateKeyLen(ks, name); err != nil {
		return err
	}
	if len(item) > e.maxKeyLen {
		return ErrKeyTooLarge
	}
	if err := e.cmsNeed(ks); err != nil {
		return err
	}
	c := e.clusterSnapshot()
	if c != nil && c.Ring != nil {
		if owner, ok := c.Ring.Owner(name); ok && owner.ID != "" && owner.ID != c.SelfID {
			if c.Transport == nil || owner.Addr == "" {
				return fmt.Errorf("%w: owner %s has no address", ErrUnavailable, owner.ID)
			}
			return e.cmsMutViaOwner(ctx, ks, name, item, n)
		}
	}
	if err := cmseng.IncrLocal(e.modeHost(ks), name, item, n); err == cmseng.ErrTooLarge {
		return ErrValueTooLarge
	} else if err != nil {
		return fmt.Errorf("%w: cms incr rejected", ErrInvalidArgument)
	}
	return nil
}

// CMSQuery is min-of-d for item. Missing name → ok=false.
func (e *Engine) CMSQuery(ctx context.Context, keyspaceName, name string, item []byte) (uint64, bool, error) {
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	if len(item) == 0 {
		return 0, false, fmt.Errorf("%w: empty cms item", ErrInvalidArgument)
	}
	if err := e.validateKey(keyspaceName, name); err != nil {
		return 0, false, err
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return 0, false, err
	}
	if ks.cfg.Mode != keyspace.ModeCMS {
		return 0, false, fmt.Errorf("%w: CMSQuery requires ModeCMS", ErrInvalidArgument)
	}
	if err := e.validateKeyLen(ks, name); err != nil {
		return 0, false, err
	}
	if ks.store.HasCMS(name) {
		n, ok := ks.store.CMSQuery(name, item)
		return n, ok, nil
	}
	ent, found, err := cmseng.FetchOwner(ctx, e.modeHost(ks), name)
	if err != nil || !found {
		return 0, false, err
	}
	return cmsx.Query(ent.Value, item), true, nil
}

func (e *Engine) cmsNeed(ks *ksRuntime) error {
	max := e.maxValueSize
	if ks.cfg.MaxValueSize > 0 {
		max = ks.cfg.MaxValueSize
	}
	if max > 0 && cmsx.DenseSize > max {
		return ErrValueTooLarge
	}
	return nil
}

func (e *Engine) cmsMutViaOwner(ctx context.Context, ks *ksRuntime, name string, item []byte, n uint64) error {
	c := e.clusterSnapshot()
	owner, _ := c.Ring.Owner(name)
	inbox, err := cmsx.EncodeInbox(n, item)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidArgument, err)
	}
	ent := store.Entry{Value: inbox, Flags: store.FlagCMSIncr, Version: 1}
	pctx, cancel := e.peerCtx(ctx, ks)
	defer cancel()
	applied, err := c.Transport.ApplyPut(pctx, owner.Addr, ks.cfg.Name, name, ent, c.Ring.Generation())
	if err != nil {
		return err
	}
	if !applied {
		return fmt.Errorf("%w: cms incr rejected", ErrInvalidArgument)
	}
	return nil
}

func (e *Engine) applyCMSIncr(ks *ksRuntime, name string, inbox []byte, expireAt int64) bool {
	return cmseng.ApplyIncr(e.modeHost(ks), name, inbox, expireAt)
}

func (e *Engine) applyCMSInstall(ks *ksRuntime, name string, blob []byte, version uint64, expireAt int64) bool {
	return cmseng.ApplyInstall(e.modeHost(ks), name, blob, version, expireAt)
}
