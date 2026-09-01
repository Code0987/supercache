package engine

import (
	"context"
	"fmt"

	"github.com/Code0987/supercache/pkg/cmsx"
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
	return e.cmsIncrLocal(ks, name, item, n)
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
	ent, found, err := e.cmsFetchOwner(ctx, ks, name)
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

func (e *Engine) cmsIncrLocal(ks *ksRuntime, name string, item []byte, n uint64) error {
	expire := e.expireAt(ks.cfg.TTL)
	max := e.maxValueSize
	if ks.cfg.MaxValueSize > 0 {
		max = ks.cfg.MaxValueSize
	}
	cur, _ := ks.store.PeekVersion(name)
	gate := cur + 1
	applied, tooLarge := ks.store.CMSIncr(name, item, n, gate, expire, max)
	if tooLarge {
		return ErrValueTooLarge
	}
	if !applied {
		return fmt.Errorf("%w: cms incr rejected", ErrInvalidArgument)
	}
	ver, _ := ks.store.PeekVersion(name)
	ks.observeVersion(name, ver)
	e.cmsReplicateSnapshot(ks, name, ver, expire)
	return nil
}

func (e *Engine) cmsReplicateSnapshot(ks *ksRuntime, name string, ver uint64, expire int64) {
	ent, ok := ks.store.Peek(name)
	if !ok || !ent.IsCMS() {
		return
	}
	e.replicate(ks.cfg.Name, name, store.Entry{
		Value:    ent.Value,
		Version:  ver,
		ExpireAt: expire,
		Flags:    store.FlagCMS,
	}, false)
}

func (e *Engine) cmsFetchOwner(ctx context.Context, ks *ksRuntime, name string) (store.Entry, bool, error) {
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
	if err != nil || !res.Found || !res.Entry.IsCMS() {
		return store.Entry{}, false, nil
	}
	if e.holdsReplica(c, ks, name) {
		_ = ks.store.CMSInstall(name, res.Entry.Value, res.Entry.Version, res.Entry.ExpireAt)
	}
	return res.Entry, true, nil
}

func (e *Engine) applyCMSIncr(ks *ksRuntime, name string, inbox []byte, expireAt int64) bool {
	n, item, err := cmsx.DecodeInbox(inbox)
	if err != nil || len(item) == 0 {
		return false
	}
	if expireAt == 0 {
		expireAt = e.expireAt(ks.cfg.TTL)
	}
	max := e.maxValueSize
	if ks.cfg.MaxValueSize > 0 {
		max = ks.cfg.MaxValueSize
	}
	cur, _ := ks.store.PeekVersion(name)
	gate := cur + 1
	ok, tooLarge := ks.store.CMSIncr(name, item, n, gate, expireAt, max)
	if !ok || tooLarge {
		return false
	}
	ver, _ := ks.store.PeekVersion(name)
	ks.observeVersion(name, ver)
	e.cmsReplicateSnapshot(ks, name, ver, expireAt)
	return true
}

func (e *Engine) applyCMSInstall(ks *ksRuntime, name string, blob []byte, version uint64, expireAt int64) bool {
	if expireAt == 0 {
		expireAt = e.expireAt(ks.cfg.TTL)
	}
	return ks.store.CMSInstall(name, blob, version, expireAt)
}
