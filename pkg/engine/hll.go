package engine

import (
	"context"
	"fmt"

	"github.com/Code0987/supercache/pkg/hllx"
	"github.com/Code0987/supercache/pkg/keyspace"
	"github.com/Code0987/supercache/pkg/store"
)

// HLLAdd hashes item into a ModeHLL sketch (Redis PFADD). ACK-only.
func (e *Engine) HLLAdd(ctx context.Context, keyspaceName, name string, item []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(item) == 0 {
		return fmt.Errorf("%w: empty hll item", ErrInvalidArgument)
	}
	if err := e.validateKey(keyspaceName, name); err != nil {
		return err
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return err
	}
	if ks.cfg.Mode != keyspace.ModeHLL {
		return fmt.Errorf("%w: HLLAdd requires ModeHLL", ErrInvalidArgument)
	}
	if err := e.validateKeyLen(ks, name); err != nil {
		return err
	}
	if len(item) > e.maxKeyLen {
		return ErrKeyTooLarge
	}
	if err := e.hllNeed(ks); err != nil {
		return err
	}
	c := e.clusterSnapshot()
	if c != nil && c.Ring != nil {
		if owner, ok := c.Ring.Owner(name); ok && owner.ID != "" && owner.ID != c.SelfID {
			if c.Transport == nil || owner.Addr == "" {
				return fmt.Errorf("%w: owner %s has no address", ErrUnavailable, owner.ID)
			}
			return e.hllMutViaOwner(ctx, ks, name, item)
		}
	}
	return e.hllAddLocal(ks, name, item)
}

// HLLCount is the sketch estimate. Missing name → ok=false.
func (e *Engine) HLLCount(ctx context.Context, keyspaceName, name string) (uint64, bool, error) {
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
	if ks.cfg.Mode != keyspace.ModeHLL {
		return 0, false, fmt.Errorf("%w: HLLCount requires ModeHLL", ErrInvalidArgument)
	}
	if err := e.validateKeyLen(ks, name); err != nil {
		return 0, false, err
	}
	if ks.store.HasHLL(name) {
		n, ok := ks.store.HLLCount(name)
		return n, ok, nil
	}
	ent, found, err := e.hllFetchOwner(ctx, ks, name)
	if err != nil || !found {
		return 0, false, err
	}
	return hllx.Count(ent.Value), true, nil
}

func (e *Engine) hllNeed(ks *ksRuntime) error {
	max := e.maxValueSize
	if ks.cfg.MaxValueSize > 0 {
		max = ks.cfg.MaxValueSize
	}
	if max > 0 && hllx.DenseSize > max {
		return ErrValueTooLarge
	}
	return nil
}

func (e *Engine) hllMutViaOwner(ctx context.Context, ks *ksRuntime, name string, item []byte) error {
	c := e.clusterSnapshot()
	owner, _ := c.Ring.Owner(name)
	ent := store.Entry{Value: append([]byte(nil), item...), Flags: store.FlagHLLAdd, Version: 1}
	pctx, cancel := e.peerCtx(ctx, ks)
	defer cancel()
	applied, err := c.Transport.ApplyPut(pctx, owner.Addr, ks.cfg.Name, name, ent, c.Ring.Generation())
	if err != nil {
		return err
	}
	if !applied {
		return fmt.Errorf("%w: hll add rejected", ErrInvalidArgument)
	}
	return nil
}

func (e *Engine) hllAddLocal(ks *ksRuntime, name string, item []byte) error {
	expire := e.expireAt(ks.cfg.TTL)
	max := e.maxValueSize
	if ks.cfg.MaxValueSize > 0 {
		max = ks.cfg.MaxValueSize
	}
	cur, _ := ks.store.PeekVersion(name)
	gate := cur + 1
	applied, tooLarge := ks.store.HLLAdd(name, item, gate, expire, max)
	if tooLarge {
		return ErrValueTooLarge
	}
	if !applied {
		return fmt.Errorf("%w: hll add rejected", ErrInvalidArgument)
	}
	ver, _ := ks.store.PeekVersion(name)
	ks.observeVersion(name, ver)
	e.hllReplicateSnapshot(ks, name, ver, expire)
	return nil
}

func (e *Engine) hllReplicateSnapshot(ks *ksRuntime, name string, ver uint64, expire int64) {
	ent, ok := ks.store.Peek(name)
	if !ok || !ent.IsHLL() {
		return
	}
	e.replicate(ks.cfg.Name, name, store.Entry{
		Value:    ent.Value,
		Version:  ver,
		ExpireAt: expire,
		Flags:    store.FlagHLL,
	}, false)
}

func (e *Engine) hllFetchOwner(ctx context.Context, ks *ksRuntime, name string) (store.Entry, bool, error) {
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
	if err != nil || !res.Found || !res.Entry.IsHLL() {
		return store.Entry{}, false, nil
	}
	if e.holdsReplica(c, ks, name) {
		_ = ks.store.HLLInstall(name, res.Entry.Value, res.Entry.Version, res.Entry.ExpireAt)
	}
	return res.Entry, true, nil
}

func (e *Engine) applyHLLAdd(ks *ksRuntime, name string, item []byte, expireAt int64) bool {
	if len(item) == 0 {
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
	ok, tooLarge := ks.store.HLLAdd(name, item, gate, expireAt, max)
	if !ok || tooLarge {
		return false
	}
	ver, _ := ks.store.PeekVersion(name)
	ks.observeVersion(name, ver)
	e.hllReplicateSnapshot(ks, name, ver, expireAt)
	return true
}

func (e *Engine) applyHLLInstall(ks *ksRuntime, name string, blob []byte, version uint64, expireAt int64) bool {
	if expireAt == 0 {
		expireAt = e.expireAt(ks.cfg.TTL)
	}
	return ks.store.HLLInstall(name, blob, version, expireAt)
}
