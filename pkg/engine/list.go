package engine

import (
	"context"
	"fmt"

	"github.com/Code0987/supercache/pkg/keyspace"
	"github.com/Code0987/supercache/pkg/listx"
	"github.com/Code0987/supercache/pkg/store"
)

// LPush prepends item on a ModeList.
func (e *Engine) LPush(ctx context.Context, keyspaceName, name string, item []byte) error {
	return e.lPush(ctx, keyspaceName, name, item, true)
}

// RPush appends item on a ModeList.
func (e *Engine) RPush(ctx context.Context, keyspaceName, name string, item []byte) error {
	return e.lPush(ctx, keyspaceName, name, item, false)
}

func (e *Engine) lPush(ctx context.Context, keyspaceName, name string, item []byte, left bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := e.validateKey(keyspaceName, name); err != nil {
		return err
	}
	if len(item) == 0 {
		return fmt.Errorf("%w: empty list item", ErrInvalidArgument)
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return err
	}
	if ks.cfg.Mode != keyspace.ModeList {
		return fmt.Errorf("%w: list op requires ModeList", ErrInvalidArgument)
	}
	if err := e.validateKeyLen(ks, name); err != nil {
		return err
	}
	if len(item) > e.maxKeyLen {
		return ErrKeyTooLarge
	}
	flag := store.FlagListRPush
	if left {
		flag = store.FlagListLPush
	}
	c := e.clusterSnapshot()
	if c != nil && c.Ring != nil {
		if owner, ok := c.Ring.Owner(name); ok && owner.ID != "" && owner.ID != c.SelfID {
			if c.Transport == nil || owner.Addr == "" {
				return fmt.Errorf("%w: owner %s has no address", ErrUnavailable, owner.ID)
			}
			ent := store.Entry{Value: append([]byte(nil), item...), Flags: flag, Version: 1}
			pctx, cancel := e.peerCtx(ctx, ks)
			defer cancel()
			_, err := c.Transport.ApplyPut(pctx, owner.Addr, ks.cfg.Name, name, ent, c.Ring.Generation())
			return err
		}
	}
	return e.lPushLocal(ks, name, item, left, true)
}

// LPop removes and returns the head.
func (e *Engine) LPop(ctx context.Context, keyspaceName, name string) ([]byte, bool, error) {
	return e.lPop(ctx, keyspaceName, name, true)
}

// RPop removes and returns the tail.
func (e *Engine) RPop(ctx context.Context, keyspaceName, name string) ([]byte, bool, error) {
	return e.lPop(ctx, keyspaceName, name, false)
}

func (e *Engine) lPop(ctx context.Context, keyspaceName, name string, left bool) ([]byte, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if err := e.validateKey(keyspaceName, name); err != nil {
		return nil, false, err
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return nil, false, err
	}
	if ks.cfg.Mode != keyspace.ModeList {
		return nil, false, fmt.Errorf("%w: list op requires ModeList", ErrInvalidArgument)
	}
	if err := e.validateKeyLen(ks, name); err != nil {
		return nil, false, err
	}
	c := e.clusterSnapshot()
	if c != nil && c.Ring != nil {
		if owner, ok := c.Ring.Owner(name); ok && owner.ID != "" && owner.ID != c.SelfID {
			if c.Transport == nil || owner.Addr == "" {
				return nil, false, fmt.Errorf("%w: owner %s has no address", ErrUnavailable, owner.ID)
			}
			pctx, cancel := e.peerCtx(ctx, ks)
			defer cancel()
			return c.Transport.ListPop(pctx, owner.Addr, ks.cfg.Name, name, left)
		}
	}
	return e.lPopLocal(ks, name, left, true)
}

// LLen returns list length.
func (e *Engine) LLen(ctx context.Context, keyspaceName, name string) (int, error) {
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
	if ks.cfg.Mode != keyspace.ModeList {
		return 0, fmt.Errorf("%w: LLen requires ModeList", ErrInvalidArgument)
	}
	if err := e.validateKeyLen(ks, name); err != nil {
		return 0, err
	}
	if e.hasListLocal(ks, name) {
		return ks.store.LLen(name), nil
	}
	ent, ok, err := e.lFetchOwner(ctx, ks, name)
	if err != nil || !ok {
		return 0, err
	}
	l, err := listx.Decode(ent.Value)
	if err != nil {
		return 0, nil
	}
	return l.Len(), nil
}

// LIndex returns a copy of the element at idx.
func (e *Engine) LIndex(ctx context.Context, keyspaceName, name string, idx int) ([]byte, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if err := e.validateKey(keyspaceName, name); err != nil {
		return nil, false, err
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return nil, false, err
	}
	if ks.cfg.Mode != keyspace.ModeList {
		return nil, false, fmt.Errorf("%w: LIndex requires ModeList", ErrInvalidArgument)
	}
	if err := e.validateKeyLen(ks, name); err != nil {
		return nil, false, err
	}
	if e.hasListLocal(ks, name) {
		it, ok := ks.store.LIndex(name, idx)
		return it, ok, nil
	}
	ent, found, err := e.lFetchOwner(ctx, ks, name)
	if err != nil || !found {
		return nil, false, err
	}
	l, err := listx.Decode(ent.Value)
	if err != nil {
		return nil, false, nil
	}
	it, ok := l.Index(idx)
	return it, ok, nil
}

// LRange returns a window of elements (Redis-style start/stop).
func (e *Engine) LRange(ctx context.Context, keyspaceName, name string, start, stop int) ([][]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := e.validateKey(keyspaceName, name); err != nil {
		return nil, err
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return nil, err
	}
	if ks.cfg.Mode != keyspace.ModeList {
		return nil, fmt.Errorf("%w: LRange requires ModeList", ErrInvalidArgument)
	}
	if err := e.validateKeyLen(ks, name); err != nil {
		return nil, err
	}
	if e.hasListLocal(ks, name) {
		return ks.store.LRange(name, start, stop), nil
	}
	ent, ok, err := e.lFetchOwner(ctx, ks, name)
	if err != nil || !ok {
		return nil, err
	}
	l, err := listx.Decode(ent.Value)
	if err != nil {
		return nil, nil
	}
	return l.Range(start, stop), nil
}

func (e *Engine) lPushLocal(ks *ksRuntime, name string, item []byte, left, fanout bool) error {
	ver := e.lNextVersion(ks, name)
	expire := e.expireAt(ks.cfg.TTL)
	ok := false
	if left {
		ok = ks.store.LPush(name, item, ver, expire)
	} else {
		ok = ks.store.RPush(name, item, ver, expire)
	}
	if !ok {
		return fmt.Errorf("%w: list push rejected", ErrInvalidArgument)
	}
	if fanout {
		e.lReplicateSnapshot(ks, name, ver, expire)
	}
	return nil
}

func (e *Engine) lPopLocal(ks *ksRuntime, name string, left, fanout bool) ([]byte, bool, error) {
	ver := e.lNextVersion(ks, name)
	expire := e.expireAt(ks.cfg.TTL)
	var item []byte
	var popped, applied bool
	if left {
		item, popped, applied = ks.store.LPop(name, ver, expire)
	} else {
		item, popped, applied = ks.store.RPop(name, ver, expire)
	}
	if !applied {
		if !e.hasListLocal(ks, name) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("%w: list pop rejected", ErrInvalidArgument)
	}
	if popped && fanout {
		e.lReplicateSnapshot(ks, name, ver, expire)
	}
	return item, popped, nil
}

func (e *Engine) lReplicateSnapshot(ks *ksRuntime, name string, ver uint64, expire int64) {
	ent, ok := ks.store.Peek(name)
	if !ok || !ent.IsList() {
		return
	}
	e.replicate(ks.cfg.Name, name, store.Entry{
		Value:    ent.Value,
		Version:  ver,
		ExpireAt: expire,
		Flags:    store.FlagList,
	}, false)
}

func (e *Engine) lNextVersion(ks *ksRuntime, name string) uint64 {
	if ver, ok := ks.store.PeekVersion(name); ok {
		return ks.nextVersion(name, ver)
	}
	return ks.nextVersion(name, 0)
}

func (e *Engine) hasListLocal(ks *ksRuntime, name string) bool {
	return ks.store.HasList(name)
}

func (e *Engine) lFetchOwner(ctx context.Context, ks *ksRuntime, name string) (store.Entry, bool, error) {
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
	if err != nil || !res.Found || !res.Entry.IsList() {
		return store.Entry{}, false, nil
	}
	if e.holdsReplica(c, ks, name) {
		_ = ks.store.LInstall(name, res.Entry.Value, res.Entry.Version, res.Entry.ExpireAt)
	}
	return res.Entry, true, nil
}

func (e *Engine) applyListLPush(ks *ksRuntime, name string, item []byte, _ uint64, expireAt int64) bool {
	ver := e.lNextVersion(ks, name)
	if expireAt == 0 {
		expireAt = e.expireAt(ks.cfg.TTL)
	}
	ok := ks.store.LPush(name, item, ver, expireAt)
	if ok {
		e.lReplicateSnapshot(ks, name, ver, expireAt)
	}
	return ok
}

func (e *Engine) applyListRPush(ks *ksRuntime, name string, item []byte, _ uint64, expireAt int64) bool {
	ver := e.lNextVersion(ks, name)
	if expireAt == 0 {
		expireAt = e.expireAt(ks.cfg.TTL)
	}
	ok := ks.store.RPush(name, item, ver, expireAt)
	if ok {
		e.lReplicateSnapshot(ks, name, ver, expireAt)
	}
	return ok
}

func (e *Engine) applyListInstall(ks *ksRuntime, name string, blob []byte, version uint64, expireAt int64) bool {
	if expireAt == 0 {
		expireAt = e.expireAt(ks.cfg.TTL)
	}
	return ks.store.LInstall(name, blob, version, expireAt)
}
