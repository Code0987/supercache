package engine

import (
	"context"
	"fmt"

	"github.com/Code0987/supercache/pkg/keyspace"
	listeng "github.com/Code0987/supercache/pkg/list/eng"
	"github.com/Code0987/supercache/pkg/store"
)

const (
	errEmptyListItem      = "%w: empty list item"
	errListOpRequiresMode = "%w: list op requires ModeList"
	errLLenRequiresMode   = "%w: LLen requires ModeList"
	errLIndexRequiresMode = "%w: LIndex requires ModeList"
	errLRangeRequiresMode = "%w: LRange requires ModeList"
	errListPushRejected   = "%w: list push rejected"
	errListPopRejected    = "%w: list pop rejected"
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
		return fmt.Errorf(errEmptyListItem, ErrInvalidArgument)
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return err
	}
	if ks.cfg.Mode != keyspace.ModeList {
		return fmt.Errorf(errListOpRequiresMode, ErrInvalidArgument)
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
				return fmt.Errorf(errOwnerNoAddress, ErrUnavailable, owner.ID)
			}
			ent := store.Entry{Value: append([]byte(nil), item...), Flags: flag, Version: 1}
			pctx, cancel := e.peerCtx(ctx, ks)
			defer cancel()
			_, err := c.Transport.ApplyPut(pctx, owner.Addr, ks.cfg.Name, name, ent, c.Ring.Generation())
			return err
		}
	}
	if err := listeng.Push(e.modeHost(ks), name, item, left, true); err != nil {
		return fmt.Errorf(errListPushRejected, ErrInvalidArgument)
	}
	return nil
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
		return nil, false, fmt.Errorf(errListOpRequiresMode, ErrInvalidArgument)
	}
	if err := e.validateKeyLen(ks, name); err != nil {
		return nil, false, err
	}
	c := e.clusterSnapshot()
	if c != nil && c.Ring != nil {
		if owner, ok := c.Ring.Owner(name); ok && owner.ID != "" && owner.ID != c.SelfID {
			if c.Transport == nil || owner.Addr == "" {
				return nil, false, fmt.Errorf(errOwnerNoAddress, ErrUnavailable, owner.ID)
			}
			pctx, cancel := e.peerCtx(ctx, ks)
			defer cancel()
			return c.Transport.ListPop(pctx, owner.Addr, ks.cfg.Name, name, left)
		}
	}
	item, popped, err := listeng.Pop(e.modeHost(ks), name, left, true)
	if err != nil {
		return nil, false, fmt.Errorf(errListPopRejected, ErrInvalidArgument)
	}
	return item, popped, nil
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
		return 0, fmt.Errorf(errLLenRequiresMode, ErrInvalidArgument)
	}
	if err := e.validateKeyLen(ks, name); err != nil {
		return 0, err
	}
	h := e.modeHost(ks)
	if h.HasList(name) {
		return ks.store.LLen(name), nil
	}
	ent, ok, err := listeng.FetchOwner(ctx, h, name)
	if err != nil || !ok {
		return 0, err
	}
	n, _ := listeng.LenFromBlob(ent.Value)
	return n, nil
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
		return nil, false, fmt.Errorf(errLIndexRequiresMode, ErrInvalidArgument)
	}
	if err := e.validateKeyLen(ks, name); err != nil {
		return nil, false, err
	}
	h := e.modeHost(ks)
	if h.HasList(name) {
		it, ok := ks.store.LIndex(name, idx)
		return it, ok, nil
	}
	ent, found, err := listeng.FetchOwner(ctx, h, name)
	if err != nil || !found {
		return nil, false, err
	}
	it, ok := listeng.IndexFromBlob(ent.Value, idx)
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
		return nil, fmt.Errorf(errLRangeRequiresMode, ErrInvalidArgument)
	}
	if err := e.validateKeyLen(ks, name); err != nil {
		return nil, err
	}
	h := e.modeHost(ks)
	if h.HasList(name) {
		return ks.store.LRange(name, start, stop), nil
	}
	ent, ok, err := listeng.FetchOwner(ctx, h, name)
	if err != nil || !ok {
		return nil, err
	}
	return listeng.RangeFromBlob(ent.Value, start, stop), nil
}

func (e *Engine) applyListLPush(ks *ksRuntime, name string, item []byte, _ uint64, expireAt int64) bool {
	return listeng.ApplyLPush(e.modeHost(ks), name, item, expireAt)
}

func (e *Engine) applyListRPush(ks *ksRuntime, name string, item []byte, _ uint64, expireAt int64) bool {
	return listeng.ApplyRPush(e.modeHost(ks), name, item, expireAt)
}

func (e *Engine) applyListInstall(ks *ksRuntime, name string, blob []byte, version uint64, expireAt int64) bool {
	return listeng.ApplyInstall(e.modeHost(ks), name, blob, version, expireAt)
}
