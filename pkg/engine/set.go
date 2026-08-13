package engine

import (
	"context"
	"fmt"

	"github.com/Code0987/supercache/pkg/keyspace"
	"github.com/Code0987/supercache/pkg/store"
)

// SetAdd inserts item into the named set (ModeSet only).
func (e *Engine) SetAdd(ctx context.Context, keyspaceName, name string, item []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := e.validateKey(keyspaceName, name); err != nil {
		return err
	}
	if len(item) == 0 {
		return fmt.Errorf("%w: empty set item", ErrInvalidArgument)
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return err
	}
	if ks.cfg.Mode != keyspace.ModeSet {
		return fmt.Errorf("%w: SetAdd requires ModeSet", ErrInvalidArgument)
	}
	if err := e.validateKeyLen(ks, name); err != nil {
		return err
	}
	if len(item) > e.maxKeyLen {
		return ErrKeyTooLarge
	}
	c := e.clusterSnapshot()
	if c != nil && c.Ring != nil {
		if owner, ok := c.Ring.Owner(name); ok && owner.ID != "" && owner.ID != c.SelfID {
			if c.Transport == nil || owner.Addr == "" {
				return fmt.Errorf("%w: owner %s has no address", ErrUnavailable, owner.ID)
			}
			return e.setMutViaOwner(ctx, ks, name, item, store.FlagSetAdd)
		}
	}
	return e.setAddLocal(ks, name, item, true)
}

// SetRemove removes item from the named set (ModeSet only).
func (e *Engine) SetRemove(ctx context.Context, keyspaceName, name string, item []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := e.validateKey(keyspaceName, name); err != nil {
		return err
	}
	if len(item) == 0 {
		return fmt.Errorf("%w: empty set item", ErrInvalidArgument)
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return err
	}
	if ks.cfg.Mode != keyspace.ModeSet {
		return fmt.Errorf("%w: SetRemove requires ModeSet", ErrInvalidArgument)
	}
	if err := e.validateKeyLen(ks, name); err != nil {
		return err
	}
	if len(item) > e.maxKeyLen {
		return ErrKeyTooLarge
	}
	c := e.clusterSnapshot()
	if c != nil && c.Ring != nil {
		if owner, ok := c.Ring.Owner(name); ok && owner.ID != "" && owner.ID != c.SelfID {
			if c.Transport == nil || owner.Addr == "" {
				return fmt.Errorf("%w: owner %s has no address", ErrUnavailable, owner.ID)
			}
			return e.setMutViaOwner(ctx, ks, name, item, store.FlagSetRemove)
		}
	}
	return e.setRemoveLocal(ks, name, item, true)
}

// SetContains reports exact membership.
func (e *Engine) SetContains(ctx context.Context, keyspaceName, name string, item []byte) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := e.validateKey(keyspaceName, name); err != nil {
		return false, err
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return false, err
	}
	if ks.cfg.Mode != keyspace.ModeSet {
		return false, fmt.Errorf("%w: SetContains requires ModeSet", ErrInvalidArgument)
	}
	if err := e.validateKeyLen(ks, name); err != nil {
		return false, err
	}
	if ks.store.SetContains(name, item) {
		return true, nil
	}
	if e.hasSetLocal(ks, name) {
		return false, nil
	}
	// Forward to owner for non-replica / missing local.
	c := e.clusterSnapshot()
	if c == nil || c.Ring == nil || c.Transport == nil {
		return false, nil
	}
	owner, ok := c.Ring.Owner(name)
	if !ok || owner.ID == "" || owner.ID == c.SelfID || owner.Addr == "" {
		return false, nil
	}
	pctx, cancel := e.peerCtx(ctx, ks)
	defer cancel()
	res, err := c.Transport.GetOrLoad(pctx, owner.Addr, ks.cfg.Name, name)
	if err != nil || !res.Found || !res.Entry.IsSet() {
		return false, nil
	}
	// Install on replica if we should hold a copy.
	if e.holdsReplica(c, ks, name) {
		_ = ks.store.SetInstall(name, res.Entry.Value, res.Entry.Version, res.Entry.ExpireAt)
		return ks.store.SetContains(name, item), nil
	}
	// Non-replica: decode once without storing.
	return setContainsBlob(res.Entry.Value, item), nil
}

// SetCard returns the number of elements (0 if missing).
func (e *Engine) SetCard(ctx context.Context, keyspaceName, name string) (int, error) {
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
	if ks.cfg.Mode != keyspace.ModeSet {
		return 0, fmt.Errorf("%w: SetCard requires ModeSet", ErrInvalidArgument)
	}
	if err := e.validateKeyLen(ks, name); err != nil {
		return 0, err
	}
	if e.hasSetLocal(ks, name) {
		return ks.store.SetCard(name), nil
	}
	c := e.clusterSnapshot()
	if c == nil || c.Ring == nil || c.Transport == nil {
		return 0, nil
	}
	owner, ok := c.Ring.Owner(name)
	if !ok || owner.ID == "" || owner.ID == c.SelfID || owner.Addr == "" {
		return 0, nil
	}
	pctx, cancel := e.peerCtx(ctx, ks)
	defer cancel()
	res, err := c.Transport.GetOrLoad(pctx, owner.Addr, ks.cfg.Name, name)
	if err != nil || !res.Found || !res.Entry.IsSet() {
		return 0, nil
	}
	if e.holdsReplica(c, ks, name) {
		_ = ks.store.SetInstall(name, res.Entry.Value, res.Entry.Version, res.Entry.ExpireAt)
		return ks.store.SetCard(name), nil
	}
	return setCardBlob(res.Entry.Value), nil
}

// SetMembers returns all members (defensive copies). Missing → empty.
func (e *Engine) SetMembers(ctx context.Context, keyspaceName, name string) ([][]byte, error) {
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
	if ks.cfg.Mode != keyspace.ModeSet {
		return nil, fmt.Errorf("%w: SetMembers requires ModeSet", ErrInvalidArgument)
	}
	if err := e.validateKeyLen(ks, name); err != nil {
		return nil, err
	}
	if e.hasSetLocal(ks, name) {
		return ks.store.SetMembers(name), nil
	}
	c := e.clusterSnapshot()
	if c == nil || c.Ring == nil || c.Transport == nil {
		return nil, nil
	}
	owner, ok := c.Ring.Owner(name)
	if !ok || owner.ID == "" || owner.ID == c.SelfID || owner.Addr == "" {
		return nil, nil
	}
	pctx, cancel := e.peerCtx(ctx, ks)
	defer cancel()
	res, err := c.Transport.GetOrLoad(pctx, owner.Addr, ks.cfg.Name, name)
	if err != nil || !res.Found || !res.Entry.IsSet() {
		return nil, nil
	}
	if e.holdsReplica(c, ks, name) {
		_ = ks.store.SetInstall(name, res.Entry.Value, res.Entry.Version, res.Entry.ExpireAt)
		return ks.store.SetMembers(name), nil
	}
	return setMembersBlob(res.Entry.Value), nil
}

func (e *Engine) setMutViaOwner(ctx context.Context, ks *ksRuntime, name string, item []byte, flag uint32) error {
	c := e.clusterSnapshot()
	owner, _ := c.Ring.Owner(name)
	ent := store.Entry{Value: append([]byte(nil), item...), Flags: flag, Version: 1}
	pctx, cancel := e.peerCtx(ctx, ks)
	defer cancel()
	_, err := c.Transport.ApplyPut(pctx, owner.Addr, ks.cfg.Name, name, ent, c.Ring.Generation())
	return err
}

func (e *Engine) setAddLocal(ks *ksRuntime, name string, item []byte, fanout bool) error {
	ver := e.setNextVersion(ks, name)
	expire := e.expireAt(ks.cfg.TTL)
	if !ks.store.SetAdd(name, item, ver, expire) {
		return fmt.Errorf("%w: set add rejected", ErrInvalidArgument)
	}
	// Re-read version after mutate (store may keep same ver on in-place for bloom; we set ver).
	if fanout {
		e.replicate(ks.cfg.Name, name, store.Entry{
			Value:    append([]byte(nil), item...),
			Version:  ver,
			ExpireAt: expire,
			Flags:    store.FlagSetAdd,
		}, false)
	}
	return nil
}

func (e *Engine) setRemoveLocal(ks *ksRuntime, name string, item []byte, fanout bool) error {
	ver := e.setNextVersion(ks, name)
	expire := e.expireAt(ks.cfg.TTL)
	if !ks.store.SetRemove(name, item, ver, expire) {
		// Missing set or tombstone reject — remove of missing is success no-op if no entry.
		if !e.hasSetLocal(ks, name) {
			// If still no set, treat as success (design: no-op if missing).
			return nil
		}
		return fmt.Errorf("%w: set remove rejected", ErrInvalidArgument)
	}
	if fanout {
		e.replicate(ks.cfg.Name, name, store.Entry{
			Value:    append([]byte(nil), item...),
			Version:  ver,
			ExpireAt: expire,
			Flags:    store.FlagSetRemove,
		}, false)
	}
	return nil
}

func (e *Engine) setNextVersion(ks *ksRuntime, name string) uint64 {
	// PeekVersion avoids flushing a dirty set blob on every Add/Remove.
	if ver, ok := ks.store.PeekVersion(name); ok {
		return ks.nextVersion(name, ver)
	}
	return ks.nextVersion(name, 0)
}

func (e *Engine) hasSetLocal(ks *ksRuntime, name string) bool {
	return ks.store.HasSet(name)
}

func (e *Engine) applySetAdd(ks *ksRuntime, name string, item []byte, version uint64, expireAt int64) bool {
	if expireAt == 0 {
		expireAt = e.expireAt(ks.cfg.TTL)
	}
	return ks.store.SetAdd(name, item, version, expireAt)
}

func (e *Engine) applySetRemove(ks *ksRuntime, name string, item []byte, version uint64, expireAt int64) bool {
	if expireAt == 0 {
		expireAt = e.expireAt(ks.cfg.TTL)
	}
	return ks.store.SetRemove(name, item, version, expireAt)
}

func (e *Engine) applySetInstall(ks *ksRuntime, name string, blob []byte, version uint64, expireAt int64) bool {
	if expireAt == 0 {
		expireAt = e.expireAt(ks.cfg.TTL)
	}
	return ks.store.SetInstall(name, blob, version, expireAt)
}
