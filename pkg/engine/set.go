package engine

import (
	"context"
	"fmt"

	"github.com/Code0987/supercache/pkg/keyspace"
	"github.com/Code0987/supercache/pkg/set/eng"
	"github.com/Code0987/supercache/pkg/store"
)

const (
	errEmptySetItem            = "%w: empty set item"
	errSetAddRequiresMode      = "%w: SetAdd requires ModeSet"
	errSetRemoveRequiresMode   = "%w: SetRemove requires ModeSet"
	errSetContainsRequiresMode = "%w: SetContains requires ModeSet"
	errSetCardRequiresMode     = "%w: SetCard requires ModeSet"
	errSetMembersRequiresMode  = "%w: SetMembers requires ModeSet"
	errSetAddRejected          = "%w: set add rejected"
	errSetRemoveRejected       = "%w: set remove rejected"
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
		return fmt.Errorf(errEmptySetItem, ErrInvalidArgument)
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return err
	}
	if ks.cfg.Mode != keyspace.ModeSet {
		return fmt.Errorf(errSetAddRequiresMode, ErrInvalidArgument)
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
				return fmt.Errorf(errOwnerNoAddress, ErrUnavailable, owner.ID)
			}
			return e.setMutViaOwner(ctx, ks, name, item, store.FlagSetAdd)
		}
	}
	if err := eng.AddLocal(e.modeHost(ks), name, item, true); err != nil {
		return fmt.Errorf(errSetAddRejected, ErrInvalidArgument)
	}
	return nil
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
		return fmt.Errorf(errEmptySetItem, ErrInvalidArgument)
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return err
	}
	if ks.cfg.Mode != keyspace.ModeSet {
		return fmt.Errorf(errSetRemoveRequiresMode, ErrInvalidArgument)
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
				return fmt.Errorf(errOwnerNoAddress, ErrUnavailable, owner.ID)
			}
			return e.setMutViaOwner(ctx, ks, name, item, store.FlagSetRemove)
		}
	}
	if err := eng.RemoveLocal(e.modeHost(ks), name, item, true); err != nil {
		return fmt.Errorf(errSetRemoveRejected, ErrInvalidArgument)
	}
	return nil
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
		return false, fmt.Errorf(errSetContainsRequiresMode, ErrInvalidArgument)
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
	return eng.ContainsBlob(res.Entry.Value, item), nil
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
		return 0, fmt.Errorf(errSetCardRequiresMode, ErrInvalidArgument)
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
	return eng.CardBlob(res.Entry.Value), nil
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
		return nil, fmt.Errorf(errSetMembersRequiresMode, ErrInvalidArgument)
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
	return eng.MembersBlob(res.Entry.Value), nil
}

func (e *Engine) hasSetLocal(ks *ksRuntime, name string) bool {
	return ks.store.HasSet(name)
}

func (e *Engine) applySetAdd(ks *ksRuntime, name string, item []byte, version uint64, expireAt int64) bool {
	return eng.ApplyAdd(e.modeHost(ks), name, item, version, expireAt)
}
func (e *Engine) applySetRemove(ks *ksRuntime, name string, item []byte, version uint64, expireAt int64) bool {
	return eng.ApplyRemove(e.modeHost(ks), name, item, version, expireAt)
}
func (e *Engine) applySetInstall(ks *ksRuntime, name string, blob []byte, version uint64, expireAt int64) bool {
	return eng.ApplyInstall(e.modeHost(ks), name, blob, version, expireAt)
}
