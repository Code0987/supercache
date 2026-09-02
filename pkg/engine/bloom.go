package engine

import (
	"context"
	"fmt"

	"github.com/Code0987/supercache/pkg/bloom"
	"github.com/Code0987/supercache/pkg/keyspace"
)

const (
	errEmptyBloomItem        = "%w: empty bloom item"
	errBloomAddRequiresMode  = "%w: BloomAdd requires ModeBloom"
	errBloomTestRequiresMode = "%w: BloomTest requires ModeBloom"
	errBloomAddRejected      = "%w: bloom add rejected"
	errOwnerNoAddress        = "%w: owner %s has no address"
)

// BloomAdd inserts item into the named filter (ModeBloom only).
func (e *Engine) BloomAdd(ctx context.Context, keyspaceName, name string, item []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := e.validateKey(keyspaceName, name); err != nil {
		return err
	}
	if len(item) == 0 {
		return fmt.Errorf(errEmptyBloomItem, ErrInvalidArgument)
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return err
	}
	if ks.cfg.Mode != keyspace.ModeBloom {
		return fmt.Errorf(errBloomAddRequiresMode, ErrInvalidArgument)
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
			// Forward as a flagged Apply via ForwardPut? Owner must BloomAdd.
			// Use peer ApplyPut with FlagBloomAdd after routing like Put.
			return e.bloomAddViaOwner(ctx, ks, name, item)
		}
	}
	return e.bloomAddLocal(ks, name, item, true)
}

// BloomTest reports whether item may be in the named filter.
func (e *Engine) BloomTest(ctx context.Context, keyspaceName, name string, item []byte) (bool, error) {
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
	if ks.cfg.Mode != keyspace.ModeBloom {
		return false, fmt.Errorf(errBloomTestRequiresMode, ErrInvalidArgument)
	}
	if err := e.validateKeyLen(ks, name); err != nil {
		return false, err
	}
	m, k := ks.cfg.EffectiveBloomBits(), ks.cfg.EffectiveBloomHashes()
	if ks.store.BloomTest(name, item, m, k) {
		return true, nil
	}
	// Local filter exists and said no, or filter missing.
	if e.hasBloomLocal(ks, name) {
		return false, nil
	}
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
	if err != nil || !res.Found || !res.Entry.IsBloom() {
		return false, nil
	}
	f, err := bloom.Open(m, k, res.Entry.Value)
	if err != nil {
		return false, nil
	}
	return f.Test(item), nil
}

func (e *Engine) hasBloomLocal(ks *ksRuntime, name string) bool {
	ent, ok := ks.store.Peek(name)
	return ok && ent.IsBloom() && !ent.IsTombstone()
}

// ApplyBloomMerge ORs a snapshot into the named filter (handoff / tests).
func (e *Engine) ApplyBloomMerge(keyspaceName, name string, bits []byte, version uint64) (bool, error) {
	if err := e.validateKey(keyspaceName, name); err != nil {
		return false, err
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return false, err
	}
	ks.observeVersion(name, version)
	return e.applyBloomMerge(ks, name, bits, version, 0), nil
}
