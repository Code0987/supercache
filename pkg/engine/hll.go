package engine

import (
	"context"
	"fmt"

	"github.com/Code0987/supercache/pkg/hllx"
	"github.com/Code0987/supercache/pkg/keyspace"
)

// HLLAdd hashes item into a ModeHLL sketch (ACK-only; no Redis changed-bool).
// Non-owners forward an inbox FlagHLLAdd; the owner applies and fans a snapshot.
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
