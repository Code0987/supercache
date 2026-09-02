package engine

import (
	"context"
	"fmt"

	"github.com/Code0987/supercache/pkg/bitmapx"
	"github.com/Code0987/supercache/pkg/keyspace"
)

// BitSet writes a bit on a ModeBitmap (Redis SETBIT). ACK-only.
// Non-owners forward an inbox FlagBitmapSet; the owner applies and fans a snapshot.
func (e *Engine) BitSet(ctx context.Context, keyspaceName, name string, offset uint64, bit bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := e.validateKey(keyspaceName, name); err != nil {
		return err
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return err
	}
	if ks.cfg.Mode != keyspace.ModeBitmap {
		return fmt.Errorf("%w: BitSet requires ModeBitmap", ErrInvalidArgument)
	}
	if err := e.validateKeyLen(ks, name); err != nil {
		return err
	}
	if err := e.bitmapNeed(ks, offset); err != nil {
		return err
	}
	c := e.clusterSnapshot()
	if c != nil && c.Ring != nil {
		if owner, ok := c.Ring.Owner(name); ok && owner.ID != "" && owner.ID != c.SelfID {
			if c.Transport == nil || owner.Addr == "" {
				return fmt.Errorf("%w: owner %s has no address", ErrUnavailable, owner.ID)
			}
			return e.bMutViaOwner(ctx, ks, name, bitmapx.EncodeSet(offset, bit))
		}
	}
	return e.bSetLocal(ks, name, offset, bit)
}

// BitGet returns the bit at offset. Missing name → ok=false.
func (e *Engine) BitGet(ctx context.Context, keyspaceName, name string, offset uint64) (bool, bool, error) {
	if err := ctx.Err(); err != nil {
		return false, false, err
	}
	if err := e.validateKey(keyspaceName, name); err != nil {
		return false, false, err
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return false, false, err
	}
	if ks.cfg.Mode != keyspace.ModeBitmap {
		return false, false, fmt.Errorf("%w: BitGet requires ModeBitmap", ErrInvalidArgument)
	}
	if err := e.validateKeyLen(ks, name); err != nil {
		return false, false, err
	}
	if ks.store.HasBitmap(name) {
		bit, ok := ks.store.BGet(name, offset)
		return bit, ok, nil
	}
	ent, found, err := e.bFetchOwner(ctx, ks, name)
	if err != nil || !found {
		return false, false, err
	}
	return bitmapx.Get(ent.Value, offset), true, nil
}

// BitCount is Redis BITCOUNT over a byte window. Missing → 0.
func (e *Engine) BitCount(ctx context.Context, keyspaceName, name string, start, end int) (int64, error) {
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
	if ks.cfg.Mode != keyspace.ModeBitmap {
		return 0, fmt.Errorf("%w: BitCount requires ModeBitmap", ErrInvalidArgument)
	}
	if err := e.validateKeyLen(ks, name); err != nil {
		return 0, err
	}
	if ks.store.HasBitmap(name) {
		n, _ := ks.store.BCount(name, start, end)
		return n, nil
	}
	ent, found, err := e.bFetchOwner(ctx, ks, name)
	if err != nil || !found {
		return 0, err
	}
	return bitmapx.Count(ent.Value, start, end), nil
}

// BitPos is Redis BITPOS over a stored-byte window.
func (e *Engine) BitPos(ctx context.Context, keyspaceName, name string, bit bool, start, end int) (int64, bool, error) {
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
	if ks.cfg.Mode != keyspace.ModeBitmap {
		return 0, false, fmt.Errorf("%w: BitPos requires ModeBitmap", ErrInvalidArgument)
	}
	if err := e.validateKeyLen(ks, name); err != nil {
		return 0, false, err
	}
	if ks.store.HasBitmap(name) {
		pos, found, _ := ks.store.BPos(name, bit, start, end)
		return pos, found, nil
	}
	ent, found, err := e.bFetchOwner(ctx, ks, name)
	if err != nil || !found {
		return 0, false, err
	}
	pos, ok := bitmapx.Pos(ent.Value, bit, start, end)
	return pos, ok, nil
}

func (e *Engine) bitmapNeed(ks *ksRuntime, offset uint64) error {
	need, ok := bitmapx.EncodedLen(offset)
	if !ok {
		return ErrValueTooLarge
	}
	max := e.maxValueSize
	if ks.cfg.MaxValueSize > 0 {
		max = ks.cfg.MaxValueSize
	}
	if max > 0 && need > max {
		return ErrValueTooLarge
	}
	return nil
}
