package engine

import (
	"context"
	"fmt"

	"github.com/Code0987/supercache/pkg/keyspace"
	"github.com/Code0987/supercache/pkg/store"
	"github.com/Code0987/supercache/pkg/topkx"
	topkeng "github.com/Code0987/supercache/pkg/topkx/eng"
)

// TopKEntry is one chart row (same shape as topkx.Entry; clients must not import topkx).
type TopKEntry struct {
	Item  []byte
	Count uint64
}

// TopKAdd records one observation of item on a ModeTopK name (ACK-only).
// Non-owners forward an inbox FlagTopKAdd; the owner applies and fans a snapshot.
func (e *Engine) TopKAdd(ctx context.Context, keyspaceName, name string, item []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(item) == 0 {
		return fmt.Errorf("%w: empty topk item", ErrInvalidArgument)
	}
	if err := e.validateKey(keyspaceName, name); err != nil {
		return err
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return err
	}
	if ks.cfg.Mode != keyspace.ModeTopK {
		return fmt.Errorf("%w: TopKAdd requires ModeTopK", ErrInvalidArgument)
	}
	if err := e.validateKeyLen(ks, name); err != nil {
		return err
	}
	if len(item) > e.itemMax(ks) {
		return ErrKeyTooLarge
	}
	if err := e.topkNeed(ks); err != nil {
		return err
	}
	c := e.clusterSnapshot()
	if c != nil && c.Ring != nil {
		if owner, ok := c.Ring.Owner(name); ok && owner.ID != "" && owner.ID != c.SelfID {
			if c.Transport == nil || owner.Addr == "" {
				return fmt.Errorf("%w: owner %s has no address", ErrUnavailable, owner.ID)
			}
			return e.topkMutViaOwner(ctx, ks, name, item)
		}
	}
	if err := topkeng.AddLocal(e.modeHost(ks), name, item); err == topkeng.ErrTooLarge {
		return ErrValueTooLarge
	} else if err != nil {
		return fmt.Errorf("%w: topk add rejected", ErrInvalidArgument)
	}
	return nil
}

// TopKList is the current chart. Missing name → ok=false, nil entries, nil error.
func (e *Engine) TopKList(ctx context.Context, keyspaceName, name string) ([]TopKEntry, bool, error) {
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
	if ks.cfg.Mode != keyspace.ModeTopK {
		return nil, false, fmt.Errorf("%w: TopKList requires ModeTopK", ErrInvalidArgument)
	}
	if err := e.validateKeyLen(ks, name); err != nil {
		return nil, false, err
	}
	k := ks.cfg.EffectiveTopKSize()
	if ks.store.HasTopK(name) {
		rows, ok := ks.store.TopKList(name, k)
		return storeToEngineTopK(rows), ok, nil
	}
	ent, found, err := topkeng.FetchOwner(ctx, e.modeHost(ks), name)
	if err != nil || !found {
		return nil, false, err
	}
	tab, decErr := topkx.Decode(ent.Value, k)
	if decErr != nil {
		return nil, false, nil
	}
	return topkxToEngineTopK(tab.List()), true, nil
}

// itemMax is the bound for both the name and a stored Top-K item
// (ks.MaxKeyLen if set, else the engine default).
func (e *Engine) itemMax(ks *ksRuntime) int {
	max := e.maxKeyLen
	if ks.cfg.MaxKeyLen > 0 {
		max = ks.cfg.MaxKeyLen
	}
	return max
}

// topkNeed rejects a table whose worst-case encoding cannot fit MaxValueSize
// before any store write or peer forward (so clustered clients see ValueTooLarge).
func (e *Engine) topkNeed(ks *ksRuntime) error {
	max := e.maxValueSize
	if ks.cfg.MaxValueSize > 0 {
		max = ks.cfg.MaxValueSize
	}
	if max > 0 && topkx.WorstEncodedSize(ks.cfg.EffectiveTopKSize(), e.itemMax(ks)) > max {
		return ErrValueTooLarge
	}
	return nil
}

func storeToEngineTopK(rows []store.TopKEntry) []TopKEntry {
	if rows == nil {
		return nil
	}
	out := make([]TopKEntry, len(rows))
	for i, r := range rows {
		out[i] = TopKEntry{Item: r.Item, Count: r.Count}
	}
	return out
}

func topkxToEngineTopK(rows []topkx.Entry) []TopKEntry {
	if rows == nil {
		return nil
	}
	out := make([]TopKEntry, len(rows))
	for i, r := range rows {
		out[i] = TopKEntry{Item: r.Item, Count: r.Count}
	}
	return out
}

func (e *Engine) applyTopKAdd(ks *ksRuntime, name string, item []byte, expireAt int64) bool {
	return topkeng.ApplyAdd(e.modeHost(ks), name, item, expireAt)
}
func (e *Engine) applyTopKInstall(ks *ksRuntime, name string, blob []byte, version uint64, expireAt int64) bool {
	return topkeng.ApplyInstall(e.modeHost(ks), name, blob, version, expireAt)
}
