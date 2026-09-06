package engine

import (
	"context"

	"github.com/Code0987/supercache/pkg/store"
)

// modeHost is the engine-owned wiring every feature package's Host needs.
type modeHost struct {
	e  *Engine
	ks *ksRuntime
}

func (e *Engine) modeHost(ks *ksRuntime) modeHost {
	return modeHost{e: e, ks: ks}
}

func (h modeHost) Store() store.Store   { return h.ks.store }
func (h modeHost) KeyspaceName() string { return h.ks.cfg.Name }
func (h modeHost) ExpireAt() int64      { return h.e.expireAt(h.ks.cfg.TTL) }
func (h modeHost) ObserveVersion(name string, ver uint64) {
	h.ks.observeVersion(name, ver)
}
func (h modeHost) Replicate(name string, ent store.Entry) {
	h.e.replicate(h.ks.cfg.Name, name, ent, false)
}
func (h modeHost) PeerCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return h.e.peerCtx(ctx, h.ks)
}
func (h modeHost) Owner(name string) (id, addr string, isSelf bool, ok bool) {
	c := h.e.clusterSnapshot()
	if c == nil || c.Ring == nil {
		return "", "", false, false
	}
	owner, ok := c.Ring.Owner(name)
	if !ok || owner.ID == "" {
		return "", "", false, false
	}
	return owner.ID, owner.Addr, owner.ID == c.SelfID, true
}
func (h modeHost) GetOrLoad(ctx context.Context, addr, keyspace, name string) (store.Entry, bool, error) {
	c := h.e.clusterSnapshot()
	if c == nil || c.Transport == nil {
		return store.Entry{}, false, nil
	}
	res, err := c.Transport.GetOrLoad(ctx, addr, keyspace, name)
	if err != nil || !res.Found {
		return store.Entry{}, false, err
	}
	return res.Entry, true, nil
}
func (h modeHost) HoldsReplica(name string) bool {
	c := h.e.clusterSnapshot()
	if c == nil {
		return false
	}
	return h.e.holdsReplica(c, h.ks, name)
}
func (h modeHost) NextVersion(name string) uint64 {
	if ver, ok := h.ks.store.PeekVersion(name); ok {
		return h.ks.nextVersion(name, ver)
	}
	return h.ks.nextVersion(name, 0)
}
func (h modeHost) MaxValue() int {
	max := h.e.maxValueSize
	if h.ks.cfg.MaxValueSize > 0 {
		max = h.ks.cfg.MaxValueSize
	}
	return max
}
func (h modeHost) BloomMK() (mBits, k int) {
	return h.ks.cfg.EffectiveBloomBits(), h.ks.cfg.EffectiveBloomHashes()
}
func (h modeHost) TopKSize() int     { return h.ks.cfg.EffectiveTopKSize() }
func (h modeHost) VectorDim() int    { return h.ks.cfg.VectorDim }
func (h modeHost) VectorMetric() int { return int(h.ks.cfg.VectorMetric) }
func (h modeHost) ItemMax() int {
	max := h.e.maxKeyLen
	if h.ks.cfg.MaxKeyLen > 0 {
		max = h.ks.cfg.MaxKeyLen
	}
	return max
}
func (h modeHost) HasList(name string) bool      { return h.ks.store.HasList(name) }
func (h modeHost) HasHash(name string) bool      { return h.ks.store.HasHash(name) }
func (h modeHost) HasSet(name string) bool       { return h.ks.store.HasSet(name) }
func (h modeHost) HasZSet(name string) bool      { return h.ks.store.HasZSet(name) }
func (h modeHost) HasGeo(name string) bool       { return h.ks.store.HasGeo(name) }
func (h modeHost) HasCounter(name string) bool   { return h.ks.store.HasCounter(name) }
func (h modeHost) HasJSON(name string) bool      { return h.ks.store.HasJSON(name) }
func (h modeHost) HasBitmap(name string) bool    { return h.ks.store.HasBitmap(name) }
func (h modeHost) HasHLL(name string) bool       { return h.ks.store.HasHLL(name) }
func (h modeHost) HasTopK(name string) bool      { return h.ks.store.HasTopK(name) }
func (h modeHost) HasCMS(name string) bool       { return h.ks.store.HasCMS(name) }
func (h modeHost) HasVectorSet(name string) bool { return h.ks.store.HasVectorSet(name) }
func (h modeHost) HasBloom(name string) bool {
	ent, ok := h.ks.store.Peek(name)
	return ok && ent.IsBloom() && !ent.IsTombstone()
}
