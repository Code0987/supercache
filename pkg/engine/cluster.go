package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Code0987/supercache/internal/peer"
	"github.com/Code0987/supercache/internal/ring"
	"github.com/Code0987/supercache/pkg/keyspace"
	"github.com/Code0987/supercache/pkg/store"
)

// Cluster binds membership ring + peer transport for multi-node operation.
type Cluster struct {
	SelfID    string
	Ring      *ring.Ring
	Transport *peer.Transport
	Fanout    *peer.FanoutPool
}

// AttachCluster enables owner routing and async fan-out.
// Pass nil to detach (single-node mode).
func (e *Engine) AttachCluster(c *Cluster) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.cluster = c
	if c != nil {
		e.nodeID = c.SelfID
	}
}

// clusterSnapshot returns cluster under RLock.
func (e *Engine) clusterSnapshot() *Cluster {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.cluster
}

// maxForwardHops is the maximum number of ForwardPut re-routes allowed for one write.
// hopCount on the wire is "prior hops"; we re-forward only while hopCount < maxForwardHops.
// This is carried in the peer proto (not context) so it survives gRPC.
const maxForwardHops uint32 = 1

// PutLocal applies a Put on this node as owner (no prior forward hop), then async fan-out.
func (e *Engine) PutLocal(ctx context.Context, keyspaceName, key string, value []byte, opts ...PutOption) error {
	return e.PutLocalAtHop(ctx, keyspaceName, key, value, 0, opts...)
}

// PutLocalAtHop is PutLocal with a wire hop count from ForwardPut.
//
// When clustered and this node is not the ring owner:
//   - if hopCount < maxForwardHops, re-forward once to the current owner (hop+1)
//   - otherwise (or on forward failure) apply locally and force-fan-out
//
// hopCount must cross the network; context values do not.
func (e *Engine) PutLocalAtHop(ctx context.Context, keyspaceName, key string, value []byte, hopCount uint32, opts ...PutOption) error {
	ctx, end := e.startSpan(ctx, "engine.PutLocal")
	defer end()

	if err := ctx.Err(); err != nil {
		return err
	}
	if err := e.validateKey(keyspaceName, key); err != nil {
		return err
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return err
	}
	if err := e.validateValueSize(ks, value); err != nil {
		return err
	}
	if err := e.validateKeyLen(ks, key); err != nil {
		return err
	}

	// Ownership re-check: avoid accepting a write as owner when the ring disagrees.
	c := e.clusterSnapshot()
	if c != nil && c.Ring != nil {
		if owner, ok := c.Ring.Owner(key); ok && owner.ID != "" && owner.ID != c.SelfID {
			if c.Transport != nil && owner.Addr != "" && hopCount < maxForwardHops {
				pc := putConfig{}
				for _, o := range opts {
					o(&pc)
				}
				var ttlNanos int64
				if pc.ttlSet {
					ttlNanos = int64(pc.ttl)
				}
				pctx, cancel := e.peerCtx(ctx, ks)
				err := c.Transport.ForwardPut(pctx, owner.Addr, keyspaceName, key, value, ttlNanos, pc.ttlSet, hopCount+1)
				cancel()
				if err == nil {
					return nil
				}
				// Fall through: apply + force fan-out so the write still propagates.
			}
			return e.putLocalApply(ctx, ks, keyspaceName, key, value, true, opts...)
		}
	}
	return e.putLocalApply(ctx, ks, keyspaceName, key, value, false, opts...)
}

func (e *Engine) putLocalApply(ctx context.Context, ks *ksRuntime, keyspaceName, key string, value []byte, forceFanout bool, opts ...PutOption) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	pc := putConfig{}
	for _, o := range opts {
		o(&pc)
	}
	ttl := ks.cfg.TTL
	if pc.ttlSet {
		ttl = pc.ttl
	}

	ent := store.Entry{
		Value:   append([]byte(nil), value...),
		Version: ks.nextVersion(key, 0),
		Flags:   0,
	}
	ent.ExpireAt = e.expireAt(ttl)
	if !ks.store.AcceptIfNewer(key, ent) {
		if cur, ok := ks.store.Peek(key); ok && !cur.IsTombstone() && cur.Version >= ent.Version {
			e.metrics.RecordPut(keyspaceName)
			return nil
		}
		// Tombstone with higher/equal version or MaxBytes rejection.
		if cur, ok := ks.store.Peek(key); ok && cur.IsTombstone() && cur.Version >= ent.Version {
			// Should not happen if nextVersion seeds from Peek; treat as success no-op.
			e.metrics.RecordPut(keyspaceName)
			return nil
		}
		return fmt.Errorf("%w: entry exceeds MaxBytes", ErrInvalidArgument)
	}
	e.metrics.RecordPut(keyspaceName)
	e.fanoutPut(keyspaceName, key, ent, forceFanout)
	return nil
}

func (e *Engine) fanoutPut(keyspaceName, key string, ent store.Entry, force bool) {
	c := e.clusterSnapshot()
	if c == nil || c.Fanout == nil || c.Ring == nil {
		return
	}
	// Only the owner should fan out authoritative fills, unless force (mis-route repair).
	if !force {
		if owner, ok := c.Ring.Owner(key); ok && owner.ID != c.SelfID {
			return
		}
	}
	peers := c.Ring.PeersExcept(c.SelfID)
	if len(peers) == 0 {
		return
	}
	entCopy := store.Entry{
		Value:    ent.CloneValue(),
		Version:  ent.Version,
		ExpireAt: ent.ExpireAt,
		Flags:    ent.Flags,
	}
	c.Fanout.Submit(peers, keyspaceName, key, entCopy, c.Ring.Generation())
}

// putViaCluster routes Put to owner or applies locally.
func (e *Engine) putViaCluster(ctx context.Context, keyspaceName, key string, value []byte, opts ...PutOption) error {
	c := e.clusterSnapshot()
	if c == nil || c.Ring == nil {
		return e.PutLocal(ctx, keyspaceName, key, value, opts...)
	}
	owner, ok := c.Ring.Owner(key)
	if !ok || owner.ID == c.SelfID || owner.ID == "" {
		return e.PutLocal(ctx, keyspaceName, key, value, opts...)
	}
	if c.Transport == nil || owner.Addr == "" {
		return fmt.Errorf("%w: owner %s has no address", ErrUnavailable, owner.ID)
	}

	pc := putConfig{}
	for _, o := range opts {
		o(&pc)
	}
	var ttlNanos int64
	if pc.ttlSet {
		ttlNanos = int64(pc.ttl)
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return err
	}
	pctx, cancel := e.peerCtx(ctx, ks)
	defer cancel()
	// hop_count=0: first forward from a non-owner client-facing node.
	if err := c.Transport.ForwardPut(pctx, owner.Addr, keyspaceName, key, value, ttlNanos, pc.ttlSet, 0); err != nil {
		return fmt.Errorf("%w: forward to owner %s: %v", ErrUnavailable, owner.ID, err)
	}
	// Metrics recorded on owner PutLocal only (avoid double-count).
	return nil
}

// DeleteAsOwner mints a delete version, applies locally, and sync-RPCs all peers.
// Returns MultiError if any peer fails (local success still applied).
func (e *Engine) DeleteAsOwner(ctx context.Context, keyspaceName, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := e.validateKey(keyspaceName, key); err != nil {
		return err
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return err
	}
	if err := e.validateKeyLen(ks, key); err != nil {
		return err
	}

	ver := ks.nextVersion(key, 0)
	_ = ks.store.DeleteIfVersion(key, ver)
	e.metrics.RecordDelete(keyspaceName)

	c := e.clusterSnapshot()
	if c == nil || c.Ring == nil || c.Transport == nil {
		return nil
	}

	peers := c.Ring.PeersExcept(c.SelfID)
	if len(peers) == 0 {
		return nil
	}

	var (
		mu   sync.Mutex
		errs []PeerError
		wg   sync.WaitGroup
	)
	ringGen := c.Ring.Generation()
	// Fresh deadline budget for multi-peer delete (do not nest under short ForwardDelete client timeout alone).
	peerTO := c.Transport.Timeout()
	if peerTO <= 0 {
		peerTO = 500 * time.Millisecond
	}
	for _, p := range peers {
		if p.Addr == "" {
			mu.Lock()
			errs = append(errs, PeerError{PeerID: p.ID, Op: "ApplyDelete", Err: fmt.Errorf("empty address")})
			mu.Unlock()
			continue
		}
		wg.Add(1)
		go func(p ring.Peer) {
			defer wg.Done()
			pctx, cancel := context.WithTimeout(context.Background(), peerTO)
			defer cancel()
			_, err := c.Transport.ApplyDelete(pctx, p.Addr, keyspaceName, key, ver, ringGen)
			if err != nil {
				mu.Lock()
				errs = append(errs, PeerError{PeerID: p.ID, Op: "ApplyDelete", Err: err})
				mu.Unlock()
			}
		}(p)
	}
	wg.Wait()
	if len(errs) == 0 {
		return nil
	}
	return &MultiError{Errors: errs}
}

// deleteViaCluster routes Delete to owner coordinator or runs as owner.
func (e *Engine) deleteViaCluster(ctx context.Context, keyspaceName, key string) error {
	c := e.clusterSnapshot()
	if c == nil || c.Ring == nil {
		// single-node
		return e.DeleteAsOwner(ctx, keyspaceName, key)
	}
	owner, ok := c.Ring.Owner(key)
	if !ok || owner.ID == c.SelfID || owner.ID == "" {
		return e.DeleteAsOwner(ctx, keyspaceName, key)
	}
	if c.Transport == nil || owner.Addr == "" {
		// Owner unreachable for coordination: best-effort local + try peers ourselves.
		return e.DeleteAsOwner(ctx, keyspaceName, key)
	}
	// Do NOT apply keyspace.PeerTimeout here. ForwardDelete coordinates multi-peer
	// ApplyDelete on the owner and uses Transport's longer delete budget.
	// A short PeerTimeout would abort deletes and force dual-coordinator fallback.
	failures, err := c.Transport.ForwardDelete(ctx, owner.Addr, keyspaceName, key)
	if err != nil {
		// Owner down: best-effort local + all known peers (mirrors Get owner-down fallback).
		return e.DeleteAsOwner(ctx, keyspaceName, key)
	}
	// Metrics recorded on owner DeleteAsOwner only.
	if len(failures) == 0 {
		return nil
	}
	errs := make([]PeerError, 0, len(failures))
	for _, f := range failures {
		errs = append(errs, PeerError{
			PeerID: f.PeerID,
			Op:     "ApplyDelete",
			Err:    fmt.Errorf("%s", f.Message),
		})
	}
	return &MultiError{Errors: errs}
}

// GetOrLoadLocal serves a LoadThrough miss as owner (no peer forward).
// Returns the stored entry (positive) or ErrNotFound.
func (e *Engine) GetOrLoadLocal(ctx context.Context, keyspaceName, key string) (store.Entry, error) {
	if err := ctx.Err(); err != nil {
		return store.Entry{}, err
	}
	if err := e.validateKey(keyspaceName, key); err != nil {
		return store.Entry{}, err
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return store.Entry{}, err
	}
	if err := e.validateKeyLen(ks, key); err != nil {
		return store.Entry{}, err
	}

	if ent, ok := ks.store.Get(key); ok {
		if ent.IsNegative() {
			// Return envelope so peers can AcceptNegative with the same version.
			return ent, ErrNotFound
		}
		return ent, nil
	}
	if ks.cfg.Mode == keyspace.ModeCacheOnly {
		return store.Entry{}, ErrNotFound
	}

	// Distinct flight key from Engine.Get ("get:"+key) — different result types.
	v, err, _ := ks.flight.Do("gol:"+key, func() (any, error) {
		if ent, ok := ks.store.Get(key); ok {
			if ent.IsNegative() {
				return ent, ErrNotFound
			}
			return ent, nil
		}
		val, err := e.loadThrough(context.WithoutCancel(ctx), ks, key, true)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				// Prefer negative envelope after storeNegative inside loadThrough.
				if ent, ok := ks.store.Get(key); ok && ent.IsNegative() {
					return ent, ErrNotFound
				}
			}
			return nil, err
		}
		// reload entry after fill
		if ent, ok := ks.store.Get(key); ok && !ent.IsNegative() {
			return ent, nil
		}
		// loaded but not stored (too large) — synthesize entry
		return store.Entry{Value: val}, nil
	})
	if err != nil {
		if ent, ok := v.(store.Entry); ok && ent.IsNegative() {
			return ent, err
		}
		// singleflight may return typed entry with ErrNotFound
		if errors.Is(err, ErrNotFound) {
			if ent, ok := ks.store.Get(key); ok && ent.IsNegative() {
				return ent, ErrNotFound
			}
		}
		return store.Entry{}, err
	}
	if err := ctx.Err(); err != nil {
		return store.Entry{}, err
	}
	return v.(store.Entry), nil
}

// peerCtx applies keyspace.PeerTimeout when configured so peer RPCs honor per-ks deadlines.
func (e *Engine) peerCtx(ctx context.Context, ks *ksRuntime) (context.Context, context.CancelFunc) {
	if ks != nil && ks.cfg.PeerTimeout > 0 {
		return context.WithTimeout(ctx, ks.cfg.PeerTimeout)
	}
	return ctx, func() {}
}

// getViaCluster handles LoadThrough miss with owner GetOrLoad + local fallback.
func (e *Engine) getViaCluster(ctx context.Context, ks *ksRuntime, key string) ([]byte, error) {
	c := e.clusterSnapshot()
	// single-node or we are owner: local fill with fan-out
	if c == nil || c.Ring == nil {
		return e.loadThrough(ctx, ks, key, true)
	}
	owner, ok := c.Ring.Owner(key)
	if !ok || owner.ID == c.SelfID || owner.ID == "" {
		return e.loadThrough(ctx, ks, key, true)
	}
	if c.Transport == nil || owner.Addr == "" {
		e.metrics.RecordOwnerFallback(ks.cfg.Name)
		return e.loadThrough(ctx, ks, key, false)
	}

	pctx, cancel := e.peerCtx(ctx, ks)
	defer cancel()
	res, err := c.Transport.GetOrLoad(pctx, owner.Addr, ks.cfg.Name, key)
	if err != nil {
		// owner down / error → local-only fill, no fan-out
		e.metrics.RecordOwnerFallback(ks.cfg.Name)
		return e.loadThrough(ctx, ks, key, false)
	}
	if !res.Found {
		// Install owner's negative envelope when present (same version); do not remint.
		if res.Entry.IsNegative() || res.Entry.Version > 0 {
			_ = ks.store.AcceptNegative(key, res.Entry)
		} else {
			// Owner returned bare not-found without envelope (should be rare).
			e.storeNegative(ks, key, false)
		}
		return nil, ErrNotFound
	}
	// Install peer entry with same version (no new version).
	if res.Entry.IsNegative() {
		_ = ks.store.AcceptNegative(key, res.Entry)
		return nil, ErrNotFound
	}
	_ = ks.store.AcceptIfNewer(key, res.Entry)
	if res.Entry.Value == nil {
		return []byte{}, nil
	}
	return append([]byte(nil), res.Entry.Value...), nil
}

// FanoutStats returns peer fan-out counters when clustered.
func (e *Engine) FanoutStats() (errors, dropped uint64) {
	c := e.clusterSnapshot()
	if c == nil || c.Transport == nil {
		return 0, 0
	}
	return c.Transport.FanoutErrors.Load(), c.Transport.FanoutDropped.Load()
}

// OwnerOf returns the ring owner for key (empty if single-node).
func (e *Engine) OwnerOf(key string) (ring.Peer, bool) {
	c := e.clusterSnapshot()
	if c == nil || c.Ring == nil {
		return ring.Peer{}, false
	}
	return c.Ring.Owner(key)
}

// ForceLoad reloads from DataSource (LoadThrough only), bypassing cache hits.
// Used for refresh-ahead. Only the ring owner performs a SoT reload and fan-out;
// non-owners no-op (return nil) so refresh-ahead cannot stampede the backend via
// owner-down Get fallbacks.
func (e *Engine) ForceLoad(ctx context.Context, keyspaceName, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := e.validateKey(keyspaceName, key); err != nil {
		return err
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return err
	}
	if err := e.validateKeyLen(ks, key); err != nil {
		return err
	}
	if ks.cfg.Mode != keyspace.ModeLoadThrough {
		_, err := e.Get(ctx, keyspaceName, key)
		return err
	}
	c := e.clusterSnapshot()
	if c != nil && c.Ring != nil {
		if owner, ok := c.Ring.Owner(key); ok && owner.ID != "" && owner.ID != c.SelfID {
			// Not the owner: do not local-load or re-Get (owner-down fallback would
			// hit this node's DataSource without coordinating a true force-refresh).
			return nil
		}
	}
	_, err = e.loadThrough(ctx, ks, key, true)
	return err
}
