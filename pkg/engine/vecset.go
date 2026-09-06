package engine

import (
	"context"
	"fmt"

	"github.com/Code0987/supercache/pkg/keyspace"
	"github.com/Code0987/supercache/pkg/store"
	"github.com/Code0987/supercache/pkg/vecset"
	veceng "github.com/Code0987/supercache/pkg/vecset/eng"
)

// VSimHit is one neighbor (clients must not import pkg/vecset).
type VSimHit struct {
	Member []byte
	Score  float32
}

func (e *Engine) vsNeed(ks *ksRuntime, name string) error {
	if err := e.validateKey(ks.cfg.Name, name); err != nil {
		return err
	}
	if ks.cfg.Mode != keyspace.ModeVectorSet {
		return fmt.Errorf("%w: vector op requires ModeVectorSet", ErrInvalidArgument)
	}
	return e.validateKeyLen(ks, name)
}

func (e *Engine) vsCheckMember(member []byte) error {
	if len(member) < 1 || len(member) > vecset.MaxMemberLen {
		return fmt.Errorf("%w: bad vector member", ErrInvalidArgument)
	}
	if len(member) > e.maxKeyLen {
		return ErrKeyTooLarge
	}
	return nil
}

func (e *Engine) vsCheckVec(ks *ksRuntime, name string, vec []float32) error {
	if !vecset.ValidDim(len(vec)) || !vecset.Finite(vec) {
		return fmt.Errorf("%w: bad vector", ErrInvalidArgument)
	}
	if ks.cfg.VectorDim > 0 && len(vec) != ks.cfg.VectorDim {
		return fmt.Errorf("%w: vector dim", ErrInvalidArgument)
	}
	if dim, ok := ks.store.VDim(name); ok && dim != len(vec) {
		return fmt.Errorf("%w: vector dim", ErrInvalidArgument)
	}
	if ks.cfg.VectorMetric == keyspace.VectorMetricCosine && vecset.IsZero(vec) {
		return fmt.Errorf("%w: zero vector", ErrInvalidArgument)
	}
	return nil
}

// VAdd inserts or replaces member's vector. ACK-only.
func (e *Engine) VAdd(ctx context.Context, keyspaceName, name string, member []byte, vec []float32) error {
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
	if err := e.vsNeed(ks, name); err != nil {
		return err
	}
	if err := e.vsCheckMember(member); err != nil {
		return err
	}
	if err := e.vsCheckVec(ks, name, vec); err != nil {
		return err
	}
	if n, ok := ks.store.VCard(name); ok {
		if _, exists := ks.store.VEmb(name, member); !exists && n >= vecset.MaxMembers {
			return fmt.Errorf("%w: vector set full", ErrInvalidArgument)
		}
	}
	c := e.clusterSnapshot()
	if c != nil && c.Ring != nil {
		if owner, ok := c.Ring.Owner(name); ok && owner.ID != "" && owner.ID != c.SelfID {
			if c.Transport == nil || owner.Addr == "" {
				return fmt.Errorf("%w: owner %s has no address", ErrUnavailable, owner.ID)
			}
			return e.vsMutViaOwner(ctx, ks, name, member, vec, true)
		}
	}
	if err := veceng.AddLocal(e.modeHost(ks), name, member, vec); err == veceng.ErrTooLarge {
		return ErrValueTooLarge
	} else if err != nil {
		return fmt.Errorf("%w: vadd rejected", ErrInvalidArgument)
	}
	return nil
}

// VRem removes member. Missing is a no-op.
func (e *Engine) VRem(ctx context.Context, keyspaceName, name string, member []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return err
	}
	if err := e.vsNeed(ks, name); err != nil {
		return err
	}
	if err := e.vsCheckMember(member); err != nil {
		return err
	}
	c := e.clusterSnapshot()
	if c != nil && c.Ring != nil {
		if owner, ok := c.Ring.Owner(name); ok && owner.ID != "" && owner.ID != c.SelfID {
			if c.Transport == nil || owner.Addr == "" {
				return fmt.Errorf("%w: owner %s has no address", ErrUnavailable, owner.ID)
			}
			return e.vsMutViaOwner(ctx, ks, name, member, nil, false)
		}
	}
	if err := veceng.RemLocal(e.modeHost(ks), name, member); err != nil {
		return fmt.Errorf("%w: vrem rejected", ErrInvalidArgument)
	}
	return nil
}

// VSim returns top-k neighbors, best first. Missing name → empty, nil error.
func (e *Engine) VSim(ctx context.Context, keyspaceName, name string, vec []float32, k int) ([]VSimHit, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return nil, err
	}
	if err := e.vsNeed(ks, name); err != nil {
		return nil, err
	}
	if err := e.vsCheckVec(ks, name, vec); err != nil {
		return nil, err
	}
	metric := int(ks.cfg.VectorMetric)
	if hits, ok := ks.store.VSim(name, vec, k, metric); ok {
		return copyHits(hits), nil
	}
	ent, found, err := veceng.FetchOwner(ctx, e.modeHost(ks), name)
	if err != nil || !found {
		return nil, err
	}
	raw, err := vecset.Sim(ent.Value, vec, k, vecset.Metric(metric))
	if err != nil {
		return nil, nil
	}
	out := make([]VSimHit, len(raw))
	for i, h := range raw {
		out[i] = VSimHit{Member: h.Member, Score: h.Score}
	}
	return out, nil
}

// VCard is member count. Missing → present=false.
func (e *Engine) VCard(ctx context.Context, keyspaceName, name string) (int, bool, error) {
	return e.vsReadInt(ctx, keyspaceName, name, func(s store.Store) (int, bool) { return s.VCard(name) }, func(ent store.Entry) (int, bool) {
		set, err := vecset.DecodeSnapshot(ent.Value)
		if err != nil {
			return 0, false
		}
		return set.Card(), true
	})
}

// VDim is the locked dim. Missing → present=false.
func (e *Engine) VDim(ctx context.Context, keyspaceName, name string) (int, bool, error) {
	return e.vsReadInt(ctx, keyspaceName, name, func(s store.Store) (int, bool) { return s.VDim(name) }, func(ent store.Entry) (int, bool) {
		set, err := vecset.DecodeSnapshot(ent.Value)
		if err != nil {
			return 0, false
		}
		return set.Dim, true
	})
}

func (e *Engine) vsReadInt(ctx context.Context, keyspaceName, name string, local func(store.Store) (int, bool), fromEnt func(store.Entry) (int, bool)) (int, bool, error) {
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return 0, false, err
	}
	if err := e.vsNeed(ks, name); err != nil {
		return 0, false, err
	}
	if n, ok := local(ks.store); ok {
		return n, true, nil
	}
	ent, found, err := veceng.FetchOwner(ctx, e.modeHost(ks), name)
	if err != nil || !found {
		return 0, false, err
	}
	n, ok := fromEnt(ent)
	return n, ok, nil
}

// VEmb returns a copy of the stored vector. Missing → ok=false.
func (e *Engine) VEmb(ctx context.Context, keyspaceName, name string, member []byte) ([]float32, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return nil, false, err
	}
	if err := e.vsNeed(ks, name); err != nil {
		return nil, false, err
	}
	if err := e.vsCheckMember(member); err != nil {
		return nil, false, err
	}
	if vec, ok := ks.store.VEmb(name, member); ok {
		return vec, true, nil
	}
	ent, found, err := veceng.FetchOwner(ctx, e.modeHost(ks), name)
	if err != nil || !found {
		return nil, false, err
	}
	set, err := vecset.DecodeSnapshot(ent.Value)
	if err != nil {
		return nil, false, nil
	}
	vec, ok := set.Emb(member)
	return vec, ok, nil
}

func (e *Engine) vsMutViaOwner(ctx context.Context, ks *ksRuntime, name string, member []byte, vec []float32, add bool) error {
	c := e.clusterSnapshot()
	owner, _ := c.Ring.Owner(name)
	var inbox []byte
	var err error
	if add {
		inbox, err = vecset.EncodeInboxAdd(member, vec)
	} else {
		inbox, err = vecset.EncodeInboxRem(member)
	}
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidArgument, err)
	}
	ent := store.Entry{Value: inbox, Flags: store.FlagVectorSet, Version: 1}
	pctx, cancel := e.peerCtx(ctx, ks)
	defer cancel()
	applied, err := c.Transport.ApplyPut(pctx, owner.Addr, ks.cfg.Name, name, ent, c.Ring.Generation())
	if err != nil {
		return err
	}
	if !applied {
		return fmt.Errorf("%w: vector mutate rejected", ErrInvalidArgument)
	}
	return nil
}

func (e *Engine) applyVectorSet(ks *ksRuntime, name string, ent store.Entry) bool {
	if len(ent.Value) == 0 {
		return false
	}
	switch ent.Value[0] {
	case vecset.PrefixAdd, vecset.PrefixRem:
		c := e.clusterSnapshot()
		if c != nil && c.Ring != nil {
			if owner, ok := c.Ring.Owner(name); ok && owner.ID != "" && owner.ID != c.SelfID {
				return false
			}
		}
		return veceng.ApplyInbox(e.modeHost(ks), name, ent.Value, ent.ExpireAt)
	case vecset.PrefixSnapshot:
		return veceng.ApplyInstall(e.modeHost(ks), name, ent.Value, ent.Version, ent.ExpireAt)
	default:
		return false
	}
}

func copyHits(in []store.VSimHit) []VSimHit {
	out := make([]VSimHit, len(in))
	for i, h := range in {
		out[i] = VSimHit{Member: h.Member, Score: h.Score}
	}
	return out
}
