package engine

import (
	"context"
	"fmt"
	"math"

	"github.com/Code0987/supercache/pkg/keyspace"
	"github.com/Code0987/supercache/pkg/store"
	"github.com/Code0987/supercache/pkg/zset"
)

const (
	errEmptyZSetMember           = "%w: empty zset member"
	errNaNScore                  = "%w: NaN score"
	errZAddRequiresMode          = "%w: ZAdd requires ModeZSet"
	errZRemRequiresMode          = "%w: ZRem requires ModeZSet"
	errZScoreRequiresMode        = "%w: ZScore requires ModeZSet"
	errZCardRequiresMode         = "%w: ZCard requires ModeZSet"
	errZRangeRequiresMode        = "%w: ZRange requires ModeZSet"
	errZRangeByScoreRequiresMode = "%w: ZRangeByScore requires ModeZSet"
	errZAddRejected              = "%w: zadd rejected"
	errZRemRejected              = "%w: zrem rejected"
	errOwnerNoAddress            = "%w: owner %s has no address"
)

// ZMember is a scored sorted-set element.
type ZMember struct {
	Member []byte
	Score  float64
}

// ZAdd inserts or updates member score in a ModeZSet.
func (e *Engine) ZAdd(ctx context.Context, keyspaceName, name string, member []byte, score float64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := e.validateKey(keyspaceName, name); err != nil {
		return err
	}
	if len(member) == 0 {
		return fmt.Errorf(errEmptyZSetMember, ErrInvalidArgument)
	}
	if math.IsNaN(score) {
		return fmt.Errorf(errNaNScore, ErrInvalidArgument)
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return err
	}
	if ks.cfg.Mode != keyspace.ModeZSet {
		return fmt.Errorf(errZAddRequiresMode, ErrInvalidArgument)
	}
	if err := e.validateKeyLen(ks, name); err != nil {
		return err
	}
	if len(member) > e.maxKeyLen {
		return ErrKeyTooLarge
	}
	c := e.clusterSnapshot()
	if c != nil && c.Ring != nil {
		if owner, ok := c.Ring.Owner(name); ok && owner.ID != "" && owner.ID != c.SelfID {
			if c.Transport == nil || owner.Addr == "" {
				return fmt.Errorf(errOwnerNoAddress, ErrUnavailable, owner.ID)
			}
			return e.zMutViaOwner(ctx, ks, name, zset.EncodeAdd(member, score), store.FlagZSetAdd)
		}
	}
	return e.zAddLocal(ks, name, member, score, true)
}

// ZRem removes a member from a ModeZSet.
func (e *Engine) ZRem(ctx context.Context, keyspaceName, name string, member []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := e.validateKey(keyspaceName, name); err != nil {
		return err
	}
	if len(member) == 0 {
		return fmt.Errorf(errEmptyZSetMember, ErrInvalidArgument)
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return err
	}
	if ks.cfg.Mode != keyspace.ModeZSet {
		return fmt.Errorf(errZRemRequiresMode, ErrInvalidArgument)
	}
	if err := e.validateKeyLen(ks, name); err != nil {
		return err
	}
	if len(member) > e.maxKeyLen {
		return ErrKeyTooLarge
	}
	c := e.clusterSnapshot()
	if c != nil && c.Ring != nil {
		if owner, ok := c.Ring.Owner(name); ok && owner.ID != "" && owner.ID != c.SelfID {
			if c.Transport == nil || owner.Addr == "" {
				return fmt.Errorf(errOwnerNoAddress, ErrUnavailable, owner.ID)
			}
			return e.zMutViaOwner(ctx, ks, name, append([]byte(nil), member...), store.FlagZSetRem)
		}
	}
	return e.zRemLocal(ks, name, member, true)
}

// ZScore returns the score if the member is present.
func (e *Engine) ZScore(ctx context.Context, keyspaceName, name string, member []byte) (float64, bool, error) {
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
	if ks.cfg.Mode != keyspace.ModeZSet {
		return 0, false, fmt.Errorf(errZScoreRequiresMode, ErrInvalidArgument)
	}
	if err := e.validateKeyLen(ks, name); err != nil {
		return 0, false, err
	}
	if sc, ok := ks.store.ZScore(name, member); ok {
		return sc, true, nil
	}
	if e.hasZSetLocal(ks, name) {
		return 0, false, nil
	}
	ent, ok, err := e.zFetchOwner(ctx, ks, name)
	if err != nil || !ok {
		return 0, false, err
	}
	z, err := zset.Decode(ent.Value)
	if err != nil {
		return 0, false, nil
	}
	sc, present := z.Score(member)
	return sc, present, nil
}

// ZCard returns the number of members.
func (e *Engine) ZCard(ctx context.Context, keyspaceName, name string) (int, error) {
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
	if ks.cfg.Mode != keyspace.ModeZSet {
		return 0, fmt.Errorf(errZCardRequiresMode, ErrInvalidArgument)
	}
	if err := e.validateKeyLen(ks, name); err != nil {
		return 0, err
	}
	if e.hasZSetLocal(ks, name) {
		return ks.store.ZCard(name), nil
	}
	ent, ok, err := e.zFetchOwner(ctx, ks, name)
	if err != nil || !ok {
		return 0, err
	}
	z, err := zset.Decode(ent.Value)
	if err != nil {
		return 0, nil
	}
	return z.Card(), nil
}

// ZRange returns members by rank (Redis-style start/stop).
func (e *Engine) ZRange(ctx context.Context, keyspaceName, name string, start, stop int) ([]ZMember, error) {
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
	if ks.cfg.Mode != keyspace.ModeZSet {
		return nil, fmt.Errorf(errZRangeRequiresMode, ErrInvalidArgument)
	}
	if err := e.validateKeyLen(ks, name); err != nil {
		return nil, err
	}
	if e.hasZSetLocal(ks, name) {
		return toEngineZMembers(ks.store.ZRange(name, start, stop)), nil
	}
	ent, ok, err := e.zFetchOwner(ctx, ks, name)
	if err != nil || !ok {
		return nil, err
	}
	z, err := zset.Decode(ent.Value)
	if err != nil {
		return nil, nil
	}
	return toEngineZMembersFromPkg(z.Range(start, stop)), nil
}

// ZRangeByScore returns members with min <= score <= max.
func (e *Engine) ZRangeByScore(ctx context.Context, keyspaceName, name string, min, max float64) ([]ZMember, error) {
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
	if ks.cfg.Mode != keyspace.ModeZSet {
		return nil, fmt.Errorf(errZRangeByScoreRequiresMode, ErrInvalidArgument)
	}
	if err := e.validateKeyLen(ks, name); err != nil {
		return nil, err
	}
	if e.hasZSetLocal(ks, name) {
		return toEngineZMembers(ks.store.ZRangeByScore(name, min, max)), nil
	}
	ent, ok, err := e.zFetchOwner(ctx, ks, name)
	if err != nil || !ok {
		return nil, err
	}
	z, err := zset.Decode(ent.Value)
	if err != nil {
		return nil, nil
	}
	return toEngineZMembersFromPkg(z.RangeByScore(min, max)), nil
}

func (e *Engine) hasZSetLocal(ks *ksRuntime, name string) bool {
	return ks.store.HasZSet(name)
}

func toEngineZMembers(in []store.ZMember) []ZMember {
	if len(in) == 0 {
		return nil
	}
	out := make([]ZMember, len(in))
	for i, m := range in {
		out[i] = ZMember{Member: m.Member, Score: m.Score}
	}
	return out
}

func toEngineZMembersFromPkg(in []zset.Member) []ZMember {
	if len(in) == 0 {
		return nil
	}
	out := make([]ZMember, len(in))
	for i, m := range in {
		out[i] = ZMember{Member: m.Member, Score: m.Score}
	}
	return out
}
