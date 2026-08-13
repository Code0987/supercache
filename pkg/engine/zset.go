package engine

import (
	"context"
	"fmt"
	"math"

	"github.com/Code0987/supercache/pkg/keyspace"
	"github.com/Code0987/supercache/pkg/store"
	"github.com/Code0987/supercache/pkg/zset"
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
		return fmt.Errorf("%w: empty zset member", ErrInvalidArgument)
	}
	if math.IsNaN(score) {
		return fmt.Errorf("%w: NaN score", ErrInvalidArgument)
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return err
	}
	if ks.cfg.Mode != keyspace.ModeZSet {
		return fmt.Errorf("%w: ZAdd requires ModeZSet", ErrInvalidArgument)
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
				return fmt.Errorf("%w: owner %s has no address", ErrUnavailable, owner.ID)
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
		return fmt.Errorf("%w: empty zset member", ErrInvalidArgument)
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return err
	}
	if ks.cfg.Mode != keyspace.ModeZSet {
		return fmt.Errorf("%w: ZRem requires ModeZSet", ErrInvalidArgument)
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
				return fmt.Errorf("%w: owner %s has no address", ErrUnavailable, owner.ID)
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
		return 0, false, fmt.Errorf("%w: ZScore requires ModeZSet", ErrInvalidArgument)
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
		return 0, fmt.Errorf("%w: ZCard requires ModeZSet", ErrInvalidArgument)
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
		return nil, fmt.Errorf("%w: ZRange requires ModeZSet", ErrInvalidArgument)
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
		return nil, fmt.Errorf("%w: ZRangeByScore requires ModeZSet", ErrInvalidArgument)
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

func (e *Engine) zMutViaOwner(ctx context.Context, ks *ksRuntime, name string, value []byte, flag uint32) error {
	c := e.clusterSnapshot()
	owner, _ := c.Ring.Owner(name)
	ent := store.Entry{Value: value, Flags: flag, Version: 1}
	pctx, cancel := e.peerCtx(ctx, ks)
	defer cancel()
	_, err := c.Transport.ApplyPut(pctx, owner.Addr, ks.cfg.Name, name, ent, c.Ring.Generation())
	return err
}

func (e *Engine) zAddLocal(ks *ksRuntime, name string, member []byte, score float64, fanout bool) error {
	ver := e.zNextVersion(ks, name)
	expire := e.expireAt(ks.cfg.TTL)
	if !ks.store.ZAdd(name, member, score, ver, expire) {
		return fmt.Errorf("%w: zadd rejected", ErrInvalidArgument)
	}
	if fanout {
		e.replicate(ks.cfg.Name, name, store.Entry{
			Value:    zset.EncodeAdd(member, score),
			Version:  ver,
			ExpireAt: expire,
			Flags:    store.FlagZSetAdd,
		}, false)
	}
	return nil
}

func (e *Engine) zRemLocal(ks *ksRuntime, name string, member []byte, fanout bool) error {
	ver := e.zNextVersion(ks, name)
	expire := e.expireAt(ks.cfg.TTL)
	if !ks.store.ZRem(name, member, ver, expire) {
		if !e.hasZSetLocal(ks, name) {
			return nil
		}
		return fmt.Errorf("%w: zrem rejected", ErrInvalidArgument)
	}
	if fanout {
		e.replicate(ks.cfg.Name, name, store.Entry{
			Value:    append([]byte(nil), member...),
			Version:  ver,
			ExpireAt: expire,
			Flags:    store.FlagZSetRem,
		}, false)
	}
	return nil
}

func (e *Engine) zNextVersion(ks *ksRuntime, name string) uint64 {
	if ver, ok := ks.store.PeekVersion(name); ok {
		return ks.nextVersion(name, ver)
	}
	return ks.nextVersion(name, 0)
}

func (e *Engine) hasZSetLocal(ks *ksRuntime, name string) bool {
	return ks.store.HasZSet(name)
}

func (e *Engine) zFetchOwner(ctx context.Context, ks *ksRuntime, name string) (store.Entry, bool, error) {
	c := e.clusterSnapshot()
	if c == nil || c.Ring == nil || c.Transport == nil {
		return store.Entry{}, false, nil
	}
	owner, ok := c.Ring.Owner(name)
	if !ok || owner.ID == "" || owner.ID == c.SelfID || owner.Addr == "" {
		return store.Entry{}, false, nil
	}
	pctx, cancel := e.peerCtx(ctx, ks)
	defer cancel()
	res, err := c.Transport.GetOrLoad(pctx, owner.Addr, ks.cfg.Name, name)
	if err != nil || !res.Found || !res.Entry.IsZSet() {
		return store.Entry{}, false, nil
	}
	if e.holdsReplica(c, ks, name) {
		_ = ks.store.ZInstall(name, res.Entry.Value, res.Entry.Version, res.Entry.ExpireAt)
	}
	return res.Entry, true, nil
}

func (e *Engine) applyZSetAdd(ks *ksRuntime, name string, value []byte, version uint64, expireAt int64) bool {
	member, score, err := zset.DecodeAdd(value)
	if err != nil {
		return false
	}
	if expireAt == 0 {
		expireAt = e.expireAt(ks.cfg.TTL)
	}
	return ks.store.ZAdd(name, member, score, version, expireAt)
}

func (e *Engine) applyZSetRem(ks *ksRuntime, name string, member []byte, version uint64, expireAt int64) bool {
	if expireAt == 0 {
		expireAt = e.expireAt(ks.cfg.TTL)
	}
	return ks.store.ZRem(name, member, version, expireAt)
}

func (e *Engine) applyZSetInstall(ks *ksRuntime, name string, blob []byte, version uint64, expireAt int64) bool {
	if expireAt == 0 {
		expireAt = e.expireAt(ks.cfg.TTL)
	}
	return ks.store.ZInstall(name, blob, version, expireAt)
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
