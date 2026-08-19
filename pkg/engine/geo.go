package engine

import (
	"context"
	"fmt"

	"github.com/Code0987/supercache/pkg/geo"
	"github.com/Code0987/supercache/pkg/keyspace"
	"github.com/Code0987/supercache/pkg/store"
)

// GeoMember is a point plus optional distance from a query.
type GeoMember struct {
	Member []byte
	Lon    float64
	Lat    float64
	Dist   float64
}

// GeoAdd inserts or updates member position in a ModeGeo index.
func (e *Engine) GeoAdd(ctx context.Context, keyspaceName, name string, member []byte, lon, lat float64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := e.validateKey(keyspaceName, name); err != nil {
		return err
	}
	if len(member) == 0 {
		return fmt.Errorf("%w: empty geo member", ErrInvalidArgument)
	}
	if !geo.ValidCoord(lon, lat) {
		return fmt.Errorf("%w: invalid lon/lat", ErrInvalidArgument)
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return err
	}
	if ks.cfg.Mode != keyspace.ModeGeo {
		return fmt.Errorf("%w: GeoAdd requires ModeGeo", ErrInvalidArgument)
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
			return e.gMutViaOwner(ctx, ks, name, geo.EncodeAdd(member, lon, lat), store.FlagGeoAdd)
		}
	}
	return e.gAddLocal(ks, name, member, lon, lat, true)
}

// GeoRem removes a member from a ModeGeo index.
func (e *Engine) GeoRem(ctx context.Context, keyspaceName, name string, member []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := e.validateKey(keyspaceName, name); err != nil {
		return err
	}
	if len(member) == 0 {
		return fmt.Errorf("%w: empty geo member", ErrInvalidArgument)
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return err
	}
	if ks.cfg.Mode != keyspace.ModeGeo {
		return fmt.Errorf("%w: GeoRem requires ModeGeo", ErrInvalidArgument)
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
			return e.gMutViaOwner(ctx, ks, name, append([]byte(nil), member...), store.FlagGeoRem)
		}
	}
	return e.gRemLocal(ks, name, member, true)
}

// GeoPos returns lon/lat if the member is present.
func (e *Engine) GeoPos(ctx context.Context, keyspaceName, name string, member []byte) (lon, lat float64, ok bool, err error) {
	if err := ctx.Err(); err != nil {
		return 0, 0, false, err
	}
	if err := e.validateKey(keyspaceName, name); err != nil {
		return 0, 0, false, err
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return 0, 0, false, err
	}
	if ks.cfg.Mode != keyspace.ModeGeo {
		return 0, 0, false, fmt.Errorf("%w: GeoPos requires ModeGeo", ErrInvalidArgument)
	}
	if err := e.validateKeyLen(ks, name); err != nil {
		return 0, 0, false, err
	}
	if lon, lat, ok := ks.store.GeoPos(name, member); ok {
		return lon, lat, true, nil
	}
	if e.hasGeoLocal(ks, name) {
		return 0, 0, false, nil
	}
	ent, found, err := e.gFetchOwner(ctx, ks, name)
	if err != nil || !found {
		return 0, 0, false, err
	}
	g, err := geo.Decode(ent.Value)
	if err != nil {
		return 0, 0, false, nil
	}
	p, present := g.Pos(member)
	return p.Lon, p.Lat, present, nil
}

// GeoCard returns the number of members.
func (e *Engine) GeoCard(ctx context.Context, keyspaceName, name string) (int, error) {
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
	if ks.cfg.Mode != keyspace.ModeGeo {
		return 0, fmt.Errorf("%w: GeoCard requires ModeGeo", ErrInvalidArgument)
	}
	if err := e.validateKeyLen(ks, name); err != nil {
		return 0, err
	}
	if e.hasGeoLocal(ks, name) {
		return ks.store.GeoCard(name), nil
	}
	ent, ok, err := e.gFetchOwner(ctx, ks, name)
	if err != nil || !ok {
		return 0, err
	}
	g, err := geo.Decode(ent.Value)
	if err != nil {
		return 0, nil
	}
	return g.Card(), nil
}

// GeoDist returns haversine meters between two members.
func (e *Engine) GeoDist(ctx context.Context, keyspaceName, name string, a, b []byte) (float64, bool, error) {
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
	if ks.cfg.Mode != keyspace.ModeGeo {
		return 0, false, fmt.Errorf("%w: GeoDist requires ModeGeo", ErrInvalidArgument)
	}
	if err := e.validateKeyLen(ks, name); err != nil {
		return 0, false, err
	}
	if e.hasGeoLocal(ks, name) {
		d, ok := ks.store.GeoDist(name, a, b)
		return d, ok, nil
	}
	ent, found, err := e.gFetchOwner(ctx, ks, name)
	if err != nil || !found {
		return 0, false, err
	}
	g, err := geo.Decode(ent.Value)
	if err != nil {
		return 0, false, nil
	}
	d, ok := g.Dist(a, b)
	return d, ok, nil
}

// GeoRadius returns members within radiusM meters (limit<=0 = all).
func (e *Engine) GeoRadius(ctx context.Context, keyspaceName, name string, lon, lat, radiusM float64, limit int) ([]GeoMember, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := e.validateKey(keyspaceName, name); err != nil {
		return nil, err
	}
	if !geo.ValidCoord(lon, lat) {
		return nil, fmt.Errorf("%w: invalid lon/lat", ErrInvalidArgument)
	}
	if radiusM < 0 || mathIsNaN(radiusM) {
		return nil, fmt.Errorf("%w: invalid radius", ErrInvalidArgument)
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return nil, err
	}
	if ks.cfg.Mode != keyspace.ModeGeo {
		return nil, fmt.Errorf("%w: GeoRadius requires ModeGeo", ErrInvalidArgument)
	}
	if err := e.validateKeyLen(ks, name); err != nil {
		return nil, err
	}
	if e.hasGeoLocal(ks, name) {
		return toEngineGeoMembers(ks.store.GeoRadius(name, lon, lat, radiusM, limit)), nil
	}
	ent, ok, err := e.gFetchOwner(ctx, ks, name)
	if err != nil || !ok {
		return nil, err
	}
	g, err := geo.Decode(ent.Value)
	if err != nil {
		return nil, nil
	}
	return toEngineGeoMembersFromPkg(g.Radius(lon, lat, radiusM, limit)), nil
}

func mathIsNaN(f float64) bool {
	return f != f
}

func (e *Engine) gMutViaOwner(ctx context.Context, ks *ksRuntime, name string, value []byte, flag uint32) error {
	c := e.clusterSnapshot()
	owner, _ := c.Ring.Owner(name)
	ent := store.Entry{Value: value, Flags: flag, Version: 1}
	pctx, cancel := e.peerCtx(ctx, ks)
	defer cancel()
	_, err := c.Transport.ApplyPut(pctx, owner.Addr, ks.cfg.Name, name, ent, c.Ring.Generation())
	return err
}

func (e *Engine) gAddLocal(ks *ksRuntime, name string, member []byte, lon, lat float64, fanout bool) error {
	ver := e.gNextVersion(ks, name)
	expire := e.expireAt(ks.cfg.TTL)
	if !ks.store.GeoAdd(name, member, lon, lat, ver, expire) {
		return fmt.Errorf("%w: geoadd rejected", ErrInvalidArgument)
	}
	if fanout {
		e.replicate(ks.cfg.Name, name, store.Entry{
			Value:    geo.EncodeAdd(member, lon, lat),
			Version:  ver,
			ExpireAt: expire,
			Flags:    store.FlagGeoAdd,
		}, false)
	}
	return nil
}

func (e *Engine) gRemLocal(ks *ksRuntime, name string, member []byte, fanout bool) error {
	ver := e.gNextVersion(ks, name)
	expire := e.expireAt(ks.cfg.TTL)
	if !ks.store.GeoRem(name, member, ver, expire) {
		if !e.hasGeoLocal(ks, name) {
			return nil
		}
		return fmt.Errorf("%w: georem rejected", ErrInvalidArgument)
	}
	if fanout {
		e.replicate(ks.cfg.Name, name, store.Entry{
			Value:    append([]byte(nil), member...),
			Version:  ver,
			ExpireAt: expire,
			Flags:    store.FlagGeoRem,
		}, false)
	}
	return nil
}

func (e *Engine) gNextVersion(ks *ksRuntime, name string) uint64 {
	if ver, ok := ks.store.PeekVersion(name); ok {
		return ks.nextVersion(name, ver)
	}
	return ks.nextVersion(name, 0)
}

func (e *Engine) hasGeoLocal(ks *ksRuntime, name string) bool {
	return ks.store.HasGeo(name)
}

func (e *Engine) gFetchOwner(ctx context.Context, ks *ksRuntime, name string) (store.Entry, bool, error) {
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
	if err != nil || !res.Found || !res.Entry.IsGeo() {
		return store.Entry{}, false, nil
	}
	if e.holdsReplica(c, ks, name) {
		_ = ks.store.GeoInstall(name, res.Entry.Value, res.Entry.Version, res.Entry.ExpireAt)
	}
	return res.Entry, true, nil
}

func (e *Engine) applyGeoAdd(ks *ksRuntime, name string, value []byte, version uint64, expireAt int64) bool {
	member, lon, lat, err := geo.DecodeAdd(value)
	if err != nil {
		return false
	}
	if expireAt == 0 {
		expireAt = e.expireAt(ks.cfg.TTL)
	}
	return ks.store.GeoAdd(name, member, lon, lat, version, expireAt)
}

func (e *Engine) applyGeoRem(ks *ksRuntime, name string, member []byte, version uint64, expireAt int64) bool {
	if expireAt == 0 {
		expireAt = e.expireAt(ks.cfg.TTL)
	}
	return ks.store.GeoRem(name, member, version, expireAt)
}

func (e *Engine) applyGeoInstall(ks *ksRuntime, name string, blob []byte, version uint64, expireAt int64) bool {
	if expireAt == 0 {
		expireAt = e.expireAt(ks.cfg.TTL)
	}
	return ks.store.GeoInstall(name, blob, version, expireAt)
}

func toEngineGeoMembers(in []store.GeoMember) []GeoMember {
	if len(in) == 0 {
		return nil
	}
	out := make([]GeoMember, len(in))
	for i, m := range in {
		out[i] = GeoMember{Member: m.Member, Lon: m.Lon, Lat: m.Lat, Dist: m.Dist}
	}
	return out
}

func toEngineGeoMembersFromPkg(in []geo.Member) []GeoMember {
	if len(in) == 0 {
		return nil
	}
	out := make([]GeoMember, len(in))
	for i, m := range in {
		out[i] = GeoMember{Member: m.Member, Lon: m.Lon, Lat: m.Lat, Dist: m.Dist}
	}
	return out
}
