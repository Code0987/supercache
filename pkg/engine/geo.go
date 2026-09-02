package engine

import (
	"context"
	"fmt"

	"github.com/Code0987/supercache/pkg/geo"
	"github.com/Code0987/supercache/pkg/keyspace"
	"github.com/Code0987/supercache/pkg/store"
)

const (
	errEmptyGeoMember        = "%w: empty geo member"
	errInvalidLonLat         = "%w: invalid lon/lat"
	errInvalidRadius         = "%w: invalid radius"
	errGeoAddRequiresMode    = "%w: GeoAdd requires ModeGeo"
	errGeoRemRequiresMode    = "%w: GeoRem requires ModeGeo"
	errGeoPosRequiresMode    = "%w: GeoPos requires ModeGeo"
	errGeoCardRequiresMode   = "%w: GeoCard requires ModeGeo"
	errGeoDistRequiresMode   = "%w: GeoDist requires ModeGeo"
	errGeoRadiusRequiresMode = "%w: GeoRadius requires ModeGeo"
	errGeoAddRejected        = "%w: geoadd rejected"
	errGeoRemRejected        = "%w: georem rejected"
	errOwnerNoAddress        = "%w: owner %s has no address"
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
		return fmt.Errorf(errEmptyGeoMember, ErrInvalidArgument)
	}
	if !geo.ValidCoord(lon, lat) {
		return fmt.Errorf(errInvalidLonLat, ErrInvalidArgument)
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return err
	}
	if ks.cfg.Mode != keyspace.ModeGeo {
		return fmt.Errorf(errGeoAddRequiresMode, ErrInvalidArgument)
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
		return fmt.Errorf(errEmptyGeoMember, ErrInvalidArgument)
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return err
	}
	if ks.cfg.Mode != keyspace.ModeGeo {
		return fmt.Errorf(errGeoRemRequiresMode, ErrInvalidArgument)
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
		return 0, 0, false, fmt.Errorf(errGeoPosRequiresMode, ErrInvalidArgument)
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
		return 0, fmt.Errorf(errGeoCardRequiresMode, ErrInvalidArgument)
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
		return 0, false, fmt.Errorf(errGeoDistRequiresMode, ErrInvalidArgument)
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
		return nil, fmt.Errorf(errInvalidLonLat, ErrInvalidArgument)
	}
	if radiusM < 0 || mathIsNaN(radiusM) {
		return nil, fmt.Errorf(errInvalidRadius, ErrInvalidArgument)
	}
	ks, err := e.getKS(keyspaceName)
	if err != nil {
		return nil, err
	}
	if ks.cfg.Mode != keyspace.ModeGeo {
		return nil, fmt.Errorf(errGeoRadiusRequiresMode, ErrInvalidArgument)
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

func (e *Engine) hasGeoLocal(ks *ksRuntime, name string) bool {
	return ks.store.HasGeo(name)
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
