package engine_test

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/geo"
	"github.com/Code0987/supercache/pkg/keyspace"
	"github.com/Code0987/supercache/pkg/store"
)

func geoKS() keyspace.Config {
	return keyspace.Config{Name: "geo", Mode: keyspace.ModeGeo, MaxBytes: 1 << 20, TTL: time.Hour}
}

func TestGeoAddPosRemCard(t *testing.T) {
	e := engine.New()
	defer e.Close()
	if err := e.UpdateKeySpace(geoKS()); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := e.GeoAdd(ctx, "geo", "city", []byte("a"), -74, 40.7); err != nil {
		t.Fatal(err)
	}
	if err := e.GeoAdd(ctx, "geo", "city", []byte("b"), -74.05, 40.7); err != nil {
		t.Fatal(err)
	}
	if err := e.GeoAdd(ctx, "geo", "city", []byte("a"), -74.01, 40.71); err != nil {
		t.Fatal(err)
	}
	lon, lat, ok, err := e.GeoPos(ctx, "geo", "city", []byte("a"))
	if err != nil || !ok || lon != -74.01 || lat != 40.71 {
		t.Fatalf("%v %v %v %v", lon, lat, ok, err)
	}
	n, err := e.GeoCard(ctx, "geo", "city")
	if err != nil || n != 2 {
		t.Fatalf("card=%d err=%v", n, err)
	}
	if err := e.GeoRem(ctx, "geo", "city", []byte("b")); err != nil {
		t.Fatal(err)
	}
	_, _, ok, err = e.GeoPos(ctx, "geo", "city", []byte("b"))
	if err != nil || ok {
		t.Fatalf("after rem: %v %v", ok, err)
	}
	n, err = e.GeoCard(ctx, "geo", "city")
	if err != nil || n != 1 {
		t.Fatalf("card after rem=%d", n)
	}
}

func TestGeoRadiusOrderLimit(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(geoKS())
	ctx := context.Background()
	_ = e.GeoAdd(ctx, "geo", "city", []byte("near"), -74.00, 40.70)
	_ = e.GeoAdd(ctx, "geo", "city", []byte("mid"), -74.05, 40.70)
	_ = e.GeoAdd(ctx, "geo", "city", []byte("far"), -75.00, 40.70)
	r, err := e.GeoRadius(ctx, "geo", "city", -74.00, 40.70, 20000, 0)
	if err != nil || len(r) != 2 || string(r[0].Member) != "near" {
		t.Fatalf("%+v %v", r, err)
	}
	lim, err := e.GeoRadius(ctx, "geo", "city", -74.00, 40.70, 1e9, 1)
	if err != nil || len(lim) != 1 || string(lim[0].Member) != "near" {
		t.Fatalf("limit %+v %v", lim, err)
	}
}

func TestGeoDistMissing(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(geoKS())
	ctx := context.Background()
	_ = e.GeoAdd(ctx, "geo", "city", []byte("a"), 0, 0)
	_, ok, err := e.GeoDist(ctx, "geo", "city", []byte("a"), []byte("b"))
	if err != nil || ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
}

func TestGeoWrongMode(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(keyspace.Config{Name: "zs", Mode: keyspace.ModeZSet, MaxBytes: 1 << 20})
	_ = e.UpdateKeySpace(geoKS())
	ctx := context.Background()
	if err := e.GeoAdd(ctx, "zs", "g", []byte("x"), 0, 0); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("GeoAdd on ZSet: %v", err)
	}
	if err := e.ZAdd(ctx, "geo", "g", []byte("x"), 1); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("ZAdd on Geo: %v", err)
	}
	if err := e.Put(ctx, "geo", "g", []byte("x")); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("Put on Geo: %v", err)
	}
	if _, err := e.Get(ctx, "geo", "g"); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("Get on Geo: %v", err)
	}
}

func TestGeoMissingEmpty(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(geoKS())
	ctx := context.Background()
	_, _, ok, err := e.GeoPos(ctx, "geo", "nope", []byte("x"))
	if err != nil || ok {
		t.Fatalf("pos: %v %v", ok, err)
	}
	n, err := e.GeoCard(ctx, "geo", "nope")
	if err != nil || n != 0 {
		t.Fatalf("card: %d %v", n, err)
	}
	r, err := e.GeoRadius(ctx, "geo", "nope", 0, 0, 100, 0)
	if err != nil || len(r) != 0 {
		t.Fatalf("radius: %v %v", r, err)
	}
}

func TestGeoInvalidCoord(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(geoKS())
	ctx := context.Background()
	if err := e.GeoAdd(ctx, "geo", "g", []byte("x"), math.NaN(), 0); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("NaN: %v", err)
	}
	if err := e.GeoAdd(ctx, "geo", "g", []byte("x"), 181, 0); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("lon: %v", err)
	}
	if err := e.GeoAdd(ctx, "geo", "g", []byte("x"), 0, 91); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("lat: %v", err)
	}
	if err := e.GeoAdd(ctx, "geo", "g", []byte("x"), math.Inf(1), 0); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("inf lon: %v", err)
	}
	if err := e.GeoAdd(ctx, "geo", "g", []byte("x"), 0, math.Inf(-1)); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("inf lat: %v", err)
	}
}

func TestGeoDeleteTombstone(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(geoKS())
	ctx := context.Background()
	_ = e.GeoAdd(ctx, "geo", "g", []byte("a"), 0, 0)
	if err := e.Delete(ctx, "geo", "g"); err != nil {
		t.Fatal(err)
	}
	_, _, ok, _ := e.GeoPos(ctx, "geo", "g", []byte("a"))
	if ok {
		t.Fatal("after delete")
	}
	g := geo.New()
	_ = g.Add([]byte("a"), 0, 0)
	applied, err := e.ApplyPut("geo", "g", store.Entry{Value: g.Encode(), Version: 1, Flags: store.FlagGeo})
	if err != nil {
		t.Fatal(err)
	}
	if applied {
		t.Fatal("stale FlagGeo")
	}
}

func TestGeoEmptyAfterLastRem(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(geoKS())
	ctx := context.Background()
	_ = e.GeoAdd(ctx, "geo", "g", []byte("a"), 0, 0)
	_ = e.GeoRem(ctx, "geo", "g", []byte("a"))
	n, err := e.GeoCard(ctx, "geo", "g")
	if err != nil || n != 0 {
		t.Fatalf("empty card: %d %v", n, err)
	}
	if err := e.GeoRem(ctx, "geo", "g", []byte("a")); err != nil {
		t.Fatal(err)
	}
}
