package geo

import (
	"bytes"
	"math"
	"testing"
)

func TestEncodeRoundTrip(t *testing.T) {
	g := New()
	_ = g.Add([]byte("b"), 2, 1)
	_ = g.Add([]byte("a"), -74.0, 40.7)
	enc := g.Encode()
	g2, err := Decode(enc)
	if err != nil {
		t.Fatal(err)
	}
	if g2.Card() != 2 {
		t.Fatal(g2.Card())
	}
	pa, ok := g2.Pos([]byte("a"))
	if !ok || pa.Lon != -74.0 || pa.Lat != 40.7 {
		t.Fatalf("%+v %v", pa, ok)
	}
	if !bytes.Equal(g2.Encode(), enc) {
		t.Fatal("encode not stable")
	}
}

func TestPosUpdateAndRem(t *testing.T) {
	g := New()
	_ = g.Add([]byte("m"), 10, 20)
	_ = g.Add([]byte("m"), 11, 21)
	p, ok := g.Pos([]byte("m"))
	if !ok || p.Lon != 11 || p.Lat != 21 {
		t.Fatalf("%+v %v", p, ok)
	}
	if g.Card() != 1 {
		t.Fatal(g.Card())
	}
	g.Rem([]byte("m"))
	if _, ok := g.Pos([]byte("m")); ok {
		t.Fatal("rem")
	}
	if g.Card() != 0 {
		t.Fatal("empty card")
	}
}

func TestHaversineSamePoint(t *testing.T) {
	d := Haversine(-74, 40.7, -74, 40.7)
	if d > 1e-6 {
		t.Fatal(d)
	}
}

func TestHaversineNYCLA(t *testing.T) {
	// NYC JFK ~ -73.7781, 40.6413 ; LAX ~ -118.4085, 33.9416 — ~3974 km
	d := Haversine(-73.7781, 40.6413, -118.4085, 33.9416)
	if d < 3.9e6 || d > 4.1e6 {
		t.Fatalf("dist=%v want ~4e6 m", d)
	}
}

func TestRadiusOrderAndLimit(t *testing.T) {
	g := New()
	_ = g.Add([]byte("near"), -74.00, 40.70)
	_ = g.Add([]byte("mid"), -74.05, 40.70)
	_ = g.Add([]byte("far"), -75.00, 40.70)
	r := g.Radius(-74.00, 40.70, 20000, 0)
	if len(r) != 2 || string(r[0].Member) != "near" || string(r[1].Member) != "mid" {
		t.Fatalf("%+v", r)
	}
	if r[0].Dist > r[1].Dist {
		t.Fatal("not nearest first")
	}
	lim := g.Radius(-74.00, 40.70, 1e9, 1)
	if len(lim) != 1 || string(lim[0].Member) != "near" {
		t.Fatalf("limit: %+v", lim)
	}
}

func TestInvalidCoord(t *testing.T) {
	g := New()
	if err := g.Add([]byte("x"), math.NaN(), 0); err == nil {
		t.Fatal("NaN")
	}
	if err := g.Add([]byte("x"), 181, 0); err == nil {
		t.Fatal("lon")
	}
	if err := g.Add([]byte("x"), 0, 91); err == nil {
		t.Fatal("lat")
	}
	if err := g.Add([]byte("x"), 0, math.Inf(1)); err == nil {
		t.Fatal("inf")
	}
}

func TestDistMissing(t *testing.T) {
	g := New()
	_ = g.Add([]byte("a"), 0, 0)
	if _, ok := g.Dist([]byte("a"), []byte("b")); ok {
		t.Fatal("want missing")
	}
}
