package engine_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Code0987/supercache/internal/benchmetrics"
	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
)

func newModeGeo(b *testing.B) *engine.Engine {
	b.Helper()
	e := engine.New()
	if err := e.UpdateKeySpace(keyspace.Config{
		Name: "geo", Mode: keyspace.ModeGeo, MaxBytes: 64 << 20, TTL: time.Hour,
	}); err != nil {
		b.Fatal(err)
	}
	return e
}

func BenchmarkEngineGeoAdd(b *testing.B) {
	e := newModeGeo(b)
	defer e.Close()
	ctx := context.Background()
	const n = 1000
	b.ReportAllocs()
	b.ResetTimer()
	before := benchmetrics.Read()
	for i := 0; i < b.N; i++ {
		if err := e.GeoAdd(ctx, "geo", "g", []byte(fmt.Sprintf("i%d", i%n)), float64(i%180), float64(i%90)); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	benchmetrics.ReportB(b, before, benchmetrics.Read())
}

func BenchmarkEngineGeoPosHit(b *testing.B) {
	e := newModeGeo(b)
	defer e.Close()
	ctx := context.Background()
	const n = 1000
	for i := 0; i < n; i++ {
		_ = e.GeoAdd(ctx, "geo", "g", []byte(fmt.Sprintf("i%d", i)), 0, 0)
	}
	member := []byte("i0")
	b.ReportAllocs()
	b.ResetTimer()
	before := benchmetrics.Read()
	for i := 0; i < b.N; i++ {
		_, _, ok, err := e.GeoPos(ctx, "geo", "g", member)
		if err != nil || !ok {
			b.Fatal(err, ok)
		}
	}
	b.StopTimer()
	benchmetrics.ReportB(b, before, benchmetrics.Read())
}

func BenchmarkEngineGeoCard(b *testing.B) {
	e := newModeGeo(b)
	defer e.Close()
	ctx := context.Background()
	for i := 0; i < 64; i++ {
		_ = e.GeoAdd(ctx, "geo", "g", []byte(fmt.Sprintf("i%d", i)), 0, 0)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n, err := e.GeoCard(ctx, "geo", "g")
		if err != nil || n != 64 {
			b.Fatal(err, n)
		}
	}
}

func BenchmarkEngineGeoRadius(b *testing.B) {
	e := newModeGeo(b)
	defer e.Close()
	ctx := context.Background()
	const n = 1000
	for i := 0; i < n; i++ {
		_ = e.GeoAdd(ctx, "geo", "g", []byte(fmt.Sprintf("i%d", i)), float64(i%10)*0.01, 0)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mem, err := e.GeoRadius(ctx, "geo", "g", 0, 0, 1e7, 10)
		if err != nil || len(mem) != 10 {
			b.Fatal(err, len(mem))
		}
	}
}
