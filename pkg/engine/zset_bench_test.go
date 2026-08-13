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

func newModeZSet(b *testing.B) *engine.Engine {
	b.Helper()
	e := engine.New()
	if err := e.UpdateKeySpace(keyspace.Config{
		Name: "zs", Mode: keyspace.ModeZSet, MaxBytes: 64 << 20, TTL: time.Hour,
	}); err != nil {
		b.Fatal(err)
	}
	return e
}

func BenchmarkEngineZAdd(b *testing.B) {
	e := newModeZSet(b)
	defer e.Close()
	ctx := context.Background()
	const n = 1000
	b.ReportAllocs()
	b.ResetTimer()
	before := benchmetrics.Read()
	for i := 0; i < b.N; i++ {
		if err := e.ZAdd(ctx, "zs", "lb", []byte(fmt.Sprintf("i%d", i%n)), float64(i%n)); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	benchmetrics.ReportB(b, before, benchmetrics.Read())
}

func BenchmarkEngineZScoreHit(b *testing.B) {
	e := newModeZSet(b)
	defer e.Close()
	ctx := context.Background()
	const n = 1000
	for i := 0; i < n; i++ {
		_ = e.ZAdd(ctx, "zs", "lb", []byte(fmt.Sprintf("i%d", i)), float64(i))
	}
	member := []byte("i0")
	b.ReportAllocs()
	b.ResetTimer()
	before := benchmetrics.Read()
	for i := 0; i < b.N; i++ {
		sc, ok, err := e.ZScore(ctx, "zs", "lb", member)
		if err != nil || !ok || sc != 0 {
			b.Fatal(err, ok, sc)
		}
	}
	b.StopTimer()
	benchmetrics.ReportB(b, before, benchmetrics.Read())
}

func BenchmarkEngineZCard(b *testing.B) {
	e := newModeZSet(b)
	defer e.Close()
	ctx := context.Background()
	for i := 0; i < 64; i++ {
		_ = e.ZAdd(ctx, "zs", "lb", []byte(fmt.Sprintf("i%d", i)), float64(i))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n, err := e.ZCard(ctx, "zs", "lb")
		if err != nil || n != 64 {
			b.Fatal(err, n)
		}
	}
}

func BenchmarkEngineZRange(b *testing.B) {
	e := newModeZSet(b)
	defer e.Close()
	ctx := context.Background()
	const n = 1000
	for i := 0; i < n; i++ {
		_ = e.ZAdd(ctx, "zs", "lb", []byte(fmt.Sprintf("i%d", i)), float64(i))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mem, err := e.ZRange(ctx, "zs", "lb", 0, 9)
		if err != nil || len(mem) != 10 {
			b.Fatal(err, len(mem))
		}
	}
}

func BenchmarkEngineZRangeByScore(b *testing.B) {
	e := newModeZSet(b)
	defer e.Close()
	ctx := context.Background()
	const n = 1000
	for i := 0; i < n; i++ {
		_ = e.ZAdd(ctx, "zs", "lb", []byte(fmt.Sprintf("i%d", i)), float64(i))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mem, err := e.ZRangeByScore(ctx, "zs", "lb", 0, 9)
		if err != nil || len(mem) != 10 {
			b.Fatal(err, len(mem))
		}
	}
}
