package engine_test

import (
	"context"
	"testing"
	"time"

	"github.com/Code0987/supercache/internal/benchmetrics"
	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
)

func newModeBitmap(b *testing.B) *engine.Engine {
	b.Helper()
	e := engine.New()
	if err := e.UpdateKeySpace(keyspace.Config{
		Name: "flags", Mode: keyspace.ModeBitmap, MaxBytes: 64 << 20, TTL: time.Hour,
	}); err != nil {
		b.Fatal(err)
	}
	return e
}

func BenchmarkEngineBitSet(b *testing.B) {
	e := newModeBitmap(b)
	defer e.Close()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	before := benchmetrics.Read()
	for i := 0; i < b.N; i++ {
		if err := e.BitSet(ctx, "flags", "seen", uint64(i%64), true); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	benchmetrics.ReportB(b, before, benchmetrics.Read())
}

func BenchmarkEngineBitGetHit(b *testing.B) {
	e := newModeBitmap(b)
	defer e.Close()
	ctx := context.Background()
	_ = e.BitSet(ctx, "flags", "seen", 0, true)
	b.ReportAllocs()
	b.ResetTimer()
	before := benchmetrics.Read()
	for i := 0; i < b.N; i++ {
		_, ok, err := e.BitGet(ctx, "flags", "seen", 0)
		if err != nil || !ok {
			b.Fatal(err, ok)
		}
	}
	b.StopTimer()
	benchmetrics.ReportB(b, before, benchmetrics.Read())
}

func BenchmarkEngineBitCount(b *testing.B) {
	e := newModeBitmap(b)
	defer e.Close()
	ctx := context.Background()
	for i := 0; i < 64; i++ {
		_ = e.BitSet(ctx, "flags", "seen", uint64(i), true)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n, err := e.BitCount(ctx, "flags", "seen", 0, -1)
		if err != nil || n != 64 {
			b.Fatal(err, n)
		}
	}
}

func BenchmarkEngineBitGetHitParallel(b *testing.B) {
	e := newModeBitmap(b)
	defer e.Close()
	ctx := context.Background()
	_ = e.BitSet(ctx, "flags", "seen", 0, true)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, ok, err := e.BitGet(ctx, "flags", "seen", 0)
			if err != nil || !ok {
				b.Fatal(err, ok)
			}
		}
	})
}

func BenchmarkEngineBitSetParallel(b *testing.B) {
	e := newModeBitmap(b)
	defer e.Close()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if err := e.BitSet(ctx, "flags", "seen", uint64(i%32), true); err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}
