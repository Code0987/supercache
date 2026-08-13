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

func newModeSet(b *testing.B) *engine.Engine {
	b.Helper()
	e := engine.New()
	if err := e.UpdateKeySpace(keyspace.Config{
		Name: "st", Mode: keyspace.ModeSet, MaxBytes: 64 << 20, TTL: time.Hour,
	}); err != nil {
		b.Fatal(err)
	}
	return e
}

func BenchmarkEngineSetAdd(b *testing.B) {
	e := newModeSet(b)
	defer e.Close()
	ctx := context.Background()
	const n = 1000
	b.ReportAllocs()
	b.ResetTimer()
	before := benchmetrics.Read()
	for i := 0; i < b.N; i++ {
		if err := e.SetAdd(ctx, "st", "s", []byte(fmt.Sprintf("i%d", i%n))); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	benchmetrics.ReportB(b, before, benchmetrics.Read())
}

func BenchmarkEngineSetContainsHit(b *testing.B) {
	e := newModeSet(b)
	defer e.Close()
	ctx := context.Background()
	const n = 1000
	for i := 0; i < n; i++ {
		_ = e.SetAdd(ctx, "st", "s", []byte(fmt.Sprintf("i%d", i)))
	}
	item := []byte("i0")
	b.ReportAllocs()
	b.ResetTimer()
	before := benchmetrics.Read()
	for i := 0; i < b.N; i++ {
		ok, err := e.SetContains(ctx, "st", "s", item)
		if err != nil || !ok {
			b.Fatal(err, ok)
		}
	}
	b.StopTimer()
	benchmetrics.ReportB(b, before, benchmetrics.Read())
}

func BenchmarkEngineSetContainsHitParallel(b *testing.B) {
	e := newModeSet(b)
	defer e.Close()
	ctx := context.Background()
	const n = 1000
	for i := 0; i < n; i++ {
		_ = e.SetAdd(ctx, "st", "s", []byte(fmt.Sprintf("i%d", i)))
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		item := []byte("i0")
		for pb.Next() {
			ok, err := e.SetContains(ctx, "st", "s", item)
			if err != nil || !ok {
				b.Fatal(err, ok)
			}
		}
	})
}

func BenchmarkEngineSetContainsMiss(b *testing.B) {
	e := newModeSet(b)
	defer e.Close()
	ctx := context.Background()
	const n = 1000
	for i := 0; i < n; i++ {
		_ = e.SetAdd(ctx, "st", "s", []byte(fmt.Sprintf("i%d", i)))
	}
	item := []byte("never")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ok, err := e.SetContains(ctx, "st", "s", item)
		if err != nil || ok {
			b.Fatal(err, ok)
		}
	}
}

func BenchmarkEngineSetCard(b *testing.B) {
	e := newModeSet(b)
	defer e.Close()
	ctx := context.Background()
	for i := 0; i < 64; i++ {
		_ = e.SetAdd(ctx, "st", "s", []byte(fmt.Sprintf("i%d", i)))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n, err := e.SetCard(ctx, "st", "s")
		if err != nil || n != 64 {
			b.Fatal(err, n)
		}
	}
}

func BenchmarkEngineSetMembers(b *testing.B) {
	e := newModeSet(b)
	defer e.Close()
	ctx := context.Background()
	for i := 0; i < 64; i++ {
		_ = e.SetAdd(ctx, "st", "s", []byte(fmt.Sprintf("i%d", i)))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mem, err := e.SetMembers(ctx, "st", "s")
		if err != nil || len(mem) != 64 {
			b.Fatal(err, len(mem))
		}
	}
}

func BenchmarkEngineSetRemove(b *testing.B) {
	e := newModeSet(b)
	defer e.Close()
	ctx := context.Background()
	const n = 256
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		item := []byte(fmt.Sprintf("i%d", i%n))
		_ = e.SetAdd(ctx, "st", "s", item)
		b.StartTimer()
		if err := e.SetRemove(ctx, "st", "s", item); err != nil {
			b.Fatal(err)
		}
	}
}
