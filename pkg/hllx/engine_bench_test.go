package hllx_test

import (
	"context"
	"testing"
	"time"

	"github.com/Code0987/supercache/internal/benchmetrics"
	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
)

func newModeHLL(b *testing.B) *engine.Engine {
	b.Helper()
	e := engine.New()
	if err := e.UpdateKeySpace(keyspace.Config{
		Name: "uniq", Mode: keyspace.ModeHLL, MaxBytes: 64 << 20, TTL: time.Hour,
	}); err != nil {
		b.Fatal(err)
	}
	return e
}

func BenchmarkEngineHLLAdd(b *testing.B) {
	e := newModeHLL(b)
	defer e.Close()
	ctx := context.Background()
	items := [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")}
	b.ReportAllocs()
	b.ResetTimer()
	before := benchmetrics.Read()
	for i := 0; i < b.N; i++ {
		if err := e.HLLAdd(ctx, "uniq", "visitors", items[i%len(items)]); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	benchmetrics.ReportB(b, before, benchmetrics.Read())
}

func BenchmarkEngineHLLCountHit(b *testing.B) {
	e := newModeHLL(b)
	defer e.Close()
	ctx := context.Background()
	_ = e.HLLAdd(ctx, "uniq", "visitors", []byte("alice"))
	b.ReportAllocs()
	b.ResetTimer()
	before := benchmetrics.Read()
	for i := 0; i < b.N; i++ {
		_, ok, err := e.HLLCount(ctx, "uniq", "visitors")
		if err != nil || !ok {
			b.Fatal(err, ok)
		}
	}
	b.StopTimer()
	benchmetrics.ReportB(b, before, benchmetrics.Read())
}

func BenchmarkEngineHLLCountHitParallel(b *testing.B) {
	e := newModeHLL(b)
	defer e.Close()
	ctx := context.Background()
	_ = e.HLLAdd(ctx, "uniq", "visitors", []byte("alice"))
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, ok, err := e.HLLCount(ctx, "uniq", "visitors")
			if err != nil || !ok {
				b.Fatal(err, ok)
			}
		}
	})
}

func BenchmarkEngineHLLAddParallel(b *testing.B) {
	e := newModeHLL(b)
	defer e.Close()
	ctx := context.Background()
	items := [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if err := e.HLLAdd(ctx, "uniq", "visitors", items[i%len(items)]); err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}
