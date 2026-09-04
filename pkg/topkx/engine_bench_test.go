package topkx_test

import (
	"context"
	"testing"
	"time"

	"github.com/Code0987/supercache/internal/benchmetrics"
	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
)

func newModeTopK(b *testing.B) *engine.Engine {
	b.Helper()
	e := engine.New()
	if err := e.UpdateKeySpace(keyspace.Config{
		Name: "plays", Mode: keyspace.ModeTopK, MaxBytes: 64 << 20, TTL: time.Hour, TopKSize: 100,
	}); err != nil {
		b.Fatal(err)
	}
	return e
}

func BenchmarkEngineTopKAdd(b *testing.B) {
	e := newModeTopK(b)
	defer e.Close()
	ctx := context.Background()
	items := [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")}
	b.ReportAllocs()
	b.ResetTimer()
	before := benchmetrics.Read()
	for i := 0; i < b.N; i++ {
		if err := e.TopKAdd(ctx, "plays", "hot", items[i%len(items)]); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	benchmetrics.ReportB(b, before, benchmetrics.Read())
}

// BenchmarkEngineTopKListHit decodes + sorts; not a 0-alloc target.
func BenchmarkEngineTopKListHit(b *testing.B) {
	e := newModeTopK(b)
	defer e.Close()
	ctx := context.Background()
	_ = e.TopKAdd(ctx, "plays", "hot", []byte("t001"))
	b.ReportAllocs()
	b.ResetTimer()
	before := benchmetrics.Read()
	for i := 0; i < b.N; i++ {
		_, ok, err := e.TopKList(ctx, "plays", "hot")
		if err != nil || !ok {
			b.Fatal(err, ok)
		}
	}
	b.StopTimer()
	benchmetrics.ReportB(b, before, benchmetrics.Read())
}

func BenchmarkEngineTopKListHitParallel(b *testing.B) {
	e := newModeTopK(b)
	defer e.Close()
	ctx := context.Background()
	_ = e.TopKAdd(ctx, "plays", "hot", []byte("t001"))
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, ok, err := e.TopKList(ctx, "plays", "hot")
			if err != nil || !ok {
				b.Fatal(err, ok)
			}
		}
	})
}

func BenchmarkEngineTopKAddParallel(b *testing.B) {
	e := newModeTopK(b)
	defer e.Close()
	ctx := context.Background()
	items := [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if err := e.TopKAdd(ctx, "plays", "hot", items[i%len(items)]); err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}
