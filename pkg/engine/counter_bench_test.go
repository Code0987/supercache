package engine_test

import (
	"context"
	"testing"
	"time"

	"github.com/Code0987/supercache/internal/benchmetrics"
	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
)

func newModeCounter(b *testing.B) *engine.Engine {
	b.Helper()
	e := engine.New()
	if err := e.UpdateKeySpace(keyspace.Config{
		Name: "ctr", Mode: keyspace.ModeCounter, MaxBytes: 64 << 20, TTL: time.Hour,
	}); err != nil {
		b.Fatal(err)
	}
	return e
}

func BenchmarkEngineIncr(b *testing.B) {
	e := newModeCounter(b)
	defer e.Close()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	before := benchmetrics.Read()
	for i := 0; i < b.N; i++ {
		if _, err := e.Incr(ctx, "ctr", "c", 1); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	benchmetrics.ReportB(b, before, benchmetrics.Read())
}

func BenchmarkEngineCounterGet(b *testing.B) {
	e := newModeCounter(b)
	defer e.Close()
	ctx := context.Background()
	_, _ = e.Incr(ctx, "ctr", "c", 1)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, ok, err := e.CounterGet(ctx, "ctr", "c")
		if err != nil || !ok {
			b.Fatal(err, ok)
		}
	}
}

func BenchmarkEngineIncrParallel(b *testing.B) {
	e := newModeCounter(b)
	defer e.Close()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := e.Incr(ctx, "ctr", "c", 1); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkEngineCounterGetParallel(b *testing.B) {
	e := newModeCounter(b)
	defer e.Close()
	ctx := context.Background()
	_, _ = e.Incr(ctx, "ctr", "c", 1)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, ok, err := e.CounterGet(ctx, "ctr", "c")
			if err != nil || !ok {
				b.Fatal(err, ok)
			}
		}
	})
}
