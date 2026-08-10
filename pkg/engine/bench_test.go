package engine_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Code0987/supercache/internal/benchmetrics"
	"github.com/Code0987/supercache/internal/testcluster"
	"github.com/Code0987/supercache/pkg/datasource"
	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
)

const (
	benchKS        = "bench"
	benchKeys      = 10_000
	benchValueSize = 256
)

func benchValue() []byte {
	v := make([]byte, benchValueSize)
	for i := range v {
		v[i] = byte('a' + i%26)
	}
	return v
}

func newCacheOnly(b *testing.B) *engine.Engine {
	b.Helper()
	e := engine.New()
	if err := e.UpdateKeySpace(keyspace.Config{
		Name: benchKS, Mode: keyspace.ModeCacheOnly,
		MaxBytes: 64 << 20, TTL: time.Hour,
	}); err != nil {
		b.Fatal(err)
	}
	return e
}

func prefill(b *testing.B, e *engine.Engine, val []byte) {
	b.Helper()
	ctx := context.Background()
	for i := 0; i < benchKeys; i++ {
		if err := e.Put(ctx, benchKS, fmt.Sprintf("k%d", i), val); err != nil {
			b.Fatal(err)
		}
	}
}

func reportLoop(b *testing.B, before benchmetrics.Snapshot) {
	b.Helper()
	b.StopTimer()
	benchmetrics.ReportB(b, before, benchmetrics.Read())
}

func BenchmarkEngineGetHit(b *testing.B) {
	e := newCacheOnly(b)
	defer e.Close()
	val := benchValue()
	prefill(b, e, val)
	ctx := context.Background()
	b.SetBytes(benchValueSize)
	b.ReportAllocs()
	b.ResetTimer()
	before := benchmetrics.Read()
	for i := 0; i < b.N; i++ {
		_, err := e.Get(ctx, benchKS, fmt.Sprintf("k%d", i%benchKeys))
		if err != nil {
			b.Fatal(err)
		}
	}
	reportLoop(b, before)
}

func BenchmarkEngineGetHitParallel(b *testing.B) {
	e := newCacheOnly(b)
	defer e.Close()
	prefill(b, e, benchValue())
	ctx := context.Background()
	b.SetBytes(benchValueSize)
	b.ReportAllocs()
	b.ResetTimer()
	before := benchmetrics.Read()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, err := e.Get(ctx, benchKS, fmt.Sprintf("k%d", i%benchKeys))
			if err != nil {
				b.Error(err)
				return
			}
			i++
		}
	})
	reportLoop(b, before)
}

func BenchmarkEngineGetMissCacheOnly(b *testing.B) {
	e := newCacheOnly(b)
	defer e.Close()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	before := benchmetrics.Read()
	for i := 0; i < b.N; i++ {
		_, err := e.Get(ctx, benchKS, fmt.Sprintf("missing-%d", i))
		if !errors.Is(err, engine.ErrNotFound) {
			b.Fatalf("want not found, got %v", err)
		}
	}
	reportLoop(b, before)
}

func BenchmarkEngineGetMissCacheOnlyParallel(b *testing.B) {
	e := newCacheOnly(b)
	defer e.Close()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	before := benchmetrics.Read()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, err := e.Get(ctx, benchKS, fmt.Sprintf("missing-%d", i))
			if !errors.Is(err, engine.ErrNotFound) {
				b.Errorf("want not found, got %v", err)
				return
			}
			i++
		}
	})
	reportLoop(b, before)
}

func BenchmarkEngineGetMissLoadThrough(b *testing.B) {
	val := benchValue()
	src := datasource.Func(func(_ context.Context, _ string) ([]byte, error) {
		out := make([]byte, len(val))
		copy(out, val)
		return out, nil
	})
	e := engine.New()
	defer e.Close()
	if err := e.UpdateKeySpace(keyspace.Config{
		Name: "benchlt", Mode: keyspace.ModeLoadThrough,
		MaxBytes: 1, TTL: time.Hour, DataSource: src,
	}); err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	loads0 := e.Metrics().Loads
	b.SetBytes(benchValueSize)
	b.ReportAllocs()
	b.ResetTimer()
	before := benchmetrics.Read()
	for i := 0; i < b.N; i++ {
		v, err := e.Get(ctx, "benchlt", "k")
		if err != nil {
			b.Fatal(err)
		}
		if len(v) != benchValueSize {
			b.Fatalf("len=%d", len(v))
		}
	}
	b.StopTimer()
	benchmetrics.ReportB(b, before, benchmetrics.Read())
	loads := e.Metrics().Loads - loads0
	if loads == 0 {
		b.Fatal("expected DataSource loads")
	}
	st, err := e.Stats("benchlt")
	if err != nil {
		b.Fatal(err)
	}
	if st.Items != 0 {
		b.Fatalf("fill must not stick with MaxBytes=1; items=%d", st.Items)
	}
}

func BenchmarkEngineGetMissLoadThroughParallel(b *testing.B) {
	val := benchValue()
	src := datasource.Func(func(_ context.Context, _ string) ([]byte, error) {
		out := make([]byte, len(val))
		copy(out, val)
		return out, nil
	})
	e := engine.New()
	defer e.Close()
	if err := e.UpdateKeySpace(keyspace.Config{
		Name: "benchlt", Mode: keyspace.ModeLoadThrough,
		MaxBytes: 1, TTL: time.Hour, DataSource: src,
	}); err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.SetBytes(benchValueSize)
	b.ReportAllocs()
	b.ResetTimer()
	before := benchmetrics.Read()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := e.Get(ctx, "benchlt", "k"); err != nil {
				b.Error(err)
				return
			}
		}
	})
	reportLoop(b, before)
}

func BenchmarkEnginePut(b *testing.B) {
	e := newCacheOnly(b)
	defer e.Close()
	val := benchValue()
	prefill(b, e, val)
	ctx := context.Background()
	b.SetBytes(benchValueSize)
	b.ReportAllocs()
	b.ResetTimer()
	before := benchmetrics.Read()
	for i := 0; i < b.N; i++ {
		if err := e.Put(ctx, benchKS, fmt.Sprintf("k%d", i%benchKeys), val); err != nil {
			b.Fatal(err)
		}
	}
	reportLoop(b, before)
}

func BenchmarkEnginePutParallel(b *testing.B) {
	e := newCacheOnly(b)
	defer e.Close()
	val := benchValue()
	prefill(b, e, val)
	ctx := context.Background()
	b.SetBytes(benchValueSize)
	b.ReportAllocs()
	b.ResetTimer()
	before := benchmetrics.Read()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if err := e.Put(ctx, benchKS, fmt.Sprintf("k%d", i%benchKeys), val); err != nil {
				b.Error(err)
				return
			}
			i++
		}
	})
	reportLoop(b, before)
}

func BenchmarkEngineDelete(b *testing.B) {
	e := newCacheOnly(b)
	defer e.Close()
	val := benchValue()
	prefill(b, e, val)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	before := benchmetrics.Read()
	for i := 0; i < b.N; i++ {
		err := e.Delete(ctx, benchKS, fmt.Sprintf("k%d", i%benchKeys))
		if err != nil && !errors.Is(err, engine.ErrNotFound) {
			b.Fatal(err)
		}
		if (i+1)%benchKeys == 0 {
			b.StopTimer()
			prefill(b, e, val)
			b.StartTimer()
		}
	}
	b.StopTimer()
	benchmetrics.ReportB(b, before, benchmetrics.Read())
}

func BenchmarkEnginePutCluster3(b *testing.B) {
	cl, err := testcluster.Start(testcluster.Config{Nodes: 3})
	if err != nil {
		b.Fatal(err)
	}
	defer cl.Close()
	e := cl.Nodes()[0].Engine
	val := benchValue()
	ctx := context.Background()
	b.SetBytes(benchValueSize)
	b.ReportAllocs()
	b.ResetTimer()
	before := benchmetrics.Read()
	for i := 0; i < b.N; i++ {
		if err := e.Put(ctx, "bench", fmt.Sprintf("k%d", i%benchKeys), val); err != nil {
			b.Fatal(err)
		}
	}
	reportLoop(b, before)
}

func BenchmarkEngineDeleteCluster3(b *testing.B) {
	cl, err := testcluster.Start(testcluster.Config{Nodes: 3})
	if err != nil {
		b.Fatal(err)
	}
	defer cl.Close()
	e := cl.Nodes()[0].Engine
	val := benchValue()
	ctx := context.Background()
	for i := 0; i < benchKeys; i++ {
		_ = e.Put(ctx, "bench", fmt.Sprintf("k%d", i), val)
	}
	b.ReportAllocs()
	b.ResetTimer()
	before := benchmetrics.Read()
	for i := 0; i < b.N; i++ {
		err := e.Delete(ctx, "bench", fmt.Sprintf("k%d", i%benchKeys))
		if err != nil && !errors.Is(err, engine.ErrNotFound) {
			b.Fatal(err)
		}
		if (i+1)%benchKeys == 0 {
			b.StopTimer()
			for j := 0; j < benchKeys; j++ {
				_ = e.Put(ctx, "bench", fmt.Sprintf("k%d", j), val)
			}
			b.StartTimer()
		}
	}
	b.StopTimer()
	benchmetrics.ReportB(b, before, benchmetrics.Read())
}
