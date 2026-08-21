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

func newModeJSON(b *testing.B) *engine.Engine {
	b.Helper()
	e := engine.New()
	if err := e.UpdateKeySpace(keyspace.Config{
		Name: "doc", Mode: keyspace.ModeJSON, MaxBytes: 64 << 20, TTL: time.Hour,
	}); err != nil {
		b.Fatal(err)
	}
	return e
}

func BenchmarkEngineJsonSet(b *testing.B) {
	e := newModeJSON(b)
	defer e.Close()
	ctx := context.Background()
	_ = e.JsonSet(ctx, "doc", "user", "$", []byte(`{}`))
	b.ReportAllocs()
	b.ResetTimer()
	before := benchmetrics.Read()
	for i := 0; i < b.N; i++ {
		if err := e.JsonSet(ctx, "doc", "user", fmt.Sprintf("$.k%d", i%64), []byte("1")); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	benchmetrics.ReportB(b, before, benchmetrics.Read())
}

func BenchmarkEngineJsonGetHit(b *testing.B) {
	e := newModeJSON(b)
	defer e.Close()
	ctx := context.Background()
	_ = e.JsonSet(ctx, "doc", "user", "$.n", []byte("1"))
	b.ReportAllocs()
	b.ResetTimer()
	before := benchmetrics.Read()
	for i := 0; i < b.N; i++ {
		_, ok, err := e.JsonGet(ctx, "doc", "user", "$.n")
		if err != nil || !ok {
			b.Fatal(err, ok)
		}
	}
	b.StopTimer()
	benchmetrics.ReportB(b, before, benchmetrics.Read())
}

func BenchmarkEngineJsonGet(b *testing.B) {
	e := newModeJSON(b)
	defer e.Close()
	ctx := context.Background()
	_ = e.JsonSet(ctx, "doc", "user", "$", []byte(`{"a":1,"b":2}`))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, ok, err := e.JsonGet(ctx, "doc", "user", "$")
		if err != nil || !ok {
			b.Fatal(err, ok)
		}
	}
}

func BenchmarkEngineJsonGetHitParallel(b *testing.B) {
	e := newModeJSON(b)
	defer e.Close()
	ctx := context.Background()
	_ = e.JsonSet(ctx, "doc", "user", "$.n", []byte("1"))
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, ok, err := e.JsonGet(ctx, "doc", "user", "$.n")
			if err != nil || !ok {
				b.Fatal(err, ok)
			}
		}
	})
}

func BenchmarkEngineJsonSetParallel(b *testing.B) {
	e := newModeJSON(b)
	defer e.Close()
	ctx := context.Background()
	_ = e.JsonSet(ctx, "doc", "user", "$", []byte(`{}`))
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if err := e.JsonSet(ctx, "doc", "user", fmt.Sprintf("$.k%d", i%32), []byte("1")); err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}
