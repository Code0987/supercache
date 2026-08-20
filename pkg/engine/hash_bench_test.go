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

func newModeHash(b *testing.B) *engine.Engine {
	b.Helper()
	e := engine.New()
	if err := e.UpdateKeySpace(keyspace.Config{
		Name: "hash", Mode: keyspace.ModeHash, MaxBytes: 64 << 20, TTL: time.Hour,
	}); err != nil {
		b.Fatal(err)
	}
	return e
}

func BenchmarkEngineHSet(b *testing.B) {
	e := newModeHash(b)
	defer e.Close()
	ctx := context.Background()
	const n = 1000
	b.ReportAllocs()
	b.ResetTimer()
	before := benchmetrics.Read()
	for i := 0; i < b.N; i++ {
		if err := e.HSet(ctx, "hash", "h", []byte(fmt.Sprintf("f%d", i%n)), []byte("v")); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	benchmetrics.ReportB(b, before, benchmetrics.Read())
}

func BenchmarkEngineHGetHit(b *testing.B) {
	e := newModeHash(b)
	defer e.Close()
	ctx := context.Background()
	const n = 1000
	for i := 0; i < n; i++ {
		_ = e.HSet(ctx, "hash", "h", []byte(fmt.Sprintf("f%d", i)), []byte("v"))
	}
	field := []byte("f0")
	b.ReportAllocs()
	b.ResetTimer()
	before := benchmetrics.Read()
	for i := 0; i < b.N; i++ {
		_, ok, err := e.HGet(ctx, "hash", "h", field)
		if err != nil || !ok {
			b.Fatal(err, ok)
		}
	}
	b.StopTimer()
	benchmetrics.ReportB(b, before, benchmetrics.Read())
}

func BenchmarkEngineHGetMiss(b *testing.B) {
	e := newModeHash(b)
	defer e.Close()
	ctx := context.Background()
	for i := 0; i < 1000; i++ {
		_ = e.HSet(ctx, "hash", "h", []byte(fmt.Sprintf("f%d", i)), []byte("v"))
	}
	field := []byte("never")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, ok, err := e.HGet(ctx, "hash", "h", field)
		if err != nil || ok {
			b.Fatal(err, ok)
		}
	}
}

func BenchmarkEngineHLen(b *testing.B) {
	e := newModeHash(b)
	defer e.Close()
	ctx := context.Background()
	for i := 0; i < 64; i++ {
		_ = e.HSet(ctx, "hash", "h", []byte(fmt.Sprintf("f%d", i)), []byte("v"))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n, err := e.HLen(ctx, "hash", "h")
		if err != nil || n != 64 {
			b.Fatal(err, n)
		}
	}
}

func BenchmarkEngineHGetAll(b *testing.B) {
	e := newModeHash(b)
	defer e.Close()
	ctx := context.Background()
	for i := 0; i < 64; i++ {
		_ = e.HSet(ctx, "hash", "h", []byte(fmt.Sprintf("f%d", i)), []byte("v"))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		all, err := e.HGetAll(ctx, "hash", "h")
		if err != nil || len(all) != 64 {
			b.Fatal(err, len(all))
		}
	}
}

func BenchmarkEngineHGetHitParallel(b *testing.B) {
	e := newModeHash(b)
	defer e.Close()
	ctx := context.Background()
	for i := 0; i < 1000; i++ {
		_ = e.HSet(ctx, "hash", "h", []byte(fmt.Sprintf("f%d", i)), []byte("v"))
	}
	field := []byte("f0")
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, ok, err := e.HGet(ctx, "hash", "h", field)
			if err != nil || !ok {
				b.Fatal(err, ok)
			}
		}
	})
}

func BenchmarkEngineHSetParallel(b *testing.B) {
	e := newModeHash(b)
	defer e.Close()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if err := e.HSet(ctx, "hash", "h", []byte(fmt.Sprintf("f%d", i%1000)), []byte("v")); err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}
