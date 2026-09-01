package engine_test

import (
	"context"
	"testing"
	"time"

	"github.com/Code0987/supercache/internal/benchmetrics"
	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
)

func newModeCMS(b *testing.B) *engine.Engine {
	b.Helper()
	e := engine.New()
	if err := e.UpdateKeySpace(keyspace.Config{
		Name: "freq", Mode: keyspace.ModeCMS, MaxBytes: 64 << 20, TTL: time.Hour,
	}); err != nil {
		b.Fatal(err)
	}
	return e
}

func BenchmarkEngineCMSIncr(b *testing.B) {
	e := newModeCMS(b)
	defer e.Close()
	ctx := context.Background()
	items := [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")}
	b.ReportAllocs()
	b.ResetTimer()
	before := benchmetrics.Read()
	for i := 0; i < b.N; i++ {
		if err := e.CMSIncr(ctx, "freq", "plays", items[i%len(items)], 1); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	benchmetrics.ReportB(b, before, benchmetrics.Read())
}

func BenchmarkEngineCMSIncrN(b *testing.B) {
	e := newModeCMS(b)
	defer e.Close()
	ctx := context.Background()
	items := [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")}
	b.ReportAllocs()
	b.ResetTimer()
	before := benchmetrics.Read()
	for i := 0; i < b.N; i++ {
		if err := e.CMSIncr(ctx, "freq", "plays", items[i%len(items)], 100); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	benchmetrics.ReportB(b, before, benchmetrics.Read())
}

func BenchmarkEngineCMSQueryHit(b *testing.B) {
	e := newModeCMS(b)
	defer e.Close()
	ctx := context.Background()
	_ = e.CMSIncr(ctx, "freq", "plays", []byte("alice"), 1)
	b.ReportAllocs()
	b.ResetTimer()
	before := benchmetrics.Read()
	for i := 0; i < b.N; i++ {
		_, ok, err := e.CMSQuery(ctx, "freq", "plays", []byte("alice"))
		if err != nil || !ok {
			b.Fatal(err, ok)
		}
	}
	b.StopTimer()
	benchmetrics.ReportB(b, before, benchmetrics.Read())
}

func BenchmarkEngineCMSQueryHitParallel(b *testing.B) {
	e := newModeCMS(b)
	defer e.Close()
	ctx := context.Background()
	_ = e.CMSIncr(ctx, "freq", "plays", []byte("alice"), 1)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, ok, err := e.CMSQuery(ctx, "freq", "plays", []byte("alice"))
			if err != nil || !ok {
				b.Fatal(err, ok)
			}
		}
	})
}

func BenchmarkEngineCMSIncrParallel(b *testing.B) {
	e := newModeCMS(b)
	defer e.Close()
	ctx := context.Background()
	items := [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if err := e.CMSIncr(ctx, "freq", "plays", items[i%len(items)], 1); err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}
