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

func newModeList(b *testing.B) *engine.Engine {
	b.Helper()
	e := engine.New()
	if err := e.UpdateKeySpace(keyspace.Config{
		Name: "ls", Mode: keyspace.ModeList, MaxBytes: 64 << 20, TTL: time.Hour,
	}); err != nil {
		b.Fatal(err)
	}
	return e
}

func BenchmarkEngineLPush(b *testing.B) {
	e := newModeList(b)
	defer e.Close()
	ctx := context.Background()
	const n = 256
	b.ReportAllocs()
	b.ResetTimer()
	before := benchmetrics.Read()
	for i := 0; i < b.N; i++ {
		if err := e.LPush(ctx, "ls", "q", []byte(fmt.Sprintf("i%d", i%n))); err != nil {
			b.Fatal(err)
		}
		if i >= n {
			_, _, _ = e.RPop(ctx, "ls", "q")
		}
	}
	b.StopTimer()
	benchmetrics.ReportB(b, before, benchmetrics.Read())
}

func BenchmarkEngineLLen(b *testing.B) {
	e := newModeList(b)
	defer e.Close()
	ctx := context.Background()
	for i := 0; i < 64; i++ {
		_ = e.RPush(ctx, "ls", "q", []byte(fmt.Sprintf("i%d", i)))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n, err := e.LLen(ctx, "ls", "q")
		if err != nil || n != 64 {
			b.Fatal(err, n)
		}
	}
}

func BenchmarkEngineLIndexHit(b *testing.B) {
	e := newModeList(b)
	defer e.Close()
	ctx := context.Background()
	for i := 0; i < 64; i++ {
		_ = e.RPush(ctx, "ls", "q", []byte(fmt.Sprintf("i%d", i)))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, ok, err := e.LIndex(ctx, "ls", "q", 0)
		if err != nil || !ok {
			b.Fatal(err, ok)
		}
	}
}

func BenchmarkEngineLRange(b *testing.B) {
	e := newModeList(b)
	defer e.Close()
	ctx := context.Background()
	for i := 0; i < 1000; i++ {
		_ = e.RPush(ctx, "ls", "q", []byte(fmt.Sprintf("i%d", i)))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, err := e.LRange(ctx, "ls", "q", 0, 9)
		if err != nil || len(r) != 10 {
			b.Fatal(err, len(r))
		}
	}
}
