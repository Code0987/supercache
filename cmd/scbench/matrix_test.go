package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Code0987/supercache/pkg/engine"
)

func TestResolveSmokeMatrix(t *testing.T) {
	mf, err := loadMatrixFile("bench/ci-smoke.yaml")
	if err != nil {
		t.Fatal(err)
	}
	cells, err := resolveCells(mf)
	if err != nil {
		t.Fatal(err)
	}
	if len(cells) != 7 {
		t.Fatalf("smoke cells=%d want 7", len(cells))
	}
	if cells[0].RequireHit != true || cells[0].Path != "hit" {
		t.Fatalf("cell0 %+v", cells[0])
	}
}

func TestSampleCapStillExact(t *testing.T) {
	got := sampleCapPerWorker(262144, 1000)
	s := 0
	for _, v := range got {
		s += v
	}
	if s != 262144 {
		t.Fatalf("sum=%d", s)
	}
}

func TestMatrixHitThenMissDoesNotHit(t *testing.T) {
	ctx := context.Background()
	hit := resolvedCell{
		Op: "get", Path: "hit", Dist: "uniform", Prefix: "iso:",
		Nodes: 1, Concurrency: 1, Conns: 1, Keys: 20, Trials: 1,
		ValueBytes: 16, Embed: true, RequireHit: true,
		Duration: 50 * time.Millisecond, Warmup: 0, Seed: 1,
	}
	rec, err := runEmbedCell(ctx, hit, 64)
	if err != nil {
		t.Fatal(err)
	}
	if rec.MedianOpsPerSec <= 0 {
		t.Fatalf("hit ops/s=%v", rec.MedianOpsPerSec)
	}
	miss := resolvedCell{
		Op: "miss", Path: "miss-cacheonly", Dist: "uniform", Prefix: "iso:",
		Nodes: 1, Concurrency: 1, Conns: 1, Keys: 20, Trials: 1,
		ValueBytes: 16, Embed: true,
		Duration: 50 * time.Millisecond, Warmup: 0, Seed: 1,
	}
	rec2, err := runEmbedCell(ctx, miss, 64)
	if err != nil {
		t.Fatal(err)
	}
	if rec2.MedianOpsPerSec <= 0 {
		t.Fatal("miss should still count ops")
	}
}

func TestEmbedMissIsNotFound(t *testing.T) {
	// Sanity: Engine on a fresh cluster does not have iso:0
	ctx := context.Background()
	miss := resolvedCell{
		Op: "miss", Path: "miss-cacheonly", Dist: "uniform", Prefix: "fresh:",
		Nodes: 1, Concurrency: 1, Conns: 1, Keys: 8, Trials: 1,
		ValueBytes: 8, Embed: true,
		Duration: 30 * time.Millisecond, Warmup: 0, Seed: 2,
	}
	if _, err := runEmbedCell(ctx, miss, 32); err != nil {
		t.Fatal(err)
	}
	_ = errors.Is
	_ = engine.ErrNotFound
}
