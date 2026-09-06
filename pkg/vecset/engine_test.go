package vecset_test

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
	"github.com/Code0987/supercache/pkg/store"
	"github.com/Code0987/supercache/pkg/vecset"
)

func vsKS() keyspace.Config {
	return keyspace.Config{Name: "items", Mode: keyspace.ModeVectorSet, MaxBytes: 1 << 20, TTL: time.Hour}
}

func TestEngineVAddVSim(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(vsKS())
	ctx := context.Background()
	if err := e.VAdd(ctx, "items", "set", []byte("near"), []float32{1, 0}); err != nil {
		t.Fatal(err)
	}
	if err := e.VAdd(ctx, "items", "set", []byte("far"), []float32{0, 1}); err != nil {
		t.Fatal(err)
	}
	hits, err := e.VSim(ctx, "items", "set", []float32{1, 0}, 2)
	if err != nil || len(hits) != 2 || string(hits[0].Member) != "near" {
		t.Fatalf("%v %+v", err, hits)
	}
	n, ok, err := e.VCard(ctx, "items", "set")
	if err != nil || !ok || n != 2 {
		t.Fatal(n, ok, err)
	}
	dim, ok, err := e.VDim(ctx, "items", "set")
	if err != nil || !ok || dim != 2 {
		t.Fatal(dim, ok, err)
	}
	vec, ok, err := e.VEmb(ctx, "items", "set", []byte("near"))
	if err != nil || !ok || vec[0] != 1 {
		t.Fatal(vec, ok, err)
	}
}

func TestEngineZeroVecCosineVsL2(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(vsKS())
	_ = e.UpdateKeySpace(keyspace.Config{Name: "l2", Mode: keyspace.ModeVectorSet, MaxBytes: 1 << 20, VectorMetric: keyspace.VectorMetricL2})
	ctx := context.Background()
	if err := e.VAdd(ctx, "items", "s", []byte("a"), []float32{0, 0}); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("cosine zero add: %v", err)
	}
	if _, err := e.VSim(ctx, "items", "missing", []float32{0, 0}, 1); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("cosine zero sim: %v", err)
	}
	if err := e.VAdd(ctx, "l2", "s", []byte("a"), []float32{0, 0}); err != nil {
		t.Fatal(err)
	}
}

func TestEngineVSimMissingVsBad(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(vsKS())
	ctx := context.Background()
	if _, err := e.VSim(ctx, "items", "nope", []float32{float32(math.Inf(1))}, 1); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("inf: %v", err)
	}
	hits, err := e.VSim(ctx, "items", "nope", []float32{1, 0}, 3)
	if err != nil || len(hits) != 0 {
		t.Fatalf("missing: %v %v", err, hits)
	}
}

func TestEngineGetPutWrongMode(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(vsKS())
	_ = e.UpdateKeySpace(keyspace.Config{Name: "tags", Mode: keyspace.ModeSet, MaxBytes: 1 << 20})
	ctx := context.Background()
	if _, err := e.Get(ctx, "items", "s"); !errors.Is(err, engine.ErrInvalidArgument) || !strings.Contains(err.Error(), "VEmb") {
		t.Fatalf("get: %v", err)
	}
	if err := e.Put(ctx, "items", "s", []byte("x")); !errors.Is(err, engine.ErrInvalidArgument) || !strings.Contains(err.Error(), "VAdd") {
		t.Fatalf("put: %v", err)
	}
	if err := e.VAdd(ctx, "tags", "s", []byte("a"), []float32{1, 0}); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("vadd on set: %v", err)
	}
}

func TestEngineGetOrLoadLocalPeek(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(vsKS())
	ctx := context.Background()
	if err := e.VAdd(ctx, "items", "s", []byte("a"), []float32{1, 0}); err != nil {
		t.Fatal(err)
	}
	ent, err := e.GetOrLoadLocal(ctx, "items", "s")
	if err != nil || !ent.IsVectorSet() || len(ent.Value) == 0 || ent.Value[0] != vecset.PrefixSnapshot {
		t.Fatalf("%v %+v", err, ent)
	}
	if _, err := e.Get(ctx, "items", "s"); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatal(err)
	}
}

func TestEngineDeleteTombstone(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(vsKS())
	ctx := context.Background()
	_ = e.VAdd(ctx, "items", "s", []byte("a"), []float32{1, 0})
	if err := e.Delete(ctx, "items", "s"); err != nil {
		t.Fatal(err)
	}
	_, ok, err := e.VCard(ctx, "items", "s")
	if err != nil || ok {
		t.Fatal(ok, err)
	}
}

func TestEngineInboxIgnoredOnNonOwner(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(vsKS())
	inbox, err := vecset.EncodeInboxAdd([]byte("a"), []float32{1, 0})
	if err != nil {
		t.Fatal(err)
	}
	// no cluster: apply inbox as owner
	ok, err := e.ApplyPut("items", "s", store.Entry{Value: inbox, Flags: store.FlagVectorSet, Version: 1})
	if err != nil || !ok {
		t.Fatalf("owner apply: %v %v", ok, err)
	}
}
