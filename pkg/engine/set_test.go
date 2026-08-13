package engine_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
	"github.com/Code0987/supercache/pkg/set"
	"github.com/Code0987/supercache/pkg/store"
)

func setKS() keyspace.Config {
	return keyspace.Config{Name: "st", Mode: keyspace.ModeSet, MaxBytes: 1 << 20, TTL: time.Hour}
}

func TestSetAddContainsRemove(t *testing.T) {
	e := engine.New()
	defer e.Close()
	if err := e.UpdateKeySpace(setKS()); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := e.SetAdd(ctx, "st", "tags", []byte("red")); err != nil {
		t.Fatal(err)
	}
	ok, err := e.SetContains(ctx, "st", "tags", []byte("red"))
	if err != nil || !ok {
		t.Fatalf("contains: %v %v", ok, err)
	}
	n, err := e.SetCard(ctx, "st", "tags")
	if err != nil || n != 1 {
		t.Fatalf("card: %d %v", n, err)
	}
	mem, err := e.SetMembers(ctx, "st", "tags")
	if err != nil || len(mem) != 1 || string(mem[0]) != "red" {
		t.Fatalf("members: %v %v", mem, err)
	}
	if err := e.SetRemove(ctx, "st", "tags", []byte("red")); err != nil {
		t.Fatal(err)
	}
	ok, err = e.SetContains(ctx, "st", "tags", []byte("red"))
	if err != nil || ok {
		t.Fatalf("after remove: %v %v", ok, err)
	}
	n, err = e.SetCard(ctx, "st", "tags")
	if err != nil || n != 0 {
		t.Fatalf("empty card: %d %v", n, err)
	}
}

func TestSetWrongMode(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(keyspace.Config{Name: "kv", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20})
	_ = e.UpdateKeySpace(setKS())
	ctx := context.Background()
	if err := e.SetAdd(ctx, "kv", "s", []byte("x")); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("SetAdd on CacheOnly: %v", err)
	}
	if err := e.Put(ctx, "st", "s", []byte("x")); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("Put on ModeSet: %v", err)
	}
	if _, err := e.Get(ctx, "st", "s"); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("Get on ModeSet: %v", err)
	}
}

func TestSetMissingContainsFalse(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(setKS())
	ctx := context.Background()
	ok, err := e.SetContains(ctx, "st", "nope", []byte("x"))
	if err != nil || ok {
		t.Fatalf("contains: %v %v", ok, err)
	}
	n, err := e.SetCard(ctx, "st", "nope")
	if err != nil || n != 0 {
		t.Fatalf("card: %d %v", n, err)
	}
	mem, err := e.SetMembers(ctx, "st", "nope")
	if err != nil || len(mem) != 0 {
		t.Fatalf("members: %v %v", mem, err)
	}
}

func TestSetIdempotentAddRemove(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(setKS())
	ctx := context.Background()
	_ = e.SetAdd(ctx, "st", "s", []byte("a"))
	_ = e.SetAdd(ctx, "st", "s", []byte("a"))
	n, _ := e.SetCard(ctx, "st", "s")
	if n != 1 {
		t.Fatal(n)
	}
	_ = e.SetRemove(ctx, "st", "s", []byte("a"))
	_ = e.SetRemove(ctx, "st", "s", []byte("a"))
	n, _ = e.SetCard(ctx, "st", "s")
	if n != 0 {
		t.Fatal(n)
	}
}

func TestSetDeleteNameTombstone(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(setKS())
	ctx := context.Background()
	_ = e.SetAdd(ctx, "st", "s", []byte("a"))
	if err := e.Delete(ctx, "st", "s"); err != nil {
		t.Fatal(err)
	}
	ok, _ := e.SetContains(ctx, "st", "s", []byte("a"))
	if ok {
		t.Fatal("after delete")
	}
	// Stale snapshot must not resurrect.
	blob := set.Encode([][]byte{[]byte("a")})
	applied, err := e.ApplyPut("st", "s", store.Entry{Value: blob, Version: 1, Flags: store.FlagSet})
	if err != nil {
		t.Fatal(err)
	}
	if applied {
		t.Fatal("stale FlagSet install")
	}
}

func TestSetAddAfterDelete(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(setKS())
	ctx := context.Background()
	_ = e.SetAdd(ctx, "st", "s", []byte("a"))
	_ = e.Delete(ctx, "st", "s")
	if err := e.SetAdd(ctx, "st", "s", []byte("b")); err != nil {
		t.Fatal(err)
	}
	ok, err := e.SetContains(ctx, "st", "s", []byte("b"))
	if err != nil || !ok {
		t.Fatalf("recreate: %v %v", ok, err)
	}
}
