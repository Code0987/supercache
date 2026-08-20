package engine_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/hashx"
	"github.com/Code0987/supercache/pkg/keyspace"
	"github.com/Code0987/supercache/pkg/store"
)

func hashKS() keyspace.Config {
	return keyspace.Config{Name: "hash", Mode: keyspace.ModeHash, MaxBytes: 1 << 20, TTL: time.Hour}
}

func TestHashSetGetDelExistsLen(t *testing.T) {
	e := engine.New()
	defer e.Close()
	if err := e.UpdateKeySpace(hashKS()); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := e.HSet(ctx, "hash", "user", []byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := e.HSet(ctx, "hash", "user", []byte("b"), []byte("2")); err != nil {
		t.Fatal(err)
	}
	if err := e.HSet(ctx, "hash", "user", []byte("a"), []byte("1x")); err != nil {
		t.Fatal(err)
	}
	v, ok, err := e.HGet(ctx, "hash", "user", []byte("a"))
	if err != nil || !ok || string(v) != "1x" {
		t.Fatalf("%q %v %v", v, ok, err)
	}
	n, err := e.HLen(ctx, "hash", "user")
	if err != nil || n != 2 {
		t.Fatalf("len=%d err=%v", n, err)
	}
	ex, err := e.HExists(ctx, "hash", "user", []byte("b"))
	if err != nil || !ex {
		t.Fatalf("exists b: %v %v", ex, err)
	}
	if err := e.HDel(ctx, "hash", "user", []byte("b")); err != nil {
		t.Fatal(err)
	}
	_, ok, err = e.HGet(ctx, "hash", "user", []byte("b"))
	if err != nil || ok {
		t.Fatalf("after del: %v %v", ok, err)
	}
}

func TestHashGetAllOrderAndCopies(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(hashKS())
	ctx := context.Background()
	_ = e.HSet(ctx, "hash", "user", []byte("b"), []byte("2"))
	_ = e.HSet(ctx, "hash", "user", []byte("a"), []byte("1"))
	all, err := e.HGetAll(ctx, "hash", "user")
	if err != nil || len(all) != 2 || string(all[0].Field) != "a" || string(all[1].Field) != "b" {
		t.Fatalf("%+v %v", all, err)
	}
	all[0].Value[0] = 'Z'
	all[0].Field[0] = 'Z'
	again, err := e.HGetAll(ctx, "hash", "user")
	if err != nil || string(again[0].Field) != "a" || string(again[0].Value) != "1" {
		t.Fatalf("copy: %+v", again)
	}
}

func TestHashGetCopyIsolation(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(hashKS())
	ctx := context.Background()
	_ = e.HSet(ctx, "hash", "user", []byte("f"), []byte("abc"))
	v, ok, err := e.HGet(ctx, "hash", "user", []byte("f"))
	if err != nil || !ok {
		t.Fatal(err, ok)
	}
	v[0] = 'Z'
	v2, ok, err := e.HGet(ctx, "hash", "user", []byte("f"))
	if err != nil || !ok || string(v2) != "abc" {
		t.Fatalf("%q %v %v", v2, ok, err)
	}
}

func TestHashEmptyStoredValue(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(hashKS())
	ctx := context.Background()
	if err := e.HSet(ctx, "hash", "user", []byte("e"), nil); err != nil {
		t.Fatal(err)
	}
	v, ok, err := e.HGet(ctx, "hash", "user", []byte("e"))
	if err != nil || !ok || v == nil || len(v) != 0 {
		t.Fatalf("%#v %v %v", v, ok, err)
	}
}

func TestHashWrongMode(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(keyspace.Config{Name: "set", Mode: keyspace.ModeSet, MaxBytes: 1 << 20})
	_ = e.UpdateKeySpace(hashKS())
	ctx := context.Background()
	if err := e.HSet(ctx, "set", "h", []byte("f"), []byte("v")); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("HSet on Set: %v", err)
	}
	if err := e.SetAdd(ctx, "hash", "h", []byte("x")); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("SetAdd on Hash: %v", err)
	}
	if err := e.Put(ctx, "hash", "h", []byte("x")); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("Put on Hash: %v", err)
	}
	if _, err := e.Get(ctx, "hash", "h"); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("Get on Hash: %v", err)
	}
}

func TestHashMissingEmpty(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(hashKS())
	ctx := context.Background()
	v, ok, err := e.HGet(ctx, "hash", "nope", []byte("f"))
	if err != nil || ok || v != nil {
		t.Fatalf("get: %v %v %v", v, ok, err)
	}
	n, err := e.HLen(ctx, "hash", "nope")
	if err != nil || n != 0 {
		t.Fatalf("len: %d %v", n, err)
	}
	ex, err := e.HExists(ctx, "hash", "nope", []byte("f"))
	if err != nil || ex {
		t.Fatalf("exists: %v %v", ex, err)
	}
	all, err := e.HGetAll(ctx, "hash", "nope")
	if err != nil || len(all) != 0 {
		t.Fatalf("getall: %v %v", all, err)
	}
	if err := e.HDel(ctx, "hash", "nope", []byte("f")); err != nil {
		t.Fatal(err)
	}
	if e.HasLocal("hash", "nope") {
		t.Fatal("HDel created")
	}
}

func TestHashEmptyFieldAllVerbs(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(hashKS())
	ctx := context.Background()
	if err := e.HSet(ctx, "hash", "h", nil, []byte("v")); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("HSet: %v", err)
	}
	if _, _, err := e.HGet(ctx, "hash", "h", []byte{}); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("HGet: %v", err)
	}
	if err := e.HDel(ctx, "hash", "h", nil); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("HDel: %v", err)
	}
	if _, err := e.HExists(ctx, "hash", "h", nil); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("HExists: %v", err)
	}
}

func TestHashOversizeFieldValue(t *testing.T) {
	e := engine.New(engine.WithLimits(8, 4, 10))
	defer e.Close()
	_ = e.UpdateKeySpace(keyspace.Config{
		Name: "hash", Mode: keyspace.ModeHash, MaxBytes: 1 << 20, MaxKeyLen: 3, MaxValueSize: 2,
	})
	ctx := context.Background()
	// name uses per-ks MaxKeyLen=3
	if err := e.HSet(ctx, "hash", "toolong", []byte("f"), []byte("v")); !errors.Is(err, engine.ErrKeyTooLarge) {
		t.Fatalf("name: %v", err)
	}
	// field uses engine maxKeyLen=8, not per-ks 3
	if err := e.HSet(ctx, "hash", "h", []byte("abcd"), []byte("v")); err != nil {
		t.Fatalf("field under engine max: %v", err)
	}
	if err := e.HSet(ctx, "hash", "h", []byte("123456789"), []byte("v")); !errors.Is(err, engine.ErrKeyTooLarge) {
		t.Fatalf("field oversize: %v", err)
	}
	if _, _, err := e.HGet(ctx, "hash", "h", []byte("123456789")); !errors.Is(err, engine.ErrKeyTooLarge) {
		t.Fatalf("HGet field: %v", err)
	}
	// value uses per-ks MaxValueSize=2
	if err := e.HSet(ctx, "hash", "h", []byte("f"), []byte("xyz")); !errors.Is(err, engine.ErrValueTooLarge) {
		t.Fatalf("value: %v", err)
	}
}

func TestHashEmptyAfterLastHDel(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(hashKS())
	ctx := context.Background()
	_ = e.HSet(ctx, "hash", "h", []byte("a"), []byte("1"))
	_ = e.HDel(ctx, "hash", "h", []byte("a"))
	n, err := e.HLen(ctx, "hash", "h")
	if err != nil || n != 0 {
		t.Fatalf("empty len: %d %v", n, err)
	}
	if !e.HasLocal("hash", "h") {
		t.Fatal("want empty entry until Delete")
	}
	if err := e.Delete(ctx, "hash", "h"); err != nil {
		t.Fatal(err)
	}
	if e.HasLocal("hash", "h") {
		t.Fatal("after delete")
	}
}

func TestHashSetAfterDelete(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(hashKS())
	ctx := context.Background()
	_ = e.HSet(ctx, "hash", "h", []byte("a"), []byte("1"))
	_ = e.Delete(ctx, "hash", "h")
	if err := e.HSet(ctx, "hash", "h", []byte("a"), []byte("2")); err != nil {
		t.Fatal(err)
	}
	v, ok, err := e.HGet(ctx, "hash", "h", []byte("a"))
	if err != nil || !ok || string(v) != "2" {
		t.Fatalf("%q %v %v", v, ok, err)
	}
}

func TestHashTombstoneBlocksStaleSnapshot(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(hashKS())
	ctx := context.Background()
	_ = e.HSet(ctx, "hash", "h", []byte("a"), []byte("1"))
	if err := e.Delete(ctx, "hash", "h"); err != nil {
		t.Fatal(err)
	}
	h := hashx.New()
	h.Set([]byte("a"), []byte("1"))
	applied, err := e.ApplyPut("hash", "h", store.Entry{Value: h.Encode(), Version: 1, Flags: store.FlagHash})
	if err != nil {
		t.Fatal(err)
	}
	if applied {
		t.Fatal("stale FlagHash")
	}
}

func TestHashTTLRefresh(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(hashKS())
	ctx := context.Background()
	_ = e.HSet(ctx, "hash", "h", []byte("a"), []byte("1"))
	ent, err := e.GetOrLoadLocal(ctx, "hash", "h")
	if err != nil || ent.ExpireAt == 0 {
		t.Fatalf("expire: %+v %v", ent, err)
	}
	first := ent.ExpireAt
	time.Sleep(2 * time.Millisecond)
	_ = e.HSet(ctx, "hash", "h", []byte("b"), []byte("2"))
	ent, err = e.GetOrLoadLocal(ctx, "hash", "h")
	if err != nil || ent.ExpireAt <= first {
		t.Fatalf("ttl not refreshed: %d then %d err=%v", first, ent.ExpireAt, err)
	}
	if !ent.IsHash() {
		t.Fatal("want FlagHash")
	}
}

func TestHashGetOrLoadLocalMissing(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(hashKS())
	_, err := e.GetOrLoadLocal(context.Background(), "hash", "nope")
	if !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("%v", err)
	}
}
