package engine_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/jsonx"
	"github.com/Code0987/supercache/pkg/keyspace"
	"github.com/Code0987/supercache/pkg/store"
)

func jsonKS() keyspace.Config {
	return keyspace.Config{Name: "doc", Mode: keyspace.ModeJSON, MaxBytes: 1 << 20, TTL: time.Hour}
}

func TestJSONSetGetDel(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(jsonKS())
	ctx := context.Background()
	if err := e.JsonSet(ctx, "doc", "user", "$", []byte(`{"name":"Ada","n":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := e.JsonSet(ctx, "doc", "user", "$.addr.city", []byte(`"Paris"`)); err != nil {
		t.Fatal(err)
	}
	v, ok, err := e.JsonGet(ctx, "doc", "user", "$.n")
	if err != nil || !ok || !jsonx.Equal(v, []byte("1")) {
		t.Fatalf("n: %s %v %v", v, ok, err)
	}
	v, ok, err = e.JsonGet(ctx, "doc", "user", "$.addr.city")
	if err != nil || !ok || !jsonx.Equal(v, []byte(`"Paris"`)) {
		t.Fatalf("city: %s %v %v", v, ok, err)
	}
	if err := e.JsonSet(ctx, "doc", "user", "$", []byte(`{"x":true}`)); err != nil {
		t.Fatal(err)
	}
	v, ok, err = e.JsonGet(ctx, "doc", "user", "$")
	if err != nil || !ok || !jsonx.Equal(v, []byte(`{"x":true}`)) {
		t.Fatalf("replace: %s %v %v", v, ok, err)
	}
}

func TestJSONIntegerRoundTrip(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(jsonKS())
	ctx := context.Background()
	const n = "9007199254740993"
	if err := e.JsonSet(ctx, "doc", "user", "$.n", []byte(n)); err != nil {
		t.Fatal(err)
	}
	v, ok, err := e.JsonGet(ctx, "doc", "user", "$.n")
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	dec, err := jsonx.Decode(v)
	if err != nil {
		t.Fatal(err)
	}
	num, ok := dec.(interface{ String() string })
	if !ok || num.String() != n {
		t.Fatalf("digits: %#v", dec)
	}
}

func TestJSONGetCopyIsolation(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(jsonKS())
	ctx := context.Background()
	_ = e.JsonSet(ctx, "doc", "user", "$", []byte(`{"a":"abc"}`))
	v, ok, err := e.JsonGet(ctx, "doc", "user", "$.a")
	if err != nil || !ok {
		t.Fatal(err, ok)
	}
	v[1] = 'Z'
	v2, ok, err := e.JsonGet(ctx, "doc", "user", "$.a")
	if err != nil || !ok || string(v2) != `"abc"` {
		t.Fatalf("%q %v %v", v2, ok, err)
	}
}

func TestJSONInvalidJSONAndPath(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(jsonKS())
	ctx := context.Background()
	if err := e.JsonSet(ctx, "doc", "user", "$", []byte("not-json")); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("bad json: %v", err)
	}
	if err := e.JsonSet(ctx, "doc", "user", "foo", []byte("1")); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("bad path: %v", err)
	}
	if _, _, err := e.JsonGet(ctx, "doc", "user", "$[-1]"); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("get path: %v", err)
	}
}

func TestJSONOversize(t *testing.T) {
	e := engine.New(engine.WithLimits(8, 4, 10))
	defer e.Close()
	_ = e.UpdateKeySpace(keyspace.Config{
		Name: "doc", Mode: keyspace.ModeJSON, MaxBytes: 1 << 20, MaxKeyLen: 3, MaxValueSize: 16,
	})
	ctx := context.Background()
	if err := e.JsonSet(ctx, "doc", "toolong", "$", []byte("1")); !errors.Is(err, engine.ErrKeyTooLarge) {
		t.Fatalf("name: %v", err)
	}
	// path uses engine maxKeyLen=8, not per-ks 3
	if err := e.JsonSet(ctx, "doc", "u", "$.abcd", []byte("1")); err != nil {
		t.Fatalf("path under engine max: %v", err)
	}
	longPath := "$." + "abcdefghij"
	if err := e.JsonSet(ctx, "doc", "u", longPath, []byte("1")); !errors.Is(err, engine.ErrKeyTooLarge) {
		t.Fatalf("path oversize: %v", err)
	}
	if err := e.JsonSet(ctx, "doc", "u", "$", []byte(`"12345678901234567890"`)); !errors.Is(err, engine.ErrValueTooLarge) {
		t.Fatalf("value: %v", err)
	}
	_ = e.JsonSet(ctx, "doc", "u", "$", []byte(`{"a":1}`))
	if err := e.JsonSet(ctx, "doc", "u", "$.b", []byte(`"zzzzzzzzzzzz"`)); !errors.Is(err, engine.ErrValueTooLarge) {
		t.Fatalf("merge overflow: %v", err)
	}
	v, ok, err := e.JsonGet(ctx, "doc", "u", "$")
	if err != nil || !ok || !jsonx.Equal(v, []byte(`{"a":1}`)) {
		t.Fatalf("mutated on overflow: %s %v %v", v, ok, err)
	}
}

func TestJSONWrongMode(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(keyspace.Config{Name: "hash", Mode: keyspace.ModeHash, MaxBytes: 1 << 20})
	_ = e.UpdateKeySpace(jsonKS())
	ctx := context.Background()
	if err := e.JsonSet(ctx, "hash", "h", "$", []byte(`{}`)); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("JsonSet on Hash: %v", err)
	}
	if _, err := e.Get(ctx, "doc", "h"); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("Get on JSON: %v", err)
	}
	if err := e.Put(ctx, "doc", "h", []byte("x")); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("Put on JSON: %v", err)
	}
	if err := e.HSet(ctx, "doc", "h", []byte("f"), []byte("v")); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("HSet on JSON: %v", err)
	}
}

func TestJSONMissing(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(jsonKS())
	ctx := context.Background()
	v, ok, err := e.JsonGet(ctx, "doc", "nope", "$")
	if err != nil || ok || v != nil {
		t.Fatalf("get: %v %v %v", v, ok, err)
	}
	if err := e.JsonDel(ctx, "doc", "nope", "$"); err != nil {
		t.Fatal(err)
	}
	if e.HasLocal("doc", "nope") {
		t.Fatal("JsonDel created")
	}
}

func TestJSONEmptyUntilDelete(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(jsonKS())
	ctx := context.Background()
	_ = e.JsonSet(ctx, "doc", "user", "$", []byte(`{"a":1}`))
	if err := e.JsonDel(ctx, "doc", "user", "$.a"); err != nil {
		t.Fatal(err)
	}
	v, ok, err := e.JsonGet(ctx, "doc", "user", "$")
	if err != nil || !ok || !jsonx.Equal(v, []byte(`{}`)) {
		t.Fatalf("last child: %s %v %v", v, ok, err)
	}
	if !e.HasLocal("doc", "user") {
		t.Fatal("want live empty")
	}
	if err := e.JsonDel(ctx, "doc", "user", "$"); err != nil {
		t.Fatal(err)
	}
	v, ok, err = e.JsonGet(ctx, "doc", "user", "$")
	if err != nil || !ok || !jsonx.Equal(v, []byte(`{}`)) {
		t.Fatalf("root del: %s %v %v", v, ok, err)
	}
	if !e.HasLocal("doc", "user") {
		t.Fatal("root del still live")
	}
	if err := e.Delete(ctx, "doc", "user"); err != nil {
		t.Fatal(err)
	}
	_, ok, err = e.JsonGet(ctx, "doc", "user", "$")
	if err != nil || ok {
		t.Fatalf("after Delete: %v %v", ok, err)
	}
}

func TestJSONSetAfterDelete(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(jsonKS())
	ctx := context.Background()
	_ = e.JsonSet(ctx, "doc", "user", "$", []byte(`{"a":1}`))
	_ = e.Delete(ctx, "doc", "user")
	if err := e.JsonSet(ctx, "doc", "user", "$.a", []byte("2")); err != nil {
		t.Fatal(err)
	}
	v, ok, err := e.JsonGet(ctx, "doc", "user", "$.a")
	if err != nil || !ok || !jsonx.Equal(v, []byte("2")) {
		t.Fatalf("%s %v %v", v, ok, err)
	}
}

func TestJSONTypeClash(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(jsonKS())
	ctx := context.Background()
	_ = e.JsonSet(ctx, "doc", "user", "$.a", []byte("1"))
	if err := e.JsonSet(ctx, "doc", "user", "$.a.b", []byte("2")); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("type clash: %v", err)
	}
	v, ok, err := e.JsonGet(ctx, "doc", "user", "$.a")
	if err != nil || !ok || !jsonx.Equal(v, []byte("1")) {
		t.Fatalf("unchanged: %s %v %v", v, ok, err)
	}
}

func TestJSONNoAutoArray(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(jsonKS())
	ctx := context.Background()
	_ = e.JsonSet(ctx, "doc", "user", "$", []byte(`{}`))
	if err := e.JsonSet(ctx, "doc", "user", "$.a[0]", []byte("1")); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("auto array: %v", err)
	}
}

func TestJSONLiveNull(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(jsonKS())
	ctx := context.Background()
	if err := e.JsonSet(ctx, "doc", "user", "$", []byte("null")); err != nil {
		t.Fatal(err)
	}
	v, ok, err := e.JsonGet(ctx, "doc", "user", "$")
	if err != nil || !ok || string(v) != "null" {
		t.Fatalf("%q %v %v", v, ok, err)
	}
	if !e.HasLocal("doc", "user") {
		t.Fatal("HasLocal")
	}
	if err := e.JsonSet(ctx, "doc", "user", "$.a", []byte("1")); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("set on null: %v", err)
	}
	v, ok, _ = e.JsonGet(ctx, "doc", "user", "$")
	if !ok || string(v) != "null" {
		t.Fatal("still null")
	}
	if err := e.JsonSet(ctx, "doc", "other", "$[0]", []byte("1")); !errors.Is(err, engine.ErrInvalidArgument) {
		t.Fatalf("missing+$[0]: %v", err)
	}
	if e.HasLocal("doc", "other") {
		t.Fatal("inserted")
	}
}

func TestJSONTTLSlide(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(jsonKS())
	ctx := context.Background()
	_ = e.JsonSet(ctx, "doc", "user", "$", []byte(`{"a":1}`))
	ent, err := e.GetOrLoadLocal(ctx, "doc", "user")
	if err != nil || !ent.IsJSON() || ent.ExpireAt == 0 {
		t.Fatalf("%+v %v", ent, err)
	}
	first := ent.ExpireAt
	time.Sleep(2 * time.Millisecond)
	_ = e.JsonSet(ctx, "doc", "user", "$.b", []byte("2"))
	ent, err = e.GetOrLoadLocal(ctx, "doc", "user")
	if err != nil || ent.ExpireAt <= first {
		t.Fatalf("ttl not slid %d then %d", first, ent.ExpireAt)
	}
}

func TestJSONGetOrLoadLocalMissing(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(jsonKS())
	_, err := e.GetOrLoadLocal(context.Background(), "doc", "nope")
	if !errors.Is(err, engine.ErrNotFound) {
		t.Fatal(err)
	}
}

func TestJSONTombstoneBlocksSnapshot(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(jsonKS())
	ctx := context.Background()
	_ = e.JsonSet(ctx, "doc", "user", "$", []byte(`{"a":1}`))
	_ = e.Delete(ctx, "doc", "user")
	applied, err := e.ApplyPut("doc", "user", store.Entry{
		Value: []byte(`{"a":1}`), Version: 1, Flags: store.FlagJSON,
	})
	if err != nil || applied {
		t.Fatalf("stale %v %v", applied, err)
	}
}

func TestJSONDelMissingPathNoSnapshot(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(jsonKS())
	ctx := context.Background()
	_ = e.JsonSet(ctx, "doc", "user", "$", []byte(`{"a":1}`))
	ent, _ := e.GetOrLoadLocal(ctx, "doc", "user")
	ver, exp := ent.Version, ent.ExpireAt
	if err := e.JsonDel(ctx, "doc", "user", "$.nope"); err != nil {
		t.Fatal(err)
	}
	ent, _ = e.GetOrLoadLocal(ctx, "doc", "user")
	if ent.Version != ver || ent.ExpireAt != exp {
		t.Fatalf("slid on no-op: %+v", ent)
	}
}

func TestJSONConcurrentDelVersions(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(jsonKS())
	ctx := context.Background()
	_ = e.JsonSet(ctx, "doc", "user", "$", []byte(`{"x":1,"y":2}`))
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = e.JsonDel(ctx, "doc", "user", "$.x")
	}()
	go func() {
		defer wg.Done()
		_ = e.JsonDel(ctx, "doc", "user", "$.y")
	}()
	wg.Wait()
	ent, err := e.GetOrLoadLocal(ctx, "doc", "user")
	if err != nil || ent.Version < 3 {
		t.Fatalf("ver=%d err=%v", ent.Version, err)
	}
	_, x, _ := e.JsonGet(ctx, "doc", "user", "$.x")
	_, y, _ := e.JsonGet(ctx, "doc", "user", "$.y")
	if x || y {
		t.Fatalf("x=%v y=%v", x, y)
	}
}

func TestJSONConcurrentSetDel(t *testing.T) {
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(jsonKS())
	ctx := context.Background()
	_ = e.JsonSet(ctx, "doc", "user", "$", []byte(`{"x":1,"y":2}`))
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = e.JsonSet(ctx, "doc", "user", "$.z", []byte("3"))
	}()
	go func() {
		defer wg.Done()
		_ = e.JsonDel(ctx, "doc", "user", "$.x")
	}()
	wg.Wait()
	ent, err := e.GetOrLoadLocal(ctx, "doc", "user")
	if err != nil || ent.Version < 3 {
		t.Fatalf("ver=%d err=%v", ent.Version, err)
	}
	_, x, _ := e.JsonGet(ctx, "doc", "user", "$.x")
	if x {
		t.Fatal("x should be gone if del applied; if set last, x gone still")
	}
	// del of x always removes x; set of z always adds z — they commute.
	_, z, _ := e.JsonGet(ctx, "doc", "user", "$.z")
	if !z {
		t.Fatal("z missing")
	}
}
