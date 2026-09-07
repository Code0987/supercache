package engine_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Code0987/supercache/pkg/datasource"
	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
	"github.com/Code0987/supercache/pkg/store"
)

func TestLocalViewKinds(t *testing.T) {
	e := engine.New()
	defer e.Close()
	src := datasource.Func(func(_ context.Context, key string) ([]byte, error) {
		if key == "missing" {
			return nil, datasource.ErrNotFound
		}
		return []byte("v"), nil
	})
	if err := e.UpdateKeySpace(keyspace.Config{
		Name: "lt", Mode: keyspace.ModeLoadThrough, MaxBytes: 1 << 20, TTL: time.Minute,
		NegativeTTL: time.Minute, DataSource: src,
	}); err != nil {
		t.Fatal(err)
	}
	if err := e.UpdateKeySpace(keyspace.Config{
		Name: "c", Mode: keyspace.ModeCacheOnly, MaxBytes: 1 << 20, TTL: time.Minute,
	}); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	miss := e.LocalView("c", "nope")
	if miss.Kind != engine.LocalMissing {
		t.Fatalf("absent key: %+v", miss)
	}
	if unknown := e.LocalView("nosuch", "k"); unknown.Kind != engine.LocalMissing {
		t.Fatalf("unknown keyspace: %+v", unknown)
	}

	if err := e.Put(ctx, "c", "k", []byte("hello")); err != nil {
		t.Fatal(err)
	}
	live := e.LocalView("c", "k")
	if live.Kind != engine.LocalLive || live.Bytes != 5 || live.Version == 0 {
		t.Fatalf("live: %+v", live)
	}
	if live.Flags&store.FlagTombstone != 0 || live.Flags&store.FlagNegative != 0 {
		t.Fatalf("live flags: %+v", live)
	}

	if err := e.Delete(ctx, "c", "k"); err != nil {
		t.Fatal(err)
	}
	tomb := e.LocalView("c", "k")
	if tomb.Kind != engine.LocalTombstone || tomb.Version == 0 {
		t.Fatalf("tombstone: %+v", tomb)
	}

	_, err := e.Get(ctx, "lt", "missing")
	if !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("want not found, got %v", err)
	}
	neg := e.LocalView("lt", "missing")
	if neg.Kind != engine.LocalNegative {
		t.Fatalf("negative: %+v", neg)
	}
}

func TestBloomDump(t *testing.T) {
	e := engine.New()
	defer e.Close()
	if err := e.UpdateKeySpace(keyspace.Config{
		Name: "bf", Mode: keyspace.ModeBloom, MaxBytes: 1 << 20,
		BloomBits: 64, BloomHashes: 4,
	}); err != nil {
		t.Fatal(err)
	}
	_, m, k, ok := e.BloomDump("bf", "users")
	if ok || m != 64 || k != 4 {
		t.Fatalf("missing: ok=%v m=%d k=%d", ok, m, k)
	}
	if err := e.BloomAdd(context.Background(), "bf", "users", []byte("alice")); err != nil {
		t.Fatal(err)
	}
	bits, m, k, ok := e.BloomDump("bf", "users")
	if !ok || m != 64 || k != 4 || len(bits) != 8 {
		t.Fatalf("dump: ok=%v m=%d k=%d len=%d", ok, m, k, len(bits))
	}
	ones := 0
	for _, b := range bits {
		for i := 0; i < 8; i++ {
			if b&(1<<i) != 0 {
				ones++
			}
		}
	}
	if ones == 0 {
		t.Fatal("expected some bits set")
	}
}
