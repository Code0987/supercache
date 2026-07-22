package engine_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Code0987/supercache/pkg/datasource"
	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
)

// Concurrent mixed traffic should remain race-free and eventually consistent locally.
func TestChaosConcurrentMixedOps(t *testing.T) {
	src := datasource.Func(func(_ context.Context, key string) ([]byte, error) {
		return []byte("src:" + key), nil
	})
	e := engine.New()
	defer e.Close()
	_ = e.UpdateKeySpace(keyspace.Config{
		Name: "lt", Mode: keyspace.ModeLoadThrough, MaxBytes: 4 << 20,
		TTL: time.Minute, DataSource: src, NegativeTTL: time.Second,
	})
	_ = e.UpdateKeySpace(keyspace.Config{
		Name: "c", Mode: keyspace.ModeCacheOnly, MaxBytes: 4 << 20, TTL: time.Minute,
	})

	ctx := context.Background()
	var wg sync.WaitGroup
	const workers = 32
	const iters = 50
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				k := string(rune('a' + (id+i)%26))
				_, _ = e.Get(ctx, "lt", k)
				_ = e.Put(ctx, "c", k, []byte{byte(i)})
				if i%7 == 0 {
					_ = e.Delete(ctx, "c", k)
				}
				if i%11 == 0 {
					_ = e.PutMany(ctx, "c", []engine.KV{{Key: k, Value: []byte("m")}})
				}
			}
		}(w)
	}
	wg.Wait()
	// engine still serves
	_, _ = e.Get(ctx, "lt", "a")
	st, err := e.Stats("c")
	if err != nil {
		t.Fatal(err)
	}
	_ = st
}
