package testcluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Code0987/supercache/pkg/datasource"
	"github.com/Code0987/supercache/pkg/engine"
)

func TestLTLastVerPruneCheap(t *testing.T) {
	val := make([]byte, 256)
	src := datasource.Func(func(context.Context, string) ([]byte, error) {
		out := make([]byte, len(val))
		copy(out, val)
		return out, nil
	})
	e := engine.New(engine.WithMaxVersionKeys(65536))
	defer e.Close()
	if err := e.UpdateKeySpace(LoadThroughBench(src)); err != nil {
		t.Fatal(err)
	}
	// Ensure MaxBytes is the 1MiB pairing, not 64MiB.
	if LoadThroughBench(src).MaxBytes != 1<<20 {
		t.Fatal("benchlt MaxBytes")
	}
	ctx := context.Background()
	const n = 200_000
	for i := 0; i < n; i++ {
		_, err := e.Get(ctx, "benchlt", fmt.Sprintf("u%d", i))
		if err != nil {
			t.Fatal(err)
		}
	}
	start := time.Now()
	const tail = 1000
	for i := n; i < n+tail; i++ {
		if _, err := e.Get(ctx, "benchlt", fmt.Sprintf("u%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	per := time.Since(start) / tail
	if per > time.Millisecond {
		t.Fatalf("Get after 200k unique keys took %v/op (want <1ms); lastVer prune storm?", per)
	}
}
