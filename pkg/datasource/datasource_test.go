package datasource

import (
	"context"
	"errors"
	"testing"
)

func TestFuncAndMap(t *testing.T) {
	ctx := context.Background()
	f := Func(func(_ context.Context, key string) ([]byte, error) {
		if key == "miss" {
			return nil, ErrNotFound
		}
		return []byte("v:" + key), nil
	})
	v, err := f.Load(ctx, "a")
	if err != nil || string(v) != "v:a" {
		t.Fatalf("Func: %q err=%v", v, err)
	}
	if _, err := f.Load(ctx, "miss"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}

	m := Map{"k": []byte("val")}
	v, err = m.Load(ctx, "k")
	if err != nil || string(v) != "val" {
		t.Fatalf("Map hit: %q err=%v", v, err)
	}
	// defensive copy
	v[0] = 'X'
	v2, _ := m.Load(ctx, "k")
	if string(v2) != "val" {
		t.Fatal("Map must copy")
	}
	if _, err := m.Load(ctx, "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
}
