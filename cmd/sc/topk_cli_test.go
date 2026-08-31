package main

import (
	"strings"
	"testing"
	"time"

	"github.com/Code0987/supercache/internal/cacheserver"
	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
)

func TestTopKCLIUsage(t *testing.T) {
	if code := run([]string{"topkadd"}); code != 2 {
		t.Fatalf("topkadd: %d", code)
	}
	if code := run([]string{"topkadd", "hot"}); code != 2 {
		t.Fatalf("topkadd name only: %d", code)
	}
	if code := run([]string{"topklist"}); code != 2 {
		t.Fatalf("topklist: %d", code)
	}
}

func TestTopKCLIRoundTrip(t *testing.T) {
	eng := engine.New()
	defer eng.Close()
	if err := eng.UpdateKeySpace(keyspace.Config{
		Name: "plays", Mode: keyspace.ModeTopK, MaxBytes: 1 << 20, TTL: time.Hour, TopKSize: 10,
	}); err != nil {
		t.Fatal(err)
	}
	gs, lis, err := cacheserver.ListenAndServe("127.0.0.1:0", eng)
	if err != nil {
		t.Fatal(err)
	}
	defer gs.Stop()
	addr := lis.Addr().String()

	out, code := captureOut(func() int {
		return run([]string{"-addr", addr, "-keyspace", "plays", "-q", "topkadd", "hot", "t001", "t002", "t001"})
	})
	if code != 0 {
		t.Fatalf("topkadd: exit %d out=%q", code, out)
	}

	out, code = captureOut(func() int {
		return run([]string{"-addr", addr, "-keyspace", "plays", "topklist", "hot"})
	})
	if code != 0 {
		t.Fatalf("topklist: exit %d out=%q", code, out)
	}
	if !strings.Contains(out, "t001 2") || !strings.Contains(out, "t002 1") {
		t.Fatalf("topklist rows: %q", out)
	}

	out, code = captureOut(func() int {
		return run([]string{"-addr", addr, "-keyspace", "plays", "topklist", "missing"})
	})
	if code != 1 || strings.TrimSpace(out) != "(nil)" {
		t.Fatalf("topklist miss: exit %d out=%q", code, out)
	}
}
