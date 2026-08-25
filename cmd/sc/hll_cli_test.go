package main

import (
	"strings"
	"testing"
	"time"

	"github.com/Code0987/supercache/internal/cacheserver"
	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
)

func TestHLLCLIUsage(t *testing.T) {
	if code := run([]string{"hlladd"}); code != 2 {
		t.Fatalf("hlladd: %d", code)
	}
	if code := run([]string{"hlladd", "visitors"}); code != 2 {
		t.Fatalf("hlladd name only: %d", code)
	}
	if code := run([]string{"hllcount"}); code != 2 {
		t.Fatalf("hllcount: %d", code)
	}
}

func TestHLLCLIRoundTrip(t *testing.T) {
	eng := engine.New()
	defer eng.Close()
	if err := eng.UpdateKeySpace(keyspace.Config{
		Name: "uniq", Mode: keyspace.ModeHLL, MaxBytes: 1 << 20, TTL: time.Hour,
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
		return run([]string{"-addr", addr, "-keyspace", "uniq", "-q", "hlladd", "visitors", "alice", "bob"})
	})
	if code != 0 {
		t.Fatalf("hlladd: exit %d out=%q", code, out)
	}

	out, code = captureOut(func() int {
		return run([]string{"-addr", addr, "-keyspace", "uniq", "hllcount", "visitors"})
	})
	if code != 0 {
		t.Fatalf("hllcount: exit %d out=%q", code, out)
	}
	if strings.TrimSpace(out) == "" || strings.TrimSpace(out) == "(nil)" {
		t.Fatalf("hllcount empty: %q", out)
	}

	out, code = captureOut(func() int {
		return run([]string{"-addr", addr, "-keyspace", "uniq", "hllcount", "missing"})
	})
	if code != 1 || strings.TrimSpace(out) != "(nil)" {
		t.Fatalf("hllcount miss: exit %d out=%q", code, out)
	}
}
