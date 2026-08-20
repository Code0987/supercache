package main

import (
	"strings"
	"testing"
	"time"

	"github.com/Code0987/supercache/internal/cacheserver"
	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
)

func TestIncrUsage(t *testing.T) {
	if code := run([]string{"incr"}); code != 2 {
		t.Fatalf("incr no args: %d", code)
	}
	if code := run([]string{"cget"}); code != 2 {
		t.Fatalf("cget: %d", code)
	}
}

func TestCounterCLIRoundTrip(t *testing.T) {
	eng := engine.New()
	defer eng.Close()
	if err := eng.UpdateKeySpace(keyspace.Config{
		Name: "ctr", Mode: keyspace.ModeCounter, MaxBytes: 1 << 20, TTL: time.Hour,
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
		return run([]string{"-addr", addr, "-keyspace", "ctr", "incr", "hits"})
	})
	if code != 0 || strings.TrimSpace(out) != "1" {
		t.Fatalf("incr default: exit %d out=%q", code, out)
	}

	out, code = captureOut(func() int {
		return run([]string{"-addr", addr, "-keyspace", "ctr", "incr", "hits", "5"})
	})
	if code != 0 || strings.TrimSpace(out) != "6" {
		t.Fatalf("incr 5: exit %d out=%q", code, out)
	}

	out, code = captureOut(func() int {
		return run([]string{"-addr", addr, "-keyspace", "ctr", "incr", "hits", "--", "-1"})
	})
	if code != 0 || strings.TrimSpace(out) != "5" {
		t.Fatalf("incr -1: exit %d out=%q", code, out)
	}

	out, code = captureOut(func() int {
		return run([]string{"-addr", addr, "-keyspace", "ctr", "cget", "hits"})
	})
	if code != 0 || strings.TrimSpace(out) != "5" {
		t.Fatalf("cget: exit %d out=%q", code, out)
	}

	out, code = captureOut(func() int {
		return run([]string{"-addr", addr, "-keyspace", "ctr", "cget", "nope"})
	})
	if code != 1 || strings.TrimSpace(out) != "(nil)" {
		t.Fatalf("cget miss: exit %d out=%q", code, out)
	}
}
