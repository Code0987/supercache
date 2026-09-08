package main

import (
	"strings"
	"testing"
	"time"

	"github.com/Code0987/supercache/internal/cacheserver"
	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
)

func TestXAddUsage(t *testing.T) {
	if code := run([]string{"xadd"}); code != 2 {
		t.Fatalf("xadd: %d", code)
	}
	if code := run([]string{"xadd", "logs", "notstar", "hi"}); code != 2 {
		t.Fatalf("xadd no star: %d", code)
	}
	if code := run([]string{"xlen"}); code != 2 {
		t.Fatalf("xlen: %d", code)
	}
	if code := run([]string{"xrange", "logs"}); code != 2 {
		t.Fatalf("xrange: %d", code)
	}
}

func TestStreamCLIRoundTrip(t *testing.T) {
	eng := engine.New()
	defer eng.Close()
	if err := eng.UpdateKeySpace(keyspace.Config{
		Name: "events", Mode: keyspace.ModeStream, MaxBytes: 1 << 20, TTL: time.Hour,
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
		return run([]string{"-addr", addr, "-keyspace", "events", "xadd", "logs", "*", "hello"})
	})
	if code != 0 || !strings.Contains(out, "-") {
		t.Fatalf("xadd: exit %d out=%q", code, out)
	}
	out, code = captureOut(func() int {
		return run([]string{"-addr", addr, "-keyspace", "events", "xlen", "logs"})
	})
	if code != 0 || strings.TrimSpace(out) != "1" {
		t.Fatalf("xlen: exit %d out=%q", code, out)
	}
	out, code = captureOut(func() int {
		return run([]string{"-addr", addr, "-keyspace", "events", "xrange", "logs", "-", "+"})
	})
	if code != 0 || !strings.Contains(out, "hello") {
		t.Fatalf("xrange: exit %d out=%q", code, out)
	}
}
