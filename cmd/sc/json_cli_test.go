package main

import (
	"strings"
	"testing"
	"time"

	"github.com/Code0987/supercache/internal/cacheserver"
	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
)

func TestJSONCLIUsage(t *testing.T) {
	if code := run([]string{"jsonset"}); code != 2 {
		t.Fatalf("jsonset no args: %d", code)
	}
	if code := run([]string{"jsonget"}); code != 2 {
		t.Fatalf("jsonget: %d", code)
	}
	if code := run([]string{"jsondel"}); code != 2 {
		t.Fatalf("jsondel: %d", code)
	}
}

func TestJSONCLIRoundTrip(t *testing.T) {
	eng := engine.New()
	defer eng.Close()
	if err := eng.UpdateKeySpace(keyspace.Config{
		Name: "doc", Mode: keyspace.ModeJSON, MaxBytes: 1 << 20, TTL: time.Hour,
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
		return run([]string{"-addr", addr, "-keyspace", "doc", "-q", "jsonset", "user", "$.name", `"Ada"`})
	})
	if code != 0 {
		t.Fatalf("jsonset: exit %d out=%q", code, out)
	}

	out, code = captureOut(func() int {
		return run([]string{"-addr", addr, "-keyspace", "doc", "jsonget", "user", "$.name"})
	})
	if code != 0 || strings.TrimSpace(out) != `"Ada"` {
		t.Fatalf("jsonget: exit %d out=%q", code, out)
	}

	out, code = captureOut(func() int {
		return run([]string{"-addr", addr, "-keyspace", "doc", "jsonget", "user", "$.nope"})
	})
	if code != 1 || strings.TrimSpace(out) != "(nil)" {
		t.Fatalf("jsonget miss: exit %d out=%q", code, out)
	}

	out, code = captureOut(func() int {
		return run([]string{"-addr", addr, "-keyspace", "doc", "jsonget", "user"})
	})
	if code != 0 || !strings.Contains(out, "Ada") {
		t.Fatalf("jsonget $: exit %d out=%q", code, out)
	}

	out, code = captureOut(func() int {
		return run([]string{"-addr", addr, "-keyspace", "doc", "-q", "jsondel", "user", "$.name"})
	})
	if code != 0 {
		t.Fatalf("jsondel: exit %d out=%q", code, out)
	}

	out, code = captureOut(func() int {
		return run([]string{"-addr", addr, "-keyspace", "doc", "jsonget", "user", "$.name"})
	})
	if code != 1 || strings.TrimSpace(out) != "(nil)" {
		t.Fatalf("after del: exit %d out=%q", code, out)
	}
}
