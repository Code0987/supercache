package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Code0987/supercache/internal/cacheserver"
	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
)

func TestSAddUsage(t *testing.T) {
	if code := run([]string{"sadd"}); code != 2 {
		t.Fatalf("sadd no args: exit %d want 2", code)
	}
	if code := run([]string{"srem", "onlyname"}); code != 2 {
		t.Fatalf("srem one arg: exit %d want 2", code)
	}
	if code := run([]string{"sismember"}); code != 2 {
		t.Fatalf("sismember: exit %d want 2", code)
	}
	if code := run([]string{"scard"}); code != 2 {
		t.Fatalf("scard: exit %d want 2", code)
	}
	if code := run([]string{"smembers"}); code != 2 {
		t.Fatalf("smembers: exit %d want 2", code)
	}
}

func TestSetStillPutAlias(t *testing.T) {
	cmd, _, pos, err := splitArgs([]string{"set", "k", "v"})
	if err != nil {
		t.Fatal(err)
	}
	if cmd != "set" || len(pos) != 2 || pos[0] != "k" || pos[1] != "v" {
		t.Fatalf("cmd=%q pos=%v", cmd, pos)
	}
}

func TestSetCLIRoundTrip(t *testing.T) {
	eng := engine.New()
	defer eng.Close()
	if err := eng.UpdateKeySpace(keyspace.Config{
		Name: "tags", Mode: keyspace.ModeSet, MaxBytes: 1 << 20, TTL: time.Hour,
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
		return run([]string{"-addr", addr, "-keyspace", "tags", "-q", "sadd", "features", "dark_mode"})
	})
	if code != 0 {
		t.Fatalf("sadd: exit %d out=%q", code, out)
	}

	out, code = captureOut(func() int {
		return run([]string{"-addr", addr, "-keyspace", "tags", "sismember", "features", "dark_mode"})
	})
	if code != 0 || strings.TrimSpace(out) != "true" {
		t.Fatalf("sismember hit: exit %d out=%q", code, out)
	}

	out, code = captureOut(func() int {
		return run([]string{"-addr", addr, "-keyspace", "tags", "sismember", "features", "nope"})
	})
	if code != 1 || strings.TrimSpace(out) != "false" {
		t.Fatalf("sismember miss: exit %d out=%q", code, out)
	}

	out, code = captureOut(func() int {
		return run([]string{"-addr", addr, "-keyspace", "tags", "scard", "features"})
	})
	if code != 0 || strings.TrimSpace(out) != "1" {
		t.Fatalf("scard: exit %d out=%q", code, out)
	}

	out, code = captureOut(func() int {
		return run([]string{"-addr", addr, "-keyspace", "tags", "smembers", "features"})
	})
	if code != 0 || strings.TrimSpace(out) != "dark_mode" {
		t.Fatalf("smembers: exit %d out=%q", code, out)
	}

	out, code = captureOut(func() int {
		return run([]string{"-addr", addr, "-keyspace", "tags", "-q", "srem", "features", "dark_mode"})
	})
	if code != 0 {
		t.Fatalf("srem: exit %d out=%q", code, out)
	}

	ok, err := eng.SetContains(context.Background(), "tags", "features", []byte("dark_mode"))
	if err != nil || ok {
		t.Fatalf("after srem: ok=%v err=%v", ok, err)
	}
}

func captureOut(fn func() int) (string, int) {
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	code := fn()
	_ = w.Close()
	os.Stdout = old
	var buf bytes.Buffer
	_, _ = io.Copy(&buf, r)
	_ = r.Close()
	return buf.String(), code
}
