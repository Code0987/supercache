package main

import (
	"strings"
	"testing"
	"time"

	"github.com/Code0987/supercache/internal/cacheserver"
	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
)

func TestHSetUsage(t *testing.T) {
	if code := run([]string{"hset"}); code != 2 {
		t.Fatalf("hset no args: exit %d want 2", code)
	}
	if code := run([]string{"hget", "onlyname"}); code != 2 {
		t.Fatalf("hget one arg: exit %d want 2", code)
	}
	if code := run([]string{"hdel"}); code != 2 {
		t.Fatalf("hdel: exit %d want 2", code)
	}
	if code := run([]string{"hexists"}); code != 2 {
		t.Fatalf("hexists: exit %d want 2", code)
	}
	if code := run([]string{"hlen"}); code != 2 {
		t.Fatalf("hlen: exit %d want 2", code)
	}
	if code := run([]string{"hgetall"}); code != 2 {
		t.Fatalf("hgetall: exit %d want 2", code)
	}
}

func TestHashCLIRoundTrip(t *testing.T) {
	eng := engine.New()
	defer eng.Close()
	if err := eng.UpdateKeySpace(keyspace.Config{
		Name: "hash", Mode: keyspace.ModeHash, MaxBytes: 1 << 20, TTL: time.Hour,
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
		return run([]string{"-addr", addr, "-keyspace", "hash", "-q", "hset", "user", "bio", "hello", "world"})
	})
	if code != 0 {
		t.Fatalf("hset: exit %d out=%q", code, out)
	}

	out, code = captureOut(func() int {
		return run([]string{"-addr", addr, "-keyspace", "hash", "hget", "user", "bio"})
	})
	if code != 0 || strings.TrimSpace(out) != "hello world" {
		t.Fatalf("hget: exit %d out=%q", code, out)
	}

	out, code = captureOut(func() int {
		return run([]string{"-addr", addr, "-keyspace", "hash", "hget", "user", "nope"})
	})
	if code != 1 || strings.TrimSpace(out) != "(nil)" {
		t.Fatalf("hget miss: exit %d out=%q", code, out)
	}

	out, code = captureOut(func() int {
		return run([]string{"-addr", addr, "-keyspace", "hash", "hexists", "user", "bio"})
	})
	if code != 0 || strings.TrimSpace(out) != "true" {
		t.Fatalf("hexists: exit %d out=%q", code, out)
	}

	out, code = captureOut(func() int {
		return run([]string{"-addr", addr, "-keyspace", "hash", "hlen", "user"})
	})
	if code != 0 || strings.TrimSpace(out) != "1" {
		t.Fatalf("hlen: exit %d out=%q", code, out)
	}

	out, code = captureOut(func() int {
		return run([]string{"-addr", addr, "-keyspace", "hash", "hgetall", "user"})
	})
	if code != 0 || strings.TrimSpace(out) != "bio\thello world" {
		t.Fatalf("hgetall: exit %d out=%q", code, out)
	}

	out, code = captureOut(func() int {
		return run([]string{"-addr", addr, "-keyspace", "hash", "-q", "hdel", "user", "bio"})
	})
	if code != 0 {
		t.Fatalf("hdel: exit %d out=%q", code, out)
	}

	out, code = captureOut(func() int {
		return run([]string{"-addr", addr, "-keyspace", "hash", "hexists", "user", "bio"})
	})
	if code != 1 || strings.TrimSpace(out) != "false" {
		t.Fatalf("hexists miss: exit %d out=%q", code, out)
	}
}
