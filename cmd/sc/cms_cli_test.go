package main

import (
	"strings"
	"testing"
	"time"

	"github.com/Code0987/supercache/internal/cacheserver"
	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
)

func TestCMSCLIUsage(t *testing.T) {
	if code := run([]string{"cmsincr"}); code != 2 {
		t.Fatalf("cmsincr: %d", code)
	}
	if code := run([]string{"cmsincr", "hot"}); code != 2 {
		t.Fatalf("cmsincr name only: %d", code)
	}
	if code := run([]string{"cmsincr", "hot", "t001", "nope"}); code != 2 {
		t.Fatalf("cmsincr bad n: %d", code)
	}
	if code := run([]string{"cmsquery"}); code != 2 {
		t.Fatalf("cmsquery: %d", code)
	}
	if code := run([]string{"cmsquery", "hot"}); code != 2 {
		t.Fatalf("cmsquery name only: %d", code)
	}
}

func TestCMSCLIRoundTrip(t *testing.T) {
	eng := engine.New()
	defer eng.Close()
	if err := eng.UpdateKeySpace(keyspace.Config{
		Name: "freq", Mode: keyspace.ModeCMS, MaxBytes: 1 << 20, TTL: time.Hour,
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
		return run([]string{"-addr", addr, "-keyspace", "freq", "-q", "cmsincr", "hot", "t001"})
	})
	if code != 0 {
		t.Fatalf("cmsincr: exit %d out=%q", code, out)
	}

	out, code = captureOut(func() int {
		return run([]string{"-addr", addr, "-keyspace", "freq", "-q", "cmsincr", "hot", "t001", "5"})
	})
	if code != 0 {
		t.Fatalf("cmsincr n: exit %d out=%q", code, out)
	}

	out, code = captureOut(func() int {
		return run([]string{"-addr", addr, "-keyspace", "freq", "cmsquery", "hot", "t001"})
	})
	if code != 0 {
		t.Fatalf("cmsquery: exit %d out=%q", code, out)
	}
	if strings.TrimSpace(out) == "(nil)" {
		t.Fatalf("cmsquery live: %q", out)
	}

	out, code = captureOut(func() int {
		return run([]string{"-addr", addr, "-keyspace", "freq", "cmsquery", "missing", "t001"})
	})
	if code != 1 || strings.TrimSpace(out) != "(nil)" {
		t.Fatalf("cmsquery miss: exit %d out=%q", code, out)
	}
}
