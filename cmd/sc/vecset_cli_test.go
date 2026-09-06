package main

import (
	"strings"
	"testing"
	"time"

	"github.com/Code0987/supercache/internal/cacheserver"
	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
)

func TestVectorSetCLIUsage(t *testing.T) {
	if code := run([]string{"vadd"}); code != 2 {
		t.Fatalf("vadd: %d", code)
	}
	if code := run([]string{"vsim", "s"}); code != 2 {
		t.Fatalf("vsim: %d", code)
	}
	if code := run([]string{"vcard"}); code != 2 {
		t.Fatalf("vcard: %d", code)
	}
}

func TestVectorSetCLIRoundTrip(t *testing.T) {
	eng := engine.New()
	defer eng.Close()
	if err := eng.UpdateKeySpace(keyspace.Config{
		Name: "items", Mode: keyspace.ModeVectorSet, MaxBytes: 1 << 20, TTL: time.Hour,
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
		return run([]string{"-addr", addr, "-keyspace", "items", "-q", "vadd", "set", "a", "1,0"})
	})
	if code != 0 {
		t.Fatalf("vadd: exit %d out=%q", code, out)
	}
	out, code = captureOut(func() int {
		return run([]string{"-addr", addr, "-keyspace", "items", "vsim", "set", "1,0", "1"})
	})
	if code != 0 || !strings.Contains(out, "a") {
		t.Fatalf("vsim: exit %d out=%q", code, out)
	}
}
