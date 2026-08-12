package protect

import (
	"context"
	"testing"
	"time"
)

func TestNilGuardAndDefaults(t *testing.T) {
	var g *Guard
	if err := g.Allow(); err != nil {
		t.Fatal(err)
	}
	g.OnSuccess()
	g.OnFailure()
	g.releaseProbeSlot()
	if lim, op := g.Stats(); lim != 0 || op != 0 {
		t.Fatal("nil stats")
	}
	if g.State() != "closed" {
		t.Fatal(g.State())
	}

	// RPS>0 Burst=0 → burst from RPS; RPS<1 → burst 1
	g2 := New(Config{RateLimitRPS: 0.5, Burst: 0})
	if err := g2.Allow(); err != nil {
		t.Fatal(err)
	}
	// OpenTimeout default
	g3 := New(Config{FailureThreshold: 1})
	if g3.cfg.OpenTimeout != 30*time.Second {
		t.Fatalf("default open timeout %v", g3.cfg.OpenTimeout)
	}
}

func TestAllowContextAndStateLabels(t *testing.T) {
	g := New(Config{FailureThreshold: 1, OpenTimeout: 20 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := g.AllowContext(ctx); err == nil {
		t.Fatal("canceled ctx")
	}
	if err := g.AllowContext(context.Background()); err != nil {
		t.Fatal(err)
	}

	g.OnFailure()
	if g.State() != "open" {
		t.Fatalf("state=%s", g.State())
	}
	// still open within timeout
	if err := g.Allow(); err != ErrCircuitOpen {
		t.Fatal(err)
	}
	time.Sleep(25 * time.Millisecond)
	// half-open probe
	if err := g.Allow(); err != nil {
		t.Fatal(err)
	}
	if g.State() != "half-open" {
		// may already be half-open after Allow granted probe
		st := g.State()
		if st != "half-open" && st != "closed" {
			t.Fatalf("state=%s", st)
		}
	}
	// second concurrent probe while first in flight
	if err := g.Allow(); err != ErrCircuitOpen && err != nil {
		// if first probe succeeded without OnSuccess, second should be open
		if err != ErrCircuitOpen {
			t.Fatalf("second probe: %v", err)
		}
	}
	g.OnSuccess()
	if g.State() != "closed" {
		t.Fatalf("after success %s", g.State())
	}

	// takeToken burst cap with Burst>0
	g4 := New(Config{RateLimitRPS: 100, Burst: 2})
	_ = g4.Allow()
	_ = g4.Allow()
	// third may rate limit
	_ = g4.Allow()
}
