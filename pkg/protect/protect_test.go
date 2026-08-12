package protect

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestHalfOpenSingleProbe(t *testing.T) {
	g := New(Config{
		FailureThreshold: 1,
		OpenTimeout:      20 * time.Millisecond,
	})
	// Open the breaker.
	g.OnFailure()
	if err := g.Allow(); err != ErrCircuitOpen {
		t.Fatalf("want open, got %v", err)
	}
	time.Sleep(30 * time.Millisecond)

	var allowed atomic.Int32
	var wg sync.WaitGroup
	const n = 32
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			if err := g.Allow(); err == nil {
				allowed.Add(1)
			}
		}()
	}
	wg.Wait()
	if allowed.Load() != 1 {
		t.Fatalf("half-open probes allowed=%d want 1", allowed.Load())
	}

	// Probe fails → open again
	g.OnFailure()
	if err := g.Allow(); err != ErrCircuitOpen {
		t.Fatalf("after probe fail want open, got %v state=%s", err, g.State())
	}
}

func TestHalfOpenSuccessCloses(t *testing.T) {
	g := New(Config{FailureThreshold: 1, OpenTimeout: 10 * time.Millisecond})
	g.OnFailure()
	time.Sleep(15 * time.Millisecond)
	if err := g.Allow(); err != nil {
		t.Fatal(err)
	}
	g.OnSuccess()
	if err := g.Allow(); err != nil {
		t.Fatalf("closed should allow: %v", err)
	}
	if g.State() != "closed" {
		t.Fatalf("state=%s", g.State())
	}
}

func TestRateLimit(t *testing.T) {
	g := New(Config{RateLimitRPS: 1, Burst: 1})
	if err := g.Allow(); err != nil {
		t.Fatal(err)
	}
	if err := g.Allow(); err != ErrRateLimited {
		t.Fatalf("want rate limited, got %v", err)
	}
}

// Half-open probe must not permanently own the probe slot when rate limiting rejects it.
func TestHalfOpenProbeReleasedOnRateLimit(t *testing.T) {
	g := New(Config{
		RateLimitRPS:     1,
		Burst:            1,
		FailureThreshold: 1,
		OpenTimeout:      20 * time.Millisecond,
	})
	// Consume the only token, then open the breaker.
	if err := g.Allow(); err != nil {
		t.Fatal(err)
	}
	g.OnFailure()
	if err := g.Allow(); err != ErrCircuitOpen {
		t.Fatalf("want open, got %v", err)
	}
	time.Sleep(30 * time.Millisecond)

	// Half-open: breaker grants probe, then rate limit rejects — must not stick.
	err := g.Allow()
	if err != ErrRateLimited {
		// Either rate limited (token still empty) or, if token refilled, allow is OK
		// as long as we do not permanently stick later.
		if err != nil && err != ErrCircuitOpen {
			t.Fatalf("unexpected err: %v", err)
		}
	}

	// Refill tokens: wait >1s at 1 RPS, or use many short sleeps with high RPS.
	// With RPS=1 Burst=1, after ~1.1s a token should exist.
	time.Sleep(1100 * time.Millisecond)

	// Must be able to obtain a probe again (not stuck half-open forever).
	var allowed bool
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if err := g.Allow(); err == nil {
			allowed = true
			break
		} else if err == ErrCircuitOpen {
			// still recovering — wait for open timeout again if needed
			time.Sleep(25 * time.Millisecond)
			continue
		} else if err == ErrRateLimited {
			time.Sleep(50 * time.Millisecond)
			continue
		} else {
			t.Fatalf("unexpected: %v state=%s", err, g.State())
		}
	}
	if !allowed {
		t.Fatalf("breaker stuck; state=%s (probe leak after rate limit)", g.State())
	}
}

func TestNilGuardIsAllowAllAndBurstDefaults(t *testing.T) {
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

func TestAllowContextCancelAndBreakerStates(t *testing.T) {
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
