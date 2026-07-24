package protect

import (
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
