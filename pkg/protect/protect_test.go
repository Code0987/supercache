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
