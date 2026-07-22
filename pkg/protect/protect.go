package protect

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrRateLimited is returned when the rate limiter rejects a load.
	ErrRateLimited = errors.New("protect: rate limited")
	// ErrCircuitOpen is returned when the circuit breaker is open.
	ErrCircuitOpen = errors.New("protect: circuit open")
)

// Config controls rate limiting and circuit breaking for DataSource loads.
type Config struct {
	// RateLimitRPS is sustained permits per second. 0 disables limiting.
	RateLimitRPS float64
	// Burst is max burst tokens; defaults to max(1, int(RPS)) when 0.
	Burst int
	// FailureThreshold opens the breaker after this many consecutive failures.
	// 0 disables the breaker.
	FailureThreshold int
	// OpenTimeout is how long the breaker stays open before half-open.
	OpenTimeout time.Duration
}

// Guard applies rate limit + circuit breaker.
type Guard struct {
	cfg Config

	mu         sync.Mutex
	tokens     float64
	lastRefill time.Time

	consecutiveFails int
	state            int32 // 0=closed, 1=open, 2=half-open
	openedAt         time.Time
	probeInFlight    bool // half-open: only one trial at a time

	limited atomic.Uint64
	opens   atomic.Uint64
}

const (
	stateClosed = iota
	stateOpen
	stateHalfOpen
)

// New creates a Guard. Zero config is a no-op allow-all guard.
func New(cfg Config) *Guard {
	if cfg.OpenTimeout <= 0 {
		cfg.OpenTimeout = 30 * time.Second
	}
	g := &Guard{cfg: cfg, lastRefill: time.Now()}
	if cfg.RateLimitRPS > 0 {
		burst := cfg.Burst
		if burst <= 0 {
			burst = int(cfg.RateLimitRPS)
			if burst < 1 {
				burst = 1
			}
		}
		g.tokens = float64(burst)
	}
	return g
}

// Allow reports whether a load may proceed.
func (g *Guard) Allow() error {
	if g == nil {
		return nil
	}
	if err := g.checkBreaker(); err != nil {
		return err
	}
	if err := g.takeToken(); err != nil {
		g.limited.Add(1)
		return err
	}
	return nil
}

// AllowContext is Allow with context cancellation (no wait — fail fast).
func (g *Guard) AllowContext(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return g.Allow()
}

// OnSuccess records a successful load.
func (g *Guard) OnSuccess() {
	if g == nil || g.cfg.FailureThreshold <= 0 {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.consecutiveFails = 0
	g.probeInFlight = false
	atomic.StoreInt32(&g.state, stateClosed)
}

// OnFailure records a failed load (not ErrNotFound — caller decides).
func (g *Guard) OnFailure() {
	if g == nil || g.cfg.FailureThreshold <= 0 {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	wasHalfOpen := atomic.LoadInt32(&g.state) == stateHalfOpen
	g.consecutiveFails++
	g.probeInFlight = false
	// Half-open probe failure always re-opens; otherwise open at threshold.
	if wasHalfOpen || g.consecutiveFails >= g.cfg.FailureThreshold {
		atomic.StoreInt32(&g.state, stateOpen)
		g.openedAt = time.Now()
		g.opens.Add(1)
	}
}

func (g *Guard) checkBreaker() error {
	if g.cfg.FailureThreshold <= 0 {
		return nil
	}
	st := atomic.LoadInt32(&g.state)
	if st == stateClosed {
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	st = atomic.LoadInt32(&g.state)
	switch st {
	case stateClosed:
		return nil
	case stateOpen:
		if time.Since(g.openedAt) < g.cfg.OpenTimeout {
			return ErrCircuitOpen
		}
		// Transition to half-open and grant this caller the single probe.
		atomic.StoreInt32(&g.state, stateHalfOpen)
		g.probeInFlight = true
		return nil
	case stateHalfOpen:
		// Only one probe at a time.
		if g.probeInFlight {
			return ErrCircuitOpen
		}
		g.probeInFlight = true
		return nil
	default:
		return ErrCircuitOpen
	}
}

func (g *Guard) takeToken() error {
	if g.cfg.RateLimitRPS <= 0 {
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(g.lastRefill).Seconds()
	g.lastRefill = now
	g.tokens += elapsed * g.cfg.RateLimitRPS
	burst := float64(g.cfg.Burst)
	if g.cfg.Burst <= 0 {
		burst = g.cfg.RateLimitRPS
		if burst < 1 {
			burst = 1
		}
	}
	if g.tokens > burst {
		g.tokens = burst
	}
	if g.tokens < 1 {
		return ErrRateLimited
	}
	g.tokens--
	return nil
}

// Stats returns limiter/breaker counters.
func (g *Guard) Stats() (limited, opens uint64) {
	if g == nil {
		return 0, 0
	}
	return g.limited.Load(), g.opens.Load()
}

// State returns a debug label: closed, open, half-open.
func (g *Guard) State() string {
	if g == nil {
		return "closed"
	}
	switch atomic.LoadInt32(&g.state) {
	case stateOpen:
		return "open"
	case stateHalfOpen:
		return "half-open"
	default:
		return "closed"
	}
}
