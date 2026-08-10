package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Code0987/supercache/internal/benchmetrics"
	"github.com/Code0987/supercache/pkg/client"
)

type distKind int

const (
	distUniform distKind = iota
	distZipf
)

type loadConfig struct {
	op          string
	prefix      string
	keys        int
	value       []byte
	concurrency int
	duration    time.Duration
	readRatio   float64
	dist           distKind
	zipfS          float64
	seed           uint64
	collectRuntime bool
}

type trialResult struct {
	Ops       int64         `json:"ops"`
	Errors    int64         `json:"errors"`
	Duration  time.Duration `json:"duration_ns"`
	OpsPerSec float64       `json:"ops_per_sec"`
	P50       time.Duration `json:"p50_ns"`
	P95       time.Duration `json:"p95_ns"`
	P99       time.Duration `json:"p99_ns"`
	P999      time.Duration `json:"p999_ns"`
	Mean      time.Duration `json:"mean_ns"`
	Samples   int           `json:"samples"`
	// Proc is process-level CPU/GC/alloc deltas (not testing.B allocs/op).
	Proc *benchmetrics.Report `json:"proc,omitempty"`
}

func prefillKeys(ctx context.Context, store kvStore, prefix string, n int, value []byte) error {
	const workers = 64
	jobs := make(chan int, workers*4)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for i := range jobs {
				if err := store.Set(ctx, fmt.Sprintf("%s%d", prefix, i), value); err != nil {
					select {
					case errCh <- err:
					default:
					}
					return
				}
			}
		}()
	}
	for i := 0; i < n; i++ {
		select {
		case err := <-errCh:
			close(jobs)
			wg.Wait()
			return err
		case jobs <- i:
		}
	}
	close(jobs)
	wg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

func runLoad(ctx context.Context, store kvStore, cfg loadConfig) (trialResult, error) {
	type workerBuf struct {
		lats []time.Duration
		ops  int64
		errs int64
	}
	bufs := make([]workerBuf, cfg.concurrency)
	const maxPerWorker = 50_000

	var zipf *zipfGen
	if cfg.dist == distZipf {
		zipf = newZipf(cfg.keys, cfg.zipfS)
	}

	var before benchmetrics.Snapshot
	if cfg.collectRuntime {
		before = benchmetrics.Read()
	}
	tWall := time.Now()
	stopAt := tWall.Add(cfg.duration)
	var wg sync.WaitGroup
	wg.Add(cfg.concurrency)
	for w := 0; w < cfg.concurrency; w++ {
		go func(worker int) {
			defer wg.Done()
			wb := &bufs[worker]
			wb.lats = make([]time.Duration, 0, 4096)
			rng := newRNG(cfg.seed ^ uint64(worker+1)*0x9e3779b97f4a7c15)

			for time.Now().Before(stopAt) {
				if ctx.Err() != nil {
					return
				}
				var idx int
				if zipf != nil {
					idx = zipf.Next(rng)
				} else {
					idx = int(rng.Uint64() % uint64(cfg.keys))
				}
				key := fmt.Sprintf("%s%d", cfg.prefix, idx)
				doGet := cfg.op == "get" ||
					(cfg.op == "mixed" && float64(rng.Uint64()%10000)/10000.0 < cfg.readRatio)

				t0 := time.Now()
				var err error
				if doGet {
					_, err = store.Get(ctx, key)
					if err != nil && (errors.Is(err, redis.Nil) || errors.Is(err, client.ErrNotFound)) {
						err = nil
					}
				} else {
					err = store.Set(ctx, key, cfg.value)
				}
				d := time.Since(t0)
				if err != nil {
					wb.errs++
					continue
				}
				wb.ops++
				if len(wb.lats) < maxPerWorker {
					wb.lats = append(wb.lats, d)
				}
			}
		}(w)
	}
	wg.Wait()

	var ops, errs int64
	var all []time.Duration
	for i := range bufs {
		ops += bufs[i].ops
		errs += bufs[i].errs
		all = append(all, bufs[i].lats...)
	}
	secs := cfg.duration.Seconds()
	if secs <= 0 {
		secs = 1
	}
	p50, p95, p99, p999, mean := latencyStats(all)
	res := trialResult{
		Ops:       ops,
		Errors:    errs,
		Duration:  cfg.duration,
		OpsPerSec: float64(ops) / secs,
		P50:       p50,
		P95:       p95,
		P99:       p99,
		P999:      p999,
		Mean:      mean,
		Samples:   len(all),
	}
	if cfg.collectRuntime {
		after := benchmetrics.Read()
		rep := benchmetrics.Delta(before, after, time.Since(tWall), ops)
		res.Proc = &rep
	}
	return res, nil
}

type rng struct{ s uint64 }

func newRNG(seed uint64) *rng {
	if seed == 0 {
		seed = 1
	}
	return &rng{s: seed}
}

func (r *rng) Uint64() uint64 {
	x := r.s
	x ^= x >> 12
	x ^= x << 25
	x ^= x >> 27
	r.s = x
	return x * 0x2545F4914F6CDD1D
}

func (r *rng) Float64() float64 {
	return float64(r.Uint64()>>11) / (1 << 53)
}

// zipfGen: P(k) ∝ 1/(k+1)^s over k in [0,n)
type zipfGen struct {
	n   int
	har []float64
	tot float64
}

func newZipf(n int, s float64) *zipfGen {
	if n < 1 {
		n = 1
	}
	if s < 0.01 {
		s = 0.01
	}
	z := &zipfGen{n: n, har: make([]float64, n)}
	var sum float64
	for i := 0; i < n; i++ {
		sum += 1.0 / math.Pow(float64(i+1), s)
		z.har[i] = sum
	}
	z.tot = sum
	return z
}

func (z *zipfGen) Next(r *rng) int {
	u := r.Float64() * z.tot
	lo, hi := 0, z.n-1
	for lo < hi {
		mid := (lo + hi) / 2
		if z.har[mid] < u {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}
