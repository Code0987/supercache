// scbench compares basic Get/Set throughput and latency: SuperCache vs Redis.
//
//	# Terminal A
//	redis-server --port 6379 --save "" --appendonly no
//	# Terminal B
//	go run ./cmd/supercache-node -cache 127.0.0.1:9000 -peer 127.0.0.1:9001 -admin 127.0.0.1:8080
//
//	go run ./cmd/scbench -backend=redis      -addr=127.0.0.1:6379 -op=get
//	go run ./cmd/scbench -backend=supercache -addr=127.0.0.1:9000 -op=get
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Code0987/supercache/pkg/client"
)

func main() {
	var (
		backend     = flag.String("backend", "supercache", "redis | supercache")
		addr        = flag.String("addr", "", "server address (default: redis 127.0.0.1:6379, supercache 127.0.0.1:9000)")
		op          = flag.String("op", "get", "get | set | mixed")
		keys        = flag.Int("keys", 10_000, "key space size")
		valueBytes  = flag.Int("value-bytes", 256, "value size in bytes")
		concurrency = flag.Int("concurrency", 64, "parallel workers")
		duration    = flag.Duration("duration", 20*time.Second, "steady-state measurement window")
		warmup      = flag.Duration("warmup", 3*time.Second, "warmup duration (discarded)")
		readRatio   = flag.Float64("read-ratio", 0.95, "fraction of ops that are GETs in mixed mode")
		keyspace    = flag.String("keyspace", "demo", "SuperCache keyspace (must exist; demo-keyspace on supercache-node)")
		prefix      = flag.String("prefix", "scbench:", "key prefix")
		prefill     = flag.Bool("prefill", true, "prefill keys before get/mixed")
	)
	flag.Parse()

	if *addr == "" {
		switch strings.ToLower(*backend) {
		case "redis":
			*addr = "127.0.0.1:6379"
		default:
			*addr = "127.0.0.1:9000"
		}
	}
	*op = strings.ToLower(*op)
	*backend = strings.ToLower(*backend)
	if *op != "get" && *op != "set" && *op != "mixed" {
		fatalf("invalid -op %q (want get|set|mixed)", *op)
	}
	if *concurrency < 1 || *keys < 1 || *valueBytes < 1 {
		fatalf("concurrency, keys, value-bytes must be >= 1")
	}
	if *readRatio < 0 || *readRatio > 1 {
		fatalf("read-ratio must be in [0,1]")
	}

	value := make([]byte, *valueBytes)
	for i := range value {
		value[i] = byte('a' + i%26)
	}

	ctx := context.Background()
	store, err := openBackend(ctx, *backend, *addr, *keyspace)
	if err != nil {
		fatalf("connect %s %s: %v", *backend, *addr, err)
	}
	defer store.Close()

	fmt.Printf("scbench backend=%s addr=%s op=%s keys=%d value=%dB concurrency=%d warmup=%s duration=%s prefill=%v\n",
		*backend, *addr, *op, *keys, *valueBytes, *concurrency, *warmup, *duration, *prefill)

	if *prefill && (*op == "get" || *op == "mixed") {
		fmt.Printf("prefill %d keys…\n", *keys)
		t0 := time.Now()
		if err := prefillKeys(ctx, store, *prefix, *keys, value); err != nil {
			fatalf("prefill: %v", err)
		}
		fmt.Printf("prefill done in %s\n", time.Since(t0).Round(time.Millisecond))
	}

	// Warmup (discard)
	if *warmup > 0 {
		fmt.Printf("warmup %s…\n", *warmup)
		_, _ = runLoad(ctx, store, loadConfig{
			op: *op, prefix: *prefix, keys: *keys, value: value,
			concurrency: *concurrency, duration: *warmup, readRatio: *readRatio,
		})
	}

	fmt.Printf("measure %s…\n", *duration)
	res, err := runLoad(ctx, store, loadConfig{
		op: *op, prefix: *prefix, keys: *keys, value: value,
		concurrency: *concurrency, duration: *duration, readRatio: *readRatio,
	})
	if err != nil {
		fatalf("bench: %v", err)
	}
	printResult(*backend, *op, res)
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

// --- backend abstraction ---

type kvStore interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, val []byte) error
	Close() error
}

func openBackend(ctx context.Context, backend, addr, keyspace string) (kvStore, error) {
	switch backend {
	case "redis":
		rdb := redis.NewClient(&redis.Options{Addr: addr})
		if err := rdb.Ping(ctx).Err(); err != nil {
			return nil, err
		}
		return &redisStore{c: rdb}, nil
	case "supercache":
		cli, err := client.Dial(ctx, addr)
		if err != nil {
			return nil, err
		}
		// Probe with a tiny put to surface "connection refused" early (gRPC dial is lazy).
		if err := cli.Put(ctx, keyspace, "__scbench_ping__", []byte("1"), client.WithTTL(time.Minute)); err != nil {
			_ = cli.Close()
			return nil, fmt.Errorf("ping put (is supercache-node up? keyspace %q registered?): %w", keyspace, err)
		}
		return &scStore{c: cli, ks: keyspace}, nil
	default:
		return nil, fmt.Errorf("unknown backend %q", backend)
	}
}

type redisStore struct{ c *redis.Client }

func (r *redisStore) Get(ctx context.Context, key string) ([]byte, error) {
	return r.c.Get(ctx, key).Bytes()
}
func (r *redisStore) Set(ctx context.Context, key string, val []byte) error {
	return r.c.Set(ctx, key, val, 0).Err()
}
func (r *redisStore) Close() error { return r.c.Close() }

type scStore struct {
	c  *client.Client
	ks string
}

func (s *scStore) Get(ctx context.Context, key string) ([]byte, error) {
	return s.c.Get(ctx, s.ks, key)
}
func (s *scStore) Set(ctx context.Context, key string, val []byte) error {
	return s.c.Put(ctx, s.ks, key, val)
}
func (s *scStore) Close() error { return s.c.Close() }

func prefillKeys(ctx context.Context, store kvStore, prefix string, n int, value []byte) error {
	const workers = 32
	jobs := make(chan int, workers*2)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for i := range jobs {
				key := fmt.Sprintf("%s%d", prefix, i)
				if err := store.Set(ctx, key, value); err != nil {
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

// --- load generator ---

type loadConfig struct {
	op          string
	prefix      string
	keys        int
	value       []byte
	concurrency int
	duration    time.Duration
	readRatio   float64
}

type result struct {
	Ops       int64
	Errors    int64
	Duration  time.Duration
	Latencies []time.Duration // sampled
}

func runLoad(ctx context.Context, store kvStore, cfg loadConfig) (result, error) {
	ctx, cancel := context.WithTimeout(ctx, cfg.duration+time.Second)
	defer cancel()

	var (
		ops    atomic.Int64
		errs   atomic.Int64
		seq    atomic.Uint64
		latMu  sync.Mutex
		lats   []time.Duration
		stopAt = time.Now().Add(cfg.duration)
	)
	// Cap samples to keep memory bounded (~2M max ints is fine; we cap at 200k).
	const maxSamples = 200_000

	var wg sync.WaitGroup
	wg.Add(cfg.concurrency)
	for w := 0; w < cfg.concurrency; w++ {
		go func(worker int) {
			defer wg.Done()
			// per-worker RNG (xorshift)
			r := uint64(time.Now().UnixNano()) ^ uint64(worker+1)*0x9e3779b97f4a7c15
			next := func() uint64 {
				r ^= r << 13
				r ^= r >> 7
				r ^= r << 17
				return r
			}
			for time.Now().Before(stopAt) {
				if ctx.Err() != nil {
					return
				}
				i := int(next() % uint64(cfg.keys))
				key := fmt.Sprintf("%s%d", cfg.prefix, i)
				doGet := cfg.op == "get" || (cfg.op == "mixed" && float64(next()%10000)/10000.0 < cfg.readRatio)

				t0 := time.Now()
				var err error
				if doGet {
					_, err = store.Get(ctx, key)
					// Missing key: count as completed op for mixed/set races, not a hard error.
					if err != nil && (errors.Is(err, redis.Nil) || errors.Is(err, client.ErrNotFound)) {
						err = nil
					}
				} else {
					// unique-ish overwrite for set path
					_ = seq.Add(1)
					err = store.Set(ctx, key, cfg.value)
				}
				elapsed := time.Since(t0)
				if err != nil {
					errs.Add(1)
					continue
				}
				ops.Add(1)
				latMu.Lock()
				if len(lats) < maxSamples {
					lats = append(lats, elapsed)
				}
				latMu.Unlock()
			}
		}(w)
	}
	wg.Wait()

	return result{
		Ops:       ops.Load(),
		Errors:    errs.Load(),
		Duration:  cfg.duration,
		Latencies: lats,
	}, nil
}

func printResult(backend, op string, r result) {
	secs := r.Duration.Seconds()
	if secs <= 0 {
		secs = 1
	}
	opsPerSec := float64(r.Ops) / secs
	p50, p95, p99 := percentiles(r.Latencies)
	fmt.Println()
	fmt.Printf("RESULT backend=%-10s op=%-5s  ops=%d  errors=%d  ops/s=%.0f\n",
		backend, op, r.Ops, r.Errors, opsPerSec)
	fmt.Printf("       latency  p50=%s  p95=%s  p99=%s  (n_samples=%d)\n",
		p50, p95, p99, len(r.Latencies))
	fmt.Println()
}

func percentiles(ds []time.Duration) (p50, p95, p99 time.Duration) {
	if len(ds) == 0 {
		return 0, 0, 0
	}
	cp := append([]time.Duration(nil), ds...)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	pct := func(p float64) time.Duration {
		if len(cp) == 1 {
			return cp[0]
		}
		idx := int(math.Ceil(p*float64(len(cp)))) - 1
		if idx < 0 {
			idx = 0
		}
		if idx >= len(cp) {
			idx = len(cp) - 1
		}
		return cp[idx]
	}
	return pct(0.50), pct(0.95), pct(0.99)
}
