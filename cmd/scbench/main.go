// scbench — reliable SuperCache vs Redis Get/Set benchmarks.
//
// Multi-trial medians, suite mode, compare both backends, zipf keys, JSON report.
//
//	redis-server --port 6379 --save "" --appendonly no
//	go run ./cmd/supercache-node -cache 127.0.0.1:9000 -peer 127.0.0.1:9001 -admin 127.0.0.1:8080
//
//	# Full reliable compare (recommended)
//	go run ./cmd/scbench -compare -suite -trials=5 -duration=15s
//
//	# Single backend
//	go run ./cmd/scbench -backend=supercache -op=get -trials=5
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"
)

func main() {
	var (
		backend     = flag.String("backend", "supercache", "redis | supercache (ignored if -compare)")
		addr        = flag.String("addr", "", "server address (default depends on backend)")
		redisAddr   = flag.String("redis-addr", "127.0.0.1:6379", "Redis address for -compare")
		scAddr      = flag.String("sc-addr", "127.0.0.1:9000", "SuperCache cache gRPC address for -compare")
		compare     = flag.Bool("compare", false, "run both redis and supercache with identical params")
		suite       = flag.Bool("suite", false, "run get + set + mixed (recommended)")
		op          = flag.String("op", "get", "get | set | mixed (if not -suite)")
		keys        = flag.Int("keys", 50_000, "key space size")
		valueBytes  = flag.Int("value-bytes", 256, "value size bytes")
		concurrency = flag.Int("concurrency", 64, "parallel workers")
		duration    = flag.Duration("duration", 15*time.Second, "per-trial measure window")
		warmup      = flag.Duration("warmup", 5*time.Second, "warmup before each trial (discarded)")
		trials      = flag.Int("trials", 5, "independent measure trials; report median")
		readRatio   = flag.Float64("read-ratio", 0.95, "GET fraction in mixed mode")
		keyspace    = flag.String("keyspace", "demo", "SuperCache keyspace")
		prefix      = flag.String("prefix", "scbench:", "key prefix")
		prefill     = flag.Bool("prefill", true, "prefill before get/mixed")
		dist        = flag.String("dist", "uniform", "uniform | zipf")
		zipfS       = flag.Float64("zipf-s", 1.1, "zipf exponent (higher = hotter head)")
		seed        = flag.Uint64("seed", 42, "RNG seed for reproducibility")
		jsonOut     = flag.String("json", "", "write full report JSON to path")
		reliable    = flag.Bool("reliable", false, "preset: suite+compare, trials=5, duration=20s, warmup=5s, keys=50k")
	)
	flag.Parse()

	if *reliable {
		*compare = true
		*suite = true
		if *trials < 5 {
			*trials = 5
		}
		if *duration < 15*time.Second {
			*duration = 20 * time.Second
		}
		if *warmup < 3*time.Second {
			*warmup = 5 * time.Second
		}
		if *keys < 10_000 {
			*keys = 50_000
		}
	}

	*op = strings.ToLower(*op)
	*backend = strings.ToLower(*backend)
	*dist = strings.ToLower(*dist)

	ops := []string{*op}
	if *suite {
		ops = []string{"get", "set", "mixed"}
	}
	for _, o := range ops {
		if o != "get" && o != "set" && o != "mixed" {
			fatalf("invalid op %q", o)
		}
	}
	if *trials < 1 {
		fatalf("trials must be >= 1")
	}
	if *concurrency < 1 || *keys < 1 || *valueBytes < 1 {
		fatalf("concurrency, keys, value-bytes must be >= 1")
	}

	var dkind distKind
	switch *dist {
	case "uniform":
		dkind = distUniform
	case "zipf":
		dkind = distZipf
	default:
		fatalf("invalid -dist %q", *dist)
	}

	value := make([]byte, *valueBytes)
	for i := range value {
		value[i] = byte('a' + i%26)
	}

	backends := []struct {
		name string
		addr string
	}{}
	if *compare {
		backends = []struct{ name, addr string }{
			{"redis", *redisAddr},
			{"supercache", *scAddr},
		}
	} else {
		addr := *addr
		if addr == "" {
			if *backend == "redis" {
				addr = *redisAddr
			} else {
				addr = *scAddr
			}
		}
		backends = []struct{ name, addr string }{{*backend, addr}}
	}

	report := envNote()
	fmt.Println("scbench reliable harness")
	fmt.Printf("  GOMAXPROCS=%d NumCPU=%d Go=%s\n", report.GOMAXPROCS, report.NumCPU, report.GoVersion)
	fmt.Printf("  keys=%d value=%dB concurrency=%d dist=%s zipf-s=%.2f\n", *keys, *valueBytes, *concurrency, *dist, *zipfS)
	fmt.Printf("  warmup=%s duration=%s trials=%d suite=%v compare=%v\n", *warmup, *duration, *trials, *suite, *compare)
	fmt.Printf("  note: %s\n\n", report.Note)

	ctx := context.Background()
	var runs []runRecord

	for _, b := range backends {
		store, err := openBackend(ctx, b.name, b.addr, *keyspace, *concurrency)
		if err != nil {
			fatalf("connect %s %s: %v\n  hint: start redis-server and/or supercache-node first", b.name, b.addr, err)
		}

		needPrefill := *prefill
		// Prefill once per backend if any op needs hits
		for _, o := range ops {
			if o == "get" || o == "mixed" {
				if needPrefill {
					fmt.Printf("[%s] prefill %d keys…\n", b.name, *keys)
					t0 := time.Now()
					if err := prefillKeys(ctx, store, *prefix, *keys, value); err != nil {
						_ = store.Close()
						fatalf("prefill: %v", err)
					}
					fmt.Printf("[%s] prefill done in %s\n", b.name, time.Since(t0).Round(time.Millisecond))
					needPrefill = false
				}
				break
			}
		}

		for _, o := range ops {
			fmt.Printf("[%s] op=%s  starting %d trials…\n", b.name, o, *trials)
			cfg := loadConfig{
				op: o, prefix: *prefix, keys: *keys, value: value,
				concurrency: *concurrency, duration: *duration, readRatio: *readRatio,
				dist: dkind, zipfS: *zipfS, seed: *seed,
			}
			var trialsOut []trialResult
			for t := 1; t <= *trials; t++ {
				if *warmup > 0 {
					wcfg := cfg
					wcfg.duration = *warmup
					_, _ = runLoad(ctx, store, wcfg)
				}
				cfg.duration = *duration
				// vary seed per trial slightly for independence while staying reproducible
				cfg.seed = *seed + uint64(t)*0x100000001b3
				res, err := runLoad(ctx, store, cfg)
				if err != nil {
					_ = store.Close()
					fatalf("trial: %v", err)
				}
				printTrial(b.name, o, t, *trials, res)
				trialsOut = append(trialsOut, res)
			}
			medOps, minOps, maxOps, medP50, medP95, medP99 := aggregateTrials(trialsOut)
			rec := runRecord{
				Backend:         b.name,
				Addr:            b.addr,
				Op:              o,
				Keys:            *keys,
				ValueBytes:      *valueBytes,
				Concurrency:     *concurrency,
				Dist:            *dist,
				Trials:          trialsOut,
				MedianOpsPerSec: medOps,
				MedianP50:       medP50,
				MedianP95:       medP95,
				MedianP99:       medP99,
				MinOpsPerSec:    minOps,
				MaxOpsPerSec:    maxOps,
			}
			printRunSummary(rec)
			runs = append(runs, rec)
		}
		_ = store.Close()
	}

	if *compare || len(runs) > 1 {
		printCompareTable(runs)
	}

	report.Runs = runs
	if *jsonOut != "" {
		if err := writeJSON(*jsonOut, report); err != nil {
			fatalf("json: %v", err)
		}
		fmt.Printf("wrote JSON report %s\n", *jsonOut)
	}
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
