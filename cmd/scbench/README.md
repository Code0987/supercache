# scbench — SuperCache vs Redis (reliable)

Localhost Get/Set/Mixed comparison with **multiple trials** and **median** reporting so you can trust the headline numbers.

## Methodology (why this is more reliable)

| Practice | Default / flag |
|----------|----------------|
| Warmup discarded | `-warmup=5s` |
| Steady measure window | `-duration=15s` (use 20s+ for publishable) |
| Independent trials | `-trials=5` → **median** ops/s & latencies |
| Per-worker latency buffers | no mutex on hot path |
| Reproducible RNG | `-seed=42` |
| Key distributions | `-dist=uniform` or `-dist=zipf` |
| Full matrix | `-suite` → get + set + mixed |
| Both backends | `-compare` |
| Machine snapshot | GOMAXPROCS, NumCPU, Go version in JSON |
| JSON export | `-json=report.json` |

**Headline number = median ops/s across trials**, not a single 3-second blip.

### Fairness

| | SuperCache | Redis |
|--|------------|--------|
| Setup | **one** `supercache-node` | **one** Redis, preferably **native** (not Docker) |
| Persistence | none (CacheOnly) | disable RDB/AOF |
| Protocol | gRPC | RESP |
| Consistency | single-node local | single-instance |

Do **not** compare 3-node SuperCache to 1 Redis and call it “faster.”

## Prerequisites

```bash
# Prefer native Redis for fairness
redis-server --port 6379 --save "" --appendonly no

# SuperCache (demo keyspace = CacheOnly "demo")
go run ./cmd/supercache-node \
  -cache 127.0.0.1:9000 -peer 127.0.0.1:9001 -admin 127.0.0.1:8080
```

## Recommended: full compare

```bash
go run ./cmd/scbench -reliable -json=bench-report.json
```

Equivalent to: `-compare -suite -trials=5 -duration=20s -warmup=5s -keys=50000`.

Shorter but still multi-trial:

```bash
go run ./cmd/scbench -compare -suite -trials=3 -duration=10s -warmup=3s -json=out.json
```

## Single backend / single op

```bash
go run ./cmd/scbench -backend=redis      -op=get -trials=5 -duration=15s
go run ./cmd/scbench -backend=supercache -op=get -trials=5 -duration=15s

go run ./cmd/scbench -backend=redis -op=mixed -dist=zipf -zipf-s=1.2

# CacheOnly miss (no prefill) / delete
go run ./cmd/scbench -backend=supercache -op=miss -prefill=false -trials=1 -duration=5s
go run ./cmd/scbench -backend=supercache -op=delete -trials=1 -duration=5s
```

LoadThrough miss (`-miss-mode=loadthrough`) needs `-embed`.

```bash
# In-process cluster (no external node)
go run ./cmd/scbench -embed -nodes=3 -op=get -require-hit -concurrency=10 -duration=3s -warmup=1s

# Presets
go run ./cmd/scbench -tier=smoke -json=smoke.json
go run ./cmd/scbench -tier=laptop -json=laptop.json
bash scripts/bench-local.sh laptop

# Compare two smoke reports (CI uses this for PR comments)
go run ./cmd/scbench-diff -old prev/smoke.json -new smoke.json \
  -old-micro prev/micro.txt -new-micro micro.txt
```

## Flags

| Flag | Default | Meaning |
|------|---------|---------|
| `-compare` | false | Run redis **and** supercache |
| `-suite` | false | get + set + mixed |
| `-reliable` | false | Opinionated full compare preset |
| `-trials` | 5 | Measure repeats; report median |
| `-duration` | 15s | Per-trial window |
| `-warmup` | 5s | Discarded before each trial |
| `-keys` | 50000 | Key space |
| `-value-bytes` | 256 | Payload size |
| `-concurrency` | 64 | Workers |
| `-conns` | 1 | SuperCache gRPC clients (striped by worker) |
| `-sample-cap` | 262144 | Max latency samples per trial (exact sum across workers) |
| `-require-hit` | false | Get not-found is an error (off for `-compare` / mixed) |
| `-op` | get | `get` \| `set` \| `mixed` \| `delete` \| `miss` |
| `-miss-mode` | cacheonly | `cacheonly` only until `-embed` |
| `-dist` | uniform | `uniform` \| `zipf` |
| `-zipf-s` | 1.1 | Zipf exponent |
| `-read-ratio` | 0.95 | Mixed GET fraction |
| `-redis-addr` | 127.0.0.1:6379 | |
| `-sc-addr` | 127.0.0.1:9000 | SuperCache **cache** port |
| `-keyspace` | demo | SuperCache keyspace |
| `-json` | | Write full report |
| `-collect-runtime` | false | Process CPU/GC/allocs per trial (`proc_*` in JSON; not `testing.B`) |

## Interpreting output

```text
SUMMARY backend=redis      op=get   trials=5  median_ops/s=...
         latency median-of-trials  p50=... p95=... p99=... p999=...

COMPARISON
backend    op      ops/s   p50   p95   p99   p999
...
op=get  supercache/redis throughput ratio = 0.9x
```

- **ratio > 1** → SuperCache higher median ops/s on that op  
- Look at **min/max ops/s** across trials; wide spreads mean you need longer duration or quieter machine  
- p99 from median-of-trials is stabler than one-shot p99  

## What this does *not* prove

- Production multi-AZ performance  
- Redis Cluster vs SuperCache cluster  
- Pipelined Redis vs unary gRPC (Redis can go much faster with pipelines)  
- LoadThrough / SoT miss paths (`-miss-mode=loadthrough` needs embed)

See also [docs/BENCHMARKS.md](../../docs/BENCHMARKS.md).
