# SuperCache benchmarks

## Tool: `cmd/scbench`

Compares **single-node SuperCache** to **single-instance Redis** on Get / Set / Mixed.

### Trustworthy run

1. Use **native** `redis-server` (avoid Docker for the Redis side when possible).  
2. Quiet machine; fix `GOMAXPROCS` if you care about reproducibility.  
3. Use multi-trial medians:

```bash
go run ./cmd/scbench -reliable -json=bench-report.json
```

4. Re-run on another day; if medians move &gt;10–15%, investigate load/noise.  
5. Publish the **JSON** (`-json=...`) with machine notes (CPU model, OS).

### What “reliable” means here

- Warmup discarded every trial  
- ≥3–5 independent measure windows  
- Median ops/s and median latency percentiles (**p50 / p95 / p99 / p99.9**)  
- Fixed key/value sizes and concurrency  
- Optional zipf for hot-key skew  
- Optional process CPU/GC (`-collect-runtime`; JSON `proc_*` fields — not `testing.B` allocs/op)

It does **not** mean formal statistical significance testing or multi-host nets.

Full matrix plan (local vs remote, 1/3/10 nodes, CI smoke): [BENCH_MATRIX.md](./BENCH_MATRIX.md).

## Local Engine / store microbenches

```bash
go test ./pkg/store ./pkg/engine -run=^$ -bench='Benchmark(Store|Engine)' -benchmem -benchtime=200ms -count=1
# quieter machine:
go test ./pkg/store ./pkg/engine -run=^$ -bench='Benchmark(Store|Engine)' -benchmem -benchtime=2s -count=5
```

| Name | What |
|------|------|
| `BenchmarkStoreGetHit` | LRU Get after fill (256 B) |
| `BenchmarkStorePut` | `AcceptIfNewer` with monotonic version |
| `BenchmarkEngineGetHit` | CacheOnly `Get` |
| `BenchmarkEngineGetMissCacheOnly` | empty Get → NotFound |
| `BenchmarkEngineGetMissLoadThrough` | `Get` on one key, `MaxBytes=1` (fill cannot stick) |
| `BenchmarkEnginePut` / `Delete` | overwrite Put; Delete + batch restore |

`cpu-ns/op`, `gc-p50-ns`, `gc-p99-ns`, `gc-frac` come from `runtime/metrics` (not `testing.B` allocs/op). **`gc-p50-ns=0` at `-benchtime=200ms` is expected** if no GC ran.

### How to read them

- **Get-hit allocs/op** is dominated by `CloneValue` (~1 alloc). A jump to 5+ is a real regression.
- **CacheOnly miss** should be cheaper than hit (no value copy).
- **LoadThrough miss** is DS + failed fill every op, not `ForceLoad` refresh-ahead.
- Cluster3 Engine benches are local-only (PR 8); not in CI.

In-process mesh: `internal/testcluster` (`Start` N=1, 3, or 10). CI: `bash scripts/bench-ci.sh` (micro + `bench/ci-smoke.yaml`). Laptop/full: `bash scripts/bench-local.sh laptop|full`.

### Fairness

| OK | Not OK |
|----|--------|
| 1 SuperCache node vs 1 Redis | 3 SuperCache nodes vs 1 Redis as “faster” |
| Memory-only both sides | Redis with AOF/RDB on |
| Same concurrency / value size | Redis pipelining vs unary SuperCache without labeling |

### Extending later

See [BENCH_MATRIX.md](./BENCH_MATRIX.md) for the staged plan:

- Engine/store `testing.B` microbenches (CPU-ns/op, B/op, allocs/op, GC)  
- Cluster read-QPS (1 / 3 / 10 in-process nodes)  
- CacheOnly / LoadThrough miss cells  
- Pipeline Redis profile (labeled unfair vs unary gRPC)
