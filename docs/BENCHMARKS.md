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
- Median ops/s and median latency percentiles  
- Fixed key/value sizes and concurrency  
- Optional zipf for hot-key skew  

It does **not** mean formal statistical significance testing or multi-host nets.

### Fairness

| OK | Not OK |
|----|--------|
| 1 SuperCache node vs 1 Redis | 3 SuperCache nodes vs 1 Redis as “faster” |
| Memory-only both sides | Redis with AOF/RDB on |
| Same concurrency / value size | Redis pipelining vs unary SuperCache without labeling |

### Extending later

- Cluster read-QPS mode (aggregate clients across N nodes)  
- Replication lag after Put  
- Pipeline Redis profile (labeled unfair vs unary gRPC)  
- LoadThrough miss path (SoT latency dominates)
