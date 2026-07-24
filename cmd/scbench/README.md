# scbench — SuperCache vs Redis (simple)

Minimal Get/Set comparison harness. **One process, one backend per run.**

## Prerequisites

**Redis** (memory-only, no persistence):

```bash
redis-server --port 6379 --save "" --appendonly no
# or: docker run --rm -p 6379:6379 redis:7 redis-server --save "" --appendonly no
```

**SuperCache** (single node; default `demo` keyspace is CacheOnly):

```bash
go run ./cmd/supercache-node \
  -cache 127.0.0.1:9000 \
  -peer 127.0.0.1:9001 \
  -admin 127.0.0.1:8080
```

## Run

```bash
# GET hit (prefill + measure)
go run ./cmd/scbench -backend=redis      -addr=127.0.0.1:6379 -op=get
go run ./cmd/scbench -backend=supercache -addr=127.0.0.1:9000 -op=get

# SET
go run ./cmd/scbench -backend=redis      -op=set
go run ./cmd/scbench -backend=supercache -op=set

# Mixed 95% GET / 5% SET
go run ./cmd/scbench -backend=redis      -op=mixed -read-ratio=0.95
go run ./cmd/scbench -backend=supercache -op=mixed -read-ratio=0.95
```

### Useful flags

| Flag | Default | Meaning |
|------|---------|---------|
| `-backend` | `supercache` | `redis` or `supercache` |
| `-addr` | backend default | Redis `host:port` or SuperCache **cache** gRPC addr |
| `-op` | `get` | `get` \| `set` \| `mixed` |
| `-keys` | `10000` | key space size |
| `-value-bytes` | `256` | payload size |
| `-concurrency` | `64` | workers |
| `-duration` | `20s` | measure window |
| `-warmup` | `3s` | discarded warmup |
| `-keyspace` | `demo` | SuperCache keyspace |
| `-prefill` | `true` | fill keys before get/mixed |

## Fairness

| | SuperCache | Redis |
|--|------------|--------|
| Protocol | gRPC | RESP |
| Process | Go node | C server |
| Consistency | eventual (single node: local) | single-instance atomic |
| Put/SET | in-memory CacheOnly | in-memory (AOF/RDB off above) |

Compare **single-node SuperCache** to **single Redis**. This is a rough localhost benchmark, not a product claim.

## Example output

```text
RESULT backend=redis      op=get   ops=...  errors=0  ops/s=...
       latency  p50=...  p95=...  p99=...  (n_samples=...)
```
