# SuperCache

Eventually consistent, read-heavy distributed cache for shared runtime storage (Go).

**Status:** Milestone 6 — polish (client API, docs, chaos tests)  
See [PLAN.md](./PLAN.md) for architecture and [docs/OPERATIONS.md](./docs/OPERATIONS.md) for ops.

## Module

```text
github.com/Code0987/supercache
```

## Quick start

### In-process Engine

```go
e := engine.New()
defer e.Close()
_ = e.UpdateKeySpace(keyspace.Config{
    Name: "demo", Mode: keyspace.ModeCacheOnly, MaxBytes: 64 << 20, TTL: time.Minute,
})
_ = e.Put(ctx, "demo", "k", []byte("v"))
v, err := e.Get(ctx, "demo", "k")
```

### Remote client

```bash
go run ./cmd/supercache-node -cache 127.0.0.1:9000 -peer 127.0.0.1:9001 -admin 127.0.0.1:8080
```

```go
cli, _ := client.Dial(ctx, "127.0.0.1:9000")
defer cli.Close()
_ = cli.Put(ctx, "demo", "k", []byte("v"), client.WithTTL(time.Minute))
v, err := cli.Get(ctx, "demo", "k")
```

### Two-node cluster

```bash
go run ./cmd/supercache-node -cluster -node-id n1 \
  -admin 127.0.0.1:8081 -cache 127.0.0.1:9010 -peer 127.0.0.1:9001 \
  -gossip-port 7946 -gossip-advertise 127.0.0.1

go run ./cmd/supercache-node -cluster -node-id n2 \
  -admin 127.0.0.1:8082 -cache 127.0.0.1:9011 -peer 127.0.0.1:9002 \
  -gossip-port 7947 -gossip-advertise 127.0.0.1 \
  -seeds 127.0.0.1:7946
```

Optional: `-gossip-secret <key>`.

### Bench vs Redis

```bash
# Redis (memory-only) + SuperCache node in other terminals, then:
go run ./cmd/scbench -reliable -json=bench-report.json
```

Multi-trial medians, get/set/mixed suite, comparison table. See [cmd/scbench/README.md](./cmd/scbench/README.md) and [docs/BENCHMARKS.md](./docs/BENCHMARKS.md).

### Music trending billboard (cluster demo)

```bash
go run ./examples/billboard -hold=false   # 3-node cluster + scripted walkthrough
# UI: http://127.0.0.1:18080/   (use -hold=true to keep serving)
```

See [examples/billboard/README.md](./examples/billboard/README.md).

### TLS (production)

```bash
go run ./cmd/supercache-node -cluster -node-id n1 \
  -tls-cert server.pem -tls-key server-key.pem \
  -tls-client-ca ca.pem -peer-mtls \
  ...
```

Apps: `client.DialTLS` with `pkg/tlsconfig.ClientFiles`. See `docs/OPERATIONS.md`.

## Packages

| Package | Role |
|---------|------|
| `pkg/engine` | Core Get/Put/Delete, keyspaces, cluster routing |
| `pkg/store` | Versioned LRU memory store (immediate Set / RYOW) |
| `pkg/keyspace` | Config, `LoadThrough` / `CacheOnly` |
| `pkg/datasource` | Backend loader interface |
| `pkg/protect` | Rate limit + circuit breaker |
| `pkg/admin` | `/healthz` `/readyz` `/peers` `/keyspaces` `/metrics` |
| `pkg/telemetry` | Counters + OpenTelemetry |
| `pkg/membership` | Gossip + ring rebuild |
| `pkg/warmup` | Hot keys, topology prefetch, refresh-ahead |
| `pkg/client` | Application gRPC client |
| `pkg/tlsconfig` | TLS/mTLS config from PEM files |
| `internal/ring` | Consistent hash |
| `internal/peer` | Peer client pool + fan-out |
| `internal/peerserver` | Peer gRPC service |
| `internal/cacheserver` | Application Cache gRPC service |
| `cmd/supercache-node` | Node binary |

## Consistency (short)

SuperCache is **eventually consistent**. Put ACKs on owner; fan-out is async. Delete is best-effort to all peers. Not for linearizable or transactional workloads. Details: `PLAN.md` §3 and `docs/OPERATIONS.md`.

## Test

```bash
go test ./... -race
```

## Design notes

- Local store is a custom LRU (not Ristretto): owner Put requires immediate visibility.
- Peer port and Cache port are separate; do not expose Peer to applications.
- `UpdateKeySpace` is **local** — re-issue on every node; compare `keyspace_hashes` on `/peers`.
