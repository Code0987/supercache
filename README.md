# SuperCache

Eventually consistent, read-heavy distributed cache for shared runtime storage (Go).

In-process Engine or dedicated nodes. Owner writes with async fan-out, local reads, gossip membership, load-through keyspaces, structured types (Bloom / Set / ZSet / Geo / List / Hash / Counter / JSON / Bitmap / HLL / TopK / CMS / VectorSet / Stream), and a bounded per-node LRU.

```text
github.com/Code0987/supercache
```

| Topic | Link |
|-------|------|
| Architecture | [PLAN.md](./PLAN.md) |
| Cluster flows | [docs/CLUSTER_FLOWS.md](./docs/CLUSTER_FLOWS.md) |
| Operations | [docs/OPERATIONS.md](./docs/OPERATIONS.md) |
| API | [docs/API.md](./docs/API.md) · [Swagger](https://code0987.github.io/supercache/) |
| Benchmarks | [docs/BENCHMARKS.md](./docs/BENCHMARKS.md) |
| Releases | [docs/RELEASING.md](./docs/RELEASING.md) · [GitHub Releases](https://github.com/Code0987/supercache/releases) |
| Change workflow | [docs/WORKFLOW.md](./docs/WORKFLOW.md) |

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
# API docs (Swagger UI): http://127.0.0.1:8080/docs
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

### CLI (`sc`)

```bash
# With supercache-node running on defaults (keyspace name = lowercase mode):
go run ./cmd/sc put greeting "hello"
go run ./cmd/sc get greeting
go run ./cmd/sc del greeting
go run ./cmd/sc -keyspace set sadd features dark_mode
go run ./cmd/sc -keyspace set sismember features dark_mode
go run ./cmd/sc -keyspace zset zadd lb 100 alice
go run ./cmd/sc -keyspace zset zrange lb 0 -1
go run ./cmd/sc -keyspace places geoadd city -74 40.7 shop
go run ./cmd/sc -keyspace places georadius city -74 40.7 20000 10
go run ./cmd/sc -keyspace inbox rpush q event1
go run ./cmd/sc -keyspace inbox lrange q 0 -1
go run ./cmd/sc -keyspace hash hset user email a@b
go run ./cmd/sc -keyspace hash hgetall user
# ModeCounter (register a ModeCounter keyspace; not in -demo-keyspace):
# go run ./cmd/sc -keyspace rl incr alice:1
go run ./cmd/sc -keyspace json jsonset user $.name '"Ada"'
go run ./cmd/sc -keyspace json jsonget user $.name
go run ./cmd/sc -keyspace bitmap bitset seen 0 1
go run ./cmd/sc -keyspace bitmap bitget seen 0
go run ./cmd/sc -keyspace vectorset vadd products east 1,0
go run ./cmd/sc -keyspace vectorset vadd products north 0,1
go run ./cmd/sc -keyspace vectorset vadd products ne 0.7,0.7
go run ./cmd/sc -keyspace vectorset vcard products
go run ./cmd/sc -keyspace vectorset vsim products 1,0.05 3
# go run ./cmd/sc -keyspace events xadd logs * '{"msg":"hello"}'
# go run ./cmd/sc -keyspace events xlen logs
# go run ./cmd/sc -keyspace events xrange logs - + 10
go run ./cmd/sc -keyspace seen bloom add users alice   # ModeBloom keyspace
# ModeTopK (register a ModeTopK keyspace, or use examples/billboard plays/hot):
# go run ./cmd/sc -keyspace plays topkadd hot t001
# go run ./cmd/sc -keyspace plays topklist hot
# ModeCMS (register a ModeCMS keyspace, or use examples/billboard plays-count/hot):
# go run ./cmd/sc -keyspace plays-count cmsincr hot t003
# go run ./cmd/sc -keyspace plays-count cmsquery hot t003
go run ./cmd/sc peers              # admin HTTP

# Multi-seed (failover entry points; owner routing is still server-side)
go run ./cmd/sc -addr 127.0.0.1:9000,127.0.0.1:9010 ping

# Interactive REPL
go run ./cmd/sc
```

Install: `go install ./cmd/sc`. See [cmd/sc/README.md](./cmd/sc/README.md).

### Benchmarks

Local SuperCache vs Redis (multi-trial medians):

```bash
# Redis (memory-only) + SuperCache node in other terminals, then:
go run ./cmd/scbench -reliable -json=bench-report.json
```

In-process matrix (no external node):

```bash
go run ./cmd/scbench -tier=laptop -json=laptop.json
# or: bash scripts/bench-local.sh laptop
```

CI runs the smoke suite once per side on the same GitHub runner and comments the diff on pull requests (not a merge gate). See [cmd/scbench/README.md](./cmd/scbench/README.md) and [docs/BENCHMARKS.md](./docs/BENCHMARKS.md).

### SuperCache Lab (interactive explorer)

```bash
cd examples/lab/ui && npm ci && npm run build && cd ../../..
go run ./examples/lab
# UI: http://127.0.0.1:19080/  — connect cache addrs, or -cluster for a local mesh
```

See [examples/lab/README.md](./examples/lab/README.md). Attach to a running node
or start a 3-node RF=2 demo; React playground for every keyspace mode.

### Music trending billboard (cluster demo)

```bash
go run ./examples/billboard -hold=false   # 3-node cluster + scripted walkthrough
# UI: http://127.0.0.1:18080/   (use -hold=true to keep serving)
```

See [examples/billboard/README.md](./examples/billboard/README.md). Official charts stay LoadThrough SoT JSON; **live plays** are `ModeTopK` (`/v1/plays/hot`); **point frequency** is `ModeCMS` (`/v1/counts/hot?track=t003`).

### ModeHash user profile

```bash
go run ./examples/hash   # 3-node in-process walkthrough (HSet/HGet/HDel, concurrent fields)
```

See [examples/hash/README.md](./examples/hash/README.md). `sc -keyspace hash` talks to the node demo keyspace.

### ModeCounter rate limiter

```bash
go run ./examples/ratelimit   # fixed-window Allow on Incr
```

See [examples/ratelimit/README.md](./examples/ratelimit/README.md).

### ModeJSON document

```bash
go run ./examples/json   # 3-node in-process walkthrough (path set/get/del)
```

See [examples/json/README.md](./examples/json/README.md). `sc -keyspace json` talks to the node demo keyspace.

### ModeBitmap packed bits

```bash
go run ./examples/bitmap   # 3-node in-process walkthrough (BitSet/BitGet/BitCount/BitPos)
```

See [examples/bitmap/README.md](./examples/bitmap/README.md). `sc -keyspace bitmap` talks to the node demo keyspace.

### ModeHLL approximate distinct count

```bash
go run ./examples/hll   # 3-node in-process walkthrough (HLLAdd/HLLCount)
```

See [examples/hll/README.md](./examples/hll/README.md). Register a `ModeHLL` keyspace (no default demo KS).

### TLS (production)

```bash
go run ./cmd/supercache-node -cluster -node-id n1 \
  -tls-cert server.pem -tls-key server-key.pem \
  -tls-client-ca ca.pem -peer-mtls \
  ...
```

Apps: `client.DialTLS` with `pkg/tlsconfig.ClientFiles`. See [docs/OPERATIONS.md](./docs/OPERATIONS.md).

## Packages

| Package | Role |
|---------|------|
| `pkg/engine` | Core Get/Put/Delete, Bloom/Set/ZSet/Geo/List/Hash/Counter/JSON/Bitmap/HLL/TopK/CMS/VectorSet/Stream, keyspaces, cluster routing |
| `pkg/store` | Versioned LRU memory store (immediate Set / RYOW; structure caches) |
| `pkg/keyspace` | Config: `LoadThrough` / `CacheOnly` / `Bloom` / `Set` / `ZSet` / `Geo` / `List` / `Hash` / `Counter` / `JSON` / `Bitmap` / `HLL` / `TopK` / `CMS` / `VectorSet` / `Stream` |
| `pkg/bloom` | Bitset Bloom filter used by `ModeBloom` |
| `pkg/set` | Exact set encode/decode for `ModeSet` |
| `pkg/zset` | Sorted-set encode/decode for `ModeZSet` |
| `pkg/geo` | Point index encode/haversine for `ModeGeo` |
| `pkg/list` | ModeList codec + owner/cluster helpers |
| `pkg/hashx` | Field-map encode for `ModeHash` |
| `pkg/counter` | int64 encode/add for `ModeCounter` |
| `pkg/jsonx` | Nested JSON path encode for `ModeJSON` |
| `pkg/bitmapx` | Packed Redis-order bit vector for `ModeBitmap` |
| `pkg/hllx` | Dense HyperLogLog sketch for `ModeHLL` |
| `pkg/topkx` | Space-Saving top-K table for `ModeTopK` |
| `pkg/cmsx` | Count-Min Sketch for `ModeCMS` |
| `pkg/vecset` | Embedding set + K-NN for `ModeVectorSet` |
| `pkg/stream` | Append-only log codec for `ModeStream` |
| `pkg/datasource` | Backend loader interface |
| `pkg/protect` | Rate limit + circuit breaker |
| `pkg/admin` | `/healthz` `/readyz` `/peers` `/keyspaces` `/metrics` + `/docs` (Swagger) |
| `api/openapi` | OpenAPI 3 specs (Admin + Cache gRPC reference) |
| `pkg/telemetry` | Counters + OpenTelemetry |
| `pkg/membership` | Gossip + ring rebuild |
| `pkg/warmup` | Hot keys, topology handoff (hot then rest), refresh-ahead |
| `pkg/client` | Application gRPC client (KV + Bloom + Set + ZSet + Geo + List + Hash + Counter + JSON + Bitmap + HLL + TopK + CMS + VectorSet + Stream) |
| `pkg/tlsconfig` | TLS/mTLS config from PEM files |
| `cmd/supercache-node` | Node binary (`-demo-keyspace`: cacheonly / set / zset / hash / json / bitmap / vectorset) |
| `cmd/sc` | CLI: get/put/del, bloom, sadd*, z*, geo*, l*, h*, incr/cget, json*, bit*, hlladd/hllcount, topkadd/topklist, cmsincr/cmsquery, vadd*, xadd* |
| `cmd/scbench` | SuperCache vs Redis load harness + in-process matrix |

## Consistency

SuperCache is **eventually consistent**. Writes ACK on the owner; fan-out is async to **R replicas** (default 3; `keyspace.Config.ReplicationFactor`). CacheOnly miss on a non-owner forwards to the owner. Delete is best-effort to the replica set. Not for linearizable or transactional workloads.

- Local store is a custom LRU (not Ristretto): owner Put requires immediate visibility.
- Peer port and Cache port are separate; do not expose Peer to applications.
- `UpdateKeySpace` is **local** — re-issue on every node; compare `keyspace_hashes` on `/peers`.
- Topology change: existing nodes async-push inventory to peers (hot keys first, then rest). See [docs/CLUSTER_FLOWS.md](./docs/CLUSTER_FLOWS.md).
- Delete installs a versioned tombstone for `TombstoneTTL` (default 5m) so a delayed ApplyPut cannot resurrect the key.
- Keyspace modes: **CacheOnly** / **LoadThrough** (KV), **ModeBloom**, **ModeSet**, **ModeZSet**, **ModeGeo**, **ModeList**, **ModeHash**, **ModeCounter**, **ModeJSON**, **ModeBitmap**, **ModeHLL**, **ModeTopK**, **ModeCMS**, **ModeVectorSet**, **ModeStream**. Wrong verb → invalid argument. API summary: [docs/API.md](./docs/API.md). Designs: [Bloom](./docs/design/2026-08-11-bloom-filter.md), [Set](./docs/design/2026-08-13-mode-set.md), [ZSet](./docs/design/2026-08-13-mode-zset.md), [Geo](./docs/design/2026-08-19-mode-geo.md), [List](./docs/design/2026-08-19-mode-list.md), [Hash](./docs/design/2026-08-20-mode-hash.md), [Counter](./docs/design/2026-08-20-mode-counter.md), [JSON](./docs/design/2026-08-21-mode-json.md), [Bitmap](./docs/design/2026-08-21-mode-bitmap.md), [HLL](./docs/design/2026-08-25-mode-hll.md), [TopK](./docs/design/2026-08-31-mode-topk.md), [CMS](./docs/design/2026-09-01-mode-cms.md), [VectorSet](./docs/design/2026-09-04-mode-vector-set.md), [Stream](./docs/design/2026-09-08-mode-stream.md).

Details: [PLAN.md](./PLAN.md) §3 / §7 and [docs/OPERATIONS.md](./docs/OPERATIONS.md).

## Releases

Merges to `main` publish a GitHub Release when the version marker appears in the
**commit message** (YAML front matter) or **PR title** (brackets):

```text
---
release: v1.2.3
---
# commit message front matter

Ship feature [release: v1.2.3]   # PR title
```

See [docs/RELEASING.md](./docs/RELEASING.md). Downloads: [GitHub Releases](https://github.com/Code0987/supercache/releases).

## Test

```bash
go test ./... -race
```
