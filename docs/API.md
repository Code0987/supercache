# SuperCache API documentation

## Hosted docs (Swagger UI)

| Where | URL |
|-------|-----|
| **On a running node** | `http://<admin-addr>/docs` (default `http://127.0.0.1:8080/docs`) |
| **GitHub Pages** | `https://code0987.github.io/supercache/` |
| **OpenAPI YAML (admin)** | `/openapi.yaml` or `/docs/admin.openapi.yaml` |
| **OpenAPI YAML (cache ref)** | `/docs/cache.openapi.yaml` |

```bash
go run ./cmd/supercache-node \
  -cache 127.0.0.1:9000 -peer 127.0.0.1:9001 -admin 127.0.0.1:8080
# open http://127.0.0.1:8080/docs
```

### Specs

| Spec | Source | Try it out? |
|------|--------|-------------|
| Admin HTTP | [`api/openapi/admin.openapi.yaml`](../api/openapi/admin.openapi.yaml) | Yes (against the node) |
| Cache gRPC | [`api/openapi/cache.openapi.yaml`](../api/openapi/cache.openapi.yaml) | No — reference only |

### Clients

- **Go:** `pkg/client`
- **CLI:** `cmd/sc` (`sc get` / `put` / `del`, `bloom`, `zadd`…, or REPL)
- **Protos:** `api/proto/cache.proto`, `api/proto/peer.proto` (peer is mesh-internal)

## Keyspace modes

Each keyspace has exactly one mode. Verbs that do not match the mode return invalid argument.

| Mode | Purpose | App verbs |
|------|---------|-----------|
| `ModeCacheOnly` | Opaque `key → []byte`, no SoT | `Get`, `Put`, `Delete` (+ batch) |
| `ModeLoadThrough` | Opaque KV + `DataSource` on miss | same as CacheOnly |
| `ModeBloom` | Approximate membership (named filter) | `BloomAdd`, `BloomTest`; `Delete(name)` wipes filter |
| `ModeSet` | Exact membership (named set) | `SetAdd`, `SetRemove`, `SetContains`, `SetCard`, `SetMembers`; `Delete(name)` |
| `ModeZSet` | Scored sorted set (named zset) | `ZAdd`, `ZRem`, `ZScore`, `ZCard`, `ZRange`, `ZRangeByScore`; `Delete(name)` |

Config: `pkg/keyspace.Config` (`Name`, `Mode`, `MaxBytes`, `TTL`, `ReplicationFactor`, …). Bloom also uses `BloomBits` / `BloomHashes`.

## Cache gRPC RPCs

Service `supercache.cache.v1.Cache` on the **`-cache`** port. Full shapes: [`api/proto/cache.proto`](../api/proto/cache.proto).

### KV (`ModeCacheOnly` / `ModeLoadThrough`)

| RPC | Notes |
|-----|--------|
| `Get` | Local observation; CacheOnly miss may owner-forward |
| `Put` / `PutMany` | Owner ACK; async fan-out to R−1 replicas |
| `Delete` / `DeleteMany` | Owner tombstone + replica apply/hint |

### Bloom (`ModeBloom`)

| RPC | Notes |
|-----|--------|
| `BloomAdd` | OR bits on owner + replicas (not LWW of the bitset) |
| `BloomTest` | `maybe=false` ⇒ definitely not; missing filter ⇒ false |
| `Delete(name)` | Tombstone whole filter |

### Set (`ModeSet`)

| RPC | Notes |
|-----|--------|
| `SetAdd` / `SetRemove` | Exact membership; item-level fan-out |
| `SetContains` | Exact; missing set ⇒ false |
| `SetCard` / `SetMembers` | Count / full members (defensive copies) |
| `Delete(name)` | Tombstone whole set |

### Sorted set (`ModeZSet`)

| RPC | Notes |
|-----|--------|
| `ZAdd` | Upsert member score (`float64`; NaN rejected) |
| `ZRem` | Remove member if present |
| `ZScore` | Score + present; missing ⇒ present=false |
| `ZCard` | Member count |
| `ZRange` | By rank (Redis-style start/stop, negatives OK) |
| `ZRangeByScore` | Inclusive score window, ascending |
| `Delete(name)` | Tombstone whole zset |

Equal scores order by member bytes. Wire: `ZMember { bytes member; double score }`.

## Enabling GitHub Pages

1. Repo **Settings → Pages → Build and deployment → GitHub Actions**
2. Push to `main` (workflow [`.github/workflows/docs.yml`](../.github/workflows/docs.yml))
3. Site URL: `https://<owner>.github.io/supercache/`

The workflow copies OpenAPI + UI from source into the Pages artifact so the site
stays aligned with the embedded node docs.
