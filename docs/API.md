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
- **CLI:** `cmd/sc` (`sc get` / `put` / `del`, `bloom`, `sadd`…, `zadd`…, `geoadd`…, `lpush`…, `hset`…, `incr` / `cget`, or REPL)
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
| `ModeGeo` | Named geospatial point index | `GeoAdd`, `GeoRem`, `GeoPos`, `GeoCard`, `GeoDist`, `GeoRadius`; `Delete(name)` |
| `ModeList` | Named ordered list | `LPush`, `RPush`, `LPop`, `RPop`, `LLen`, `LIndex`, `LRange`; `Delete(name)` |
| `ModeHash` | Named field map | `HSet`, `HGet`, `HDel`, `HExists`, `HLen`, `HGetAll`; `Delete(name)` |
| `ModeCounter` | Named int64 | `Incr`, `CounterGet`; `Delete(name)` |
| `ModeJSON` | Named nested JSON document | `JsonSet`, `JsonGet`, `JsonDel`; `Delete(name)` |

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

### Geo (`ModeGeo`)

| RPC | Notes |
|-----|--------|
| `GeoAdd` | Upsert member lon/lat (WGS84; NaN/Inf/OOB rejected) |
| `GeoRem` | Remove member if present |
| `GeoPos` | lon + lat + present; missing ⇒ present=false |
| `GeoCard` | Member count |
| `GeoDist` | Haversine **meters** between two members; missing ⇒ present=false |
| `GeoRadius` | Points within `radius_meters`; nearest first; `limit<=0` = all |
| `Delete(name)` | Tombstone whole index |

Wire: `GeoMember { bytes member; double lon; double lat; double dist_meters }`.

### List (`ModeList`)

| RPC | Notes |
|-----|--------|
| `LPush` / `RPush` | Prepend / append; creates list if missing |
| `LPop` / `RPop` | Head / tail; missing or empty ⇒ `present=false` |
| `LLen` | Length; missing ⇒ 0 |
| `LIndex` | Element at index (Redis negatives); OOB ⇒ `present=false` |
| `LRange` | Inclusive window, Redis-style start/stop (`-1` = last) |
| `Delete(name)` | Tombstone whole list |

Replicas get a **full list snapshot** after each owner mutate (item-level fan-out would drop earlier pushes under hint coalesce). Non-owner pop uses peer `ListPop`. Empty after last pop: `LLen` 0 until `Delete(name)`.

### Hash (`ModeHash`)

| RPC | Notes |
|-----|--------|
| `HSet` | Upsert field; creates hash if missing |
| `HGet` | Value + present; missing hash or field ⇒ `present=false` |
| `HDel` | Remove field if present; does not `Delete` the name |
| `HExists` | Exact; missing ⇒ false |
| `HLen` | Field count; missing ⇒ 0 |
| `HGetAll` | All pairs in field-byte order (defensive copies) |
| `Delete(name)` | Tombstone whole hash |

Wire: `HashField { bytes field; bytes value }`. Item-level fan-out (`FlagHashSet` / `FlagHashDel`). Replica with a local hash does **not** owner-forward a field miss. Empty after last `HDel`: `HLen` 0 until `Delete(name)`.

### Counter (`ModeCounter`)

| RPC | Notes |
|-----|--------|
| `Incr` | Add `delta` (default 1 on `sc`); create if missing; returns **new** int64 |
| `CounterGet` | Missing ⇒ `present=false`, value 0 |
| `Delete(name)` | Tombstone whole counter |

Owner serializes `Incr` (non-owner uses peer `CounterIncr`). Replicas install an 8-byte **snapshot** (`FlagCounter`). Overflow is invalid argument (no wrap). Live `0` stays until `Delete(name)`. Replica `CounterGet` may lag — **`Incr` is authoritative**.

### JSON (`ModeJSON`)

| RPC | Notes |
|-----|--------|
| `JsonSet` | Upsert JSON at path; creates the document if missing |
| `JsonGet` | JSON + present; missing doc or path ⇒ `present=false`. Path `$` or omitted = whole document |
| `JsonDel` | Remove node at path. `$` / omitted clears to live `{}`. Missing path is a no-op |
| `Delete(name)` | Tombstone whole document |

Path subset: `$` / `.ident` / `["utf8"]` / `[n>=0]`. Object parents are created; arrays are **not**. Integers stay integers (`UseNumber`). Live JSON `null` is present; a missing name is not. Replicas install a **full document snapshot** (`FlagJSON`). Replica `JsonGet` may lag.

## Enabling GitHub Pages

1. Repo **Settings → Pages → Build and deployment → GitHub Actions**
2. Push to `main` (workflow [`.github/workflows/docs.yml`](../.github/workflows/docs.yml))
3. Site URL: `https://<owner>.github.io/supercache/`

The workflow copies OpenAPI + UI from source into the Pages artifact so the site
stays aligned with the embedded node docs.
