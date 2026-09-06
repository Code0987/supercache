# SuperCache operations

## Ports

| Port flag | Purpose | Exposure |
|-----------|---------|----------|
| `-cache` (default `:9000`) | Application gRPC (`Cache` service) | Apps / mesh internal |
| `-peer` (default `:9001`) | Peer mesh gRPC (`Peer` service) | **Nodes only** — do not expose to apps |
| `-admin` (default `:8080`) | Diagnostics HTTP + **API docs** (`/docs`) | Private / localhost |
| `-gossip-port` | memberlist | Nodes only |

## API documentation (Swagger)

Each node hosts interactive OpenAPI docs on the admin port:

| Path | Purpose |
|------|---------|
| `/docs` | Swagger UI (Admin try-it-out + Cache gRPC reference) |
| `/openapi.yaml` | Admin OpenAPI YAML |
| `/openapi/cache.yaml` | Cache gRPC reference OpenAPI YAML |

```bash
# with default admin bind
open http://127.0.0.1:8080/docs
```

Public static copy (GitHub Pages): see [docs/API.md](./API.md). Specs live in `api/openapi/`.

## TLS / mTLS

Plaintext is the default for local demos. Production should enable TLS.

| Flag | Purpose |
|------|---------|
| `-tls-cert` / `-tls-key` | Server certificate and key for **both** Cache and Peer listeners |
| `-tls-client-ca` | CA PEM used to verify peer (and optionally cache) client certs |
| `-peer-mtls` | Require client certs on the Peer port (needs `-tls-client-ca`) |
| `-peer-client-cert` / `-peer-client-key` | Outbound peer identity (default: server cert/key) |
| `-peer-server-name` | TLS `ServerName` for peer dials when not using DNS names in peer addrs |
| `-cache-client-ca` / `-cache-mtls` | Optional app-client mTLS on the Cache port |

Apps:

```go
cfg, err := tlsconfig.ClientFiles("ca.pem", "cache.example", "", "")
cli, err := client.DialTLS(ctx, "cache.example:9000", cfg)
```

Peer mesh with mTLS: every node uses the same CA; each node presents a cert signed by that CA.

## Keyspace config rollout

`UpdateKeySpace` / `DeleteKeySpace` apply **only on the calling node**.

1. Deploy the same config to every node (GitOps, automation loop).
2. Compare `GET /peers` → `keyspace_hashes` across nodes.
3. Drift is unsupported: divergent TTLs/MaxBytes change behavior silently.

## Keyspace modes (ops view)

| Mode | Verbs | Notes |
|------|-------|--------|
| `CacheOnly` | Get / Put / Delete | Miss = not found (may owner-forward for repair) |
| `LoadThrough` | Get / Put / Delete | Miss loads `DataSource` on owner |
| `ModeBloom` | BloomAdd / BloomTest / Delete(name) | Approximate membership; no per-item delete |
| `ModeSet` | SetAdd / SetRemove / SetContains / SetCard / SetMembers / Delete(name) | Exact membership |
| `ModeZSet` | ZAdd / ZRem / ZScore / ZCard / ZRange / ZRangeByScore / Delete(name) | Scored sorted set |
| `ModeGeo` | GeoAdd / GeoRem / GeoPos / GeoCard / GeoDist / GeoRadius / Delete(name) | WGS84 points; radius in meters |
| `ModeList` | LPush / RPush / LPop / RPop / LLen / LIndex / LRange / Delete(name) | Ordered list; snapshot fan-out |
| `ModeHash` | HSet / HGet / HDel / HExists / HLen / HGetAll / Delete(name) | Field map; item-level fan-out |
| `ModeCounter` | Incr / CounterGet / Delete(name) | int64; snapshot fan-out; Incr returns new n |
| `ModeJSON` | JsonSet / JsonGet / JsonDel / Delete(name) | nested JSON; snapshot fan-out; replica JsonGet may lag |
| `ModeBitmap` | BitSet / BitGet / BitCount / BitPos / Delete(name) | packed bits; snapshot fan-out + owner-inbox; replica BitGet may lag |
| `ModeHLL` | HLLAdd / HLLCount / Delete(name) | dense 12 KiB sketch; snapshot fan-out + owner-inbox; replica HLLCount may lag |
| `ModeTopK` | TopKAdd / TopKList / Delete(name) | Space-Saving table; snapshot fan-out + owner-inbox; replica TopKList may lag |
| `ModeCMS` | CMSIncr / CMSQuery / Delete(name) | Count-Min 64 KiB; snapshot fan-out + owner-inbox; replica CMSQuery may lag |
| `ModeVectorSet` | VAdd / VRem / VSim / VCard / VDim / VEmb / Delete(name) | embeddings; cosine/L2/IP; snapshot fan-out + A/R inbox; replica VSim may lag |

Wrong verb for the mode → invalid argument. Configure the same modes on every node (see rollout above).

Demo node (`-demo-keyspace`): registers `demo` (CacheOnly), `tags` (ModeSet), `board` (ModeZSet), `profile` (ModeHash), `doc` (ModeJSON), `flags` (ModeBitmap), `embeddings` (ModeVectorSet). Geo/List/Counter/HLL/TopK/CMS keyspaces are configured by the app. Live plays billboard: [examples/billboard](../examples/billboard/README.md). Hash walkthrough: [examples/hash](../examples/hash/README.md). Rate limiter: [examples/ratelimit](../examples/ratelimit/README.md). JSON document: [examples/json](../examples/json/README.md). Bitmap flags: [examples/bitmap](../examples/bitmap/README.md). HLL sketch: [examples/hll](../examples/hll/README.md).

## Consistency cheatsheet

| Op | Guarantee |
|----|-----------|
| Get | Local observation on the queried node |
| Put | ACK after **owner** accept; async fan-out to **R−1 replicas** (`ReplicationFactor`, default 3). Failed `ApplyPut`s are hinted per replica and replayed when that peer is reachable again (bounded; oldest dropped). |
| Delete | Owner tombstone, then the **same replica apply+hint pool as Put** (sync first attempt). Failed peers are hinted and replayed. `MultiError` if any replica fails on that first attempt. Tombstones expire after `TombstoneTTL` (default 5m; negative = never). Join handoff uses the same pool. Applies to KV keys **and** named Bloom/Set/ZSet/Geo/List/Hash/Counter/JSON/Bitmap/HLL/TopK entries. |
| BloomAdd / BloomTest | `ModeBloom` only. Add ORs bits on the owner and replicas (not LWW of the bitset). Test is local on a replica, owner-forward otherwise. There is no per-item delete. |
| SetAdd / SetRemove / SetContains | `ModeSet` only. Owner serializes mutations; item-level fan-out (`FlagSetAdd` / `FlagSetRemove`). Contains is local on a replica, owner-forward otherwise. |
| ZAdd / ZRem / ZScore / ZRange* | `ModeZSet` only. Same ownership pattern as ModeSet; item-level `FlagZSetAdd` / `FlagZSetRem`; handoff ships full encoded zset (`FlagZSet`). |
| GeoAdd / GeoRem / GeoPos / GeoRadius | `ModeGeo` only. Same ownership as ModeSet; item-level `FlagGeoAdd` / `FlagGeoRem`; handoff `FlagGeo`. Distance is haversine meters. |
| LPush / RPush / LPop / RPop / LRange | `ModeList` only. Owner applies the op then fans out a **full `FlagList` snapshot** (hints coalesce per name; item-level replica apply would drop earlier pushes). Non-owner pop uses peer `ListPop`. |
| HSet / HGet / HDel / HGetAll | `ModeHash` only. Same ownership as ModeSet; item-level `FlagHashSet` / `FlagHashDel`; handoff `FlagHash`. Replica with a local hash is local-only on field miss (`hintID` still coalesces per name). |
| Incr / CounterGet | `ModeCounter` only. Owner `Incr` returns the new int64 (peer `CounterIncr` from a non-owner). Replicas install a `FlagCounter` snapshot. Replica `CounterGet` may lag. Overflow is invalid argument. Fixed-window rate limits put the window id in the **name**. |
| JsonSet / JsonGet / JsonDel | `ModeJSON` only. Owner applies the op then fans out a **full `FlagJSON` snapshot** (inbox flags stay owner-only). Replica `JsonGet` may lag. `JsonDel $` leaves live `{}`. |
| BitSet / BitGet / BitCount / BitPos | `ModeBitmap` only. Owner applies the op then fans out a **full `FlagBitmap` snapshot** (inbox `FlagBitmapSet` stays owner-only). Replica `BitGet` may lag. Missing name → `BitGet` `ok=false`. Live all-zero stays until `Delete(name)`. `BitSet` is ACK-only (no old bit). |
| HLLAdd / HLLCount | `ModeHLL` only. Owner applies the op then fans out a **full `FlagHLL` snapshot** (inbox `FlagHLLAdd` stays owner-only). Replica `HLLCount` may lag. Missing name → `ok=false`. `HLLAdd` is ACK-only (no changed-bool). |
| TopKAdd / TopKList | `ModeTopK` only. Owner applies the op then fans out a **full `FlagTopK` snapshot** (inbox `FlagTopKAdd` stays owner-only). Replica `TopKList` may lag. Missing name → `ok=false`. `TopKAdd` is ACK-only (+1). |
| CMSIncr / CMSQuery | `ModeCMS` only. Owner applies the op then fans out a **full `FlagCMS` snapshot** (inbox `FlagCMSIncr` stays owner-only). Replica `CMSQuery` may lag. Missing name → `ok=false`. `CMSIncr` is ACK-only (`n==0` means 1). See [`examples/billboard`](../examples/billboard/) for the ModeCMS complement. |
| VAdd / VSim / VRem | `ModeVectorSet` only. Owner applies then fans a **full `FlagVectorSet` snapshot** (`A`/`R` inbox owner-only). Metric is keyspace `VectorMetric`. Replica `VSim` may lag. See [`examples/vecset`](../examples/vecset/). |
| Failures | Fan-out errors are metrics-only on Put (and analogous async structure fan-out) |

Set TTLs to your max acceptable staleness (TTL applies to the **whole** named structure, not per member).

### `ring_generation` on peer Apply*

Peer `ApplyPut` / `ApplyDelete` carry the sender's hash-ring generation. **LWW version
still decides whether the apply is stored.** A wire generation that differs from the
local ring bumps admin metric `ring_gen_mismatch` (topology churn / delayed fan-out).
Do not treat a mismatch as a hard error; use it for diagnostics.

## Topology change / join

On membership join/leave, every node rebuilds the hash ring and runs warmup:

1. Prefetch configured `WarmKeys` and tracked hot keys.
2. **Handoff:** push local live entries **and tombstones** to each key's **replica set** — **hot first, then the rest** (async `ApplyPut` / `ApplyDelete`).

A new node starts empty and warms from peers; there is a short cold window before handoff completes. Keys nobody holds stay cold until Put or LoadThrough traffic.

Disable or cap via `warmup.Config` (`DisableHandoff`, `HandoffMaxEntries`). Flow diagrams: [CLUSTER_FLOWS.md](./CLUSTER_FLOWS.md).

## Anti-patterns

- Linearizable locks / leader election
- Counters that must not double-apply
- Sole durable store of truth
- One gossip peer per app replica (use `supercache-node` + `pkg/client`)
- Calling `UpdateKeySpace` on a single node and assuming cluster-wide apply

## Client read-your-writes

`pkg/client` does **not** buffer last Put locally. After Put:

- Get on the **owner** (or same node that accepted Put) sees the value immediately.
- Get on another node may lag until fan-out.

Optional app-side sticky cache is out of scope for the library.
