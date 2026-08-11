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

## Consistency cheatsheet

| Op | Guarantee |
|----|-----------|
| Get | Local observation on the queried node |
| Put | ACK after **owner** accept; async fan-out to **R−1 replicas** (`ReplicationFactor`, default 3). Failed `ApplyPut`s are hinted per replica and replayed when that peer is reachable again (bounded; oldest dropped). |
| Delete | Owner + replica set: each accepted delete **installs a versioned tombstone** (not a bare remove). `MultiError` / peer_failures if any replica RPC fails. Tombstones expire after 5m so they do not pin RAM; until then a delayed ApplyPut cannot resurrect the key. |
| Failures | Fan-out errors are metrics-only on Put |

Set TTLs to your max acceptable staleness.

### `ring_generation` on peer Apply*

Peer `ApplyPut` / `ApplyDelete` carry the sender's hash-ring generation. **LWW version
still decides whether the apply is stored.** A wire generation that differs from the
local ring bumps admin metric `ring_gen_mismatch` (topology churn / delayed fan-out).
Do not treat a mismatch as a hard error; use it for diagnostics.

## Topology change / join

On membership join/leave, every node rebuilds the hash ring and runs warmup:

1. Prefetch configured `WarmKeys` and tracked hot keys.
2. **Handoff:** push local live entries to each key's **replica set** — **hot first, then the rest** (async `ApplyPut`).

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
