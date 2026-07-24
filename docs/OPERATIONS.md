# SuperCache operations

## Ports

| Port flag | Purpose | Exposure |
|-----------|---------|----------|
| `-cache` (default `:9000`) | Application gRPC (`Cache` service) | Apps / mesh internal |
| `-peer` (default `:9001`) | Peer mesh gRPC (`Peer` service) | **Nodes only** — do not expose to apps |
| `-admin` (default `:8080`) | Diagnostics HTTP | Private / localhost |
| `-gossip-port` | memberlist | Nodes only |

## Keyspace config rollout

`UpdateKeySpace` / `DeleteKeySpace` apply **only on the calling node**.

1. Deploy the same config to every node (GitOps, automation loop).
2. Compare `GET /peers` → `keyspace_hashes` across nodes.
3. Drift is unsupported: divergent TTLs/MaxBytes change behavior silently.

## Consistency cheatsheet

| Op | Guarantee |
|----|-----------|
| Get | Local observation on the queried node |
| Put | ACK after **owner** accept; async fan-out (no retry) |
| Delete | Owner + all peers best-effort; `MultiError` / peer_failures if any fail |
| Failures | Fan-out errors are metrics-only on Put |

Set TTLs to your max acceptable staleness.

### `ring_generation` on peer Apply*

Peer `ApplyPut` / `ApplyDelete` carry the sender's hash-ring generation. **LWW version
still decides whether the apply is stored.** A wire generation that differs from the
local ring bumps admin metric `ring_gen_mismatch` (topology churn / delayed fan-out).
Do not treat a mismatch as a hard error; use it for diagnostics.

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
