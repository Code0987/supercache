# SuperCache — Architecture Plan

**Project:** `supercache`  
**Module:** `github.com/Code0987/supercache`

This document freezes product intent and the technical decisions for v1.

**How to change this product** (humans and agents) is normative below and detailed in [docs/WORKFLOW.md](./docs/WORKFLOW.md) and [AGENTS.md](./AGENTS.md). Do not invent a shorter path for data-plane work.

---

## 0. Change workflow (agents + humans)

### Default path

```text
docs → review → revision → tests → coding → review → revision
  → bench (local) → revision → commit → PR → bench monitor (CI)
  → merge only if overall drift < 10%  (and user says merge)
```

| Step | What | Gate |
|------|------|------|
| **Docs** | Design in `docs/design/<yyyy-mm-dd>-<short-name>.md` | — |
| **Review** | Person reviews design | Explicit go-ahead: `looks good` / `do it` / `implement` |
| **Revision** | Update design from feedback | Re-approval if contract changed |
| **Tests** | Failing tests from the design table | — |
| **Coding** | Minimum code to pass `go test ./...` | — |
| **Review** | Check vs design / reviewer feedback | Issues fixed |
| **Revision** | Address code review | Tests still green |
| **Bench (local)** | Micros / smoke flagged in the design | No Get-hit alloc jump |
| **Revision** | Perf/correctness from local bench | Re-test + re-bench |
| **Commit** | Feature branch only — never `main` | — |
| **PR** | `gh pr create` | — |
| **Bench monitor** | `gh pr checks <n> --watch` + `<!-- supercache-bench-comment -->` | CI `test` + `bench` green |
| **Merge** | User says **merge** (or merge-if-green) **and** gate passes | See below |

`revision` means fix feedback and re-stop at the next gate. Do not skip gates.

### Tracks

| Kind of change | Path |
|----------------|------|
| New product behavior (types, API, replication, consistency) | Full path above |
| Bug on Get / Put / Delete / store / fan-out / membership | Full path (design may be short) |
| Refactor, same contract | Short design note in PR; keep tests; PR + CI bench + 10% gate |
| Docs / comments / this workflow | Docs → commit → PR (no product bench required) |

One design → one PR. No stacking a second feature on the same branch. Never push or commit to `main`.

### Merge gate (CI)

After a green `bench` job, read the sticky PR comment `<!-- supercache-bench-comment -->` (same runner, main vs PR).

| Check | Pass |
|-------|------|
| Shared smoke Δ ops/s | each cell within **±10%** |
| Shared micro Δ ns/op | each bench within **±10%** |
| Get-hit / StoreGetHit **allocs/op** | **unchanged** (any increase = fail) |
| New micros only on PR | report only; not a fail by themselves |

Eligible to merge only if the table passes **and** the user said **merge** (or clearly pre-authorized merge-if-green). CI green alone is not ship. If the user said merge but drift ≥10% or allocs jumped: **do not merge**; report numbers.

```text
gh pr create
gh pr checks <n> --watch
# read <!-- supercache-bench-comment -->
# summarize; wait for merge
gh pr merge <n> --merge --delete-branch
```

Full template, stop rules, and resume checklist: [docs/WORKFLOW.md](./docs/WORKFLOW.md). Agent short card: [AGENTS.md](./AGENTS.md). Bench how-to: [docs/BENCHMARKS.md](./docs/BENCHMARKS.md).

---

## 1. Goals and non-goals

### Goals

- Shared in-cluster cache for **read-heavy** runtime storage
- **Thread-safe / multi-service-safe** access via gRPC clients and an embeddable Engine (single-node / test)
- **Automatic fetch on miss** via pluggable `DataSource` (reduce backend / DB load) in load-through keyspaces
- **Multi-node cluster** for availability and aggregate **read QPS** (not for unbounded memory growth — see §2)
- **Bounded memory** with per-keyspace LRU (`MaxBytes`) and **TTL** / optional **negative TTL**
- **Automatic node discovery** and reaction to topology changes among **cache nodes**
- **KeySpace** isolation with runtime updates, warmup, hot-key tracking, and DataSource protection
- **Structured keyspace types** (in addition to opaque KV): Bloom filters, exact sets, sorted sets — same owner + RF fan-out model
- **Ops surface**: admin diagnostics, health probes, cluster events, OpenTelemetry, hosted OpenAPI
- **Secure peer mesh**: TLS, Client vs Peer isolation, optional gossip membership auth

### Non-goals (v1)

- Linearizable or transactional workloads
- Quorum, fencing, or split-brain prevention
- Disk persistence / WAL / snapshots
- Multi-key atomicity or strict global write order (beyond per-entry LWW versioning)
- Full Redis protocol; hashes, lists, streams, geospatial, HyperLogLog, multi-key ZUNION/ZINTER
- Counters or CRDTs
- **Memory capacity that scales linearly with node count** under the v1 replication model
- Embedding every application replica as a ring peer (anti-pattern; see §4)

---

## 2. Data-plane model (decision gate 1)

### Chosen model: **replicated mesh + owner coordinator**

v1 is an **eventually consistent, multi-replica cache**:

| Concern | Behavior |
|---------|----------|
| **Memory** | Each node holds its own LRU-bounded working set. After fan-out, a key exists on **R** ring members (owner + successors), not every node. Cluster memory ≈ **R × unique working set / N** per node. |
| **Owner** | Consistent-hash **coordinator** for write ACK, preferred miss-load coalescing, and version allocation — **not** a sole storage shard. Ring owner of a **name** (KV key, Bloom/set/zset name). |
| **Reads** | Local hit if this node has a copy. CacheOnly miss **forwards to the owner** (replicas store the repair; non-replicas do not). Structured reads (BloomTest / SetContains / ZScore / range) local on replica, owner-forward otherwise. |
| **Writes** | Owner applies + assigns version → ACK → **async fan-out** to the other **R−1 replicas** (KV `ApplyPut`; structured types use item-level flags or bit OR — see §7 / §9.6). |
| **Scale-out value** | More nodes → more **unique key capacity** (at fixed R) and more read QPS; HA is R copies, not N. |

### Capacity formula (ops)

```
per_node_budget   ≈ sum(keyspace.MaxBytes) + overhead (indexes, hot-key tracker, buffers)
hot_set_cluster   ≈ size of keys that stay hot longer than fan-out + TTL dynamics
effective_hot_cost ≈ hot_set_cluster × N   (worst case: full replication of hot set)
```

**Each key costs O(R × value size)** after fan-out (`R = keyspace.ReplicationFactor`, default 3, cap N). Size `MaxBytes` for the slice of keys this node replicas; unique cluster capacity grows with N/R.

### Rejected for v1: true shard-only storage

Owner-only storage + remote Get would scale memory with N but would **break** the stated Put contract (async fan-out to every peer) and local-hit latency goals for read-heavy workloads. Revisit as v2 if capacity becomes the bottleneck.

---

## 3. Locked decisions

| Topic | Choice |
|-------|--------|
| Language | **Go** |
| Consistency | **Eventually consistent**, bounded by TTL / Delete / versioned LWW apply |
| Peer write path | Owner ACK + **async fan-out to R−1 replicas**; peer failures **logged, not retried, not returned** on Put |
| Persistence | **None** |
| Workload | **Read-heavy** |
| Client API | **gRPC** `Cache` service (KV + Bloom + Set + ZSet) + optional in-process Engine + `cmd/sc` |
| Local store | Custom versioned **LRU** (`pkg/store`) with structure caches for set/zset — **not** stock groupcache |
| Membership | **hashicorp/memberlist** (gossip); optional `WithGossipSecret` |
| Hash ring | Consistent hash with virtual nodes over **cache-node** peers only |
| v1 topology | **Only `supercache-node` processes join the ring**; apps use `pkg/client` |
| Write metadata | **Owner-assigned monotonic version** per key (see §6) |
| Security | TLS everywhere; **separate Client and Peer listeners**; Peer mTLS |

---

## 4. Deployment topology (decision gate 5)

```
┌────────────┐  ┌────────────┐  ┌────────────┐
│ App pod A  │  │ App pod B  │  │ App pod C  │
│ pkg/client │  │ pkg/client │  │ pkg/client │
└─────┬──────┘  └─────┬──────┘  └─────┬──────┘
      │ gRPC Cache    │               │
      └───────────────┼───────────────┘
                      ▼
         ┌────────────────────────┐
         │   supercache-node × N  │  ← gossip ring members only
         │   Cache :client-port   │
         │   Peer  :peer-port     │  ← mesh internal only
         │   Admin :admin-port    │
         └────────────────────────┘
```

| Mode | Who | Ring member? | Use |
|------|-----|--------------|-----|
| **Cluster node** | `cmd/supercache-node` | **Yes** | Production |
| **Client library** | `pkg/client` | **No** | Apps dial seeds / service discovery |
| **Embedded Engine** | `pkg/engine` in-process | **No** (default) | Unit tests, single-process demos via `WithSingleNode()` |

**Anti-pattern:** one ring peer per request-serving app replica (churn, ownership thrash, warmup storms, huge attack surface).

**Heterogeneous apps:** share SuperCache nodes; keyspaces and DataSources are configured **on the cache nodes** (or sidecars that *are* cache nodes), not differently on every app binary.

---

## 5. Consistency contract

**SuperCache is eventually consistent.** Fast reads with bounded staleness; not linearizable or transactional.

### Per-operation contract

| Operation | Contract |
|-----------|----------|
| **Get** | Returns a local copy if present. On CacheOnly miss, **forwards to the owner** (replica stores the result; non-replica does not). May lag other replicas or the source-of-truth. **Invalid** on ModeBloom / ModeSet / ModeZSet. |
| **Put / PutMany** | Returns once the key's **owner** has accepted the write (assigned version, local apply). Value is **async fan-out** to the other **R−1 replicas** on the ring (`ReplicationFactor`, default 3; negative = all peers). Non-replica peer failures are not contacted. Replica failures: **log + metric only** (not in Put error). **Invalid** on structured modes. |
| **Delete / DeleteMany** | Owner installs a tombstone and fans it through the **same replica apply+hint pool as Put** (`Fanout.Apply`, sync first attempt). Failed RPCs are hinted and replayed (LWW so a later delete supersedes a queued put). Returns **structured multi-error** if any replica is unreachable on the first attempt. Topology handoff uses the same pool. Applies to KV keys **and** named Bloom / set / zset entries. |
| **BloomAdd / BloomTest** | `ModeBloom` only. Add ORs bits on owner + replicas (not LWW of the bitset). Test: local if replica has the filter, else owner-forward. Missing filter → test false. No per-item delete. |
| **SetAdd / SetRemove / SetContains / SetCard / SetMembers** | `ModeSet` only. Owner serializes mutations; item-level fan-out (`FlagSetAdd` / `FlagSetRemove`). Contains/card/members: local on replica, owner-forward otherwise. Missing set → contains false, card 0, empty members. |
| **ZAdd / ZRem / ZScore / ZCard / ZRange / ZRangeByScore** | `ModeZSet` only. Same ownership pattern as ModeSet; item-level `FlagZSetAdd` / `FlagZSetRem`; score `float64` (NaN rejected). Equal scores order by member bytes. Range by Redis-style rank or inclusive score window. |
| **UpdateKeySpace / DeleteKeySpace** | **Local to the calling node.** Re-issue on every node for cluster-wide rollout. Drift is unsupported in v1; expose config generation on `/peers` for detection. |

### Read-your-writes (normative)

| Where | Guarantee |
|-------|-----------|
| **Owner node** after successful Put / SetAdd / ZAdd / BloomAdd | Local read verbs see the update immediately. |
| **Initiating client** (via gRPC) | **No** automatic same-socket RYOW unless the client dials the owner and reads there, **or** the client enables optional **client-side sticky buffer** (out of scope for v1 server). Document: *Write success means owner has the value; other nodes may lag until fan-out.* |
| **Optional v1.1** | Client library may cache last write locally for RYOW within process — not server contract. |

This **replaces** the earlier ambiguous “same node sees Put immediately” wording that conflicted with owner-only apply.

### What this means in practice

- Steady state: peers usually learn Puts within about **one fan-out RTT** under light load; **TTL** is the hard bound when fan-out fails.
- **Network partition:** each side accepts reads/writes independently; **no quorum, no fencing**. Short TTLs bound post-heal staleness; versions reduce flip-flop from delayed applies.
- **Out-of-band SoT writes:** cache keeps old value until TTL or Delete (load-through keyspaces).
- **Good fit:** read-heavy, tolerate seconds of staleness.  
- **Bad fit:** linearizable reads, counters, strict write order, sole copy of irreplaceable data.

### Staleness bounds

| Mechanism | Role |
|-----------|------|
| TTL | Primary bound on serving a value |
| Versioned LWW | Prevents older ApplyPut/Prefetch from overwriting newer data |
| LRU / MaxBytes | Memory bound; may drop before TTL |
| Delete | Best-effort invalidate |
| Negative TTL | Bounds repeated SoT misses |

---

## 6. Versioning and LWW (decision gate 3)

Every stored entry carries:

```text
Entry {
  value       []byte
  version     uint64    // owner-assigned; strictly increasing per key on that owner generation
  expire_at   int64     // unix nano absolute; 0 = no TTL
  flags       uint32    // bit0 = negative entry
  keyspace    string    // implicit in map; on wire explicit
}
```

### Version allocation

- On Put, **owner** sets `version = max(prev, last_issued)+1` for that key (in-memory counter per key or per keyspace with care).
- On owner change, new owner starts from `max(observed versions for key, 1)` when it first sees the key; delayed applies from old owners are rejected if `version <= local`.
- **Do not** use wall clock as the sole LWW key (skew). Wall clock may still set `expire_at = now + ttl` at apply time on each peer (**receive-time + remaining TTL** on fan-out is simpler and skew-tolerant):
  - **Normative:** wire carries `ttl_nanos` and/or absolute `expire_at` from owner; peers store owner’s `expire_at` so expiry is aligned cluster-wide when clocks are roughly OK; document short TTLs still required.

### Apply rules (all peers, including owner)

```
ApplyPut(entry):
  if local missing OR entry.version > local.version → store entry
  else → ignore (metric: apply_stale)

ApplyDelete(key, delete_version):
  if delete_version < local.version → ignore
  else → install versioned tombstone (required; never a bare remove)

Prefetch / DataSource fill:
  must go through same version assignment on owner (or use version 0 fill only if absent — see modes)
```

**Tombstones (v1, required):** every accepted Delete / ApplyDelete installs a versioned marker so a delayed ApplyPut cannot resurrect the key. LRU must not evict an unexpired tombstone (MaxBytes may overshoot). Marker lifetime is `keyspace.Config.TombstoneTTL` (0 → `DefaultTombstoneTTL` 5m; negative → never expire). After expiry a late Put may land; the key’s own TTL still bounds any value.

### Concurrent writers

Last **owner-accepted** Put with highest version wins cluster-wide **as fan-outs land**. Temporary divergence is allowed; versions make convergence **monotonic**, not random last-apply.

---

## 7. Keyspace modes (decision gate 4)

Each keyspace has a mode. Opaque KV modes use Get/Put/Delete. Structured modes reject Get/Put and expose type-specific verbs (see `docs/API.md`).

### `LoadThrough` (default for backend offload)

- Get miss → load via `DataSource` (owner-coalesced; see §9.1)
- Put allowed as **explicit override / warm**; gets a new version like any Put
- After eviction of a Put value, miss may reload SoT (**Put is not durable**)
- Out-of-band SoT change: caller must **Delete** or wait TTL
- `DataSource` **required**

### `CacheOnly`

- No DataSource (or never consulted)
- Get miss → not found (no load)
- Put / Delete are the only mutators
- Eviction / TTL → data gone (acceptable; no persistence)

### Mode summary (product surface)

| Mode | Ring identity | App verbs | Wire flags (peer ApplyPut) |
|------|---------------|-----------|----------------------------|
| `LoadThrough` / `CacheOnly` | KV key | Get, Put, Delete (+ batch) | value entry / negative |
| `ModeBloom` | filter `name` | BloomAdd, BloomTest; Delete(name) | `FlagBloom` snapshot, item-add OR path |
| `ModeSet` | set `name` | SetAdd, SetRemove, SetContains, SetCard, SetMembers; Delete(name) | `FlagSet`, `FlagSetAdd`, `FlagSetRemove` |
| `ModeZSet` | zset `name` | ZAdd, ZRem, ZScore, ZCard, ZRange, ZRangeByScore; Delete(name) | `FlagZSet`, `FlagZSetAdd`, `FlagZSetRem` |

Public reference: [docs/API.md](./docs/API.md), OpenAPI `api/openapi/cache.openapi.yaml`, proto `api/proto/cache.proto`.

### `ModeBloom` (approximate membership)

- Named filter per key (`name`); `BloomAdd` / `BloomTest`
- Bit OR on add (not LWW of the whole bitset); no per-item delete
- `Delete(name)` tombstones the filter; handoff **OR-merges** bitsets
- Config: `BloomBits` / `BloomHashes` (defaults in `pkg/keyspace`); bitset size must fit `MaxBytes`
- Package: `pkg/bloom`
- Design: [docs/design/2026-08-11-bloom-filter.md](./docs/design/2026-08-11-bloom-filter.md)

### `ModeSet` (exact membership)

- Named set; `SetAdd` / `SetRemove` / `SetContains` / `SetCard` / `SetMembers`
- Owner-serialized writes; item-level fan-out; versioned **full-set** handoff snapshot
- Encode: length-prefixed members; store keeps live set cache + lazy encode on Peek/handoff
- Empty set after last remove may remain until `Delete(name)` (Card 0)
- Package: `pkg/set`
- Design: [docs/design/2026-08-13-mode-set.md](./docs/design/2026-08-13-mode-set.md)

### `ModeZSet` (sorted set)

- Named zset; `ZAdd` / `ZRem` / `ZScore` / `ZCard` / `ZRange` / `ZRangeByScore`
- Score is IEEE-754 `float64` (NaN → invalid argument; ±Inf allowed)
- Equal scores ordered by raw **member** byte order (Redis-like)
- `ZRange` ranks: 0-based inclusive, negative indices like Redis (`-1` = last)
- `ZRangeByScore`: inclusive `[min, max]`, ascending
- Owner-serialized writes; item-level fan-out; versioned **full zset** handoff
- Encode: sorted records of `float64 LE score` + uvarint(len) + member
- Package: `pkg/zset`; CLI: `sc zadd|zrem|zscore|zcard|zrange|zrangebyscore`
- Design: [docs/design/2026-08-13-mode-zset.md](./docs/design/2026-08-13-mode-zset.md)

### Precedence matrix

| Incoming | vs local | Result |
|----------|----------|--------|
| Put / ApplyPut higher version | any | accept |
| Put / ApplyPut lower/equal version | any | reject |
| Load fill (LoadThrough) | missing | accept with **new owner version** then fan-out |
| Load fill | present non-expired | **do not** overwrite (hit path shouldn’t load) |
| Prefetch | same as load fill rules |
| Negative entry | higher version / miss path | store negative; **Put always overrides** negative with higher version |
| ApplyDelete adequate version | present or missing | install tombstone |
| FlagSetAdd / FlagZSetAdd / Bloom item-add | structure present | mutate under mutex; version gate |
| FlagSet / FlagZSet / FlagBloom snapshot | structure or tombstone | install if version ≥ local (tombstone blocks stale) |

---

## 8. High-level architecture

```
                 App services
                 pkg/client · cmd/sc (not in ring)
                         │
                    gRPC Cache :client
                    (KV · Bloom · Set · ZSet)
                         ▼
              ┌─────────────────────┐
              │  supercache-node    │
              │       Engine        │
              │  KeySpaces · Events │
              └──────────┬──────────┘
                         │
        ┌────────────────┼────────────────┐
        ▼                ▼                ▼
  Local store      Owner routing     Peer mesh
  (LRU + TTL/      (hash ring)       gRPC Peer :peer
   version +                         async fan-out
   set/zset/bloom                    (value / item flags)
   caches)
        │                │                │
        ▼                ▼                ▼
  DataSource ◄── protect          memberlist gossip
  (LoadThrough only)
        │
  Admin HTTP · /docs · OpenTelemetry
```

---

## 9. Normative request paths

### 9.1 Get (v1 normative)

```
Get(keyspace, key) on node N:
  1. local := store.Get(ks, key)
  2. if local present and not expired:
       if negative → return NotFound
       else → return value                    // HIT
  3. if keyspace.mode == CacheOnly:
       return NotFound
  4. // LoadThrough miss
     singleflight(ks, key):
       owner := ring.Owner(key)
       if owner != self:
         // Prefer owner-coalesced load (cluster singleflight)
         resp, err := peer.GetOrLoad(owner, ks, key)  // owner runs steps 5–7
         if err == nil:
           // optional: do not require local store; client got value from owner path
           // v1: cache locally with same version/expiry from response (no new version)
           store.AcceptIfNewer(resp.entry)
           return resp
         // owner down / error → fall through to local load once
       // owner == self OR fallback:
       5. protect.Allow(ks) or return Unavailable
       6. val, err := DataSource.Load(ctx, key)
          if err == NotFound → store negative (owner assigns version) → fan-out ApplyPut(negative) → return NotFound
          if err != nil → return err
       7. entry := owner.AssignVersion(val, ttl); store; async FanoutApplyPut(entry); return val
```

**Owner down:** non-owner may load locally (availability over singleflight purity); still assigns version only if it becomes… **v1 simplification:** on owner-down fallback, node loads and stores **locally only** without fan-out **or** temporarily acts as loader with version from `time`/`local counter` tagged with `loader_id` — **normative v1:** local-only fill on owner-down (no fan-out), metric `get_owner_fallback_local`; rely on TTL and later owner Put/load for repair.

### 9.2 Put / PutMany

```
Put(ks, key, val, ttl):
  1. owner := ring.Owner(key)
  2. if self != owner → return peer.ForwardPut(owner, ...)
  3. entry := {val, version: next(key), expire_at, flags:0}
  4. store.Set(entry)
  5. async fanout:
       for peer in ring.PeersExceptSelf():
         workerPool.Go: ApplyPut(peer, entry)  // timeout T; on err metric; no retry
  6. return OK

PutMany: group keys by owner; per-key independent success; max batch size B (default 100);
         return per-key errors; not atomic.
```

### 9.3 Delete / DeleteMany

```
Delete(ks, key):
  1. owner := ring.Owner(key)  // still used to mint delete_version
  2. delete_version := owner.NextVersion(key)  // or max(local, known)+1 via forward to owner
  3. results := parallel ApplyDelete(ks, key, delete_version) to owner + all peers
     (owner applies locally if self)
  4. if any failure → return MultiError{PeerErrors...}
  5. else → OK

// Document: success = all currently known peers ACKed ApplyDelete.
// Not a guarantee against concurrent Put with higher version racing in.
```

### 9.4 Topology change

```
1. memberlist join/leave/update
2. recompute ring; bump ring_generation
3. emit Events
4. warmup: bounded workers; version-checked AcceptIfNewer only
5. in-flight fan-out uses membership snapshot at schedule time; include ring_generation on peer RPC
```

Ownership change does **not** migrate bytes; refill via Get miss, Put, or warmup.

### 9.5 UpdateKeySpace / DeleteKeySpace

Local only; validate; atomic swap config; if MaxBytes shrinks, rely on store cost eviction.  
`/peers` and metrics expose `keyspace_config_hash` for drift detection. Cluster rollout = ops re-issue.

### 9.6 Structured types (Bloom / Set / ZSet)

Normative shape shared by Set and ZSet (Bloom differs only in mutate semantics):

```
Mutate (SetAdd / SetRemove / ZAdd / ZRem / BloomAdd) on node N:
  1. reject if keyspace.mode wrong or member/score invalid
  2. owner := ring.Owner(name)
  3. if self != owner → peer.ApplyPut(owner, name, Flag*Add|Rem, payload)
  4. else: nextVersion(name); store mutate under mutex; async fan-out Flag* to R−1
  5. return OK  // fan-out failures: metric/hint (like Put)

Read (SetContains / ZScore / ZCard / ZRange* / BloomTest):
  1. if local live structure → answer from store cache
  2. else if owner self → missing → empty/false
  3. else → peer.GetOrLoad(owner) structure snapshot; optional install if holdsReplica
  4. decode / answer

Handoff: include structure entries in LocalEntries; ApplyPut FlagSet|FlagZSet|FlagBloom
         if incoming version ≥ local (tombstone still blocks stale).
```

Bloom **add** ORs bits (no full-blob LWW on item add). Set/ZSet **item** ops apply under version gates; concurrent ZAdd same member → last owner write wins for that score.

---

## 10. Local store (decision gate 2)

### Choice: **custom versioned LRU** (`pkg/store`) — shipped

| Need | Approach |
|------|----------|
| MaxBytes | cost-based eviction; tombstones / Bloom / Set / ZSet protected while live |
| TTL | `expire_at` on entry; lazy expire |
| Set / Delete | First-class AcceptIfNewer / DeleteIfVersion |
| Negative | Envelope flag |
| Concurrent | Store mutex |
| ModeSet / ModeZSet | Live `setCache` / `zCache` + dirty lazy encode |
| ModeBloom | Bitset in entry value; in-place OR under mutex |

**Not using stock golang/groupcache** for distribution or primary API (get-only, HTTP peers, no Put/Delete/TTL).

```go
type Store interface {
    Get(key string) (Entry, bool)
    Peek(key string) (Entry, bool)
    AcceptIfNewer(key string, e Entry) bool
    // Bloom / Set / ZSet methods (see pkg/store)
    // metrics: items, cost, hits, misses, evictions
}
```

---

## 11. Engine and KeySpace

### Engine API

```go
type Engine interface {
    // KV — ModeCacheOnly / ModeLoadThrough
    Get(ctx context.Context, keyspace, key string) ([]byte, error)
    Put(ctx context.Context, keyspace, key string, value []byte, opts ...PutOption) error
    PutMany(ctx context.Context, keyspace string, kvs []KV, opts ...PutOption) error
    Delete(ctx context.Context, keyspace, key string) error
    DeleteMany(ctx context.Context, keyspace string, keys []string) error

    // ModeBloom
    BloomAdd(ctx context.Context, keyspace, name string, item []byte) error
    BloomTest(ctx context.Context, keyspace, name string, item []byte) (bool, error)

    // ModeSet
    SetAdd(ctx context.Context, keyspace, name string, item []byte) error
    SetRemove(ctx context.Context, keyspace, name string, item []byte) error
    SetContains(ctx context.Context, keyspace, name string, item []byte) (bool, error)
    SetCard(ctx context.Context, keyspace, name string) (int, error)
    SetMembers(ctx context.Context, keyspace, name string) ([][]byte, error)

    // ModeZSet
    ZAdd(ctx context.Context, keyspace, name string, member []byte, score float64) error
    ZRem(ctx context.Context, keyspace, name string, member []byte) error
    ZScore(ctx context.Context, keyspace, name string, member []byte) (float64, bool, error)
    ZCard(ctx context.Context, keyspace, name string) (int, error)
    ZRange(ctx context.Context, keyspace, name string, start, stop int) ([]ZMember, error)
    ZRangeByScore(ctx context.Context, keyspace, name string, min, max float64) ([]ZMember, error)

    UpdateKeySpace(cfg KeySpaceConfig) error
    DeleteKeySpace(name string) error
    Events() <-chan ClusterEvent
}

type PutOption func(*putConfig) // WithTTL(d time.Duration)

type PeerError struct {
    PeerID string
    Op     string
    Err    error
}

type MultiError struct {
    Errors []PeerError
}
func (m *MultiError) Error() string
func (m *MultiError) Unwrap() []error
```

### KeySpaceConfig (sketch)

```go
type KeySpaceMode int
const (
    ModeLoadThrough KeySpaceMode = iota
    ModeCacheOnly
    ModeBloom
    ModeSet
    ModeZSet
)

type KeySpaceConfig struct {
    Name           string
    Mode           KeySpaceMode
    TTL            time.Duration
    NegativeTTL    time.Duration // 0 = disabled; ignored for structure modes
    TombstoneTTL   time.Duration // 0 = 5m; negative = never expire
    MaxBytes       int64
    LoadTimeout    time.Duration
    PeerTimeout    time.Duration
    WarmKeys       []string
    RefreshInterval time.Duration // 0 = disabled refresh-ahead
    RateLimitRPS   float64        // 0 = use global
    CircuitBreaker CircuitConfig
    DataSource     DataSource     // required if LoadThrough
    ReplicationFactor int         // 0 = default 3; negative = all peers
    BloomBits      int            // ModeBloom; 0 = default
    BloomHashes    int            // ModeBloom; 0 = default
}
```

---

## 12. Feature catalog

| Feature | Placement |
|---------|-----------|
| Automatic fetch on miss | LoadThrough Get path §9.1 |
| Multi-node availability + read scale | Replicated mesh §2 |
| Reduced backend load | Local hits, owner singleflight, negative TTL, protect |
| TTL + LRU + MaxBytes + negative TTL | Store envelope + cost eviction |
| ModeBloom / ModeSet / ModeZSet | §7, §9.6; `pkg/bloom`, `pkg/set`, `pkg/zset` |
| Node discovery | memberlist among supercache-nodes |
| KeySpace overrides | KeySpaceConfig |
| Dynamic keyspace updates | Local UpdateKeySpace + config hash |
| Warmup / hot keys / refresh-ahead | warmup pkg; version-checked apply |
| DataSource protection | protect pkg global + per-KS |
| Cluster events | Engine.Events buffered; drop-oldest on slow consumer |
| Admin diagnostics + OpenAPI docs | admin HTTP `/docs`; GitHub Pages |
| OTel | telemetry pkg |
| TLS + gossip auth | transport + memberlist secret |
| CLI | `cmd/sc` get/put/del, bloom, z* |

---

## 13. Fan-out operability (scalability)

| Control | v1 default |
|---------|------------|
| Worker pool | fixed size (e.g. 64) or `min(64, 8*N)` |
| Per-peer timeout | KeySpace `PeerTimeout` (e.g. 100–500ms) |
| Max in-flight ApplyPuts | bound queue (e.g. 10k); on overflow drop + metric `fanout_dropped` (still no retry) |
| Batching | optional `ApplyPutMany` later; v1 unary OK |
| ring_generation | sent on peer RPC; peer may accept applies regardless (cache) but metrics on mismatch |
| Delete | same pool; gather errors into MultiError |

**Write scalability:** O(N) RPCs per Put — acceptable for moderate N (e.g. 3–15 nodes). Document upper design target; not infinite horizontal write scale.

---

## 14. Wire format, limits, APIs

### Limits (v1 defaults)

| Limit | Default | Notes |
|-------|---------|-------|
| Max key length | 512 bytes | reject larger |
| Max value size | 1 MiB | reject larger |
| PutMany/DeleteMany batch | 100 keys | |
| Hot-key top-K per keyspace | 1024 | fixed memory |
| Events channel buffer | 64 | drop-oldest |

### gRPC

**Client listener** (`Cache` only) — see `api/proto/cache.proto` and OpenAPI `api/openapi/cache.openapi.yaml`:

```text
service Cache {
  // KV
  rpc Get(GetRequest) returns (GetResponse);
  rpc Put(PutRequest) returns (PutResponse);
  rpc PutMany(PutManyRequest) returns (PutManyResponse);
  rpc Delete(DeleteRequest) returns (DeleteResponse);
  rpc DeleteMany(DeleteManyRequest) returns (DeleteManyResponse);
  // ModeBloom
  rpc BloomAdd(BloomAddRequest) returns (BloomAddResponse);
  rpc BloomTest(BloomTestRequest) returns (BloomTestResponse);
  // ModeSet
  rpc SetAdd(SetAddRequest) returns (SetAddResponse);
  rpc SetRemove(SetRemoveRequest) returns (SetRemoveResponse);
  rpc SetContains(SetContainsRequest) returns (SetContainsResponse);
  rpc SetCard(SetCardRequest) returns (SetCardResponse);
  rpc SetMembers(SetMembersRequest) returns (SetMembersResponse);
  // ModeZSet
  rpc ZAdd(ZAddRequest) returns (ZAddResponse);
  rpc ZRem(ZRemRequest) returns (ZRemResponse);
  rpc ZScore(ZScoreRequest) returns (ZScoreResponse);
  rpc ZCard(ZCardRequest) returns (ZCardResponse);
  rpc ZRange(ZRangeRequest) returns (ZRangeResponse);
  rpc ZRangeByScore(ZRangeByScoreRequest) returns (ZRangeResponse);
}

// Messages carry: keyspace, key/name, value/item/member, score, start/stop,
// and on internal paths: version, expire_at_unix_nano, flags, ring_generation
// DeleteResponse / DeleteManyResponse: repeated PeerFailure { peer_id, message }
// PutManyResponse: repeated KeyError { key, message }  // partial OK
// ZMember { bytes member; double score }
```

**Peer listener** (internal only — **never** same port/credentials as public Cache):

```text
service Peer {
  rpc ApplyPut(ApplyPutRequest) returns (ApplyPutResponse);
  rpc ApplyDelete(ApplyDeleteRequest) returns (ApplyDeleteResponse);
  rpc GetOrLoad(GetOrLoadRequest) returns (GetOrLoadResponse);
  rpc ForwardPut(PutRequest) returns (PutResponse);
  rpc Prefetch(PrefetchRequest) returns (PrefetchResponse);
}
```

### Admin HTTP

| Endpoint | Purpose |
|----------|---------|
| `GET /healthz` | Process up |
| `GET /readyz` | gRPC servers up; memberlist running; (single-node: ready with ring size 1) |
| `GET /peers` | Peers, ring_generation, keyspace_config_hash |
| `GET /keyspaces` | Stats: bytes, items, hits, misses, breaker, hot keys, fan-out errors/drops |

---

## 15. Security

| Control | v1 requirement |
|---------|----------------|
| TLS client↔node | Yes (configurable disable only for local dev) |
| TLS peer↔peer | Yes |
| **Separate ports** | Client / Peer / Admin |
| Peer auth | **mTLS** (mesh certs); reject Cache credentials on Peer port |
| Client auth | Optional mTLS or static API token in v1; network policy assumed in k8s |
| Gossip | Optional `WithGossipSecret`; document rotation = rolling restart with dual secrets if needed later |
| Admin | Bind localhost / private network; optional token |

Threat: client that can reach Peer port can poison cache — **mitigated by port split + mTLS**.

---

## 16. Warmup and hot keys

- Approximate top-K / sampled hits per keyspace (bounded)
- On join/leave: bounded concurrent prefetch of WarmKeys + hot keys
- Refresh-ahead if `RefreshInterval > 0` and key still hot
- All fills use DataSource protections and **version rules** (never clobber newer Put)
- Hot-key sketches are **local**; joiners start cold (acceptable v1)

---

## 17. Observability

### Traces

`engine.Get|Put|Delete`, `peer.ApplyPut|ApplyDelete|GetOrLoad`, `datasource.Load`, `warmup.Prefetch`

### Metrics (minimum)

- hits / misses / negative_hits
- get/put/delete latency
- datasource latency / errors
- `put_fanout_errors`, `fanout_dropped`, `apply_stale`
- `delete_peer_errors`
- store: evictions, cost_used, item_count
- breaker state, ratelimit_drops
- `ring_generation`, peer_count
- `get_owner_fallback_local`
- warmup counts

### SLO-oriented signals

- Fan-out error ratio
- Fan-out queue depth
- Owner Put latency p99

---

## 18. Failure and partition matrix

| Scenario | Behavior |
|----------|----------|
| Steady state | Owner Put → async fan-out; Gets mostly fresh after ~RTT |
| Fan-out fail | Owner fresh; failed peers stale until TTL / Delete / newer Put |
| Partition | Independent accepts; versions reduce delayed overwrite chaos |
| Heal | No merge protocol; LWW + TTL + Delete converge |
| SoT out-of-band (LoadThrough) | Stale until TTL or Delete |
| Peer down on Delete | MultiError; that peer may serve key |
| Owner down on Put | Put fails (no ACK authority) |
| Owner down on Get miss | Local-only fill fallback (LoadThrough); no fan-out |
| Backend down | Breaker open; fail fast |
| Config drift across nodes | Unsupported; detect via config hash metric |

---

## 19. Package layout

```text
github.com/Code0987/supercache/
  cmd/supercache-node/
  api/proto/                 // Cache + Peer
  pkg/engine/
  pkg/keyspace/
  pkg/store/                 // Store interface + Ristretto impl
  pkg/datasource/
  pkg/protect/
  pkg/membership/            // memberlist wrapper, ring
  pkg/client/                // apps import this
  pkg/warmup/
  pkg/admin/
  pkg/telemetry/
  internal/peer/             // Peer service impl (not for apps)
  internal/ring/
```

---

## 20. Alternatives considered

| Area | Options | v1 choice | Why |
|------|---------|-----------|-----|
| Local cache | groupcache, mailgun/groupcache, Ristretto, freecache, bigcache, custom | **Ristretto** (+ Store interface) | Put/Delete/TTL; avoid get-only + dual peer stacks |
| Distribution | Redis cluster, groupcache peers, true shard, full mesh replicate | **Full mesh replicate + owner coord** | Matches Put fan-out contract + local read hits; memory trade-off documented |
| Membership | K8s endpoints only, serf, memberlist, etcd | **memberlist** | Decoupled from K8s; standard gossip |
| Consistency | Quorum, linearizable owner reads, pure TTL no versions | **EC + version LWW** | Keeps no-quorum product goal; fixes multi-writer/prefetch races |
| Topology | Peer-per-app-pod vs dedicated nodes | **Dedicated supercache-node** | Churn and security |
| Embed vs sidecar | Library-only mesh | **Client lib + nodes** | Clear ops boundary |

---

## 21. Implementation milestones

| # | Milestone | Deliverables |
|---|-----------|--------------|
| 1 | Store + single-node Engine | `Store` interface, Ristretto impl, envelope (version/ttl/negative), KeySpace modes, Get/Put/Delete local, LoadThrough singleflight, limits, unit tests |
| 2 | Protections + OTel + admin | rate limit, breaker, traces/metrics, `/healthz` `/readyz` `/keyspaces` |
| 3 | Membership + ring + Put fan-out | memberlist, ring, owner ForwardPut, ApplyPut + versions, worker pool, TLS + port split, gossip secret |
| 4 | Cluster Get/Delete | GetOrLoad, Delete multi-peer MultiError, owner-down fallback |
| 5 | Warmup + events | hot keys, join/leave prefetch, refresh-ahead, Events |
| 6 | Polish | UpdateKeySpace docs + config hash, PutMany/DeleteMany, load/chaos tests, client RYOW docs |

Milestone 1 does **not** depend on groupcache.

---

## 22. Fit statement (user-facing)

> SuperCache is a Go, gRPC, gossip-clustered, keyspace-oriented cache with a bounded local store (Ristretto), load-through DataSources, and async multi-peer fan-out. Dedicated cache nodes form the mesh; applications use a client library. It optimizes **read latency and backend offload** for workloads that tolerate **seconds of staleness**. Memory is **per-node LRU-bounded** and hot data is **replicated**, not sharded for capacity. It is **not** a coordination system, durable source of truth, or linearizable store.

---

## 23. Anti-patterns

Do **not** use SuperCache for:

- Leader election or distributed locks requiring linearizability
- Financial balances / counters that must not double-apply
- Sole copy of irreplaceable data
- Cross-key transactions
- Assuming one `UpdateKeySpace` updates the whole cluster
- Expecting cluster RAM capacity ≈ N × single-node for the **same** hot working set without budgeting **R×** replication
- Running every app replica as a gossip peer

---

## 24. Success criteria

- Race-free concurrent Get/Put under `go test -race`
- Singleflight: one DataSource load per key under stampede (single owner path)
- Memory respects MaxBytes under load
- Ring updates on join/leave; Events fire
- Put succeeds when owner up even if some fan-out targets fail (`fanout_errors` increments)
- Stale ApplyPut with lower version does not overwrite newer value
- Delete returns MultiError when a peer is down
- TTL expires entries in tests; negative TTL suppresses repeat loads
- Peer port rejects unauthenticated clients in integration test
- OTel metrics include hits, misses, load latency, fan-out errors

---

## 25. Review resolution map

| Architect issue | Resolution in this plan |
|-----------------|-------------------------|
| Sharding vs fan-out | §2 replicated mesh; capacity formula; non-goal linear memory scale |
| RYOW contradiction | §5 owner-only immediate visibility; client lag documented |
| No versioning | §6 owner versions + LWW apply rules |
| groupcache unfit | §10 custom LRU Store interface |
| DataSource vs Put | §7 LoadThrough vs CacheOnly + precedence |
| Structured types | §7 ModeBloom / ModeSet / ModeZSet; §9.6; §11 Engine API; §14 Cache RPCs |
| Library vs peers | §4 nodes-only ring; pkg/client for apps |
| Multi-error / PutMany | §11 MultiError; §9.2–9.3 batch rules |
| Get miss underspecified | §9.1 normative algorithm |
| Limits / codec | §6 Entry; §14 limits |
| Security Peer/Client | §15 separate ports + Peer mTLS |
| Fan-out backpressure | §13 worker pool + queue bounds |
| Alternatives | §20 |
| Milestones invent design | §21 store-first; pinned deps |
| Warmup races | version-checked apply §6 §16 |

---

## 26. Related process docs

| Doc | Role |
|-----|------|
| [docs/WORKFLOW.md](./docs/WORKFLOW.md) | Normative change path (design → TDD → PR → bench → merge) |
| [AGENTS.md](./AGENTS.md) | Short agent rules + 10% drift bar |
| [docs/BENCHMARKS.md](./docs/BENCHMARKS.md) | How to read CI and local benches |
| [docs/design/](./docs/design/) | Per-change design drafts (approved before code) |

---

*End of plan. Implement against this document unless a later revision supersedes it. Process changes go through §0 / WORKFLOW.md first.*
