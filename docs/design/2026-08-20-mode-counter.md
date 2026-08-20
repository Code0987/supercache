# Counter type (`ModeCounter`) + fixed-window rate-limiter example

**Status:** approved  
**Branch:** `feat/mode-counter`  
**Author:** —  
**Date:** 2026-08-20

## Problem

SuperCache has no **named int64 counter** whose increment returns the new value. A rate limiter needs that: `n, err := Incr(ks, name, 1); if n > limit { deny }`.

Nothing shipped is a counter:

| Mode | Why not a counter |
|------|-------------------|
| CacheOnly blob | App read-modify-write; concurrent incrs lost |
| Hash field | `HINCRBY` excluded — increments are **not commutative** under lost hints ([ModeHash](./2026-08-20-mode-hash.md)) |
| ZSet score | Ranking, not a returned incr |
| List | Sequence, not a number |

`Incr(+1)` then `Incr(+1)` ≠ last `Incr(+1)` alone. Same class as List, not Hash. Today `internal/peer/hints.go` `hintID` for non-`FlagBloomAdd` is `(ks, name)`: a second mutate of the same name **replaces** the pending hint. Item-level `FlagCounterIncr` to replicas would drop earlier deltas. The replica would install only the last `+1` and the count would be wrong.

`ApplyPut` is ACK-only (`applied bool`). It cannot return the post-incr int64. List already solved “op must return a value” with peer `ListPop` (`api/proto/peer.proto`).

PLAN.md §1 currently lists **“Counters or CRDTs”** as a v1 non-goal. This design **moves named int64 counters out of that non-goal**. CRDTs stay out.

## Non-goals

- GETSET, compare-and-swap, float counters
- `HINCRBY` / `HINCRBYFLOAT` (Hash stays last-write-wins per field)
- HyperLogLog, CRDT PN-counters, Redis `INCR` overflow wrap
- Sliding-window or token-bucket limiter in the engine
- Per-delta TTL / `WithIncrTTL` (see [TTL / window](#ttl--window-locked-scheme-b))
- Blocking / wait-for-reset
- Changing RF, tombstones, **hint identity** (`hintID` stays `(ks, name)` except `FlagBloomAdd`), or existing Get/Put/Delete / Bloom / Set / ZSet / Geo / List / Hash contracts
- Persistence across process restart
- A new default node demo keyspace (`-demo-keyspace` stays `demo` / `tags` / `board` / `profile`)
- scbench YAML cells in v1
- Token bucket in the example (needs tokens+timestamp atomically — not a single int64)

## Why not item-level fan-out (locked)

Increments are **not commutative** under the current hint scheme. If a replica is down for `Incr(+1)` then `Incr(+1)`, a pending `FlagCounterIncr(+1)` hint is **only the last delta**. Replay would leave the replica at `+1` while the owner is at `+2`.

**Do not change `hintID`.** Actual identity in `internal/peer/hints.go`:

```go
func hintID(ks, key string, ent store.Entry) string {
    if ent.IsBloomAdd() {
        return ks + "\x00" + key + "\x00" + string(ent.Value)
    }
    return ks + "\x00" + key
}
```

**v1 wire:** every owner `Incr` fans out a **full counter snapshot** (`FlagCounter` = 8-byte little-endian int64 + version). Hint coalesce keeps the newest count — that is correct.

Item-level counter ops would need a hintID change (version or payload). That is a **separate** design; not this PR.

Rejected for v1: op-log + snapshot-on-gap; `FlagCounterIncr` on the replica wire.

## TTL / window (locked: scheme B)

Every shipped structure mutate **slides** `ExpireAt` from keyspace `TTL` (`store` `*MutateLocked`: `if expireAt != 0 { it.entry.ExpireAt = expireAt }`). Create-only expire is **not** a one-liner — Hash/Geo/List all refresh on every mutate.

**Lock B:**

- Keyspace `TTL` slides on every owner `Incr` (match Geo/Hash).
- No `WithIncrTTL` / create-only `ExpireAt` in v1.
- Fixed-window rate limits put the **window id in the name** so each window is a new key:

```go
secs := int64(window / time.Second) // must be >= 1; see Allow
name := fmt.Sprintf("%s:%d", client, time.Now().Unix()/secs)
```

- Example keyspace `TTL` is **≥ 2× window** so a live window cannot expire mid-bucket (TTL < window would reset the count early). Slack also keeps the previous bucket around briefly for `cget`.
- After TTL/lazy expire, the next `Incr` **creates a new counter at `delta`**. It does **not** resume the expired value.

Scheme A (Redis-like EXPIRE only on first INCR) is deferred. Sliding window is **not** v1.

## Contract

### API (new; existing verbs unchanged)

On `pkg/engine.Engine`, `pkg/client.Client`, Cache proto, and **`cmd/sc`** (same PR as the type **and** the example):

| Call | Meaning |
|------|---------|
| `Incr(ks, name, delta int64) (int64, error)` | Add `delta`; create if missing at `delta`; return **new** value |
| `CounterGet(ks, name) (int64, ok bool, err error)` | Missing → `0`, `ok=false`, nil error |
| `Delete(ks, name)` | Existing Delete: whole-counter tombstone |

```go
// Engine / client — no IncrOption / WithIncrTTL in v1 (scheme B).
Incr(ctx context.Context, keyspace, name string, delta int64) (int64, error)
CounterGet(ctx context.Context, keyspace, name string) (int64, bool, error)
```

- `name` is the counter identity; ring owner = `Owner(name)`. Validated with `validateKey` + `validateKeyLen` (per-keyspace `MaxKeyLen` applies to **name**).
- One `Incr` with `delta`. No separate `IncrBy` RPC.
- `Incr(0)` is **allowed**: returns the current value; creates `0` if missing (idempotent touch / create). Slides TTL like any other Incr.
- Negative `delta` is allowed (decrement). Crossing or landing on `0` is fine.
- **Overflow:** `int64` add that overflows → `ErrInvalidArgument`. Do **not** wrap. Entry unchanged.
- Get/Put on `ModeCounter` → `ErrInvalidArgument` (`use CounterGet` / `use Incr`).
- Counter verbs on non-`ModeCounter` → `ErrInvalidArgument`.
- Cross-mode misuse (Incr on ModeHash, HSet on ModeCounter, …) → `ErrInvalidArgument`.

**Not in v1:** GETSET, CAS, float, HINCRBY, HLL, CRDT, wrap-on-overflow, `WithIncrTTL`.

### Empty-until-delete

A counter at `0` after decrements **stays** until `Delete(name)` (same spirit as empty hash/list). `CounterGet` of a live `0` is `0, ok=true`. Missing is `0, ok=false`.

### Keyspace

New mode `keyspace.ModeCounter` (next iota after `ModeHash`). `Mode.String()` must return `"Counter"` (else `/keyspaces` prints `Mode(8)`).

| Field | Role |
|-------|------|
| `MaxBytes` | Cost budget. Live `FlagCounter` is LRU-protected (tiny: 8 + envelope). One protected counter may **overshoot** (same as Set/Geo/List/Hash) |
| `TTL` | Expires the **whole** counter; owner `Incr` **slides** `ExpireAt` (scheme B) |
| `NegativeTTL` | Ignored |
| `TombstoneTTL` | On `Delete(name)` |
| `ReplicationFactor` | Same as KV / ModeList |
| `MaxKeyLen` | Bounds `name` |
| `MaxValueSize` | Unused (payload is always 8 bytes) |

No extra counter-size knob.

### Replication (locked: snapshot like List, not item-apply like Hash)

| Hop | Payload |
|-----|---------|
| Client → any node → **owner** | peer **`CounterIncr`** (`delta`) if this node is not owner; owner applies the op |
| Owner → **replicas** | `FlagCounter` snapshot (8-byte LE int64 + version + expire) |
| Handoff / `GetOrLoad` | `FlagCounter` install if incoming version **>** local (equal = ignore; tombstone blocks if incoming ≤ tombstone) |

There is **no** `FlagCounterIncr` on the wire. Replicas **install** the int64; they do not add deltas.

**Owner-inbox:** a non-owner must **not** `ApplyPut` a delta (ApplyPut cannot return the new value). Non-owner `Incr` → `Transport.CounterIncr(addr, ks, name, delta) (int64, error)` modeled on `Transport.ListPop`. Owner `Engine.Incr` applies, snapshot-fans-out, returns value.

**Client-hit-owner:** `cNextVersion` (`PeekVersion`) + store `CIncr` + `replicate` `FlagCounter` snapshot at that version + return value.

**ApplyPut `FlagCounter`:** `CInstall` version gate (incoming `>` local; tombstone blocks). Same shape as `HInstall` / `LInstall`, not Hash item-apply.

**Reads (locked: same as `GeoPos` / `hFetchOwner`):**

- If `HasCounter` local → **store only**. Replica snapshot may **lag**. Do not owner-forward a local hit.
- Else if this node is owner → missing → `0, ok=false, nil`.
- Else → `cFetchOwner` (copy of `hFetchOwner` / `lFetchOwner`):
  - `GetOrLoad` RPC error / owner down / `!Found` / `!IsCounter` / `Decode` fail → **miss** (`0, false, nil`). Do **not** return `ErrUnavailable` on `CounterGet` (that is locked only for **Incr**).
  - `holdsReplica` → `CInstall` then answer from the installed/decoded value.
  - Non-replica → decode `ent.Value` in process; **do not** `CInstall`.

**`Incr` is always owner-serialized.** The returned `n` is the owner’s count (authoritative). A rate limiter **must `Incr`** (any node is fine — Engine forwards to the owner). Do **not** implement allow/deny with replica-local `CounterGet` + 1.

Owner down on `Incr` → `ErrUnavailable` (no ACK authority), same as Put. Do not apply locally.

**Hint-after-down (normative):** exactly **two** `Incr(+1)` while replica B’s Peer is down; then start B and wait for hint flush. **No third Incr** after listen (List’s extra `RPush("z")` would make the count 3). The pending hint is the latest snapshot → `engB.CounterGet == 2` and `HasLocal`. That test is why we chose snapshot fan-out.

### What existing clients can assume

- CacheOnly / LoadThrough / Bloom / Set / ZSet / Geo / List / Hash unchanged if they never open `ModeCounter`.
- No new default node keyspace. Hash already added `profile`. Optional `hits` demo KS is **follow-up**.
- Rolling: do **not** register a `ModeCounter` keyspace until every node has the Counter `ApplyPut` branch **and** peer `CounterIncr`. An old node would fall through `ApplyPutWithRingGen` to `AcceptIfNewer` and treat `Mode=8` as KV; `CounterIncr` would be unimplemented.

## Approach

### In-memory (`pkg/counter`)

Package **`pkg/counter`**. No live struct — the value **is** an `int64`.

```go
package counter

var ErrOverflow = errors.New("counter overflow")

func Encode(v int64) []byte                    // exactly 8 bytes, two’s-complement LE
func Decode(b []byte) (int64, error)           // error unless len == 8
func Add(cur, delta int64) (int64, error)      // overflow → ErrOverflow; no wrap
```

`Add` uses the usual checked-add (`delta > 0 && cur > math.MaxInt64-delta` or `delta < 0 && cur < math.MinInt64-delta`).

**Encode (locked two’s-complement LE):**

```go
func Encode(v int64) []byte {
    b := make([]byte, 8)
    binary.LittleEndian.PutUint64(b, uint64(v))
    return b
}
func Decode(b []byte) (int64, error) {
    if len(b) != 8 {
        return 0, errBad
    }
    return int64(binary.LittleEndian.Uint64(b)), nil
}
```

No dirty cache, no `ApproxWireBytes` beyond 8.

### Store (`pkg/store`)

One new flag. Last shipped bit is `FlagHashDel` `1<<18` in `pkg/store/entry.go`.

```go
FlagCounter uint32 = 1 << 19 // value is 8-byte LE int64 snapshot
```

`Entry.IsCounter()`. **No** `FlagCounterIncr`.

**Eager 8-byte `Value`.** No `cCache`, no `cDirty`, no `flushCounterValueLocked`. `Get`/`Peek` already call the other flush helpers; those stay 0-alloc on KV. Adding a counter flush that ran on every Get would be unnecessary risk — do not add one.

```go
// store.Store
CIncr(key string, delta int64, version uint64, expireAt int64) (newVal int64, applied, overflow bool)
CGet(key string) (int64, bool) // missing / tombstone / non-counter / bad blob → 0, false
HasCounter(key string) bool
CInstall(key string, blob []byte, version uint64, expireAt int64) bool // incoming > local; equal ignore; tombstone ≤ blocks
```

`CIncr` (locked). Gate first; **only a successful apply mutates**. Overflow / stale-tombstone / wrong-type leave the entry **and** cost unchanged.

1. Live non-counter → `applied=false`, `overflow=false`; no write.
2. Live unexpired tombstone and `version <=` tombstone → `applied=false`; no write.
3. Live `FlagCounter` → `Add(cur, delta)`. Overflow → return current, `applied=false`, `overflow=true`; **do not mutate**.
4. Missing / expired → new value **is** `delta` (create, not resume). `Incr(0)` creates `0`. Expired entries are removed then inserted fresh — the old count is gone.
5. Tombstone with `version >` tombstone → replace with `delta` (same create path).

**On every successful apply** (create, replace, or live add), write the full envelope like a tiny KV `Set` / `hMutateLocked` update — not an ad-hoc field poke:

- `Version = version` (the argument; Engine’s `cNextVersion`)
- `Flags = FlagCounter`
- `Value = Encode(newVal)` (eager 8 bytes)
- if `expireAt != 0` { `ExpireAt = expireAt` } (slide, scheme B)
- `cost = entryCost(key, ent)` (`len(key)+64+8`)
- update `m.bytes`; `MoveToFront`; `evictLocked`

Create/replace uses the same insert helper shape as other structures (`insert*Locked` / `Set`): new `lruItem`, `PushFront`, account bytes, `evictLocked`. Live add updates the existing item in place (version/flags/value/expire/cost) then `MoveToFront` + `evictLocked`.

Protect live `FlagCounter` in `lruVictim` alongside Hash/List/… (`memory.go` today lists Bloom/Set/ZSet/Geo/List/Hash only).

`CInstall`: same version gate as `HInstall` (`incoming > local`; equal ignore; tombstone blocks if `<=`). Decode must be exactly 8 bytes or return false. Replace with a copy of the blob; write `Version`/`Flags=FlagCounter`/`ExpireAt`/`cost`; `MoveToFront`; `evictLocked`. Failed gate: no cost change.

`CGet` decodes in place (0 alloc). Corrupt 8-byte rule: not exactly 8 → treat as miss (`0, false`).

`Set` / `AcceptIfNewer` need no new cache fields to clear.

### Engine (`pkg/engine/counter.go`)

Mirror `pkg/engine/list.go` **transport** (value-returning peer RPC) and `applyListInstall` / `HInstall` **install**. Do **not** copy Hash item-apply or List owner-inbox push flags.

```go
func (e *Engine) Incr(ctx context.Context, keyspaceName, name string, delta int64) (int64, error)
func (e *Engine) CounterGet(ctx context.Context, keyspaceName, name string) (int64, bool, error)
```

1. Validate mode `ModeCounter`, `name` via `validateKey` + `validateKeyLen`.
2. **Non-owner `Incr`:** `c.Transport.CounterIncr(...)`. Do not `ApplyPut` a delta.
3. **Owner / single-node `Incr`:** `cNextVersion` + `expire := e.expireAt(ks.cfg.TTL)` + `store.CIncr`. Overflow → `ErrInvalidArgument` (`%w: counter overflow`). `!applied` → `ErrInvalidArgument`. Then `cReplicateSnapshot` (`FlagCounter`, 8-byte `Value` from `Peek` or `Encode`) at **that** version (store must have written `Version` — see `CIncr`).
4. **`CounterGet`:** if `HasCounter` → `CGet` (0 alloc when warm). Else `cFetchOwner` (below).
5. Get/Put reject `ModeCounter`.
6. `GetOrLoadLocal`: Peek + `IsCounter` (same as Hash/List).
7. `LocalEntries` already walks `RangeAll`; eager `Value` is current. Store `Version` must match the last owner mint.

**`cFetchOwner` (locked: copy of `hFetchOwner`):**

```go
func (e *Engine) cFetchOwner(ctx context.Context, ks *ksRuntime, name string) (store.Entry, bool, error) {
    c := e.clusterSnapshot()
    if c == nil || c.Ring == nil || c.Transport == nil {
        return store.Entry{}, false, nil
    }
    owner, ok := c.Ring.Owner(name)
    if !ok || owner.ID == "" || owner.ID == c.SelfID || owner.Addr == "" {
        return store.Entry{}, false, nil
    }
    pctx, cancel := e.peerCtx(ctx, ks)
    defer cancel()
    res, err := c.Transport.GetOrLoad(pctx, owner.Addr, ks.cfg.Name, name)
    if err != nil || !res.Found || !res.Entry.IsCounter() {
        return store.Entry{}, false, nil // swallow transport / owner-down as miss
    }
    if _, decErr := counter.Decode(res.Entry.Value); decErr != nil {
        return store.Entry{}, false, nil
    }
    if e.holdsReplica(c, ks, name) {
        _ = ks.store.CInstall(name, res.Entry.Value, res.Entry.Version, res.Entry.ExpireAt)
    }
    return res.Entry, true, nil
}
```

`CounterGet` after a successful fetch: decode `ent.Value` (non-replica does not store). `holdsReplica` already `CInstall`ed.

**`ApplyPutWithRingGen` (after `IsHash`, before `IsNegative`):**

```go
if ent.IsCounter() {
    return e.applyCounterInstall(ks, key, ent.Value, ent.Version, ent.ExpireAt), nil
}
```

```go
func (e *Engine) applyCounterInstall(ks *ksRuntime, name string, blob []byte, version uint64, expireAt int64) bool {
    if expireAt == 0 {
        expireAt = e.expireAt(ks.cfg.TTL)
    }
    return ks.store.CInstall(name, blob, version, expireAt)
}
```

No owner re-fan-out of inbound item flags (there are none). Snapshot install is enough; the owner already replicated after local `Incr`.

Peer server (`internal/peerserver`): `CounterIncr` calls `s.eng.Incr` (same recursion pattern as `ListPop` → `LPop`). If that node is not owner, Engine forwards again; hop is one in the steady state.

```mermaid
sequenceDiagram
  participant C as Client
  participant N as Non-owner
  participant O as Owner
  participant R as Replica
  C->>N: Incr(name, +1)
  N->>O: Peer CounterIncr(delta=+1)
  O->>O: cNextVersion + CIncr + replicate FlagCounter
  O->>R: ApplyPut FlagCounter snapshot (int64 LE)
  O-->>N: value = n
  N-->>C: n
  Note over R: CInstall only; do not add delta
  C->>R: CounterGet(name)
  Note over R: HasCounter → local (may lag)
  C->>N: CounterGet(name)
  alt N holds replica copy
    N-->>C: local CGet
  else non-replica
    N->>O: GetOrLoad(name)
    O-->>N: FlagCounter snapshot
    N-->>C: decode int64
  end
```

### Client / proto / sc

Cache RPCs (`api/proto/cache.proto`). Locked names **`Incr`** / **`CounterGet`** (not `CGet` on the wire — avoids looking like a typo; sc still uses `cget` so KV `get` is untouched).

```protobuf
rpc Incr(IncrRequest) returns (IncrResponse);
rpc CounterGet(CounterGetRequest) returns (CounterGetResponse);

message IncrRequest         { string keyspace = 1; string name = 2; int64 delta = 3; }
message IncrResponse        { int64 value = 1; }
message CounterGetRequest   { string keyspace = 1; string name = 2; }
message CounterGetResponse  { bool present = 1; int64 value = 2; }
```

Peer (`api/proto/peer.proto`), next to `ListPop`:

```protobuf
rpc CounterIncr(CounterIncrRequest) returns (CounterIncrResponse);

message CounterIncrRequest  { string keyspace = 1; string name = 2; int64 delta = 3; }
message CounterIncrResponse { int64 value = 1; }
```

`pkg/client`:

```go
func (c *Client) Incr(ctx context.Context, keyspace, name string, delta int64) (int64, error)
func (c *Client) CounterGet(ctx context.Context, keyspace, name string) (int64, bool, error)
```

`internal/cacheserver` via existing `grpcmap.Status`. Overflow / wrong mode → InvalidArgument.

**`cmd/sc` (required in the product PR):**

| CLI | Maps to |
|-----|---------|
| `incr <name> [delta]` | `Incr`; **default delta = 1**. Print the new int64 (decimal). |
| `cget <name>` | `CounterGet`: print decimal value or `(nil)`; exit 1 if missing |

Do **not** overload KV `get`.

Negative delta on the process CLI: the existing `sc` flag splitter treats tokens starting with `-` as flags (`cmd/sc/main.go`). Lock:

- `sc incr hits -- -1` ( `--` already copies the rest into `posArgs`)
- REPL `incr hits -1` works (no flag splitter)
- Do not add a `-delta` flag

Bad / missing delta parse → usage, exit 2. `incr` with extra args → usage.

`printUsage`, REPL help, `cmd/sc/README.md` updated.

OpenAPI `info.version` **0.6.0 → 0.7.0** in `api/openapi/cache.openapi.yaml` **and** the `docs/api/cache.openapi.yaml` snapshot.

### PLAN.md (same PR)

| Section | Change |
|---------|--------|
| §1 Non-goals | **Remove “Counters”.** Keep **CRDTs** (and HyperLogLog / streams / Redis wire as today). Wording: `CRDTs` (not “Counters or CRDTs”). |
| §5 **Get** sentence | Today: “**Invalid** on ModeBloom / … / ModeHash”. **Add ModeCounter** (same sentence as Hash). |
| §5 verb table | New row: `Incr` / `CounterGet` — ModeCounter only; owner-serialized incr returns new int64; replica `CounterGet` may lag; `Delete(name)` tombstone. Get/Put **invalid** on ModeCounter. |
| §5 **Delete** row | Today: “Bloom / set / zset / geo / list / hash”. **Add counter** names. |
| §5 **Read-your-writes** | Today: “Put / SetAdd / ZAdd / GeoAdd / LPush / HSet / BloomAdd”. **Add owner `Incr`.** |
| §5 “Bad fit” | Today: “linearizable reads, **counters**, …”. **Rewrite:** financial / at-most-once ledgers and **CRDT** counters stay a bad fit. Named `ModeCounter` is in-scope for rate limits and other best-effort counts. Keep §23 “Financial balances / counters that must not double-apply”. |
| §7 mode table | `ModeCounter` row: verbs `Incr`, `CounterGet`, `Delete(name)`; wire `FlagCounter` snapshot only. |
| §7 subsection | New `ModeCounter` (named int64; snapshot fan-out; peer `CounterIncr`; scheme B TTL; `pkg/counter`). |
| §9.6 title / handoff | Today: “Bloom / Set / ZSet / Geo / List / Hash” and `FlagSet\|…\|FlagHash`. **Add `FlagCounter`.** Mutate bullet: `Incr` is owner-only apply + **snapshot** (List class) — **not** step-3 `ApplyPut` item flag. Read: `CounterGet` local-if-`HasCounter` else GetOrLoad. |
| §10 | Store: eager 8-byte `FlagCounter`; no live cache. |
| §11 Engine + Mode const | `Incr`, `CounterGet`; `ModeCounter`. |
| §12 catalog | ModeCounter + `pkg/counter` + `sc incr`/`cget`. |
| §14 Cache + Peer | `rpc Incr`, `rpc CounterGet`; Peer `rpc CounterIncr`. |
| §25 | Structured-types row includes ModeCounter. |

### Benchmarks (v1 micros)

CI already picks `Benchmark(Store|Engine)*`. Ship:

| Benchmark | Notes |
|-----------|--------|
| `BenchmarkStoreCIncr` | fixed name, `+1` (method name, not `StoreIncr`) |
| `BenchmarkStoreCGetHit` | prefill; **0 alloc** |
| `BenchmarkEngineIncr` | single-node ModeCounter |
| `BenchmarkEngineCounterGet` | **allocs/op → 0** when warm |
| `BenchmarkEngineIncrParallel` | parallel Incr |
| `BenchmarkEngineCounterGetParallel` | parallel CounterGet |

Not in scbench smoke. KV Get-hit / StoreGetHit **allocs/op must not rise**. Local bench of `BenchmarkEngineGetHit` / `BenchmarkStoreGetHit` stays in the plan (no new flush helper on the Get path).

## Rate-limiter example (same implementation PR)

**Fixed-window** limiter — the honest use of one atomic counter + TTL. Not a token bucket.

Package: **`examples/ratelimit`** (same shape as `examples/hash`: `main.go`, `demo.go`, `demo_test.go`, `README.md`).

| Piece | Lock |
|-------|------|
| Cluster | `testcluster` 3 nodes, ephemeral ports |
| Keyspace | **`rl`**, `ModeCounter`, RF=2 |
| TTL | `2 * window` (scheme B slack) |
| **Asserted test window** | **`60 * time.Second`** — do **not** assert 3-allow/1-deny on a 1s Unix bucket |
| README default | 1s may be *described* as a walkthrough default; **`runDemo` / `go test` use 60s** |
| Name | `fmt.Sprintf("%s:%d", key, time.Now().Unix()/int64(window/time.Second))` |
| API | `Allow(...) (allowed bool, remaining, n int64, name string, err error)` — **returns the name** used |
| Allow | error if `int64(window/time.Second) < 1` (no divide-by-zero); else `Incr(+1)` on that name; `n > limit` → deny; `remaining = max(0, limit-n)` |
| Reset | next window id (new name) **or** `Delete(name)` |
| HTTP | **not** in v1 (hash had none). Printed walkthrough + `go test` |
| Demo node KS | **not** in this PR. README may show `sc` against a **manually** registered `rl` / a follow-up `hits` KS |

```go
func Allow(ctx context.Context, cli *client.Client, ks, key string, limit int64, window time.Duration) (allowed bool, remaining, n int64, name string, err error) {
    secs := int64(window / time.Second)
    if secs < 1 {
        return false, 0, 0, "", fmt.Errorf("window must be >= 1s")
    }
    name = fmt.Sprintf("%s:%d", key, time.Now().Unix()/secs)
    n, err = cli.Incr(ctx, ks, name, 1)
    if err != nil {
        return false, 0, 0, name, err
    }
    if n > limit {
        return false, 0, n, name, nil
    }
    return true, limit - n, n, name, nil
}
```

Walkthrough (`runDemo`) — **testable, not 1s-flake:**

1. Why not KV / Hash (`HINCRBY` excluded; blob RMW loses incrs).
2. `window := 60 * time.Second`. Four `Allow("alice", 3, window)` **first**, capturing `name` from the first return. Assert allow, allow, allow, deny (`n` 1,2,3,4; remaining 2,1,0,0). **Do not** recompute the bucket after the Allows.
3. **Then** RF=2 `HasLocal` on **that saved `name`** (wait for two locals). A 60s bucket will not roll during a `waitLocals`-style poll.
4. Optional hardening (not required if window is 60s): if the first Allow is within a few ms of a 60s Unix boundary, retry the whole burst once on a fresh name.
5. `CounterGet` on a replica may lag; **`Incr` is the authority**.
6. `Delete` of the **saved** window name resets; next `Allow` on the same computed name creates `n==1`.
7. Optional: show a *different* explicit name (`alice:other`) allows independently — do **not** sleep across a 1s boundary to prove rollover.

`go test ./examples/ratelimit` calls `runDemo` and requires the `OK:` line (copy `examples/hash/demo_test.go`). Assert 3-allow/1-deny only against the **60s** window and the **captured name**.

README: `go run ./examples/ratelimit`; contract reminders (snapshot, owner `Incr`, scheme B names); optional `sc incr` / `sc cget` snippet **if** the operator registered a `ModeCounter` keyspace (not via `-demo-keyspace` in this PR).

**Not in the example:** token bucket, sliding window, HTTP `-listen`. Mention token bucket as rejected/follow-up (needs two values atomically).

Clock: the example uses the **caller’s** `time.Now()`. In-process demo is one clock. Document: split app processes can disagree on the bucket near a boundary (inherent to fixed-window + local clocks).

## Tests (write these first, after approval)

| Test | Package | Asserts |
|------|---------|---------|
| Encode/decode | `pkg/counter` | 8-byte LE; empty / 7 / 9 bytes error; `MinInt64` / `MaxInt64` |
| Add overflow | `pkg/counter` | `MaxInt64+1`, `MinInt64-1` → `ErrOverflow`; `0+x`, `x+0` ok |
| CIncr create / add | `pkg/store` | missing → delta; two `+1` → 2; `Incr(0)` creates 0; Version/Flags/cost written |
| CIncr overflow leaves value | `pkg/store` | `MaxInt64` then `+1` → overflow, value **and cost** still `MaxInt64` |
| CIncr after expire | `pkg/store` | expired then `CIncr(5)` → 5 (create new, not resume) |
| CGet missing vs zero | `pkg/store` | miss `ok=false`; live 0 `ok=true` |
| CInstall version gate | `pkg/store` | equal ignore; lower ignore; tombstone blocks stale |
| Incr / CounterGet | `pkg/engine` | create, add, decrement through 0, Incr(0) touch |
| Overflow | `pkg/engine` | `ErrInvalidArgument`; subsequent `CounterGet` unchanged |
| Wrong mode | `pkg/engine` | Incr on ModeHash / Get on ModeCounter / HSet on ModeCounter |
| Missing | `pkg/engine` | CounterGet `0, ok=false`; no error |
| Empty-until-delete | `pkg/engine` | Incr to 0; `HasLocal` true until `Delete(name)` |
| Incr after Delete | `pkg/engine` | recreates at delta |
| TTL slide | `pkg/engine` | owner Incr refreshes `ExpireAt` from keyspace TTL |
| Mode.String | `pkg/keyspace` | `ModeCounter.String() == "Counter"` |
| GetOrLoadLocal | `pkg/engine` | Peek + `IsCounter`; missing → NotFound |
| Replica snapshot | `pkg/engine` | RF=2 `HasLocal`; replica CounterGet matches after fan-out |
| Non-replica CounterGet | `pkg/engine` | RF=2; third node **without** `HasLocal` still `CounterGet`s the owner value via GetOrLoad |
| Non-owner Incr | `pkg/engine` | value equals owner; uses `CounterIncr` (not ApplyPut delta) |
| Hint after peer down | `pkg/engine` | exactly two `Incr(+1)` while B down; **no third Incr**; after flush `CounterGet==2` |
| Client gRPC | `pkg/client` | Incr / CounterGet / Delete against cacheserver |
| `cmd/sc` CLI | `cmd/sc` | default delta 1; explicit 5; `cget` `(nil)` + exit 1; `sc incr hits -- -1` |
| Example | `examples/ratelimit` | `OK:`; 60s window; captured name; 3-allow/1-deny; then HasLocal; `window<1s` errors |

**Hint after peer down (locked):** copy `TestListHintAfterPeerDownKeepsBothPushes` **ring/transport/down setup only** (`pkg/engine/list_cluster_test.go`). Exactly **two** owner `Incr(+1)` while B’s Peer is down; start B; wait for hint flush (`FlushHints` / sleep like List). **Do not** add a post-listen Incr “to poke the pool” (List’s third `RPush("z")` would make `CounterGet == 3`). Assert `engB.CounterGet == 2` and `HasLocal`. Do **not** copy Hash’s “do not require both fields.”

## Bench risk

**Hot path?** No — KV Get must not call counter **logic**. Do **not** add a flush helper. `Get`/`Peek` keep the existing flush chain; those helpers already return immediately when not dirty.

Local bench before commit: `BenchmarkEngineGetHit` / `BenchmarkStoreGetHit` allocs/op must not rise.

Smoke: merge bar ±10% shared cells. Get-hit / StoreGetHit **allocs/op** flat.

New micros: first-merge baseline only; later PRs treat `EngineCounterGet` allocs as “must stay 0 when warm.”

## Implementation order (after approval)

1. Design approved in chat (`looks good` / `do it` / `implement`).
2. Failing tests from the table (TDD).
3. `pkg/counter` + `FlagCounter` + store `CIncr`/`CGet`/`CInstall` + LRU protect (no flush helper).
4. Engine + ApplyPut `IsCounter` (after Hash, before Negative) + GetOrLoadLocal + Get/Put reject.
5. Peer proto `CounterIncr` + `Transport.CounterIncr` + peerserver.
6. Cache proto / `pkg/client` / cacheserver + **`cmd/sc incr`/`cget`**.
7. Cluster + List-shaped hint test.
8. Micros.
9. **`examples/ratelimit`** (printed demo + `go test`).
10. Product docs in the **same PR** (PLAN §1/§5 including Delete + RYOW + Get-invalid + §7/§9.6/`FlagCounter`/§11/§14). Do not call this “§5b” — `WORKFLOW.md` has no such heading.
11. Local bench of flagged micros + Get-hit / StoreGetHit allocs.
12. Commit on `feat/mode-counter` → PR → CI bench comment → merge only when the user says **merge**.

## Key Decisions

| Decision | Lock | Rationale |
|----------|------|-----------|
| Name | `Counter` / `ModeCounter` / `Incr` + `CounterGet` | Same naming as Geo/Hash; Cache `Incr` avoids KV `Get` clash; sc `cget` |
| API surface | `Incr(delta)`, `CounterGet`, `Delete(name)` | Smallest complete counter; no IncrBy twin |
| Fan-out | **`FlagCounter` snapshot only** | Non-commutative + current `hintID` coalesce |
| hintID | **unchanged** `(ks, name)` | Out of scope |
| Owner-inbox | **peer `CounterIncr`**, no ApplyPut delta | ApplyPut cannot return `n`; ListPop pattern |
| Replica apply | install snapshot only | Adding deltas would drop earlier incrs under hints |
| Reads | `HasCounter` → local; else GetOrLoad | Match GeoPos; document replica lag |
| Authoritative count | **`Incr` only** (owner-serialized) | Rate limiter must not decide on replica `CounterGet` |
| TTL | **Scheme B:** keyspace TTL slides; window id in the name | Store already slides; create-only expire is not a one-liner |
| `WithIncrTTL` | **not in v1** | Would special-case store; example does not need it |
| Incr(0) | **allowed** | Idempotent touch / create 0 |
| Negative delta | **allowed** | Decrement; 0 stays until Delete |
| Overflow | **`ErrInvalidArgument`, no wrap** | Explicit; Redis wrap is a non-goal |
| Missing CounterGet | `0, ok=false`, nil error | Same present-bit as HGet / GeoPos |
| Empty-until-delete | live 0 stays | Same as empty hash/list |
| Encode | eager 8-byte **two’s-complement** LE (`PutUint64` / `int64(Uint64)`) | No dirty cache; Get flush stays 0-alloc |
| CIncr persist | every apply writes Version, Flags, Value, ExpireAt, cost, MoveToFront, evict | PeekVersion / CInstall / handoff read the store envelope |
| Expire | missing/expired Incr **creates at delta** | Do not resume the expired count |
| Example window | **60s in `runDemo` / test**; `Allow` errors if `< 1s`; returns `name` | 1s Unix bucket flakes; Hash name was a constant |
| Package | `pkg/counter` | Value is int64; no `counterx` needed |
| Flags | `FlagCounter` `1<<19` only | Next free after `FlagHashDel` |
| Persistence | none | Same as every other type |
| sc | `incr [delta]`, `cget`; negatives via `--` | Do not overload `get` |
| Demo KS | **not** in this PR | Example registers `rl` itself; optional `hits` follow-up |
| Example | **same PR**, `examples/ratelimit`, printed + test | User asked for type **and** example; one design → one PR |
| Token bucket | **not** in v1 example | Needs two values atomically |
| Product docs | same implementation PR (not “§5b”) | PLAN §1 + §5 Delete/RYOW/Get-invalid + §9.6 FlagCounter |
| Peer RPCs | **one new:** `CounterIncr` | Required return path |
| CLUSTER_FLOWS | table + E11 `CounterGet` + Client `OPS` line (`Incr` / `CounterGet`); do **not** put Incr on E10 | E10 is item-level; Incr is List-class snapshot |

## PR Plan

SuperCache hard rule: **one approved design per PR**. **Single implementation PR** on `feat/mode-counter`: tests + code + proto + sc + product docs (same PR) + **`examples/ratelimit`**. Do **not** split Counter vs example unless review forces it. (`WORKFLOW.md` has no “§5b” heading; the file list below is the requirement.)

| # | Title | Files / components | Depends on | Description |
|---|-------|--------------------|------------|-------------|
| 1 | **feat: ModeCounter + ratelimit example** | `pkg/counter`, `pkg/store` (flag, `CIncr`, LRU), `pkg/engine/counter.go` + ApplyPut + Get/Put + GetOrLoadLocal, `pkg/keyspace` (`ModeCounter`), `api/proto/cache.proto` + `peer.proto` + gen, `pkg/client`, `internal/cacheserver`, `internal/peer` `CounterIncr`, `internal/peerserver`, `cmd/sc`, `examples/ratelimit`, tests + micros, **product docs** (`docs/API.md`, OpenAPI 0.6.0→0.7.0 + snapshot, `PLAN.md` §1/§5 Get-invalid+Delete+RYOW+verb/§7/§9.6 FlagCounter/§10/§11/§12/§14/§25, `docs/OPERATIONS.md`, `docs/CLUSTER_FLOWS.md` table+E11+OPS, `README.md`, `docs/design/README.md`, sc help) | Design **approved** in chat | One PR. No default `hits` demo keyspace. |
| 2 | **optional follow-up: `hits` demo KS** | `cmd/supercache-node -demo-keyspace` | PR 1 merged | Only if we want `sc -keyspace hits incr` against a stock node. |

Usually we do **not** open a design-docs-only PR; this draft lives on `feat/mode-counter` with the implementation after approval.

## Product docs to update (implementation PR)

| File | Change |
|------|--------|
| `docs/API.md` | Mode table + Counter RPC section (`Incr` returns new int64; `CounterGet` present bit) |
| `api/openapi/cache.openapi.yaml` | Incr / CounterGet; bump `info.version` 0.6.0 → 0.7.0 |
| `docs/api/cache.openapi.yaml` | same snapshot copy |
| `PLAN.md` | §1 drop Counters from non-goals; §5 Get-invalid sentence **+ Delete row + RYOW `Incr` + verb row**; §7/§9.6 title/handoff **`FlagCounter`** + List-class mutate (not item-flag); §10/§11/§12/§14/§25; refine “bad fit: counters” |
| `docs/OPERATIONS.md` | mode table + consistency cheatsheet (snapshot + `CounterIncr` + replica CounterGet lag) + example link; do **not** add a default demo KS |
| `docs/CLUSTER_FLOWS.md` | structured-types table **ModeCounter** (snapshot like List). Extend existing E11 label with `CounterGet`. Client `OPS` line: add `Incr` / `CounterGet` (today `L* / H*` only). Do **not** add new E10/E11 nodes. Do **not** add `Incr` to E10 (item-level). |
| `README.md` | modes, packages, `sc incr` / `cget`, example pointer |
| `cmd/sc/README.md` + `printUsage` / REPL | incr / cget |
| `docs/design/README.md` | this design once approved/shipped |

## Security & privacy

No new auth surface. Counter names/values ride the existing Cache gRPC (TLS) and Peer ApplyPut / `CounterIncr` (mTLS). Do not log names as PII (rate-limit keys are often user ids). Limits (`MaxKeyLen`; 8-byte value; `MaxBytes` may overshoot one live counter) are the DoS bound. Same tenant isolation: keyspace config is local and must match across nodes.

**Retry hazard (medium):** a client that retries `Incr` after a timeout may double-apply if the owner already committed. SuperCache does not have request ids. Mitigation: document; rate limiter is best-effort; CAS/GETSET are non-goals. This is why PLAN §23 still forbids **financial** counters.

## Observability

Reuse existing fan-out / hint / `staleSkip` / store hit-miss counters. No new required metrics in v1. Optional later: incr overflow count. Wrong-mode and overflow map through `grpcmap` as InvalidArgument.

## Rollout / rollback

- Opt-in: nothing happens until an operator registers a `ModeCounter` keyspace on **every** node (and every node speaks `CounterIncr`).
- Deploy the Counter build cluster-wide first; then `UpdateKeySpace`.
- Rollback: stop writing Incr; `Delete(name)` or drop the keyspace. No on-disk format.
- No feature flag (mode is the flag).
- Example is in-process only; it does not change node defaults.

## Open points (resolved)

1. **Naming:** **ModeCounter** / **Incr** / **CounterGet** (Cache); sc **`cget`**.
2. **Fan-out:** **snapshot `FlagCounter` only**; no `FlagCounterIncr`.
3. **Return path:** peer **`CounterIncr`** (ListPop pattern). No ApplyPut delta inbox.
4. **TTL:** **scheme B** (slide keyspace TTL; window id in the name). No `WithIncrTTL`.
5. **Incr(0):** **allowed**.
6. **Overflow:** **error, no wrap**.
7. **Empty-until-delete:** keep live 0.
8. **Encode:** **eager 8-byte two’s-complement LE**; no live cache / flush.
9. **Example:** **same PR**, `examples/ratelimit`, keyspace `rl`; **60s test window**; `Allow` returns `name` and errors if `window < 1s`.
10. **Demo KS:** **not** in this PR.
11. **PLAN:** Counters leave §1 non-goals; CRDTs stay; §23 financial still out; Delete / RYOW / Get-invalid / §9.6 `FlagCounter` in the same PR.
12. **CLUSTER_FLOWS:** table + E11 `CounterGet` + Client `OPS` line; Incr not on E10.
13. **Hint test:** exactly two `Incr(+1)` while B down; no third mutate.
14. **CIncr:** every apply persists Version/Flags/Value/cost; overflow does not.

## Rejected alternatives

| Idea | Why not |
|------|---------|
| App RMW on a CacheOnly blob | Lost concurrent incrs; no owner serialize |
| `HINCRBY` on ModeHash | Non-commutative under hint coalesce; Hash design already excluded it |
| ZSet score as a count | No returned atomic incr; ranking type |
| Item-level `FlagCounterIncr` to replicas | Lost hints drop earlier deltas; replica count wrong |
| Change hintID in this PR | Touches global hint path; separate design |
| ApplyPut delta as owner inbox | Cannot return `n` |
| Pop-on-owner-only (no peer RPC) | Bad UX; client already multi-seed but Engine must still forward |
| Scheme A create-only ExpireAt | Store mutate always slides; extra special case; B is enough with windowed names |
| Keyspace TTL as the window without name bucketing | Every Incr would slide the window (not fixed) |
| Token-bucket example | Needs tokens + timestamp atomically; not one int64 |
| CRDT PN-counter | PLAN non-goal; owner serialize makes a single int64 enough |
| Redis INCR wrap | Silent wrap is a footgun; we error |
| Split type vs example PRs | User asked for both; one design → one PR |
| Default `hits` demo keyspace | Hash already added `profile`; example owns `rl` |

## References

- [WORKFLOW.md](../WORKFLOW.md) (template + product docs same PR)
- [AGENTS.md](../../AGENTS.md)
- [PLAN.md](../../PLAN.md) §1 (today: “Counters or CRDTs”), §5, §7, §9.6, §11, §14, §23
- [API.md](../API.md)
- Shipped types: [ModeList](./2026-08-19-mode-list.md) (snapshot + `ListPop`), [ModeHash](./2026-08-20-mode-hash.md) (HINCRBY excluded; item-level; example style), [ModeGeo](./2026-08-19-mode-geo.md) (expire slide + GeoPos reads)
- `pkg/store/entry.go` (`FlagHashDel` `1<<18`; next free `1<<19`)
- `internal/peer/hints.go` `hintID` (only `FlagBloomAdd` is per-item)
- `pkg/engine/list.go` (`Transport.ListPop`, `applyListInstall`, `lReplicateSnapshot`)
- `api/proto/peer.proto` `rpc ListPop`
- `examples/hash/` (in-process 3-node + README + `go test`)
- Store expire slide: `pkg/store/memory.go` `hMutateLocked` / `gMutateLocked` / `lMutateLocked`
