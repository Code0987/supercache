# Bloom filter membership type

**Status:** draft  
**Branch (later):** `feat/bloom-filter`

## Problem

SuperCache only stores opaque `key → []byte` (`Get` / `Put` / `Delete`). There is no membership type.

A Bloom filter answers **“is this item possibly in the set?”** with no false negatives (when the filter is intact) and a bounded false-positive rate. That is the first extra data structure worth adding: small, mergeable, and a common cache/edge need (seen-ID, “don’t hit SoT if we never stored this”).

`Get`/`Put` cannot express it. `Get` takes one key and returns a value. A filter has a **name** plus an **item**. Mapping `Put(filter, item)` would overload Put and still have no `Test`.

Today a user who wants membership either stores every item as its own KV (RF copies, MaxBytes, tombstones) or keeps a Bloom outside SuperCache.

## Non-goals

- RedisBloom / Cuckoo / Count-Min / HyperLogLog / lists / hashes / sets
- Removing a **single item** from a filter (classic Bloom cannot)
- Per-item TTL inside a filter
- Using a Bloom as an internal index in front of every CacheOnly `Get` (that would sit on the KV hot path; separate design)
- Changing default RF, tombstone TTL, or existing Get/Put/Delete contracts
- Persistence of filters across process restart (same as KV)

## Contract

### API (new; existing Get/Put/Delete unchanged)

On `pkg/engine.Engine` and `pkg/client.Client`, and on the Cache proto:

| Call | Meaning |
|------|---------|
| `BloomAdd(ks, name, item)` | Insert item into the named filter. Creates the filter if missing. |
| `BloomTest(ks, name, item) (bool, error)` | `false` = definitely not present. `true` = maybe present. Missing filter → `false`, not an error. |
| `Delete(ks, name)` | Existing Delete: drops the **whole** filter (tombstone on `name`). |

`item` is `[]byte`, same max length as a key (`MaxKeyLen`). `name` is a normal cache key (filter identity).

Get/Put on a `ModeBloom` keyspace return `ErrInvalidArgument` (wrong verb). BloomAdd/BloomTest on CacheOnly/LoadThrough also return `ErrInvalidArgument`.

### Keyspace

New mode `keyspace.ModeBloom`.

Sizing (fixed at `UpdateKeySpace`; changing them wipes the in-memory store like any other config update):

| Field | `0` means | Role |
|-------|-----------|------|
| `BloomBits` | `1 << 20` (1 MiB bitset, 8,388,608 bits) | `m` |
| `BloomHashes` | `7` | `k` independent hashes |

False-positive rate is not a config field; it follows `m`, `k`, and how many adds you do. Document the usual approximation `≈ (1 - e^{-kn/m})^k`.

`MaxBytes` still bounds the bitset blob (`m/8` + envelope). If `BloomBits/8` exceeds `MaxBytes`, `UpdateKeySpace` fails validation.

`TTL` if set expires the **whole filter** (same `ExpireAt` as a KV entry). `NegativeTTL` is ignored. `TombstoneTTL` applies when the filter is Deleted. `ReplicationFactor` is the same as KV.

### Replication

Same owner + `R-1` successors as KV. Owner is `ring.Owner(name)` (filter name, not item).

**Add must not LWW-replace the bitset.** Two concurrent adds on different items would otherwise drop bits → **false negatives**.

- `BloomAdd`: owner sets bits locally, then `replicate` of an **item-add** (not a full bitset snapshot).
- Peer `ApplyBloomAdd`: each replica hashes the item with the same `m,k` and ORs bits. No `AcceptIfNewer` of a blob.
- Failed replica adds use the existing hint queue (entry carries item bytes + a bloom-add flag, or a dedicated hint kind). Replay is `ApplyBloomAdd`, not `ApplyPut`.
- Join handoff: ship the **bitset** and **OR-merge** (`ApplyBloomMerge`). A joiner that only got later item-adds would false-negative on older items.

Non-replicas: `BloomTest` forwards to the owner (like CacheOnly miss). Do **not** keep a bitset copy on a non-replica (filters are large).

If a replica missed adds (down, hint dropped), `BloomTest` on that replica can false-negative until repair. Same availability class as a missed Put; document it. Owner is the repair source for Test forwards.

### What existing clients can still assume

- CacheOnly / LoadThrough Get/Put/Delete bit-identical.
- No new default keyspace.
- Peer proto gains RPCs; old peers that do not implement them fail the RPC → hint/retry (rolling upgrade: add RPCs first, then send them).

## Approach

### `pkg/bloom`

Standard bitset Bloom:

- `New(mBits, k int) *Filter`
- `Add(item []byte)`
- `Test(item []byte) bool`
- `Merge(other *Filter)` — bitwise OR; same `m,k` or error
- `Bytes() []byte` / `Open(m, k, bits []byte)`

Hashes: two 64-bit seeds from `hash/fnv` or `cespare/xxhash` (stdlib FNV is enough for v1) + Kirsch-Mitzenmacher `h1 + i*h2`. Deterministic across processes. No `math/rand`.

Not on the KV Get path. Package is small and unit-tested on its own.

### Engine

`ksRuntime` for `ModeBloom` holds `map[string]*bloomSlot` (name → filter + expire + version for delete/LWW of **resets only**).

Alternatively store the bitset as a `store.Entry` with a new flag `FlagBloom` and **never** `AcceptIfNewer` the blob on add — only OR. Delete still uses `DeleteIfVersion` tombstone on `name`. Prefer this so MaxBytes, TTL, tombstones, `HasLocal`, and handoff `LocalEntries` stay one store.

Locked: **bitset lives in `store.Memory` as an entry** (`Flags` include `FlagBloom`, `Value` is packed bits). 

- `BloomAdd`: if missing or expired, allocate zeroed `m/8` bytes, then set bits, `Set` (unconditional, same version or `nextVersion` only on create). If present and `FlagBloom`, mutate bits in place (store needs `UpdateBloom(name, item)` or Get-copy-Set under the store mutex).
- In-place update under the store mutex is required so two Adds do not clobber bits. Add `Memory.BloomAdd(key, item, m, k) error` next to `AcceptIfNewer`.
- `BloomTest`: `Peek`; if missing/tombstone/expired → not present; else test bits.
- `Delete`: existing `DeleteIfVersion` on `name`.

### Peer / fan-out

Extend the centralized apply path:

| Entry / op | RPC |
|------------|-----|
| normal KV | `ApplyPut` |
| tombstone | `ApplyDelete` |
| bloom item-add | `ApplyBloomAdd` (new): `name` + `item` |
| bloom snapshot (handoff) | `ApplyBloomMerge` (new): `name` + bitset + `m,k` |

Hints LWW: a later **tombstone** on `name` supersedes queued adds for that filter. A queued add after a tombstone with higher delete version is dropped. Item-adds do not supersede each other (different items); hint id must be `(ks, name, item)` or we keep a bitset hint. **Locked:** hint id stays `(ks, name)` only for snapshots/deletes; **item-adds use `(ks, name, item)`** so two in-flight adds are not coalesced away.

Handoff `LocalEntries`: bloom entries included (already in `RangeAll`). `ReplicateToPeers` sends `ApplyBloomMerge`.

### Proto

Cache:

```protobuf
rpc BloomAdd(BloomAddRequest) returns (BloomAddResponse);
rpc BloomTest(BloomTestRequest) returns (BloomTestResponse);
// BloomAddRequest { keyspace, name, item }
// BloomTestResponse { maybe bool }  // false = definitely not
```

Peer:

```protobuf
rpc ApplyBloomAdd(ApplyBloomAddRequest) returns (ApplyBloomAddResponse);
rpc ApplyBloomMerge(ApplyBloomMergeRequest) returns (ApplyBloomMergeResponse);
```

`cmd/sc`: `bloom add <name> <item>`, `bloom test <name> <item>`.

### Rejected alternatives

| Idea | Why not |
|------|---------|
| Internal Bloom in front of every CacheOnly Get | Touches the KV hot path; different problem; would need a cluster-wide perfect set of keys |
| One Bloom per keyspace (no names) | Cannot isolate tenants/filters; Delete would wipe everything |
| Get/Put overloads | Get has no room for filter+item; Put semantics are replace, not OR |
| LWW the whole bitset on Add | Concurrent adds drop bits → false negatives |
| Counting Bloom / deletion of one item | Bigger, slower, still approximate; out of v1 |
| Depend on bits-and-blooms / willf/bloom | Extra module for a small bitset; keep stdlib |

## Tests (write these first, after approval)

| Test | Package | Asserts |
|------|---------|---------|
| `TestFilterAddTest` | `pkg/bloom` | Added item tests true; never-added item usually false; no panic on empty |
| `TestFilterNoFalseNegative` | `pkg/bloom` | N added items all Test true |
| `TestFilterMergeOR` | `pkg/bloom` | Merge of two filters tests true for items from both |
| `TestBloomAddTestDelete` | `pkg/engine` | ModeBloom: Add, Test true, Delete name, Test false |
| `TestBloomWrongMode` | `pkg/engine` | BloomAdd on CacheOnly and Get on ModeBloom → `ErrInvalidArgument` |
| `TestBloomMissingIsFalse` | `pkg/engine` | Test on unknown name → false, nil error |
| `TestBloomAddDoesNotClobber` | `pkg/store` | Two BloomAdds on different items; both Test true (no LWW loss) |
| `TestBloomTombstoneBlocksAdd` | `pkg/engine` | Delete then stale merge/add with lower version does not resurrect; new Add after delete version works |
| `TestBloomReplicaAddAndTest` | `pkg/engine` | 3-node RF=2: Add on node 0; replicas HasLocal bitset; Test true; non-replica Test true via owner, no local bitset |
| `TestBloomHintAfterPeerDown` | `pkg/engine` | Replica down during Add, returns, Test becomes true (hint replay) |

No new CI smoke cells in v1 (keeps the published matrix stable). Optional later: `pkg/bloom` microbench, not in PR smoke.

## Bench risk

**Hot path?** No, if KV `Get`/`Put` do not call `pkg/bloom`.

CI smoke is CacheOnly get/put/miss. Expected delta: noise (±20%). Watch `BenchmarkEngineGetHit` **allocs/op** — must stay put. If someone accidentally hooks Bloom into `Engine.Get`, that test is the tripwire.
