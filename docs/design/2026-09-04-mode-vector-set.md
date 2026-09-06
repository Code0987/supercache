# Vector set type (`ModeVectorSet`)

**Author:** SuperCache
**Status:** approved (chat: ok got it, lets continue)
**Branch (later):** `feat/mode-vector-set`
**Date:** 2026-09-04

## Overview

SuperCache can store exact membership (`ModeSet`), scored members (`ModeZSet`), and lon/lat points (`ModeGeo`). It cannot store a **named set of embedding vectors** and answer “which members are closest to this query vector?”

`ModeVectorSet` is a Redis-8-like **vector set**: one keyspace name holds many members, each member has a fixed-dimension `float32` vector. v1 is brute-force K-NN under the store mutex with a **keyspace metric** (`cosine` default, plus `l2` and `ip`), **snapshot fan-out**, feature-folder layout (`pkg/vecset` + `pkg/vecset/eng`). It is **not** an ANN service, not HNSW, not Faiss, and not RESP `VADD`/`VSIM` wire parity.

This is new product behavior on the data path. Full path in [WORKFLOW.md](../WORKFLOW.md). **No tests or implementation until this draft is approved** (`looks good` / `do it` / `implement`).

## Problem

Closest existing types fail the job:

| Mode | Why it is not a vector set |
|------|----------------------------|
| `ModeZSet` | Score is a scalar the **caller writes**. No multi-dim vector, no cosine. |
| `ModeGeo` | 2-D lon/lat + haversine. Not an embedding. |
| `ModeHash` | Field → blob. No similarity. A client-side map in one KV value is a lost-update race under LWW `Put`. |
| `ModeJSON` | Path set/get. Same LWW-blob problem if the client stores `[{id, vec}]`. |
| CacheOnly `Put` | Whole-blob replace. Two clients adding different members clobber each other. |

There is no verb that takes `(name, member, []float32)` and a query that returns top-K neighbors. `Get`/`Put` cannot express it without overloading Put and still having no `VSim`.

Dedicated use: a small in-process “similar items” cache (tracks, products, docs) where dim is tens–hundreds and cardinality per name is hundreds — RAM-only, owner-serialized, RF copies of one blob.

## Goals & Non-Goals

### Goals

- Named, keyspace-scoped vector set with v1 verbs: `VAdd`, `VRem`, `VSim`, `VCard`, `VDim`, `VEmb`, plus existing `Delete(name)`.
- Brute-force K-NN. Keyspace metric: **cosine** (default), **L2**, **inner product**. First successful `VAdd` locks dim on that name.
- Owner ACK + async `FlagVectorSet` **snapshot** to RF−1. No new Peer RPC.
- Present-bit on the **name**. Missing ≠ empty result that looks like “dim 0”.
- Feature folders: codec `pkg/vecset`, owner write / GetOrLoad `pkg/vecset/eng`, thin `pkg/engine/vecset.go`.
- Product docs + `cmd/sc` + `examples/vecset` in the **same** implementation PR.

### Non-goals

- Redis 8 Vector Sets numeric/wire parity (`VADD REDUCE`, `Q8`/`BIN`/`NOQUANT`, `EF`, `M`, `FILTER`, `SETATTR`, `VLINKS`, `VRANDMEMBER`, `TRUTH`).
- HNSW / IVF / PQ / Faiss / CGO / GPU / disk indexes.
- Per-**query** metric (would disagree across RF and need an sc flag). Metric is a keyspace knob, like `TopKSize`.
- Extra metrics beyond cosine / L2 / IP (L1, angular, Hamming, Mahalanobis).
- `VSim` by member id (caller does `VEmb` then `VSim`).
- Quantization, dim reduce, JSON attributes, CAS, per-member TTL.
- A default `-demo-keyspace` on `supercache-node` (optional follow-up, like HLL/CMS).
- Changing RF, tombstones, hint identity, or any existing mode contract.
- Persistence / WAL / Redis dump import.
- New scbench YAML cells.
- Expanding `Entry.Flags` to `uint64`.

## Contract

### API (new; existing verbs unchanged)

On `pkg/engine.Engine`, `pkg/client.Client`, Cache proto, and `cmd/sc` (same PR):

| Call | Meaning |
|------|---------|
| `VAdd(ks, name, member, vec []float32)` | Insert or **replace** `member`’s vector. Creates the set if missing. ACK-only. |
| `VRem(ks, name, member)` | Remove `member` if present. No-op if missing / set missing. |
| `VSim(ks, name, vec []float32, k int) ([]VSimHit, error)` | Top-`k` by the keyspace metric, **best first**. Missing name → empty, nil error. |
| `VCard(ks, name) (n int, present bool, err error)` | Member count. Missing → `present=false`, `n=0`. |
| `VDim(ks, name) (dim int, present bool, err error)` | Locked dim. Missing → `present=false`. Empty live set still reports dim. |
| `VEmb(ks, name, member) (vec []float32, ok bool, err error)` | Stored vector copy. Missing name/member → `ok=false`. |
| `Delete(ks, name)` | Existing Delete: whole-set tombstone. |

```go
type VSimHit struct {
    Member []byte
    Score  float32 // meaning depends on keyspace VectorMetric (below)
}

// pkg/keyspace — next to Mode / TopKSize
type VectorMetric int

const (
    VectorMetricCosine VectorMetric = iota // 0 = default
    VectorMetricL2
    VectorMetricIP
)
```

| `VectorMetric` | `Score` | Best first |
|----------------|---------|------------|
| `cosine` (0) | `dot/(\|a\|\|b\|)` in **[-1, 1]** | high → low |
| `l2` | Euclidean distance **≥ 0** | low → high |
| `ip` | raw `dot(a,b)` (unbounded) | high → low |

Do **not** negate L2 to fake a similarity. Callers that need “higher is better” pick cosine or IP. Equal scores still break ties by member bytes ascending.

- `name` is the set identity; ring owner = `Owner(name)`.
- `member` is `[]byte`, **1..255 bytes** (tighter than ZSet/Geo: snapshot codec uses `u8 idLen`). Empty or `len>255` → `ErrInvalidArgument` even when `MaxKeyLen` is 512. Engine still rejects `len > e.maxKeyLen` first if someone raises the 255 cap later.
- `vec` is IEEE-754 `float32`. Length must match the locked dim (or keyspace `VectorDim` when set). NaN / ±Inf → `ErrInvalidArgument`. **Zero vector:** invalid on **cosine** (`VAdd` and `VSim`); **allowed** on L2 and IP.
- `k <= 0` means **10**. `k > 50` is clamped to **50**. Result length is `min(k, card)`. Ties by **member bytes ascending**.
- Get/Put on `ModeVectorSet` → `ErrInvalidArgument` (`use VEmb` / `use VAdd`), same as ModeCMS / ModeZSet.
- Vector verbs on any other mode → `ErrInvalidArgument`.
- Last `VRem` **keeps** an empty live set (dim stays locked, `VCard=0`, `VSim` empty). `Delete` is what drops the name.

**Check order (locked, CMS-shaped):** validate **before** present-bit. Empty member, `len(member)>255`, NaN/Inf, cosine zero-vector, `len(vec)<2` or `>256`, `len(vec) ≠ VectorDim` when keyspace `VectorDim>0`, `len(vec) ≠` locked dim when the name is **live** → `ErrInvalidArgument` even if the name is missing. Then missing name / owner-down / GetOrLoad `!Found` / `!IsVectorSet` → `VSim` empty + nil error; `VCard`/`VDim` `present=false`; `VEmb` `ok=false`. First `VAdd` with `VectorDim=0` still rejects dim 1 (not a legal lock).

### Keyspace

New mode `keyspace.ModeVectorSet` (next iota after `ModeCMS` = 13, so **14**).

| Field | Role |
|-------|------|
| `MaxValueSize` | Per-blob cap (default `1 MiB`). Validate rejects if `WorstEncodedSize` > this (TopK pattern). |
| `MaxBytes` | LRU **store** budget, not the per-blob cap. May be smaller than a live set; `lruVictim` **must not** evict live `FlagVectorSet` (CMS/HLL). |
| `TTL` | Expires the **whole** set |
| `TombstoneTTL` | On `Delete(name)` |
| `ReplicationFactor` | Same as KV |
| `VectorDim` | **0** = unlocked until first `VAdd`. **>0** = every `VAdd`/`VSim` vector must be this dim. Range **2..256**. Included in `Config.ConfigHash`. |
| `VectorMetric` | **0 / unset = cosine**. Else `l2` or `ip`. Included in `Config.ConfigHash`. Not stored in the blob (brute force has no index). `UpdateKeySpace` **may** change it; the next `VSim` uses the new metric. |

No `VectorMaxMembers` knob. Cardinality is bounded by encoded size vs `MaxValueSize` (see encoding). Metric is **not** per `VSim` call and **not** per name.

`Validate` (like ModeTopK):

- `VectorDim < 0` or `VectorDim == 1` or `VectorDim > 256` → error.
- `VectorMetric` not in `{0, cosine, l2, ip}` → error. (`String()` / config parse: `"cosine"`, `"l2"`, `"ip"`; empty/`""` → cosine.)
- `WorstEncodedSize(dim, maxMembers, maxMemberLen) > MaxValueSize` → error, where `maxMembers` is the v1 cap **512** and `dim` is `VectorDim` if set, else **256**.

### Caps (locked)

| Cap | Value | Why |
|-----|------:|-----|
| Dim | 2..256 | Fits 512 members in 1 MiB even with 255-byte ids (`5 + 512*(1+255+1024) = 655365` ≈ 640 KiB). |
| Members / name | 512 | Brute-force 512×256 dots is fine under the store mutex; 4096×1024 is not a 1 MiB snapshot. |
| `k` | 1..50 (default 10) | Result set, not index size. |
| Member id | 1..255 bytes | `u8 idLen`. Tighter than ZSet/Geo (`MaxKeyLen` default is **512**, uvarint). Validate uses `min(MaxKeyLen, 255)`. |

### Replication

**Snapshot family** (HLL / CMS / TopK / List), **not** item fan-out.

| Path | Flag / payload | Who applies |
|------|----------------|-------------|
| Non-owner → owner | `FlagVectorSet` + **inbox** prefix (`A` add / `R` rem) | Owner only. Replicas **ignore** inbox (same as `FlagTopKAdd` / `FlagCMSIncr`). |
| Owner → RF−1 | `FlagVectorSet` + **snapshot** prefix (`S`) | Replicas `VSInstall` LWW (`incoming > local`). |
| Join / GetOrLoad | snapshot | RF holders install; non-replicas do not keep a copy. |

`hintID` stays `(ks, name)`. That is why item-level `VAdd` fan-out is **rejected**: two adds of different members coalesce to one hint; K-NN on the replica would miss a neighbor. Snapshot after owner mutate is complete.

Hint-loss of a **snapshot** is the usual “replica lags one successful replay” story. Replica `VSim` may miss a just-added member for one fan-out RTT.

### Who stores a copy

- **Owner:** always, after ACK.
- **Replicas (RF−1):** snapshot copy. Local `VSim` / `VEmb` / `VCard` / `VDim` allowed (may lag).
- **Non-replicas:** no copy. Writes forward inbox to owner. Reads use CMS/HLL `FetchOwner`: `GetOrLoad` the snapshot, **compute `VSim`/`VEmb`/`VCard`/`VDim` on the caller**, `VSInstall` **only if** `holdsReplica`. Owner does **not** run K-NN for other nodes (would hold the owner store mutex on every remote query). Owner-down / `!Found` / `!IsVectorSet` → miss (empty / `present=false` / `ok=false`, **nil error** — not `ErrUnavailable`).

### Failure / TTL

- Owner down → **writes** return the existing owner-forward error (same as `HLLAdd`). **Reads** that need GetOrLoad treat owner-down as a miss (nil error), same as CMS/HLL.
- Wrong dim / zero / NaN / oversize / too many members → `ErrInvalidArgument` **before** mutate; no version bump.
- Full (512 members and `VAdd` of a **new** id) → `ErrInvalidArgument` (`vector set full`). Replace of an existing id is allowed.
- Encoded blob > `MaxValueSize` → reject (should not happen if Validate + caps hold).
- TTL expires the whole name. Per-member expire is not v1.

### Existing clients

Unchanged. New mode is opt-in via `UpdateKeySpace`. Old binaries without the RPCs cannot use the type (same as every new mode).

## Approach

### Flag bit (last `uint32` slot)

`pkg/store/entry.go` uses bits 0..30. **One bit left:**

```go
FlagVectorSet uint32 = 1 << 31 // snapshot *or* owner-inbox; discriminate by Value[0]

func (e Entry) IsVectorSet() bool { return e.Flags&FlagVectorSet != 0 }
```

`1<<31` is `0x80000000` on unsigned `uint32` (same as `peer.proto` `uint32 flags`). Use the existing `IsCMS` bitwise helper style. Do **not** assign the shift to `int32` or compare Flags as signed.

Do **not** add `FlagVectorSetAdd` / `FlagVectorSetRem` (would require `uint64` Flags). Payload byte 0:

| `Value[0]` | Meaning | Replicas |
|------------|---------|----------|
| `'S'` | Snapshot | `VSInstall` |
| `'A'` | Inbox add: `A` + u8 idLen + member + dim×float32 LE | Ignore if not owner |
| `'R'` | Inbox rem: `R` + member (remainder of `Value` is the id; 1..255 bytes) | Ignore if not owner |

### Snapshot encoding (`pkg/vecset`)

Little-endian, no compression:

```
'S' | u16 dim | u16 count | record*
record = u8 idLen | id | dim × float32
```

Header is **5** bytes. `WorstEncodedSize(dim, count, idLen) = 5 + count*(1 + idLen + 4*dim)`. `idLen` 1..255. Count 0..512. Dim 2..256. The 1 MiB proof is `5 + 512*(1+255+1024) = 655365` bytes (~640 KiB).

`Decode` rejects truncated / leftover / dim 0 / count>512 / NaN/Inf. Empty set (`count=0`) is valid and **still carries dim**.

Store the blob **value-in-place** in `Entry.Value` (HLL/CMS/TopK class). **No** `vCache` / dirty map. Under the store mutex: decode → mutate map → encode → write `Value`, `Flags=FlagVectorSet`, `Version=local+1`.

`VSim` under the same mutex: score each member with the **keyspace** metric, partial sort top-`min(k,card)` (cosine/IP high→low, L2 low→high), ties by member bytes ascending, clone member bytes into the result (caller owns the slice).

```
cosine: score = dot(a,b) / (||a|| * ||b||)     // [-1, 1]
l2:     score = sqrt(sum (a_i-b_i)^2)          // >= 0
ip:     score = dot(a,b)                       // unbounded
```

Vectors are stored **raw** (not pre-normalized) so `VEmb` round-trips and the same blob works if `VectorMetric` is changed. Cosine still rejects a zero stored vector (cannot have been added) and a zero query. L2/IP may store and query zeros.

### Engine / feature folder

```
pkg/vecset/                 # package vecset — codec only
  codec.go
  codec_test.go
  engine_test.go            # package vecset_test; engine.New()
  engine_cluster_test.go
  engine_bench_test.go
  eng/                      # package eng
    host.go                 # Host interface (Store, Owner, Replicate, …)
    local.go                # owner VAdd/VRem/VSim/VCard/VDim/VEmb + GetOrLoad
    cluster.go              # FetchOwner via Host

pkg/engine/vecset.go        # public VAdd/… : validate, route, vecseteng.*
pkg/store/vecset.go         # Memory.VAdd / VRem / VSim / HasVectorSet / VSInstall
```

`ApplyPut` (`pkg/engine/engine.go`) — **one** `if ent.IsVectorSet()` block (do not fall through to `AcceptIfNewer`):

- empty `Value` or unknown prefix → `return false, nil`
- `'A'` / `'R'` → if not owner, `return false, nil`; else mutate. Wire `Version` is a tombstone **gate** only; stored version is `1` on create / `local+1` under the store mutex
- `'S'` → `VSInstall` (LWW replace, `incoming > local`)

Owner write assigns version **under the store mutex** (`local.Version+1`). Do **not** pre-lock `nextVersion` as stored.

### CMS-class wiring (required)

Same plumbing CMS/TopK/HLL needed. An engineer who only adds `vecset.go` + two ApplyPut lines will ship a type whose cluster reads 500 and whose sets vanish under MaxBytes.

| Hook | File | What |
|------|------|------|
| `GetOrLoadLocal` Peek + `IsVectorSet` | `pkg/engine/cluster.go` (CMS ~389) | Client `Get` rejects the mode; without this, owner `GetOrLoad` returns invalid-argument and replica/non-replica `VSim` cannot load a snapshot. |
| `lruVictim` skip live `FlagVectorSet` | `pkg/store/memory.go` | Else MaxBytes pressure evicts a live set. |
| `store.Store` methods | `pkg/store/store.go` | `VAdd` / `VRem` / `VSim(name, vec, k, metric)` / `VCard` / `VDim` / `VEmb` / `HasVectorSet` / `VSInstall` |
| `modeHost` | `pkg/engine/modehost.go` | `HasVectorSet()`, `VectorDim()`, `VectorMetric()` (like `TopKSize()`) |
| `Config.ConfigHash` | `pkg/keyspace` | **Must** include `VectorDim` **and** `VectorMetric` (BloomBits / TopKSize are hashed). |
| `Mode.String()` | `pkg/keyspace` | `"VectorSet"` (else admin prints `Mode(14)`). |

### sc CLI

| CLI | Maps to | Output |
|-----|---------|--------|
| `vadd <name> <member> <f1,f2,…>` | `VAdd` | `OK vadd <name> <member>` |
| `vrem <name> <member>` | `VRem` | `OK vrem …` |
| `vsim <name> <f1,f2,…> [k]` | `VSim` | `score member` per line |
| `vcard <name>` | `VCard` | integer; missing → `0`, exit 0. Present-bit is `vdim`. |
| `vdim <name>` | `VDim` | integer or `missing` + exit 1 |
| `vemb <name> <member>` | `VEmb` | comma-separated floats or `missing` + exit 1 |

Session `-keyspace` same as other verbs. Empty args → usage exit 2. **No** `vsim -metric` flag — metric is on the keyspace (`UpdateKeySpace` / node config), like `TopKSize`.

### Example

`examples/vecset`: three nodes, ModeVectorSet `items`, add ~8 members (dim 4 is enough), `VSim` a query, print neighbors. Not a billboard rewrite.

### Proto / docs (same PR)

`api/proto/cache.proto` (CMS-shaped; `repeated float` is packed on the wire):

```
rpc VAdd(VAddRequest) returns (VAddResponse);
rpc VRem(VRemRequest) returns (VRemResponse);
rpc VSim(VSimRequest) returns (VSimResponse);
rpc VCard(VCardRequest) returns (VCardResponse);
rpc VDim(VDimRequest) returns (VDimResponse);
rpc VEmb(VEmbRequest) returns (VEmbResponse);

message VAddRequest  { string keyspace = 1; string name = 2; bytes member = 3; repeated float vec = 4; }
message VAddResponse {}
message VRemRequest  { string keyspace = 1; string name = 2; bytes member = 3; }
message VRemResponse {}
message VSimRequest  { string keyspace = 1; string name = 2; repeated float vec = 3; int32 k = 4; }
message VSimHit      { bytes member = 1; float score = 2; }
message VSimResponse { repeated VSimHit hits = 1; }          // no present; missing = empty
message VCardRequest { string keyspace = 1; string name = 2; }
message VCardResponse{ int64 n = 1; bool present = 2; }
message VDimRequest  { string keyspace = 1; string name = 2; }
message VDimResponse { int32 dim = 1; bool present = 2; }
message VEmbRequest  { string keyspace = 1; string name = 2; bytes member = 3; }
message VEmbResponse { repeated float vec = 1; bool found = 2; }
```

Also: regen `api/gen`; `pkg/client`; `internal/cacheserver`; `internal/grpcmap` (wrong-mode / invalid → InvalidArgument); `api/openapi/cache.openapi.yaml` + `docs/api/cache.openapi.yaml`; `docs/API.md`; `docs/OPERATIONS.md`; root `README.md` mode table.

**`PLAN.md` (same PR)** — append ModeVectorSet next to ModeCMS everywhere that list ends today:

- §5 Get/Put invalid list; Delete “named … CMS” sentence; verb table: `VAdd` / `VSim` / `VRem` / `VCard` / `VDim` / `VEmb`
- §7 mode table row: `| ModeVectorSet | set name | VAdd, VRem, VSim, VCard, VDim, VEmb; Delete(name) | FlagVectorSet snapshot; owner-inbox same flag + A/R prefix |`
- §7 short section (CMS-shaped): brute force; `VectorMetric` cosine/L2/IP; dim lock; snapshot fan-out; Get/Put invalid
- §9.6 `lruVictim` protect `FlagVectorSet`; Config snippet `VectorDim int`, `VectorMetric VectorMetric`
- §11 Engine API + §14 Cache RPCs
- Package map: `pkg/vecset` (not `listx` — that path is already `pkg/list` on `main`)

**`docs/CLUSTER_FLOWS.md`:** structured-types table row `ModeVectorSet | owner VAdd/VRem then FlagVectorSet snapshot | VSim / VEmb / VCard / VDim | brute-force cosine/L2/IP`; E11 node adds `VSim`.

## Tests (write these first, after approval)

| Test | Package | Asserts |
|------|---------|---------|
| Codec round-trip / reject NaN / leftover | `pkg/vecset` | encode→decode equal; bad blobs error |
| `VAdd` locks dim; mismatch rejected | `pkg/store` | second dim ≠ first → error; version unchanged |
| Replace same member | `pkg/store` | `VCard` unchanged; `VEmb` new vec; version +1 |
| `VRem` last keeps empty + dim | `pkg/store` | `HasVectorSet`; `VDim` still dim; `VCard=0` |
| Full at 512 | `pkg/store` | 513th **new** id rejected; replace still works |
| Cosine order | `pkg/store` | query closer to A than B → A first; scores in [-1,1] |
| L2 order | `pkg/store` | nearer neighbor first; scores ≥ 0; smaller first |
| IP order | `pkg/store` | larger dot first |
| Cosine rejects zero; L2 accepts | `pkg/engine` | cosine `VAdd`/`VSim` zero → invalid; L2 keyspace accepts |
| Zero / Inf / dim-1 vector | `pkg/engine` | `ErrInvalidArgument` even if name missing |
| `VSim` missing vs bad vec | `pkg/engine` | NaN query → invalid; missing + legal vec → empty |
| 256-byte member | `pkg/store` | rejected (`u8` cap), version unchanged |
| Cosine ties | `pkg/store` | equal scores ordered by member bytes |
| `k > card` | `pkg/store` | returns `card` hits |
| Get/Put rejected | `pkg/engine` | `use VEmb` / `use VAdd` |
| Wrong mode | `pkg/engine` | `VAdd` on ModeSet / `ZAdd` on ModeVectorSet fail |
| `GetOrLoadLocal` Peek | `pkg/engine` | owner GetOrLoad returns snapshot (Get still rejected) |
| Live set not evicted | `pkg/store` | MaxBytes=1 still keeps FlagVectorSet |
| `ConfigHash` / `String` | `pkg/keyspace` | hash changes with VectorDim **and** VectorMetric; `String()=="VectorSet"` |
| Owner ACK + replica `VSim` | `pkg/vecset` cluster | after wait, replica sees member |
| Inbox on non-owner replica ignored | `pkg/engine` | `'A'` apply on non-owner → not applied |
| Snapshot LWW | `pkg/store` | lower version `VSInstall` dropped |
| Non-replica `VSim` | `pkg/vecset` cluster | result from owner; non-replica `HasVectorSet` false |
| `Delete` tombstone | `pkg/engine` | `VCard` missing; stale snapshot does not resurrect |
| Join handoff | `pkg/engine` | joiner RF holder gets snapshot; `VSim` works |
| sc usage | `cmd/sc` | missing args exit 2; `vadd`/`vsim` against in-process node |
| Example | `examples/vecset` | `VSim` returns the planted neighbor |

## Bench risk

- **Hot path?** No. Get-hit / Put do not decode vector sets.
- **Must stay flat:** `BenchmarkEngineGetHit` allocs **15**, `BenchmarkStoreGetHit` allocs **2**.
- New micros only (not smoke): `BenchmarkStoreVAdd`, `BenchmarkStoreVSim` (dim=64, n=128, k=10). Report ns/op; no CI gate on them in v1.
- `VSim` holds the store mutex for O(n·dim). Caps keep this bounded. Do not add a Get/Peek helper that decodes vector sets.

## Alternatives Considered

| Alternative | Why rejected |
|-------------|--------------|
| Item fan-out `FlagVectorSetAdd` like ZSet | `hintID` is `(ks, name)` — second add clobbers the first hint; K-NN would miss neighbors. Also **no flag bits left**. |
| HNSW in v1 | Graph + LWW snapshot is a second design (rebuild vs serialize layers). Brute force is honest at n≤512. |
| Store one KV per member (`vec:name:id`) | No atomic K-NN over the name; ring ownership splits the set; Get-hit path polluted. |
| Reuse ModeZSet scores | Scalar only. |
| Expand `Flags` to `uint64` | Touches every peer codec and every mode. Separate refactor, not this PR. |
| Per-query metric on `VSim` | RF nodes and `sc` would disagree; metric is a keyspace knob like `TopKSize`. |
| L1 / Hamming / more metrics | Cosine + L2 + IP is the usual trio; each extra metric is a sort-direction + zero-vector rule. |
| Pre-normalize on `VAdd` | `VEmb` would not round-trip; L2/IP would be wrong; cosine of raw vectors is enough. |
| Drop empty set on last `VRem` | Would unlock dim and surprise `VDim`. Keeping empty matches “name still exists”. |

## Security & Privacy

Embeddings can be PII-adjacent (text/image features). Same trust model as other values: mesh is internal, client gRPC auth is existing TLS/mTLS. No extra ACL. Vectors are not logged. `sc vemb` prints floats to stdout (operator already can `Get` other types).

## Observability

Reuse store hit/miss + existing fan-out / hint / `staleSkip` counters. No new required metrics in v1. Wrong-mode and validation map through `grpcmap` as InvalidArgument.

## Rollout

1. Merge one PR on `feat/mode-vector-set` after tests + local Get-hit bench + CI bench gate.
2. Callers opt in with `UpdateKeySpace{Mode: ModeVectorSet}`.
3. Rollback = don’t create the keyspace; binary rollback is the usual “old node doesn’t understand Mode 14 / `FlagVectorSet`” — mixed-version mesh is **not** supported for new flags (same as CMS). Roll the whole mesh.

## Risks

| Risk | Severity | Mitigation |
|------|----------|------------|
| Last `uint32` flag | high | One flag + payload prefix. Document that the **next** structured type needs `uint64` Flags or a packed op nibble. |
| `VSim` holds store mutex | medium | Cap n=512, dim=256; no Get-hit involvement. |
| Snapshot size | medium | Validate worst-case vs `MaxValueSize`; default 1 MiB. |
| Replica lag | low | Same as CMS snapshot; accepted. |
| Mixed-version peers | medium | Same as every new flag; roll mesh together. |

## Open Questions

None that block implementation. Defaults above are locked for v1 (including member ids 1..255, last-`VRem` keeps empty, dim cap 256, metrics cosine/L2/IP at keyspace scope). User can override before approval.

## Key Decisions

1. **New mode, not a ZSet hack** — need a vector + K-NN, not a caller-written scalar.
2. **Brute force + three metrics** — cosine (default), L2, IP. Keyspace `VectorMetric`, not per query. HNSW is a later design.
3. **Snapshot fan-out, not item hints** — K-NN cannot survive hint coalesce; also only one flag bit remains.
4. **One flag (`1<<31`) + `'S'`/`'A'`/`'R'` prefix** — avoid `uint64` Flags in this PR.
5. **Value-in-place, no dirty cache** — Get/Put never see this blob; decode only on vector verbs.
6. **Version under store mutex** — same as Hash/List/CMS so replica LWW cannot drop the later snapshot.
7. **Dim locked on first add** — optional `VectorDim` keyspace field for fail-fast.
8. **Caps 256 / 512 / 1 MiB** — keep one LRU entry and one mutex-held `VSim` bounded.
9. **Feature folders** — match post-#43 layout; public API stays `engine.Engine.VAdd`.
10. **One implementation PR** — SuperCache rule; demo node KS is the only optional follow-up.
11. **Non-replica reads are CMS `FetchOwner`** — GetOrLoad snapshot, compute on the **caller**, install only if `holdsReplica`. Owner does not run K-NN for others. Owner-down is a miss, not `ErrUnavailable`.
12. **Last rem keeps empty set** — dim stays locked until `Delete`.
13. **CMS-class wiring is in v1** — `GetOrLoadLocal`, `lruVictim`, `store.Store`, `modeHost.VectorDim`/`VectorMetric`/`HasVectorSet`, `ConfigHash`, `Mode.String()`.
18. **Zero vector is cosine-only invalid** — L2/IP may store and query zeros.
19. **VSim sort is metric-specific** — cosine/IP high→low; L2 low→high. Score is not rewritten into a fake similarity.
14. **Member ids 1..255** — `u8 idLen`; not `MaxKeyLen` (512).
15. **Validate before present-bit** — bad vec/member is invalid even on a missing name.
16. **ApplyPut is one `IsVectorSet` block** — unknown prefix never falls through to `AcceptIfNewer`.
17. **VSim ties** — `min(k,card)`; equal cosine by member bytes ascending.

## PR Plan

### PR 1 — `feat/mode-vector-set` (this design)

- **Title:** `feat: add ModeVectorSet (VAdd / VSim / VRem)`
- **Depends on:** nothing (landed on current `main`)
- **Files:** `pkg/keyspace` (`Mode`, `String`, `Validate`, `ConfigHash`, `VectorDim`, `VectorMetric`), `pkg/store/entry.go` + `store.go` + `vecset.go` + `memory.go` `lruVictim`, `pkg/vecset/**`, `pkg/vecset/eng/**`, `pkg/engine/vecset.go` + `engine.go` ApplyPut/Get/Put + `cluster.go` GetOrLoadLocal + `modehost.go`, proto + gen, `pkg/client`, `internal/cacheserver`, `internal/grpcmap`, `cmd/sc`, OpenAPI, `docs/API.md`, `docs/OPERATIONS.md`, `PLAN.md`, `docs/CLUSTER_FLOWS.md`, README, `docs/design/README.md` shipped row, `examples/vecset`
- **Changes:** tests first from the table; minimum code to pass `go test ./...`; local Get-hit / StoreGetHit allocs unchanged; CI bench ±10% gate.

### PR 2 — optional follow-up only

- **Title:** `feat: demo keyspace embeddings=ModeVectorSet`
- **Depends on:** PR 1 merged
- **Files:** `cmd/supercache-node/main.go`, node README
- **Changes:** `-demo-keyspace` registers `embeddings` so `sc -keyspace embeddings vadd …` works on a stock node.

## References

- [WORKFLOW.md](../WORKFLOW.md)
- [2026-09-01-mode-cms.md](./2026-09-01-mode-cms.md) — snapshot + inbox-ignore-on-replica + last flag bits
- [2026-08-13-mode-zset.md](./2026-08-13-mode-zset.md) — named members + sc verbs
- [2026-09-02-feature-folders.md](./2026-09-02-feature-folders.md) — `pkg/<mode>/eng`
- Redis 8 Vector Sets (`VADD`/`VSIM`) — product analog only, not a wire target
- `pkg/store/entry.go` flags 0..30; `pkg/keyspace/config.go` `ModeCMS` last iota; `DefaultMaxValueSize = 1<<20`
