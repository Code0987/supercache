# Bitmap type (`ModeBitmap`)

**Status:** approved  
**Branch:** `feat/mode-bitmap`  
**Date:** 2026-08-21

## Problem

SuperCache has no **named addressable packed bit vector**. Callers who want SETBIT / GETBIT / BITCOUNT today abuse a wrong type:

| Mode | Shape | Why it is not a bitmap |
|------|--------|------------------------|
| CacheOnly / LoadThrough / `Put` | `key → []byte` | Whole-blob LWW. Two `SETBIT`s of **different** offsets rewrite the entire value; the later `Put` clobbers the earlier offset (`pkg/engine/engine.go` `Put`). |
| `ModeBloom` | fixed-size probabilistic filter | Approximate membership (`pkg/bloom`). Not addressable bits. No `GETBIT` of offset *n*. No exact `BITCOUNT`. Bit packing is LSB-first (`1 << (idx % 8)` in `pkg/bloom/bloom.go`) and size is `BloomBits`, not a growable string. OR-merge is the opposite of LWW snapshot. |
| `ModeHash` | `field → []byte` | A map, not a bit vector. Fields are named bytes, not packed offsets. |
| `ModeCounter` | one `int64` | A scalar. Cannot address bit *n*. |
| `ModeJSON` | nested JSON tree | A document. Not packed bits. |
| `ModeSet` / `ModeZSet` / `ModeGeo` | membership / score / point | Wrong shape. |
| `ModeList` | sequence of `[]byte` | Wrong shape. |

Bitmap is the first **addressable packed bit vector** (`name → bits[offset] ∈ {0,1}`), Redis-compatible in bit order and in `BITCOUNT`/`BITPOS` **byte** windows.

This is new product behavior on the data path. Full path in [WORKFLOW.md](../WORKFLOW.md): design → review → (after approval) tests → code → bench → PR. **No tests or implementation until this draft is approved** (`looks good` / `do it` / `implement`).

## Non-goals

- Redis `BITOP` `AND`/`OR`/`XOR`/`NOT` (multi-key; PLAN §1 already excludes multi-key atomicity)
- Redis `BITFIELD` (overflow widths, `INCRBY` on bit-fields)
- `GET` / `SET` of the raw bit string as KV (`Get`/`Put` stay **invalid** on `ModeBitmap`)
- Per-bit TTL / `BITEXPIRE`
- Redis 7 `BITCOUNT`/`BITPOS` `BIT|BYTE` unit flag — v1 windows are **bytes only**
- Sparse encodings (Roaring, RLE). Storage is a dense Redis-style byte string
- Returning the previous bit from `SETBIT` (that would need a Peer RPC; see [Fan-out](#why-not-item-level-fan-out-locked))
- Shrinking the stored byte string when high bits are cleared
- A `BitDel` / “clear all” verb (clearing bits is `BitSet(..., false)`)
- Changing RF, tombstones, **hint identity** (`hintID` stays `(ks, name)` except `FlagBloomAdd` in `internal/peer/hints.go`), or existing Get/Put/Delete / Bloom / Set / ZSet / Geo / List / Hash / Counter / JSON contracts
- Persistence across process restart
- A new default `-demo-keyspace` Bitmap keyspace (Hash added `profile`; JSON added `doc` in a follow-up)
- scbench YAML cells
- A new Peer RPC (`BitSet` is ACK-only; reads are local extract or `GetOrLoad`)

Bitmap was never listed as a PLAN §1 non-goal. Do not invent a §1 fight. `BITOP` stays out because it is multi-key.

## Why not item-level fan-out (locked)

Offset updates of **different** offsets commute like Hash fields, but `hintID` is still `(ks, name)`:

```go
// internal/peer/hints.go
func hintID(ks, key string, ent store.Entry) string {
    if ent.IsBloomAdd() {
        return ks + "\x00" + key + "\x00" + string(ent.Value)
    }
    return ks + "\x00" + key
}
```

A second mutate of the same name **replaces** the pending hint. Item-level `FlagBitmapSet(offset, bit)` on the replica wire would drop an earlier *other* offset — the Hash caveat ([2026-08-20-mode-hash.md](./2026-08-20-mode-hash.md)): two `SETBIT`s while a replica is down keep only the latest offset.

**v1 wire:** every owner mutate fans out a **full packed bit string** (`FlagBitmap` `1<<23`) at the post-op version. Hint coalesce keeps the newest snapshot — that is correct. O(n) network per set; `MaxValueSize` (default 1 MiB, `keyspace.DefaultMaxValueSize`) caps n.

**No `FlagBitmapSet` on the replica wire.** That flag is **owner-inbox only** (List `FlagListLPush` / `FlagListRPush` in `pkg/engine/list.go` / `ApplyPutWithRingGen`; JSON `FlagJSONSet` / `FlagJSONDel` in `pkg/engine/json.go`).

Rejected for v1: per-offset replica apply; hintID change; op-log + snapshot-on-gap; Hash-class item-level with the Hash “do not require both fields” caveat.

## Contract

### API (new; existing verbs unchanged)

On `pkg/engine.Engine`, `pkg/client.Client`, Cache proto, and **`cmd/sc`** (same PR as the type):

| Call | Meaning |
|------|---------|
| `BitSet(ks, name, offset uint64, bit bool)` | Redis `SETBIT`. Creates the bitmap if missing. Grows the vector with zero-fill. **Does not return the old bit** (ACK-only). |
| `BitGet(ks, name, offset uint64) (bit bool, ok bool, err error)` | Redis `GETBIT` with a SuperCache present-bit. Missing **name** → `ok=false`. Live bitmap, offset past stored length → `bit=false`, `ok=true`. |
| `BitCount(ks, name, start, end int) (n int64, err error)` | Redis `BITCOUNT` over an inclusive **byte** window. Missing name → `0`, nil error (like `LLen`). |
| `BitPos(ks, name, bit bool, start, end int) (pos int64, found bool, err error)` | Redis `BITPOS` over an inclusive **byte** window. Missing name or bit not present in the window → `found=false`. |
| `Delete(ks, name)` | Existing Delete: whole-bitmap tombstone. |

```go
BitSet(ctx context.Context, keyspace, name string, offset uint64, bit bool) error
BitGet(ctx context.Context, keyspace, name string, offset uint64) (bit bool, ok bool, err error)
BitCount(ctx context.Context, keyspace, name string, start, end int) (n int64, err error)
BitPos(ctx context.Context, keyspace, name string, bit bool, start, end int) (pos int64, found bool, err error)
```

- `name` is the bitmap identity; ring owner = `Owner(name)`. Validated with `validateKey` + `validateKeyLen` (per-keyspace `MaxKeyLen` applies to **name**).
- `offset` is a **bit** index, `uint64`, 0-based. There is no negative offset (unlike `BITCOUNT` windows).
- `bit` is 0/1 (`false`/`true`). `BitSet(..., false)` **writes a zero bit** and may **grow**. It is not a delete.
- `BitSet` is ACK-only. SuperCache does **not** return the previous bit (Redis `SETBIT` does). A return payload would need a new Peer RPC (`ListPop` / `CounterIncr` class). Do not add one.
- Each **owner** `BitSet` that applies assigns a **new stored version under the store mutex** (see [Version assignment](#version-assignment-locked)) and refreshes whole-bitmap `ExpireAt` from keyspace `TTL` (`0` = none) — scheme B / Hash / List / Counter / JSON. ApplyPut paths use inbound `expireAt`, or `e.expireAt(TTL)` when the wire value is `0`. The inbound ApplyPut `Version: 1` on inbox flags is **not** the stored snapshot version.
- `BitSet` of a bit that is **already** that value still applies: new version, TTL slide, snapshot fan-out (a touch). Same as `JsonSet` of an equal node / `Incr(0)`.
- Get/Put on `ModeBitmap` → `ErrInvalidArgument` (`use BitGet` / `use BitSet`).
- Bit* on non-`ModeBitmap` → `ErrInvalidArgument`.
- Cross-mode misuse (BitSet on ModeJSON, JsonSet on ModeBitmap, …) → `ErrInvalidArgument`.

**Not in v1:** `BITOP`, `BITFIELD`, raw-string GET/SET, per-bit TTL, `BIT` unit, return-old-bit, shrink-on-clear.

### Present-bit vs Redis 0 (locked)

Redis `GETBIT` of a missing key **and** `GETBIT` past the end of a live string both return `0`. SuperCache keeps a **present-bit** on name identity, matching Hash / List / Counter / JSON:

| State | `HasBitmap` | `BitGet(offset)` | `BitCount` | `BitPos` |
|-------|-------------|------------------|------------|----------|
| Missing name / expired / tombstone | false | `bit=false`, **`ok=false`**, nil error | `0`, nil | `found=false` |
| Live bitmap, `offset < 8*len(Value)` | true | stored bit, **`ok=true`** | count in window | first match in window |
| Live bitmap, `offset ≥ 8*len(Value)` | true | `bit=false`, **`ok=true`** | count in window | first match in window |
| Live all-zero (every stored bit is 0) | **true** | in-range `0` / past-end `0`, **`ok=true`** | `0` | `1`-search `found=false`; `0`-search finds bit 0 if the window covers byte 0 |

A live all-zero bitmap is **present** until `Delete(name)`. `BitCount` of missing and of live-all-zero are both `0`; callers that need the distinction use `BitGet` (`ok`) or `HasLocal`.

`sc bitget` prints `0`/`1` when `ok=true`, `(nil)` + exit 1 when `ok=false`.

### `BITCOUNT` / `BITPOS` windows (locked: Redis **byte** offsets)

Redis treats `start`/`end` as **byte** offsets into the string, not bit offsets ([BITCOUNT](https://redis.io/docs/latest/commands/bitcount/), [BITPOS](https://redis.io/docs/latest/commands/bitpos/)). SuperCache matches that. Redis 7 `BIT|BYTE` is **not** v1 — the unit is always bytes.

Negatives follow `pkg/listx.Range` / Redis `GETRANGE`:

```text
n = len(stored bytes)
if start < 0 { start = n + start }
if end   < 0 { end   = n + end }
if start < 0 { start = 0 }
if end  >= n { end   = n - 1 }
if n == 0 || start > end || start >= n { empty window → count 0 / found=false }
```

Inclusive `[start, end]` after normalize. Whole bitmap is **`start=0, end=-1`** (Redis `BITCOUNT key 0 -1`). `sc bitcount <name>` with no range sends `0, -1`. `sc bitcount <name> 0 0` is **byte 0 only**.

`BitPos` searches bits `start*8 … end*8+7` of the **stored** bytes only (Redis “end specified” behavior). An all-`1`s bitmap looking for `0` with `0, -1` → `found=false`. Do **not** return the implicit bit past the end of the string (Redis does that only when `end` is omitted).

`BitPos` looking for `1` in an all-zero / empty window → `found=false`.

Engine proto `start`/`end` are `int32` (same as `LRange`). Overflow converting proto `int32` is not an issue; `sc` parses with `strconv.Atoi`.

### Size / DoS (locked)

`SETBIT` of offset `2^32` would allocate 512 MiB. SuperCache bounds the **encoded byte length** with existing `MaxValueSize` (default `keyspace.DefaultMaxValueSize` = 1 MiB):

```text
need = offset/8 + 1          // bytes after grow
max  = ks.MaxValueSize > 0 ? ks.MaxValueSize : e.maxValueSize
if EncodedLen(offset) overflows int           → ErrValueTooLarge
if max > 0 && need > max                      → ErrValueTooLarge
if after grow len(blob) > max                 → ErrValueTooLarge, no mutate
```

Default max bits = `8 * 1 MiB` = **8_388_608** bits; legal offsets are `0 … 8*MaxValueSize - 1`.

- **Initiating node** rejects an oversize offset **before** apply or `ApplyPut` forward (so clustered clients get `ErrValueTooLarge`, not a swallowed `!applied`).
- Owner `BSet` also checks after grow; encoded `> MaxValueSize` → `tooLarge=true`, **no mutate**, previous bytes **and cost** unchanged.
- Inbox apply that still overflows (race on `MaxValueSize`, corrupt inbox) → `applied=false`. ACK-only `ApplyPut` cannot carry `ErrValueTooLarge`; the non-owner maps `!applied` to `ErrInvalidArgument` (`bitmap set rejected`). Owner-hit / single-node path **does** return `ErrValueTooLarge` (tests lock that).

`MaxBytes` may still **overshoot** one live `FlagBitmap` (same as Set/Geo/List/Hash/JSON). One 1 MiB bitmap is allowed to sit in an LRU that is slightly over budget.

### Empty-until-delete (locked)

Prefer empty-until-delete (same spirit as Hash / List / Counter / JSON):

| Op | Result |
|----|--------|
| `BitSet(offset, false)` of the last remaining `1` | Bitmap stays live (possibly all-zero); `HasBitmap` true; stored **byte length does not shrink** |
| `BitSet(offset, false)` on a **missing** name | **Creates** a zero-filled bitmap of length `offset/8+1`; `HasBitmap` true |
| `BitSet` never calls `Delete(name)` | Clearing bits is not a tombstone |
| `Delete(name)` | Tombstone; `BitGet` → `ok=false` |

There is no `BitDel`. “Clear” means `BitSet(..., false)`.

### Keyspace

New mode `keyspace.ModeBitmap` (next iota after `ModeJSON`). `Mode.String()` must return `"Bitmap"` (else `/keyspaces` prints `Mode(10)`).

Current iota (`pkg/keyspace/config.go`): `LoadThrough=0` … `ModeJSON=9`. `ModeBitmap=10`.

| Field | Role |
|-------|------|
| `MaxBytes` | Cost budget. Live `FlagBitmap` is LRU-protected; one bitmap may **overshoot** (same as Set/Geo/List/Hash/JSON) |
| `TTL` | Expires the **whole** bitmap; owner `BitSet` **slides** `ExpireAt` (scheme B) |
| `NegativeTTL` | Ignored |
| `TombstoneTTL` | On `Delete(name)` |
| `ReplicationFactor` | Same as KV / ModeList |
| `MaxKeyLen` | Bounds `name` |
| `MaxValueSize` | Bounds the **encoded bit string** (and therefore `offset`) |

No extra bitmap-size knob. Do not reuse `BloomBits` — that is ModeBloom’s fixed *m*.

### Replication (locked: snapshot like List / Counter / JSON)

| Hop | Payload |
|-----|---------|
| Client → any node → **owner** | `ApplyPut` `FlagBitmapSet` (offset + bit) if this node is not owner |
| Owner → **replicas** | `FlagBitmap` full snapshot after the op (packed bytes + version + expire) |
| Handoff / `GetOrLoad` | `FlagBitmap` install if incoming version **>** local (equal = ignore; tombstone blocks if incoming ≤ tombstone). **Not OR-merge** (that is Bloom). |

`BitSet` is ACK-only — **no new Peer RPC**. Inbox flags + snapshot fan-out (List **push** / JSON `JsonSet` pattern). `BitGet` / `BitCount` / `BitPos` never need a Peer RPC: local extract or `GetOrLoad`.

**Version assignment (locked — one rule for `BSet`):**

Assign the stored snapshot version **inside the store mutex**. There is **no** second version space (`bNextVersion` / `max(local, incoming)+1` is **not** used).

```text
if missing / expired (no live entry):     stored = 1          // BSet create
if live FlagBitmap or replacing tombstone: stored = local.Version + 1
incoming `version` argument:              tombstone-gate floor only
                                          (reject if live tombstone && incoming <= tombstone.Version)
```

- Do **not** write the engine’s pre-lock `PeekVersion+1` as `Entry.Version`.
- Engine fans out **`PeekVersion` after the store write** (then `observeVersion` that same value). Never fan out the candidate passed in.
- Concurrent owner `BitSet`s serialize on the store mutex → distinct versions → `BInstall` (`incoming > local`, equal = ignore — same as `JInstall` / `HInstall` at `pkg/store/memory.go`) cannot drop the later snapshot.

**Owner-inbox vs replica-apply (locked — copy List / JSON, not Hash):**

- Non-owner: `Transport.ApplyPut` `{Flags: FlagBitmapSet, Value: inbox payload, Version: 1}`.
- Owner `ApplyPut` of that flag: **apply the op** (`BSet` first). Pass `PeekVersion+1` (0→1 if missing) **only** as the tombstone-gate floor. Do **not** use inbound `Version: 1` as the stored version. If the store reports a mutate, `ver, _ := PeekVersion(name)` then `ks.observeVersion(name, ver)` + `bReplicateSnapshot(..., ver, ...)`.
- Replica `ApplyPut` of `FlagBitmapSet`: **ignore** (`return false, nil`) if this node is not owner — same guard as `IsListLPush` / `IsJSONSet` in `pkg/engine/engine.go` `ApplyPutWithRingGen`. Do not send inbox flags to replicas.
- `FlagBitmap` → `BInstall` version gate only.

**Owner-local `BitSet`:** same as inbox apply — store first, fan out `PeekVersion` after write.

**Non-owner `applied=false`:** initiating node already rejected oversize **offset**. Owner reject (tombstone, non-bitmap entry) surfaces as `ErrInvalidArgument` (`bitmap set rejected`). Encoded `ErrValueTooLarge` is guaranteed on the **owner-hit / single-node** path.

**Reads (locked: same as `hFetchOwner` / `lFetchOwner` / `jFetchOwner`):**

- If `HasBitmap` local → extract from the **local** bit string. Replica snapshot may **lag**. **Do not** owner-forward a local hit.
- Else if this node is owner → missing bitmap → `BitGet` `ok=false` / `BitCount` `0` / `BitPos` `found=false`.
- Else → `bFetchOwner` (copy of `jFetchOwner`):
  - `GetOrLoad` RPC error / owner down / `!Found` / `!IsBitmap` → **miss** (`BitGet` `ok=false`; `BitCount` `0`; `BitPos` `found=false`; nil error). Do **not** return `ErrUnavailable` on reads.
  - `holdsReplica` → `BInstall` then extract.
  - Non-replica → use `ent.Value` in process; **do not** `BInstall`.

Owner down on `BitSet` → `ErrUnavailable` (no ACK authority), same as Put / JsonSet. Do not apply locally.

```mermaid
sequenceDiagram
  participant C as Client
  participant N as Non-owner
  participant O as Owner
  participant R as Replica
  C->>N: BitSet(name, 0, true)
  N->>O: ApplyPut FlagBitmapSet inbox Version=1
  O->>O: BSet (stored=local+1 under mutex) + PeekVersion + replicate FlagBitmap
  O->>R: ApplyPut FlagBitmap snapshot
  O-->>N: applied
  N-->>C: OK
  Note over R: BInstall only; ignore inbox flags
  C->>R: BitGet(name, 0)
  Note over R: HasBitmap → local extract (may lag)
  C->>N: BitGet(name, 0)
  alt N holds replica copy
    N-->>C: local extract
  else non-replica
    N->>O: GetOrLoad(name)
    O-->>N: FlagBitmap snapshot
    N-->>C: extract bit
  end
```

**Hint-after-down (normative):** two `BitSet`s of **different** offsets while replica B’s Peer is down; then start B and wait for hint flush. The pending hint is the **latest snapshot** → B has **both** bits. Copy List/JSON ring/transport/down setup (`TestListHintAfterPeerDownKeepsBothPushes` / `TestJSONHintAfterPeerDownKeepsBothPaths`). **Do** require both offsets (this is why we chose snapshot). **Do not** copy Hash’s “do not require both fields.”

### What existing clients can assume

- CacheOnly / LoadThrough / Bloom / Set / ZSet / Geo / List / Hash / Counter / JSON unchanged if they never open `ModeBitmap`.
- No new default node keyspace (`-demo-keyspace` stays `demo` / `tags` / `board` / `profile` / `doc`).
- Rolling: do **not** register a `ModeBitmap` keyspace until every node has the Bitmap `ApplyPut` branches. An old node would fall through `ApplyPutWithRingGen` to `AcceptIfNewer` and treat `Mode=10` as KV.

## Approach

### In-memory (`pkg/bitmapx`)

Package **`pkg/bitmapx`** (`bitmap` / `bitx` are too clash-prone; matches `pkg/jsonx` / `pkg/listx` / `pkg/hashx`).

**Bit order (locked — Redis, not Bloom):**

> The bit at offset 0 is the most significant bit of the first byte.
> — [Redis SETBIT](https://redis.io/docs/latest/commands/setbit/)

```text
byte 0:  bit 0 = 0x80 (MSB), bit 1 = 0x40, …, bit 7 = 0x01 (LSB)
byte 1:  bit 8 = 0x80, …
```

`pkg/bloom` uses the **opposite** packing (`1 << (idx % 8)` = LSB-first). Do **not** copy Bloom helpers.

```go
package bitmapx

var (
    ErrInbox = errors.New("bitmapx: invalid inbox")
)

// EncodedLen is the byte length needed to store offset (offset/8 + 1).
// ok=false if the length does not fit in int.
func EncodedLen(offset uint64) (n int, ok bool)

// Get reports the bit at offset. Past len(buf)*8 → false (implicit zero).
func Get(buf []byte, offset uint64) bool

// Set writes bit at offset, zero-filling / growing as needed. Returns buf
// (same backing array if no grow). Caller owns the result.
func Set(buf []byte, offset uint64, bit bool) []byte

// Count is Redis BITCOUNT over an inclusive byte window (negatives like listx.Range).
func Count(buf []byte, start, end int) int64

// Pos is Redis BITPOS over an inclusive stored-byte window. found=false if none.
func Pos(buf []byte, bit bool, start, end int) (pos int64, found bool)

// NormalizeByteRange is the Redis / listx.Range clamp. empty=true → Count 0 / Pos miss.
func NormalizeByteRange(n, start, end int) (lo, hi int, empty bool)

func EncodeSet(offset uint64, bit bool) []byte
func DecodeSet(b []byte) (offset uint64, bit bool, err error)
```

```go
func bitMask(offset uint64) (byteIdx int, mask byte) {
    byteIdx = int(offset / 8)
    shift := 7 - int(offset%8) // MSB first
    return byteIdx, 1 << shift
}

func Get(buf []byte, offset uint64) bool {
    i, mask := bitMask(offset)
    if i < 0 || i >= len(buf) {
        return false
    }
    return buf[i]&mask != 0
}

func Set(buf []byte, offset uint64, bit bool) []byte {
    need, ok := EncodedLen(offset)
    if !ok {
        return buf // caller rejected; do not panic
    }
    if len(buf) < need {
        n := make([]byte, need)
        copy(n, buf)
        buf = n
    }
    i, mask := bitMask(offset)
    if bit {
        buf[i] |= mask
    } else {
        buf[i] &^= mask
    }
    return buf
}
```

`Count` uses `math/bits.OnesCount8` on the normalized byte window. Do not walk bits in Go when counting a whole byte.

`Pos` walks bits MSB-first inside each byte of `[lo, hi]`. Search **only stored bytes**.

**Inbox payload (locked):**

```text
FlagBitmapSet Value = uvarint(offset) + 1 byte {0,1}
```

```go
func EncodeSet(offset uint64, bit bool) []byte {
    var scratch [binary.MaxVarintLen64 + 1]byte
    n := binary.PutUvarint(scratch[:], offset)
    if bit {
        scratch[n] = 1
    }
    return append([]byte(nil), scratch[:n+1]...)
}

func DecodeSet(b []byte) (uint64, bool, error) {
    v, w := binary.Uvarint(b)
    if w <= 0 { // truncated (w==0) or overflow (w<0)
        return 0, false, ErrInbox
    }
    rest := b[w:]
    if len(rest) != 1 {
        return 0, false, ErrInbox
    }
    switch rest[0] {
    case 0:
        return v, false, nil
    case 1:
        return v, true, nil
    default:
        return 0, false, ErrInbox
    }
}
```

Copy Hash/JSON `v, w := binary.Uvarint`; **`w <= 0`** is the only uvarint failure. Extra trailing bytes after the bit byte are an error (unlike JSON `DecodeSet`, the tail is not a payload).

The encoded snapshot **is** the raw byte string (no length prefix, no header). `BInstall` copies `blob` as `Entry.Value`. Empty blob is a legal live 0-byte bitmap (`HasBitmap` true); owner `BitSet` never produces it (first set grows to ≥ 1 byte).

### Store (`pkg/store`)

Last shipped bits in `pkg/store/entry.go`: `FlagJSON` `1<<20`, `FlagJSONSet` `1<<21`, `FlagJSONDel` `1<<22`. Next free is `1<<23`:

```go
FlagBitmap    uint32 = 1 << 23 // value is packed Redis-order bit string snapshot
FlagBitmapSet uint32 = 1 << 24 // owner-inbox: uvarint(offset) + 0/1
```

`Entry.IsBitmap()` / `IsBitmapSet()`. **No `FlagBitmapClear`** — `SETBIT 0` is a write, not a delete.

**No `bCache` / `bDirty`.** The live structure **is** `Entry.Value` (like Counter / Bloom, unlike JSON/List/Hash). Mutate under the store mutex: in-place bit flip when no grow; replace `Value` on grow. **Do not** add `flushBitmapValueLocked` — a Get/Peek helper that is easy to get wrong on the KV hot path. Peek/Get already clone `Value`.

```go
// store.Store
BSet(key string, offset uint64, bit bool, version uint64, expireAt int64, maxValue int) (applied, tooLarge bool)
BGet(key string, offset uint64) (bit bool, ok bool) // ok=false if missing name; past end → false, true
BCount(key string, start, end int) (n int64, ok bool)
BPos(key string, bit bool, start, end int) (pos int64, found, ok bool)
HasBitmap(key string) bool // live unexpired FlagBitmap
BInstall(key string, blob []byte, version uint64, expireAt int64) bool // incoming > local
```

**`BSet` (locked).** Gate first; **only a successful apply mutates**.

1. Live non-bitmap → `applied=false`; no write.
2. Live unexpired tombstone and `version <=` tombstone → `applied=false`; no write.
3. `EncodedLen(offset)` fail → `tooLarge=true`, `applied=false`.
4. `maxValue > 0 && need > maxValue` → `tooLarge=true`, `applied=false` (**before** allocate).
5. Missing / expired / tombstone with `version >` tombstone → **create**: `bitmapx.Set(nil, offset, bit)`, `Version=1` (or `tombstone.Version+1` when replacing a tombstone), `Flags=FlagBitmap`.
6. Live bitmap: `bitmapx.Set(copy-or-in-place Value, offset, bit)`. If `maxValue > 0 && len(new) > maxValue` → `tooLarge=true`, **do not mutate**.
7. Commit under this mutex: `Version = 1` on create (missing/expired) or `local.Version+1` when replacing live bitmap / a superseded tombstone; `Flags=FlagBitmap`; `Value=new`; slide `ExpireAt` if `expireAt != 0`; cost; `MoveToFront`; `evictLocked`. Incoming `version` is **not** written (tombstone-gate floor only — step 2).

In-place flip (no grow) may mutate `it.entry.Value[i]` under the mutex. Peek/handoff clone. On grow, replace with a new slice (zero-fill + copy + set).

**`HasBitmap` (locked):** copy `HasJSON`’s **flag** gate — live, unexpired, not tombstone, `IsBitmap`. Do not require `len(Value) > 0`.

**`BGet`:** `HasBitmap` + `bitmapx.Get`. Missing → `ok=false`. Past end of a live value → `bit=false`, `ok=true`.

**`BCount` / `BPos`:** `HasBitmap` + `bitmapx.Count` / `Pos`. Missing → `ok=false` (engine maps to `0` / `found=false`).

**`BInstall`:** same gate as `JInstall` / `CInstall` (`incoming > local`; equal ignore; tombstone `<=` blocks). Replace with a **copy** of `blob`. No decode beyond treating bytes as the bit string.

**Stored version (locked):**

```text
stored = local.Version+1          // live FlagBitmap, or replace tombstone after gate
stored = 1                        // BSet create (missing / expired; no local entry)
incoming version                  // tombstone-gate floor only
```

Engine **must** replicate `PeekVersion` **after** `BSet` returns applied, not the candidate it passed in. `observeVersion` that same post-write version.

Protect live `FlagBitmap` in `lruVictim` (`pkg/store/memory.go` today lists Bloom/Set/ZSet/Geo/List/Hash/Counter/JSON).

`Set` / `AcceptIfNewer` need no bitmap cache to clear.

### Engine (`pkg/engine/bitmap.go`)

Mirror `pkg/engine/json.go` **inbox + snapshot**, not Hash item-apply.

```go
func (e *Engine) BitSet(ctx context.Context, keyspaceName, name string, offset uint64, bit bool) error
func (e *Engine) BitGet(ctx context.Context, keyspaceName, name string, offset uint64) (bool, bool, error)
func (e *Engine) BitCount(ctx context.Context, keyspaceName, name string, start, end int) (int64, error)
func (e *Engine) BitPos(ctx context.Context, keyspaceName, name string, bit bool, start, end int) (int64, bool, error)
```

1. Validate mode `ModeBitmap`, `name` via `validateKey` + `validateKeyLen`.
2. `BitSet` only: `bitmapNeed(ks, offset)` — `EncodedLen` + `need > max` → `ErrValueTooLarge` **before** any store/forward.
3. **Non-owner mutate:** `bMutViaOwner` = `ApplyPut` inbox (`EncodeSet`) `Version: 1`. If `err != nil` return it. If `!applied` → `ErrInvalidArgument`.
4. **Owner / single-node `BitSet`:** `expire := e.expireAt(ks.cfg.TTL)` + `max := e.maxValueSize` (ks override) + `store.BSet(..., gate, ...)` where `gate` is `PeekVersion+1` (tombstone floor only). `tooLarge` → `ErrValueTooLarge`. `!applied` → `ErrInvalidArgument`. If applied → `ver, _ := PeekVersion(name)` then `ks.observeVersion(name, ver)` + `bReplicateSnapshot(..., ver, ...)`. **Never** fan-out the pre-lock candidate.
5. **`BitGet`:** if `HasBitmap` → `BGet`. Else `bFetchOwner` + `bitmapx.Get`.
6. **`BitCount` / `BitPos`:** if `HasBitmap` → store extract. Else fetch + extract. Missing → `0` / `found=false`.
7. Get/Put reject `ModeBitmap` (`use BitGet` / `use BitSet`) next to the ModeJSON branches in `pkg/engine/engine.go`.
8. `GetOrLoadLocal`: Peek + `IsBitmap` (after JSON, same shape). Missing → `ErrNotFound`.
9. `LocalEntries` already walks `RangeAll`; Value is always encoded.

**`ApplyPutWithRingGen` (after `IsJSON`, before `IsNegative`):**

```go
if ent.IsBitmapSet() {
    if c := e.clusterSnapshot(); c != nil && c.Ring != nil {
        if owner, ok := c.Ring.Owner(key); ok && owner.ID != "" && owner.ID != c.SelfID {
            return false, nil
        }
    }
    return e.applyBitmapSet(ks, key, ent.Value, ent.ExpireAt), nil
}
if ent.IsBitmap() {
    return e.applyBitmapInstall(ks, key, ent.Value, ent.Version, ent.ExpireAt), nil
}
```

```go
func (e *Engine) applyBitmapSet(ks *ksRuntime, name string, inbox []byte, expireAt int64) bool {
    offset, bit, err := bitmapx.DecodeSet(inbox)
    if err != nil {
        return false
    }
    if expireAt == 0 {
        expireAt = e.expireAt(ks.cfg.TTL)
    }
    max := e.maxValueSize
    if ks.cfg.MaxValueSize > 0 {
        max = ks.cfg.MaxValueSize
    }
    cur, _ := ks.store.PeekVersion(name)
    gate := cur + 1 // tombstone floor only; BSet assigns the stored version
    ok, tooLarge := ks.store.BSet(name, offset, bit, gate, expireAt, max)
    if !ok || tooLarge {
        return false
    }
    ver, _ := ks.store.PeekVersion(name) // unique version BSet just wrote
    ks.observeVersion(name, ver)
    e.bReplicateSnapshot(ks, name, ver, expireAt)
    return true
}
```

`bReplicateSnapshot`: `Peek` + `IsBitmap` + `replicate` `{Flags: FlagBitmap, Value, Version: ver, ExpireAt}` — copy `jReplicateSnapshot`.

`bFetchOwner`: copy `jFetchOwner` with `IsBitmap` + optional `BInstall`.

### Client / proto / sc

Cache RPCs (`api/proto/cache.proto`). **No Peer proto change.**

```protobuf
rpc BitSet(BitSetRequest) returns (BitSetResponse);
rpc BitGet(BitGetRequest) returns (BitGetResponse);
rpc BitCount(BitCountRequest) returns (BitCountResponse);
rpc BitPos(BitPosRequest) returns (BitPosResponse);

message BitSetRequest  { string keyspace = 1; string name = 2; uint64 offset = 3; bool bit = 4; }
message BitSetResponse {}
message BitGetRequest  { string keyspace = 1; string name = 2; uint64 offset = 3; }
message BitGetResponse { bool present = 1; bool bit = 2; }
message BitCountRequest { string keyspace = 1; string name = 2; int32 start = 3; int32 end = 4; }
message BitCountResponse { int64 count = 1; }
message BitPosRequest  { string keyspace = 1; string name = 2; bool bit = 3; int32 start = 4; int32 end = 5; }
message BitPosResponse { bool found = 1; int64 pos = 2; }
```

Whole-bitmap `BitCount`/`BitPos`: clients send `start=0`, `end=-1`. There is no `has_range` flag.

`pkg/client` wrappers match Engine. `internal/cacheserver` via existing `grpcmap.Status`. Wrong mode / bad inbox → `codes.InvalidArgument`. Engine sentinel **`ErrValueTooLarge`** (`pkg/engine/errors.go`) maps to **gRPC `InvalidArgument`** (`internal/grpcmap/status.go`).

**`cmd/sc` (required in the product PR):**

| CLI | Maps to |
|-----|---------|
| `bitset <name> <offset> <0\|1>` | `BitSet`. Offset = `strconv.ParseUint` (reject `-1`). Bit must be the token `0` or `1` (not `true`/`false`). |
| `bitget <name> <offset>` | `BitGet`. Print `0`/`1`, or `(nil)` + **exit 1** on miss. |
| `bitcount <name> [start end]` | `BitCount`. Omitted range → `0, -1`. Print the decimal count. |
| `bitpos <name> <0\|1> [start end]` | `BitPos`. Omitted range → `0, -1`. Print the bit index, or `(nil)` + **exit 1** if `!found`. |

Session keyspace same as get/put. `printUsage`, REPL help, `cmd/sc/README.md` updated.

OpenAPI `info.version` **0.8.0 → 0.9.0** in `api/openapi/cache.openapi.yaml` **and** the `docs/api/cache.openapi.yaml` snapshot.

### Example (same implementation PR)

Small **`examples/bitmap`** walkthrough — constant name, like `examples/json` (`user` / `doc`). Testable; does not bloat.

| Piece | Lock |
|-------|------|
| Cluster | `testcluster` 3 nodes, ephemeral ports |
| Keyspace | **`flags`**, `ModeBitmap`, RF=2 |
| Name | **`seen`** (constant) |
| Shape | `main.go`, `demo.go`, `demo_test.go`, `README.md` |

`runDemo`:

1. Why not Put / Bloom / Hash / Counter (whole-blob LWW; approx membership; field map; one int64).
2. `BitSet(ctx, ks, "seen", 0, true)` then `BitSet(..., 8, true)` — two offsets, one name.
3. `BitGet(..., 0)` / `BitGet(..., 8)` are `true, ok=true`.
4. `BitGet(..., 3)` is `false, ok=true` (in-range zero).
5. `BitGet(..., 100)` is `false, ok=true` (past end of a 2-byte live bitmap).
6. `BitGet` of a missing name → `ok=false`.
7. `BitCount(..., 0, -1)` is `2`; `BitCount(..., 0, 0)` is `1` (byte 0 only).
8. `BitPos(..., true, 0, -1)` is bit 0; `BitPos(..., true, 1, 1)` is bit 8.
9. `BitSet(..., 0, false)` + `BitSet(..., 8, false)` — name still live (`HasLocal`); `BitCount` is 0.
10. Oversize offset (`8*MaxValueSize`) → `ErrValueTooLarge`.
11. Wait RF=2 `HasLocal` on `seen` (copy json/hash `waitLocals`).
12. `Delete("seen")` then `BitGet` → `ok=false`.

README: `sc bitset seen 0 1` / `sc bitget seen 0`.

`go test ./examples/bitmap` requires the `OK:` line (copy `examples/json/demo_test.go`).

No default demo keyspace on the node.

### PLAN.md (same PR)

| Section | Change |
|---------|--------|
| §5 **Get** sentence | Today ends `ModeJSON`. **Add ModeBitmap**. |
| §5 verb table | New row: `BitSet` / `BitGet` / `BitCount` / `BitPos` — ModeBitmap only; Redis bit order; byte windows; owner apply + **FlagBitmap snapshot**; replica `BitGet` may lag; empty-until-delete; Get/Put **invalid**; `BitSet` ACK-only (no old bit). |
| §5 **Delete** row | Add bitmap names. |
| §5 **Read-your-writes** | Add owner `BitSet`. |
| §7 mode table | `ModeBitmap` row: verbs `BitSet`, `BitGet`, `BitCount`, `BitPos`, `Delete(name)`; wire `FlagBitmap` snapshot; owner-inbox `FlagBitmapSet`. |
| §7 subsection | New `ModeJSON`-shaped `ModeBitmap` (named packed bits; `pkg/bitmapx`; Redis MSB-first; snapshot fan-out). |
| §8 diagram | Mention bitmap next to list/hash/json if the label lists types. |
| §9.6 title / handoff | Add `FlagBitmap`. Mutate: BitSet is List-class snapshot — **not** step-3 item flag on replicas. Read: `BitGet`/`BitCount`/`BitPos` local-if-`HasBitmap` else GetOrLoad. |
| §10 | Store: packed `Value` (no dirty cache); protect `FlagBitmap`. |
| §11 Engine + Mode const | `BitSet`, `BitGet`, `BitCount`, `BitPos`; `ModeBitmap`. |
| §12 catalog | ModeBitmap + `pkg/bitmapx` + `sc bitset`/`bitget`/`bitcount`/`bitpos`. |
| §14 Cache | `rpc BitSet`, `BitGet`, `BitCount`, `BitPos`. Peer: **no** new RPC. |
| §25 | Structured-types row includes ModeBitmap. |

Do not add Bitmap to §1 non-goals. `BITOP` is already covered by “multi-key atomicity”.

### Benchmarks (v1 micros)

CI already picks `Benchmark(Store|Engine)*`. Ship:

| Benchmark | Notes |
|-----------|--------|
| `BenchmarkStoreBitSet` | fixed name, rotating small offsets |
| `BenchmarkEngineBitSet` | single-node ModeBitmap |
| `BenchmarkEngineBitGetHit` | bit extract; not a 0-alloc target (bool copy is cheap) |
| `BenchmarkEngineBitCount` | whole-bitmap OnesCount |
| `BenchmarkEngineBitGetHitParallel` | parallel BitGetHit |
| `BenchmarkEngineBitSetParallel` | parallel BitSet (same name; owner mutex) |

Not in scbench smoke. KV Get-hit / StoreGetHit **allocs/op must not rise**. Local bench of `BenchmarkEngineGetHit` / `BenchmarkStoreGetHit` stays in the plan. **No new Peek/Get flush helper** is the alloc-safety plan.

## Tests (write these first, after approval)

| Test | Package | Asserts |
|------|---------|---------|
| Bit order | `pkg/bitmapx` | offset 0 sets `buf[0]&0x80`; offset 7 sets `buf[0]&0x01`; offset 8 sets `buf[1]&0x80`; Bloom-style LSB packing **fails** this test |
| Set grow / zero-fill | `pkg/bitmapx` | `Set(nil, 8, true)` → 2 bytes, byte 0 is `0x00`, byte 1 is `0x80` |
| Get past end | `pkg/bitmapx` | `Get(oneByte, 8)` is false |
| Count window | `pkg/bitmapx` | bits at 0 and 8; `Count(0,-1)==2`; `Count(0,0)==1`; `Count(1,1)==1`; `Count(2,2)==0`; `Count(-1,-1)==1` |
| Count empty / inverted | `pkg/bitmapx` | `start > end` after normalize → 0; `n==0` → 0 |
| Pos | `pkg/bitmapx` | first `1` at 0; `Pos(1, 1, 1)` → 8; all-zero looking for `1` → `found=false`; all-`0xff` looking for `0` with `0,-1` → `found=false` (no implicit past-end bit) |
| EncodeSet / DecodeSet | `pkg/bitmapx` | `w<=0` error; trailing junk error; bit byte not 0/1 error; offset `0` + bit `false` round-trips |
| EncodedLen | `pkg/bitmapx` | offset 0 → 1; offset 7 → 1; offset 8 → 2; huge offset → `ok=false` |
| BSet / BGet | `pkg/store` | create; overwrite 1→0; grow; past-end get `ok=true` bit false |
| BSet too large | `pkg/store` | `need > maxValue` → `tooLarge`, previous bytes **and cost** unchanged |
| BSet missing + bit 0 | `pkg/store` | creates zero-filled live bitmap; `HasBitmap` true |
| BSet stored version | `pkg/store` | create → `Version==1`; second `BSet` → `2`; incoming gate `99` on a v2 still stores `3` |
| BInstall version gate | `pkg/store` | equal ignore; lower ignore; tombstone blocks stale |
| HasBitmap vs all-zero | `pkg/store` | after last `1` cleared, `HasBitmap` true; `BCount` 0 |
| BitSet / BitGet / BitCount / BitPos | `pkg/engine` | create, two offsets, windows, past-end, missing name |
| BitGet copy / isolation | `pkg/engine` | later `BitSet` does not change a previously returned bool (trivial) — store unchanged under concurrent read |
| Oversize offset | `pkg/engine` | `offset` such that `need > MaxValueSize` → engine `ErrValueTooLarge` (gRPC InvalidArgument via grpcmap); no mutate |
| Wrong mode | `pkg/engine` | BitSet on ModeJSON / Get on ModeBitmap / JsonSet on ModeBitmap / BitGet on ModeHash |
| Missing | `pkg/engine` | BitGet `ok=false`; BitCount `0`; BitPos `found=false`; BitSet missing + 0 creates |
| Empty-until-delete | `pkg/engine` | clear last `1`; `HasLocal` until `Delete` |
| BitSet after Delete | `pkg/engine` | recreates bitmap |
| Length never shrinks | `pkg/engine` / `pkg/store` | `BitSet(100, true)` then `BitSet(100, false)`; `BitGet(50)` still `ok=true` (in the grown length) |
| TTL slide | `pkg/engine` | owner BitSet refreshes `ExpireAt` |
| Mode.String | `pkg/keyspace` | `ModeBitmap.String() == "Bitmap"` |
| GetOrLoadLocal | `pkg/engine` | Peek + `IsBitmap`; missing → NotFound |
| Replica snapshot | `pkg/engine` | RF=2 `HasLocal`; replica BitGet matches after fan-out |
| Non-replica BitGet | `pkg/engine` | third node without `HasLocal` still BitGets via GetOrLoad |
| Non-owner BitSet | `pkg/engine` | owner bitmap matches; uses inbox ApplyPut (not a client-built snapshot) |
| Replica ignores inbox flags | `pkg/engine` | ApplyPut `FlagBitmapSet` on non-owner returns applied=false; local bits unchanged |
| Hint after peer down | `pkg/engine` | two different-offset BitSets while B down; after flush **both** bits present |
| Concurrent BitSet versions | `pkg/store` / `pkg/engine` | two overlapping owner BitSets of different offsets; stored/fan-out versions **differ**; replica `BInstall` of both snapshots in version order keeps **both** bits |
| Tombstone blocks stale snapshot | `pkg/engine` | FlagBitmap v1 after Delete ignored |
| Client gRPC | `pkg/client` | BitSet/BitGet/BitCount/BitPos/Delete against cacheserver |
| `cmd/sc` CLI | `cmd/sc` | `bitset` 0/1 only; `bitget` `(nil)` + exit 1; `bitcount` omitted range; `bitpos` `(nil)` + exit 1 |
| Example | `examples/bitmap` | `OK:`; constant `seen`; two offsets + past-end + empty-until-delete + HasLocal |

**Hint after peer down (locked):** List/JSON shape. Two owner `BitSet`s (offset `0` and offset `8`) while B is down; start B; wait for hint flush. Assert `engB.BitGet(0)` and `BitGet(8)` both `ok && bit`. **No** third mutate required if flush is observed; a third `BitSet(16)` “to poke the pool” is allowed only if the assert still requires **both original bits**, not only the poke.

## Bench risk

**Hot path?** No — KV Get must not call bitmap **logic**. Do **not** add a `flushBitmapValueLocked` on `Get`/`Peek` (Value is always encoded). Isolate ModeBitmap in `Get`/`Put`/`ApplyPut`/`lruVictim` so Get-hit allocs stay flat.

Local bench before commit: `BenchmarkEngineGetHit` / `BenchmarkStoreGetHit` allocs/op must not rise.

Smoke: merge bar ±10% shared cells. Get-hit / StoreGetHit **allocs/op** flat.

New micros: first-merge baseline only.

## Implementation order (after approval)

1. Design approved in chat (`looks good` / `do it` / `implement`).
2. Failing tests from the table (TDD).
3. `pkg/bitmapx` + store flags + `BSet`/`BGet`/`BCount`/`BPos`/`HasBitmap`/`BInstall` + LRU protect. **No** Peek/Get flush helper.
4. Engine + ApplyPut branches (after JSON, before Negative) + GetOrLoadLocal + Get/Put reject.
5. Cache proto / `pkg/client` / cacheserver + **`cmd/sc bit*`**. **No** peer proto.
6. Cluster + List-shaped hint test (both offsets present).
7. Micros.
8. **`examples/bitmap`** (printed demo + `go test`).
9. Product docs in the **same PR** (PLAN §5/§7/§9.6/§11/§14 — not “§5b”).
10. Local bench of flagged micros + Get-hit / StoreGetHit allocs.
11. Commit on `feat/mode-bitmap` → PR → CI bench comment → merge only when the user says **merge**.

## Key Decisions

| Decision | Lock | Rationale |
|----------|------|-----------|
| Name | `Bitmap` / `ModeBitmap` / `BitSet` `BitGet` `BitCount` `BitPos` | Same naming as Geo/Hash/JSON. Not ModeBitMap / ModeBitset |
| API surface | four verbs + `Delete(name)` | Smallest complete Redis bitmap; skip BITOP/BITFIELD |
| Fan-out | **`FlagBitmap` snapshot only** | `hintID` is `(ks, name)`; item flags would drop the other offset |
| hintID | **unchanged** | Out of scope |
| Owner-inbox | one `FlagBitmapSet` `{offset, bit}` via ApplyPut | ACK-only; List/JSON pattern; `SETBIT 0` is a write, not Del |
| Replica apply | install snapshot only; **ignore** inbox flags | Same as List LPush / JSON Set |
| Peer RPCs | **none new** | No return payload; reads are extract / GetOrLoad |
| `BitSet` return | **no old bit** | Redis SETBIT returns it; we would need a Peer RPC |
| Reads | `HasBitmap` → local extract; else GetOrLoad | Match GeoPos / Hash / JSON; document replica lag |
| Missing name | `BitGet` `ok=false` (not Redis 0) | SuperCache present-bit; live all-zero is present |
| Past-end `BitGet` | `bit=false`, `ok=true` | Redis GETBIT 0, plus present-bit on the name |
| Windows | Redis **byte** offsets; negatives like `listx.Range`; whole = `0,-1` | Compatible with Redis BITCOUNT/BITPOS; not bit offsets |
| `BitPos` implicit tail | **no** — stored bytes only | Redis “end specified” semantics; `0,-1` is specified |
| Empty-until-delete | clear last `1` leaves live (possibly all-zero) bitmap; length does not shrink | Same as Hash/List/Counter/JSON |
| Grow | zero-fill to `offset/8+1`; never shrink | Redis string |
| Size | encoded `> MaxValueSize` → `ErrValueTooLarge`, no mutate; initiating node rejects offset first | DoS bound (SETBIT 2^32 = 512 MiB) |
| Bit order | Redis MSB-first of byte 0 | Cite SETBIT docs; **not** Bloom LSB-first |
| Encoding | raw dense bytes | Snapshot **is** Value; no header |
| TTL | scheme B (slide on owner BitSet) | Match Hash/List/Counter/JSON |
| Package | `pkg/bitmapx` | Avoid `bitmap` / `bitx` clash |
| Flags | `1<<23` snapshot, `1<<24` inbox | Next free after FlagJSONDel `1<<22` |
| Store | mutate `Value` in place / grow; **no** dirty cache / flush | Get-hit allocs must not rise |
| ApplyPut slot | after JSON, before Negative | |
| Persistence | none | Same as every other type |
| sc | `bitset`/`bitget`/`bitcount`/`bitpos` | 0/1 tokens; miss → `(nil)` exit 1 |
| Demo KS | **not** in this PR | JSON already added `doc` |
| Example | **same PR**, `examples/bitmap`, constant `seen` | Testable; user asked for the type |
| `BITOP` / `BITFIELD` / return-old-bit | **not v1** | Multi-key / extra RPC / extra surface |
| Product docs | same implementation PR | PLAN §5/§7/§9.6/§11/§14 |
| CLUSTER_FLOWS | table + E11 `BitGet` + Client `OPS`; do **not** put BitSet on E10 | E10 is item-level; Bitmap is List-class snapshot |

## PR Plan

SuperCache hard rule: **one approved design per PR**. **Single implementation PR** on `feat/mode-bitmap`: tests + code + proto + sc + product docs + **`examples/bitmap`**. Do **not** split Bit* verbs. Example stays in the type PR (constant name, small). Optional follow-up only for a node demo keyspace.

| # | Title | Files / components | Depends on | Description |
|---|-------|--------------------|------------|-------------|
| 1 | **feat: ModeBitmap (named packed bit vector)** | `pkg/bitmapx`, `pkg/store` (flags, B*, LRU), `pkg/engine/bitmap.go` + ApplyPut + Get/Put + GetOrLoadLocal, `pkg/keyspace` (`ModeBitmap`), `api/proto/cache.proto` + gen, `pkg/client`, `internal/cacheserver`, `cmd/sc`, `examples/bitmap`, tests + micros, **product docs** (`docs/API.md`, OpenAPI 0.8.0→0.9.0 + snapshot, `PLAN.md` §5/§7/§9.6/§10/§11/§12/§14/§25, `docs/OPERATIONS.md`, `docs/CLUSTER_FLOWS.md` table+E11+OPS, `README.md`, `docs/design/README.md`, sc help) | Design **approved** in chat | One PR. No default Bitmap demo keyspace. |
| 2 | **optional follow-up: `flags` demo KS** | `cmd/supercache-node -demo-keyspace` | PR 1 merged | Only if we want `sc -keyspace flags bitget seen 0` against a stock node. |

Usually we do **not** open a design-docs-only PR; this draft lives on `feat/mode-bitmap` with the implementation after approval.

## Product docs to update (implementation PR)

| File | Change |
|------|--------|
| `docs/API.md` | Mode table + Bitmap RPC section (present-bit; byte windows; empty-until-delete; no old-bit return) |
| `api/openapi/cache.openapi.yaml` | BitSet / BitGet / BitCount / BitPos; bump `info.version` 0.8.0 → 0.9.0 |
| `docs/api/cache.openapi.yaml` | same snapshot copy |
| `PLAN.md` | §5 Get-invalid + Delete + RYOW + verb row; §7/§9.6 `FlagBitmap` + List-class mutate; §10/§11/§12/§14/§25 |
| `docs/OPERATIONS.md` | mode table + consistency cheatsheet (snapshot + inbox flag + replica BitGet lag); no default demo KS |
| `docs/CLUSTER_FLOWS.md` | structured-types table **ModeBitmap** (snapshot like List). Extend E11 with `BitGet`. Client `OPS` line: add `BitSet` / `BitGet` / `BitCount` / `BitPos`. Do **not** add new E10/E11 nodes. Do **not** add `BitSet` to E10 (item-level). |
| `README.md` | modes, packages, `sc bitset` / `bitget` / `bitcount` / `bitpos`, example pointer |
| `cmd/sc/README.md` + `printUsage` / REPL | bitset / bitget / bitcount / bitpos |
| `docs/design/README.md` | this design once approved/shipped |

## Security & privacy

No new auth surface. Document names/offsets/bits ride the existing Cache gRPC (TLS) and Peer ApplyPut (mTLS). Do not log bit-string bodies (can be PII-shaped flags). Limits (`MaxKeyLen` on name; `MaxValueSize` on encoded length / offset; `MaxBytes` may overshoot one live bitmap) are the DoS bound — this is the type where an unbounded offset is a real allocation bomb. Same tenant isolation: keyspace config is local and must match across nodes.

Owner **store mutex** serializes `BSet` **and** assigns `stored = local.Version+1` (or `1` on create). There is **no** extra engine owner-mutate mutex — same as Hash/List/JSON. Two clients mutating the same name apply in store-lock order; successive snapshots have distinct versions so `BInstall` cannot drop the later one. A client that retries `BitSet` after a timeout may double-apply (last write wins at that offset). SuperCache does not have request ids.

## Observability

Reuse existing fan-out / hint / `staleSkip` / store hit-miss counters. No new required metrics in v1. Optional later: bitmap-byte histogram. Wrong-mode and oversize map through `grpcmap`.

## Rollout / rollback

- Opt-in: nothing happens until an operator registers a `ModeBitmap` keyspace on **every** node.
- Deploy the Bitmap build cluster-wide first; then `UpdateKeySpace`.
- Rollback: stop writing BitSet; `Delete(name)` or drop the keyspace. No on-disk format.
- No feature flag (mode is the flag).
- Example is in-process only; it does not change node defaults.

## Open points (resolved)

1. **Naming:** **ModeBitmap** / **BitSet** / **BitGet** / **BitCount** / **BitPos**. Not ModeBitMap / ModeBitset.
2. **Fan-out:** **snapshot `FlagBitmap` only**; inbox flags never go to replicas.
3. **Return path:** none; ApplyPut inbox + snapshot (List push). `BitSet` does **not** return the old bit.
4. **Present-bit:** missing **name** → `BitGet` `ok=false`; live past-end → `ok=true`, bit `false`. Not Redis “missing is 0”.
5. **Windows:** Redis **byte** offsets; negatives like List; whole bitmap `0,-1`; no Redis 7 `BIT` unit.
6. **`BitPos` tail:** stored bytes only; all-`1`s + search `0` → `found=false`.
7. **Empty-until-delete:** last `1`→`0` leaves live bitmap; length never shrinks; missing + `BitSet(0)` creates.
8. **Size:** encoded `> MaxValueSize` aborts; initiating node rejects offset first. Engine `ErrValueTooLarge` → gRPC InvalidArgument.
9. **Version:** **one rule** — `stored = local.Version+1` (or `1` on create) under the store mutex; incoming `version` is tombstone-gate only; engine fans out `PeekVersion` after the write.
10. **Bit order:** Redis MSB of byte 0. Not Bloom LSB.
11. **`BitPos`:** **in v1** (cheap local extract; no extra flag/RPC).
12. **Example:** **same PR**, `examples/bitmap`, name `seen`, keyspace `flags`.
13. **Demo KS:** **not** in this PR.
14. **CLUSTER_FLOWS:** table + E11 `BitGet` + OPS; BitSet not on E10.
15. **Hint test:** both offsets present after snapshot replay.
16. **Flags:** `1<<23` / `1<<24`.
17. **Store cache:** none; Value is the packed string; no Get/Peek flush.

## Rejected alternatives

| Idea | Why not |
|------|---------|
| CacheOnly blob rewritten by the app | Concurrent offset updates LWW the whole string |
| ModeBloom | Approximate; not addressable; LSB packing; fixed *m* |
| Hash fields `"0"`/`"1"` | Not packed; `BITCOUNT` is a scan; not Redis bit order |
| Item-level `FlagBitmapSet` to replicas | Lost hints drop the other offset; `hintID` is `(ks, name)` |
| Hash-class item-level + “don’t require both bits” | Accepts silent data loss; we can snapshot instead |
| Change hintID in this PR | Touches global hint path; separate design |
| Client-built snapshot ApplyPut to owner | Two writers clobber; owner must apply the **op** |
| New Peer RPC / return old bit | BitSet is ACK-only; ApplyPut suffices |
| `BITOP` / `BITFIELD` in v1 | Multi-key / extra surface; PLAN already excludes multi-key |
| Bit-offset `BITCOUNT` windows | Surprises Redis users; Redis default is bytes |
| Sparse / Roaring | Different product; MaxValueSize already caps dense n |
| Shrink on clear | Redis does not; empty-until-delete is about the **name** |
| Default Bitmap demo keyspace | JSON already added `doc` |
| `BSet` pre-lock `bNextVersion` | Two version spaces; replica `BInstall` can drop the later snapshot |
| Peek/Get `flushBitmap*` helper | Unnecessary (Value is encoded) and a Get-hit alloc risk |
| Copy Bloom bit packing | Wrong endianness vs Redis SETBIT |

## References

- [WORKFLOW.md](../WORKFLOW.md) (template + product docs same PR)
- [AGENTS.md](../../AGENTS.md)
- [PLAN.md](../../PLAN.md) §5, §7, §9.6, §11, §14
- [API.md](../API.md)
- Redis [SETBIT](https://redis.io/docs/latest/commands/setbit/) (bit 0 = MSB of byte 0), [GETBIT](https://redis.io/docs/latest/commands/getbit/), [BITCOUNT](https://redis.io/docs/latest/commands/bitcount/) (byte windows), [BITPOS](https://redis.io/docs/latest/commands/bitpos/)
- Shipped types: [ModeJSON](./2026-08-21-mode-json.md) (snapshot + inbox + version lock — **copy this**), [ModeList](./2026-08-19-mode-list.md) (snapshot + owner-inbox flags), [ModeCounter](./2026-08-20-mode-counter.md) (snapshot; return-path lesson: we do **not** need a Peer RPC here), [ModeHash](./2026-08-20-mode-hash.md) (item-level caveat we refuse), [ModeBloom](./2026-08-11-bloom-filter.md) (bits, but not this type)
- `pkg/store/entry.go` (`FlagJSONDel` `1<<22`; next free `1<<23`)
- `pkg/keyspace/config.go` (`ModeJSON` last iota; next is `ModeBitmap`)
- `internal/peer/hints.go` `hintID` (only `FlagBloomAdd` is per-item)
- `pkg/engine/json.go` (`applyJSONSet`, `jReplicateSnapshot`, replica ignore of inbox flags)
- `pkg/engine/list.go` (`applyListLPush`, `lReplicateSnapshot`)
- `pkg/engine/engine.go` `ApplyPutWithRingGen` (insert Bitmap after JSON, before Negative)
- `pkg/bloom/bloom.go` (LSB-first — do not copy)
- `pkg/listx/listx.go` `Range` (negative window clamp)
- Store: `pkg/store/memory.go` (`JSet` version rule, `lruVictim`, `HasJSON`)
- `examples/json/` (in-process 3-node + README + `go test`)
- `internal/grpcmap/status.go` (`ErrValueTooLarge` → `codes.InvalidArgument`)
