# Stream type (`ModeStream`)

**Author:** SuperCache
**Status:** approved (chat: continue with coding now)
**Branch (later):** `feat/mode-stream`
**Date:** 2026-09-08

## Overview

SuperCache has an ordered **list** (`ModeList`: push/pop at the ends, no stable ids). It cannot store an **append-only log** you address by id: “add this event, then read everything after `1710000000000-0`.”

`ModeStream` is a Redis-Streams-like **named append-only log**. Each entry has a monotonic id (`millis-seq`) and a single `[]byte` payload (the client owns the encoding). v1 is owner-serialized, **snapshot fan-out**, feature-folder layout (`pkg/stream` + `pkg/stream/eng`). It is **not** consumer groups, not blocking `XREAD`, and not RESP wire parity.

This is new product behavior on the data path. Full path in [WORKFLOW.md](../WORKFLOW.md). **No tests or implementation until this draft is approved** (`looks good` / `do it` / `implement`).

## Problem

Closest existing types fail the job:

| Mode | Why it is not a stream |
|------|------------------------|
| `ModeList` | No ids. `LPop` **removes**. `LRange` is by index, which shifts. Two writers `RPush` is fine, but you cannot resume from a cursor. |
| `ModeZSet` | Score is a caller-written float. Not an append log; `ZRem` deletes; ties by member bytes. |
| `ModeJSON` / CacheOnly `Put` | Whole-blob LWW. Concurrent appends clobber. |
| `ModeHash` | Field map, no order, no range-after-id. |

There is no verb that appends an event and returns a stable id, then ranges by id. Callers today either misuse List as a queue or keep Redis Streams outside SuperCache.

Dedicated use: small in-process event / audit / “last N messages” cache — RAM-only, owner-serialized, RF copies of one blob. Not a Kafka.

## Non-goals

- Consumer groups (`XGROUP`, `XREADGROUP`, `XACK`, PEL, `XCLAIM`).
- Blocking `XREAD` / `XREAD BLOCK`.
- `XADD` with a **caller-chosen** id (v1 is `*` only). No `MINID`, no `NOMKSTREAM`.
- Multi-stream `XREAD`, `XINFO`, `XREVRANGE` extras beyond the table, `XAUTOCLAIM`.
- Per-entry TTL, persistence / WAL, Redis dump import.
- Changing RF, tombstones, hint identity `(ks, name)`, or any existing mode contract.
- New scbench YAML cells.
- A Lab widget or `examples/stream` walkthrough in this PR (follow-up). `cmd/sc` stream verbs **are** in this PR.
- Mixed-version mesh (same as CMS / VectorSet).

## Contract

### API (new; existing verbs unchanged)

On `pkg/engine.Engine`, `pkg/client.Client`, Cache proto, and `cmd/sc` (same PR):

| Call | Meaning |
|------|---------|
| `XAdd(ks, name, payload []byte) (id string, err error)` | Append. Creates the stream if missing. Returns the minted id. |
| `XRange(ks, name, start, end string, count int) ([]StreamEntry, error)` | Inclusive id window, oldest first. Missing → empty, nil error. |
| `XRevRange(ks, name, start, end string, count int) ([]StreamEntry, error)` | Inclusive, newest first. |
| `XLen(ks, name) (n int, present bool, err error)` | Entry count. Missing → `present=false`, `n=0`. |
| `XDel(ks, name, id string) error` | Remove one id if present. No-op if missing. ACK-only. |
| `XTrim(ks, name, maxLen int) error` | Keep the **newest** `maxLen` entries. `maxLen < 0` invalid. ACK-only. |
| `Delete(ks, name)` | Existing Delete: whole-stream tombstone. |

```go
type StreamEntry struct {
    ID      string // "millis-seq", e.g. "1710000000000-0"
    Payload []byte // opaque; client encodes JSON / proto / raw
}
```

- `name` is the stream identity; ring owner = `Owner(name)`.
- `payload` is opaque `[]byte`. Empty payload is **allowed** (present empty). Size checked against `MaxValueSize`. The engine does not parse it.
- `start` / `end`: Redis-ish `-` (min) and `+` (max). Bare millis (`1710000000000`) means that ms, seq 0 (start) or max seq (end). Invalid id syntax → `ErrInvalidArgument`.
- **Exclusive start:** a leading `(` on `start` (e.g. `(1710000000000-0`) skips that id — “everything after this cursor.” No separate `XRead` verb. `end` is always inclusive. `(` on `end` is invalid.
- `count <= 0` means **no cap**. `count > 512` is clamped to **512**.
- Get/Put on `ModeStream` → `ErrInvalidArgument`. Stream verbs on any other mode → `ErrInvalidArgument`.
- **Empty after last `XDel` / `XTrim 0`:** live empty stream (`XLen` 0, `present=true`) until `Delete(name)`.
- **XAdd returns the id** (unlike ACK-only `BitSet` / `HLLAdd`). Needed so a reader can resume.

### IDs

Owner-only mint under the store mutex (Redis `*`):

- `id = fmt.Sprintf("%d-%d", ms, seq)`
- `ms` = `time.Now().UnixMilli()`. If `ms < lastMs`, keep `lastMs` and bump `seq` (clock went backward).
- If `ms == lastMs`, `seq = lastSeq+1`. Else `seq = 0`.

v1 does **not** accept a client-supplied id. Resume: `XRange(ks, name, "("+lastID, "+", n)`. Inclusive start is the default (no `(`).

### Keyspace

New mode `keyspace.ModeStream`.

| Field | Role |
|-------|------|
| `MaxBytes` | LRU store budget. Live `FlagStream` is **not** evicted (`lruVictim` skip, same as HLL/CMS/VectorSet). |
| `TTL` | Whole stream |
| `NegativeTTL` | Ignored |
| `TombstoneTTL` | On `Delete(name)` |
| `ReplicationFactor` | Same as other modes |
| `StreamMaxLen` | **0** = no auto-trim. `>0` = after each `XAdd`, drop oldest until `len <= StreamMaxLen`. Cap **4096**. |

Hard cap **4096** entries even if `StreamMaxLen` is 0 (MaxBytes still applies). Over cap → `XAdd` is `ErrInvalidArgument` unless auto-trim can make room.

### Replication

Same story as List: append/trim/del **do not commute**. Item-level fan-out under `(ks, name)` hint coalesce would drop an earlier `XAdd`.

| Hop | Payload |
|-----|---------|
| Client → any node → **owner** | inbox (`A` add / `D` del-id / `T` trim) |
| Owner → RF−1 | **full snapshot** (`S`) |
| Handoff | snapshot if version ≥ local |

Reads: replica with a local stream answers `XRange` / `XLen` **locally** (may lag). Missing name on a replica → owner `GetOrLoad` snapshot; install only if this node `holdsReplica`. Non-replica does not install.

**XAdd from a non-owner** must return the id. `ApplyPut` is ACK-only. Locked: new Peer RPC `StreamAdd` (same job as `CounterIncr` / `ListPop`). `XDel` / `XTrim` stay inbox `ApplyPut` (ACK-only).

### Flags — `uint32` is full

`FlagVectorSet` is `1<<31`. There is **no** free bit on `store.Entry.Flags uint32`.

Locked:

- `Entry.Flags` becomes **`uint64`**.
- Peer proto `Entry.flags` field 4 becomes **`uint64`** (proto3 varint; values `< 2^32` still decode on old binaries, but mixed-version mesh is **not** supported).
- `FlagStream uint64 = 1 << 32`
- Inbox vs snapshot: **`Value[0]`** like VectorSet — do **not** add `FlagStreamAdd`.

| `Value[0]` | Meaning | Replicas |
|------------|---------|----------|
| `S` | full snapshot | install LWW |
| `A` | owner-inbox XAdd (raw payload) | **ignore** |
| `D` | owner-inbox XDel (id bytes) | **ignore** |
| `T` | owner-inbox XTrim (uvarint maxLen) | **ignore** |

`IsStream() bool` = `Flags&FlagStream != 0`.

This is the one-time Flags widening. Future modes use `1<<33` etc.

### Present-bit

Missing name ≠ empty range that looks like “no events, but the stream exists.” `XLen` carries `present`. `XRange` on a missing name is empty + nil error (same as `VSim`). Callers that need present use `XLen`.

## Approach

Feature folders (same as VectorSet / List):

```
pkg/stream/            # codec + in-memory log
  codec.go
  codec_test.go
  eng/                 # owner write, GetOrLoad, StreamAdd
    host.go
    local.go
    cluster.go
pkg/store/stream.go    # Memory XAdd / XRange / … under mutex
pkg/engine/stream.go   # thin public verbs
```

Codec (snapshot `S`):

```
'S' | uvarint lastMs | uvarint lastSeq | uvarint n
    | repeated { uvarint ms | uvarint seq | uvarint plen | payload }
```

In-place mutate under the store mutex (HLL/CMS/VectorSet class): decode → append/trim → encode → `Flags=FlagStream`, bump version. **No** dirty cache.

Owner `XAdd`: mint id → append → optional `StreamMaxLen` trim → snapshot fan-out → return id.

`sc`:

```text
sc -keyspace events xadd logs * '{"msg":"hello"}'
sc -keyspace events xlen logs
sc -keyspace events xrange logs - + 10
sc -keyspace events xrange logs '(1710000000000-0' + 10
sc -keyspace events xrevrange logs - + 10
sc -keyspace events xdel logs 1710000000000-0
sc -keyspace events xtrim logs 100
```

`*` is required in the `xadd` argv (documents “auto id”). No `examples/stream` in this PR.

Docs in the same PR: `docs/API.md`, PLAN §7 row, `docs/design/README.md` shipped table, OpenAPI cache ref.

### Rejected alternatives

| Idea | Why not |
|------|---------|
| Reuse `ModeList` + document “don’t pop” | No ids, index shifts, pop is in the API. |
| Item-level `FlagStreamAdd` fan-out | `hintID` is `(ks, name)` — second add drops the first. Same as List. |
| New `FlagStreamAdd` bits on uint32 | **No bits left.** |
| Consumer groups in v1 | Separate product (PEL, ack, claim). Not a cache-shaped first cut. |
| ACK-only `XAdd` (no returned id) | Callers cannot resume. Redis `XADD` returns the id. |
| Caller-chosen ids | Ordering / clock fights; owner mint is enough. |
| Lab widget / `examples/stream` in this PR | Second feature. |
| Redis field/value pairs per entry | Client has less control; they encode JSON/proto themselves. |

## Tests (write these first, after approval)

| Test | Package | Asserts |
|------|---------|---------|
| `TestStreamXAddRangeLen` | `pkg/stream` | XAdd two entries; XLen 2; XRange `-` `+` both, order oldest-first; ids `ms-seq` |
| `TestStreamXAddSameMilliSeq` | `pkg/stream` | Two adds in the same ms → `…-0` then `…-1` |
| `TestStreamXRevRangeAndCount` | `pkg/stream` | Newest first; `count=1` is the newest |
| `TestStreamXDelAndEmptyUntilDelete` | `pkg/stream` | XDel one; XLen 0 + present after last del; Delete → present=false |
| `TestStreamXTrimMaxLen` | `pkg/stream` | Trim 1 keeps newest |
| `TestStreamAutoTrim` | `pkg/stream` | `StreamMaxLen=2`; third XAdd drops oldest |
| `TestStreamWrongMode` | `pkg/stream` | Get/Put on ModeStream and XAdd on CacheOnly → invalid argument |
| `TestStreamExclusiveStart` | `pkg/stream` | `XRange` start `(id` skips that id |
| `TestStreamClusterRF` | `pkg/stream` | RF=2; every node XRange same order; non-replica does not `HasLocal` |
| `TestStreamAddReturnsIdFromNonOwner` | `pkg/stream` | XAdd via non-owner returns the same id the owner would mint; replica fills |
| `TestFlagStreamUint64Wire` | `pkg/store` / peer | ApplyPut with `FlagStream` (bit 32) installs; `IsStream` true |
| `TestStreamNotEvicted` | `pkg/store` | MaxBytes=1 still keeps live FlagStream |

`cmd/sc` stream CLI tests like `hash_cli_test.go`.

## Bench risk

- **Hot path?** Get-hit / StoreGetHit: **no** if Stream is not on those benches.
- Flags `uint32`→`uint64` widens `store.Entry` by 4 bytes. **Could** move StoreGetHit ns/op; **must not** raise Get-hit / StoreGetHit **allocs/op**.
- No new scbench cells.
- Gate: shared smoke ±10%; Get-hit allocs flat.

## Key Decisions

1. **Append-only log with Redis-like ids and one `[]byte` payload** — client owns the encoding.
2. **Snapshot fan-out** — appends do not commute under hint coalesce.
3. **`XAdd` returns the id** — new Peer `StreamAdd`.
4. **`Flags uint64` + `FlagStream = 1<<32` + `Value[0]` inbox** — uint32 is full.
5. **No consumer groups / blocking read in v1.**
6. **Feature folders + `sc` in one PR.** No example binary.

## Open Questions

None. Closed in chat: exclusive start is leading `(`; payload is a single `[]byte`.

## PR Plan

One design → one PR.

### PR 1 — `feat: ModeStream append-only log`

- **Title:** feat: ModeStream (Redis-like append-only log)
- **Branch:** `feat/mode-stream`
- **Files:** `pkg/store` Flags uint64 + stream methods; `api/proto` cache + peer; `pkg/stream` + `eng`; `pkg/engine/stream.go`; `pkg/client`; `cmd/sc`; API/PLAN/OpenAPI; this design status → approved.
- **Dependencies:** none
- **Description:** New mode only. No Get/Put/RF default change beyond Flags width.
