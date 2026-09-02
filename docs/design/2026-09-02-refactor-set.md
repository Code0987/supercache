# Refactor ModeSet layout (no contract change)

**Status:** approved (chat: continue)  
**Branch:** `feat/refactor-set`  
**Date:** 2026-09-02

## Problem

Set store methods still live in `pkg/store/memory.go`. Engine Set is one file mixing public verbs, owner write, and item fan-out.

## Non-goals

- No API / proto / version / fan-out / dirty-cache change
- No `pkg/set` algorithm change
- Not extracting other modes (List / Hash / JSON / Bloom stay on their own branches)
- Not moving `PeekVersion` (generic; stays in `memory.go`)

## Contract

Unchanged: SetAdd/SetRemove ACK-only inbox (`FlagSetAdd` / `FlagSetRemove` item fan-out). SetRemove of missing is a no-op. Dirty `setCache` flushed on Get/Peek only. Replica GetOrLoad installs; non-replica decodes the blob once.

## Approach

Same file layout as Bloom:

| Before | After |
|--------|--------|
| `pkg/store/memory.go` Set* | `pkg/store/set.go` (including `flushSetValueLocked` / `insertSetLocked`) |
| `pkg/engine/set.go` (verbs + apply) | `set.go` public verbs; `set_apply.go` owner write + inbox apply; `set_cluster.go` owner forward |

`pkg/engine/set_blob.go` stays (non-replica decode helpers). Set-local error format strings live in a `const` block at the top of `set.go` (same text as before).

Rejected: rewrite dirty-cache / flush. Switch Set to snapshot fan-out. Split `pkg/set`.

## Tests (already exist; keep them)

Existing `pkg/set`, `pkg/store` set, `pkg/engine` set unit + cluster.

## Bench risk

No new Get/Peek logic. Local Get-hit / StoreGetHit allocs/op must stay 15 / 2.
