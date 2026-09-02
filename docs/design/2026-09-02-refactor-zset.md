# Refactor ModeZSet layout (no contract change)

**Status:** approved (chat: continue)  
**Branch:** `feat/refactor-zset`  
**Date:** 2026-09-02

## Problem

ZSet store methods still live in `pkg/store/memory.go`. Engine ZSet is one file mixing public verbs, owner write, and item fan-out.

## Non-goals

- No API / proto / version / fan-out / dirty-cache change
- No `pkg/zset` algorithm change
- Not extracting other modes (Set / Bloom / List / Hash / JSON stay on their own branches)

## Contract

Unchanged: ZAdd/ZRem ACK-only inbox (`FlagZSetAdd` / `FlagZSetRem` item fan-out). ZRem of missing is a no-op. NaN score rejected. Dirty `zCache` flushed on Get/Peek only.

## Approach

Same file layout as Set:

| Before | After |
|--------|--------|
| `pkg/store/memory.go` Z* | `pkg/store/zset.go` (including `flushZSetValueLocked` / `insertZSetLocked`) |
| `pkg/engine/zset.go` (all) | `zset.go` public verbs; `zset_apply.go` owner write + inbox apply; `zset_cluster.go` owner forward / GetOrLoad |

ZSet-local error format strings live in a `const` block at the top of `zset.go` (same text as before).

Rejected: rewrite dirty-cache / flush. Switch ZSet to snapshot fan-out. Split `pkg/zset`.

## Tests (already exist; keep them)

Existing `pkg/zset`, `pkg/store` zset, `pkg/engine` zset unit + cluster.

## Bench risk

No new Get/Peek logic. Local Get-hit / StoreGetHit allocs/op must stay 15 / 2.
