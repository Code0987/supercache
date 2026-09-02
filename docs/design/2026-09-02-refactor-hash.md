# Refactor ModeHash layout (no contract change)

**Status:** approved (chat: continue)  
**Branch:** `feat/refactor-hash`  
**Date:** 2026-09-02

## Problem

Hash store methods still live in `pkg/store/memory.go`. Engine Hash is one file mixing public verbs, owner write, item fan-out, and GetOrLoad.

## Non-goals

- No API / proto / version / fan-out / dirty-cache change
- No `pkg/hashx` algorithm change
- Not extracting other modes (List stays on `feat/refactor-list`)

## Contract

Unchanged: HSet/HDel ACK-only inbox (`FlagHashSet` / `FlagHashDel` item fan-out, not snapshot). HGet present-bit + field miss. Version via `nextVersion`. Dirty `hCache` flushed on Get/Peek only.

## Approach

Same file layout as List:

| Before | After |
|--------|--------|
| `pkg/store/memory.go` H* | `pkg/store/hash.go` (including `flushHashValueLocked` / `insertHashLocked`) |
| `pkg/engine/hash.go` (all) | `hash.go` public verbs; `hash_apply.go` owner write + inbox apply; `hash_cluster.go` GetOrLoad / owner forward |

Hash-local error format strings live in a `const` block at the top of `hash.go` (same text as before).

Rejected: rewrite dirty-cache / flush. Switch Hash to snapshot fan-out. Split `pkg/hashx`.

## Tests (already exist; keep them)

Existing `pkg/hashx`, `pkg/store` hash, `pkg/engine` hash unit + cluster.

## Bench risk

No new Get/Peek logic. Local Get-hit / StoreGetHit allocs/op must stay 15 / 2.
