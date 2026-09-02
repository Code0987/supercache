# Refactor ModeList layout (no contract change)

**Status:** approved (chat: continue)  
**Branch:** `feat/refactor-list`  
**Date:** 2026-09-02

## Problem

List store methods still live in `pkg/store/memory.go`. Engine List is one file mixing public verbs, owner write, and GetOrLoad.

## Non-goals

- No API / proto / version / fan-out / dirty-cache change
- No `pkg/listx` algorithm change
- Not extracting other modes (HLL / Bitmap / Counter stay on their own branches)

## Contract

Unchanged: LPush/RPush ACK-only inbox; LPop/RPop Peer `ListPop`; FlagList snapshot; version under store mutex; dirty `lCache` flushed on Get/Peek only.

## Approach

Same file layout as HLL / Bitmap / Counter:

| Before | After |
|--------|--------|
| `pkg/store/memory.go` L* | `pkg/store/list.go` (including `flushListValueLocked` / `insertListLocked`) |
| `pkg/engine/list.go` (all) | `list.go` public verbs; `list_apply.go` owner write + inbox apply; `list_cluster.go` GetOrLoad / snapshot |

List-local error format strings live in a `const` block at the top of `list.go` (same text as before).

Rejected: rewrite dirty-cache / flush (JSON/Hash share that pattern; leave it). Split `pkg/listx` (166 lines).

## Tests (already exist; keep them)

Existing `pkg/listx`, `pkg/store` list, `pkg/engine` list unit + cluster.

## Bench risk

No new Get/Peek logic. Local Get-hit / StoreGetHit allocs/op must stay 15 / 2.
