# Refactor ModeJSON layout (no contract change)

**Status:** approved (chat: continue)  
**Branch:** `feat/refactor-json`  
**Date:** 2026-09-02

## Problem

JSON store methods still live in `pkg/store/memory.go`. Engine JSON is one file mixing public verbs, owner write, inbox apply, and GetOrLoad.

## Non-goals

- No API / proto / version / fan-out / dirty-cache change
- No `pkg/jsonx` algorithm change
- Not extracting other modes (List / Hash stay on their own branches)

## Contract

Unchanged: JsonSet/JsonDel ACK-only inbox (`FlagJSONSet` / `FlagJSONDel`) then `FlagJSON` snapshot fan-out. JsonGet present-bit. Version under store mutex. Dirty `jCache` flushed on Get/Peek only.

## Approach

Same file layout as List / Hash:

| Before | After |
|--------|--------|
| `pkg/store/memory.go` J* | `pkg/store/json.go` (including `flushJSONValueLocked` / `insertJSONLocked`) |
| `pkg/engine/json.go` (all) | `json.go` public verbs; `json_apply.go` owner write + inbox apply; `json_cluster.go` owner forward / snapshot / GetOrLoad |

JSON-local error format strings live in a `const` block at the top of `json.go` (same text as before).

Rejected: rewrite dirty-cache / flush. Drop inbox and snapshot-only. Split `pkg/jsonx`.

## Tests (already exist; keep them)

Existing `pkg/jsonx`, `pkg/store` json, `pkg/engine` json unit + cluster.

## Bench risk

No new Get/Peek logic. Local Get-hit / StoreGetHit allocs/op must stay 15 / 2.
