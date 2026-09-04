# Refactor ModeBitmap layout (no contract change)

**Status:** approved (chat: continue after HLL)  
**Branch:** `feat/refactor-bitmap`  
**Date:** 2026-09-02

## Problem

Bitmap store methods still live in `pkg/store/memory.go`. Engine Bitmap is one ~250-line file mixing public verbs, owner apply, and cluster hops.

## Non-goals

- No API / proto / flag / version / fan-out / hintID change
- No `bitmapx` algorithm change
- Not extracting other modes (HLL is a separate PR)

## Contract

Unchanged: `BitSet` / `BitGet` / `BitCount` / `BitPos` / `Delete(name)`, `FlagBitmap` + inbox `FlagBitmapSet`, snapshot fan-out, version under store mutex, present-bit, empty-until-delete.

## Approach

Same file layout as HLL / TopK / CMS:

| Before | After |
|--------|--------|
| `pkg/store/memory.go` B* | `pkg/store/bitmap.go` |
| `pkg/engine/bitmap.go` (all) | `bitmap.go` public verbs; `bitmap_apply.go` owner write; `bitmap_cluster.go` inbox/fan-out/GetOrLoad |

Rejected: rewrite bit packing; split `bitmapx` (150 lines is one file).

## Tests (already exist; keep them)

Existing `pkg/bitmapx`, `pkg/store` bitmap, `pkg/engine` bitmap unit + cluster + hint-after-down.

## Bench risk

No new Get/Peek logic. Local `BenchmarkEngineGetHit` / `BenchmarkStoreGetHit` allocs/op must stay 15 / 2.
