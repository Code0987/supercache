# Refactor ModeCounter layout (no contract change)

**Status:** approved (chat: continue)  
**Branch:** `feat/refactor-counter`  
**Date:** 2026-09-02

## Problem

Counter store methods still live in `pkg/store/memory.go`. Engine Counter is one file mixing public verbs, owner write, and GetOrLoad.

## Non-goals

- No API / proto / Peer `CounterIncr` / version / fan-out change
- No `pkg/counter` algorithm change
- Not extracting other modes (HLL #40, Bitmap #41)

## Contract

Unchanged: `Incr` returns the new int64 (peer `CounterIncr` from non-owner). `CounterGet` present-bit. `FlagCounter` snapshot. Version under store mutex. Overflow → invalid argument, no wrap.

## Approach

Same file layout as HLL / Bitmap:

| Before | After |
|--------|--------|
| `pkg/store/memory.go` C* | `pkg/store/counter.go` |
| `pkg/engine/counter.go` (all) | `counter.go` public verbs; `counter_apply.go` owner write + install; `counter_cluster.go` GetOrLoad / snapshot |

Rejected: drop the Peer RPC (that is the return-*n* contract); split `pkg/counter` (39 lines).

## Tests (already exist; keep them)

Existing `pkg/counter`, `pkg/store` counter, `pkg/engine` counter unit + cluster.

## Bench risk

No new Get/Peek logic. Local Get-hit / StoreGetHit allocs/op must stay 15 / 2.
