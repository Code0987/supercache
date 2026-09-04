# Refactor ModeHLL layout (no contract change)

**Status:** approved (chat: start with HLL)  
**Branch:** `feat/refactor-hll`  
**Date:** 2026-09-02

## Problem

HLL store methods still live in `pkg/store/memory.go` (~2400 lines). TopK and CMS already have their own files. Engine HLL is one 190-line file mixing public API, owner apply, and cluster hops. `pkg/hllx.Merge` is exported but unused on the data path (install is LWW replace).

## Non-goals

- No API / proto / flag / version / fan-out / hintID change
- No `hllx` algorithm change
- No decoded cache / Get-Peek flush helper
- Not extracting other modes

## Contract

Unchanged: `HLLAdd` / `HLLCount` / `Delete(name)`, `FlagHLL` + inbox `FlagHLLAdd`, snapshot fan-out, version under store mutex, present-bit, empty-until-delete.

`pkg/hllx.Merge` becomes unexported (`merge`). Engine never called it. Package tests still cover per-register max.

## Approach

Standard Go file layout (one concern per file, same package):

| Before | After |
|--------|--------|
| `pkg/store/memory.go` HLL* | `pkg/store/hll.go` (same as `cms.go` / `topk.go`) |
| `pkg/engine/hll.go` (all) | `hll.go` public verbs; `hll_apply.go` owner write; `hll_cluster.go` inbox/fan-out/GetOrLoad |
| `pkg/hllx.Merge` | `merge` (unexported) |

Rejected: rewrite registers; split `hllx` (150 lines is one file); touch `ApplyPut` order.

## Tests (already exist; keep them)

Existing `pkg/hllx`, `pkg/store` HLL, `pkg/engine` HLL unit + cluster + hint-after-down. No new behavior tests.

## Bench risk

Hot path? No new Get/Peek logic. Extra file split must not add allocs. Local `BenchmarkEngineGetHit` / `BenchmarkStoreGetHit` allocs/op must stay 15 / 2.
