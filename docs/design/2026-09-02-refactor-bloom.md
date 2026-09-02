# Refactor ModeBloom layout (no contract change)

**Status:** approved (chat: continue)  
**Branch:** `feat/refactor-bloom`  
**Date:** 2026-09-02

## Problem

Bloom store methods still live in `pkg/store/memory.go`. Engine Bloom is one file mixing public verbs, owner write, and item fan-out.

## Non-goals

- No API / proto / version / fan-out change
- No `pkg/bloom` algorithm change
- Not extracting other modes (List / Hash / JSON stay on their own branches)

## Contract

Unchanged: BloomAdd ACK-only inbox (`FlagBloomAdd` item hint — hintID includes the item). BloomTest present-bit + local miss does not install from GetOrLoad. Merge-OR on handoff. Version stays on a live filter (OR, not LWW bump).

## Approach

Same file layout as List / Hash / JSON:

| Before | After |
|--------|--------|
| `pkg/store/memory.go` Bloom* | `pkg/store/bloom.go` |
| `pkg/engine/bloom.go` (all) | `bloom.go` public verbs; `bloom_apply.go` owner write + inbox apply; `bloom_cluster.go` owner forward |

Bloom-local error format strings live in a `const` block at the top of `bloom.go` (same text as before).

Rejected: switch BloomAdd to snapshot fan-out. Install-on-GetOrLoad. Split `pkg/bloom`.

## Tests (already exist; keep them)

Existing `pkg/bloom`, `pkg/store` bloom, `pkg/engine` bloom.

## Bench risk

No new Get/Peek logic. Local Get-hit / StoreGetHit allocs/op must stay 15 / 2.
