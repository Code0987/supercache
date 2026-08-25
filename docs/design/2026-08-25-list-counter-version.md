# List / Counter snapshot version (store mutex)

**Status:** approved  
**Branch:** `feat/list-counter-version`  
**Date:** 2026-08-25

## Problem

List and Counter mint a version with `lNextVersion` / `cNextVersion` (verMu + PeekVersion) **then** mutate under the store mutex and fan that pre-lock candidate. Two concurrent owner LPushes / Incrs can invert store-lock order relative to version order. Replica `LInstall` / `CInstall` (`incoming > local`) keeps the earlier snapshot and rejects the complete later one.

JSON / Bitmap / HLL already assign `stored = local.Version+1` (or `1` on create) under the store mutex and fan `PeekVersion` after the write.

## Non-goals

- Hash / Set / ZSet / Geo / Bloom via-owner `Version: 1` (review issue 4)
- List via-owner `applied=false` ACK (review issue 3)
- JsonDel-after-Delete (review issue 2)
- Changing `*Install` equal-ignore (`>` not `≥`)
- Public API / proto / sc changes

## Contract

One version rule for owner List and Counter mutates, same as Bitmap / HLL / JSON:

```
if missing / expired:                     stored = 1
if live entry or replacing tombstone:     stored = local.Version + 1
incoming `version` argument:              tombstone-gate floor only
```

Engine fans **`PeekVersion` after the store write** (and `observeVersion` that value). Never fan the pre-lock candidate. Counter fans `Encode(n)` with that post-write version (n is the store result; do not Peek-clone the 8-byte blob).

Applies to: store `LPush` / `RPush` / `LPop` / `RPop` / `CIncr`; engine `lPushLocal` / `lPopLocal` / `applyListLPush` / `applyListRPush` / `cIncrLocal`.

`LInstall` / `CInstall` unchanged (`incoming > local`).

## Tests (first)

| Test | Asserts |
|------|---------|
| Store LPush stored version | create → 1; second push → 2; incoming gate 99 on a v2 still stores 3 |
| Store CIncr stored version | same |
| Store LPop stored version | pop bumps local+1, not the incoming gate |
| Concurrent owner LPush | two overlapping pushes; stored version ≥ 3; both items present |
| Concurrent owner Incr | two overlapping Incr(1); value = 1+2 deltas; version ≥ 3 |

## Bench risk

Not the KV Get-hit path. Local `BenchmarkEngineGetHit` / `BenchmarkStoreGetHit` allocs must not rise.
