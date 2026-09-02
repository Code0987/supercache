# Refactor ModeGeo layout (no contract change)

**Status:** approved (chat: continue)  
**Branch:** `feat/refactor-geo`  
**Date:** 2026-09-02

## Problem

Geo store methods still live in `pkg/store/memory.go`. Engine Geo is one file mixing public verbs, owner write, and item fan-out.

## Non-goals

- No API / proto / version / fan-out / dirty-cache change
- No `pkg/geo` algorithm change
- Not extracting other modes (ZSet / Set / Bloom / List / Hash / JSON stay on their own branches)

## Contract

Unchanged: GeoAdd/GeoRem ACK-only inbox (`FlagGeoAdd` / `FlagGeoRem` item fan-out). GeoRem of missing is a no-op. Invalid lon/lat and NaN/negative radius rejected. Dirty `gCache` flushed on Get/Peek only.

## Approach

Same file layout as ZSet:

| Before | After |
|--------|--------|
| `pkg/store/memory.go` Geo* | `pkg/store/geo.go` (including `flushGeoValueLocked` / `insertGeoLocked`) |
| `pkg/engine/geo.go` (all) | `geo.go` public verbs; `geo_apply.go` owner write + inbox apply; `geo_cluster.go` owner forward / GetOrLoad |

Geo-local error format strings live in a `const` block at the top of `geo.go` (same text as before).

Rejected: rewrite dirty-cache / flush. Switch Geo to snapshot fan-out. Split `pkg/geo`.

## Tests (already exist; keep them)

Existing `pkg/geo`, `pkg/store` geo, `pkg/engine` geo unit + cluster.

## Bench risk

No new Get/Peek logic. Local Get-hit / StoreGetHit allocs/op must stay 15 / 2.
