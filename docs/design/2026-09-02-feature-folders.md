# Feature folders (same contract)

**Status:** approved (chat: this sounds good)  
**Branch (later):** `feat/feature-folders`  
**Date:** 2026-09-02

## Problem

`pkg/engine` is ~84 files. Most are per-mode (`list.go`, `list_apply.go`, `list_cluster.go`, tests, benches) sitting next to the cluster/Get/Put core.

Go still forbids `pkg/engine/list/` as `package engine` (one directory = one package). Unexported `ksRuntime` cannot be seen from another folder.

## Non-goals

- No API / proto / flag / version / fan-out / dirty-cache change
- Callers still use `engine.Engine.LPush` (not `list.LPush`)
- Not moving `Memory` methods out of `pkg/store` in this design (`lruItem` / dirty cache stay unexported there)
- Not exporting `ksRuntime`

## Contract

Unchanged public surface: `pkg/engine`, `pkg/store`, `pkg/client`. Codec import paths change (`pkg/listx` → `pkg/list`, same for the other `*x` packages).

## Approach

Each mode gets a top-level feature package. Engine keeps **thin public verbs + ApplyPut switch**. Mode logic lives in the feature package and talks to engine through a small **Host** interface defined **in the feature package** (so `pkg/list` does not import `pkg/engine` — no cycle). `*Engine` implements those interfaces.

```
pkg/list/                 # package list — codec only (store imports this; no store import here)
  codec.go
  codec_test.go
  engine_test.go          # moved engine list tests (package list_test)
  eng/                    # package eng — owner write + GetOrLoad (imports store; engine imports eng)
    host.go
    local.go
    cluster.go

pkg/engine/list.go        # LPush/RPush/LPop/… only: validate, route, call list.*
pkg/engine/engine.go      # ApplyPut: list.ApplyLPush(host, store, …)
pkg/store/list.go         # unchanged (Memory receivers)
```

`pkg/engine` after a full roll-out: core (`engine.go`, `cluster.go`, `errors.go`, shared tests) plus **one file per mode**. Apply/cluster/mode tests leave.

Same map for the other modes (`pkg/hash`, `pkg/hll`, `pkg/bitmap`, `pkg/cms`, `pkg/topk`; `pkg/bloom` / `set` / `zset` / `geo` / `counter` already exist — add engine helpers there).

**First PR = List only** (this design). Other modes copy the pattern.

Rejected: `pkg/engine/list/` same package (illegal). Codec-only rename (does not shrink engine). Export `ksRuntime` so feature packages import engine (cycle + contract leak).

## Tests (already exist; keep them)

Move `pkg/engine/list_*test.go` into `pkg/list` as `package list_test` (still `engine.New()`, same asserts). Existing `pkg/store` list tests stay. `go test ./...`.

## Bench risk

Hot path is Get/Put, not List. Local Get-hit / StoreGetHit allocs/op must stay 15 / 2. No extra alloc on the Host interface if it is a concrete `struct { e *Engine; ks *ksRuntime }` passed by value — verify List micros if they exist.
