# Lab widget for `ModeStream`

**Status:** approved (chat: looks good, implement it)
**Branch (later):** `feat/lab-mode-stream`
**Date:** 2026-09-08

## Overview

[ModeStream](./2026-09-08-mode-stream.md) shipped without a Lab chapter (explicit non-goal). The explorer still stops at VectorSet. This PR adds the missing Stream chapter so the same 3-node Lab can append, range, trim, and delete a named log.

Lab-only. No Engine / proto / RF change.

## Problem

`examples/lab` registers every other structured mode (`cluster.go` `labKeyspaces`, `ui/src/api.ts` `CHAPTERS`, `Playground.tsx`). There is no `stream` keyspace, no `/v1/op` dispatch for `xadd` / `xrange` / …, and no widget. Callers cannot see owner-minted ids or snapshot fan-out the way they can for List / VectorSet.

## Non-goals

- Consumer groups, blocking `XREAD`, caller-chosen ids.
- Standalone `examples/stream` binary (Lab is the walkthrough).
- Adding `stream` to stock `supercache-node -demo-keyspace`.
- Changing Stream contract, Flags, or peer `StreamAdd`.
- New scbench cells.

## Contract

- **API / proto:** no public change. Lab HTTP `/v1/op` gains Stream verbs; same `client.Client` calls.
- **Keyspace (in-process Lab mesh only):** name `stream`, `ModeStream`, RF=2, `StreamMaxLen=0` (no auto-trim; widget has XTrim).
- **Identity:** stream `name` (default `logs`). Owner = `Owner(name)`.
- **Existing clients:** unchanged. Remote attach works if the remote mesh already has a `stream` keyspace; otherwise ops return the usual missing-keyspace / invalid-argument errors.

## Approach

Same pattern as VectorSet / List.

**Backend (`examples/lab`):**

| File | Change |
|------|--------|
| `cluster.go` | `labKeyspaces` + `modeNames` include `stream` / `ModeStream` |
| `op.go` | `opArgs` fields `ID`, `Start`, `End` (string), `Count`, `MaxLen`; dispatch `xadd` / `xrange` / `xrevrange` / `xlen` / `xdel` / `xtrim`; `isWriteOp` adds `xadd`, `xdel`, `xtrim` |
| `scene.go` | scene `stream`: two `XAdd`, then `XRange - +`; demo name `logs` |
| `lab_test.go` | keyspace list + a Stream op test |

Dispatch shapes (JSON `result`):

| Op | Args | Result |
|----|------|--------|
| `xadd` | `value` (payload string) | `{id}` |
| `xrange` / `xrevrange` | `start`, `end`, `count` | `{entries:[{id,payload}]}` |
| `xlen` | — | `{n, present}` |
| `xdel` | `id` | `{acked}` |
| `xtrim` | `max_len` | `{acked}` |
| `delete` | existing | `{acked}` |

`start` / `end` are strings (`-`, `+`, `millis-seq`, exclusive `(id`). `count<=0` means no cap (engine clamps >512). `XRevRange` uses the same min/max window as `XRange` (newest first) — not Redis high-then-low.

**UI:**

- Chapter `{ id: "stream", label: "Stream", ks: "stream", name: "logs" }`
- `widgets/Stream.tsx`: stacked labeled controls + placeholders (same as List / Geo)
  - name, payload → **XAdd** (prints minted id in the results pane)
  - start / end / count → **XRange** / **XRevRange** / **XLen**
  - id → **XDel**
  - maxLen → **XTrim**
  - **Delete** whole stream
- Visual: after a range, a vertical log of `id` + payload (order from the RPC, no client-side filter). Empty range is empty, not an error. Missing name: `XLen` `present=false`.
- Results stay in the existing right-hand results pane (`last` JSON). Do not add a second results column inside the widget.

**Product docs (same PR):** `examples/lab/README.md` chapter table. `docs/design/README.md` shipped row. No API/OpenAPI/PLAN change (no new public verb).

### Rejected alternatives

| Idea | Why not |
|------|---------|
| `examples/stream` binary | Second walkthrough; Lab already covers modes |
| StreamMaxLen=16 in Lab | Auto-trim would surprise; XTrim is enough |
| Redis `XREVRANGE + -` in the widget | Engine contract is start/end min/max |
| Client-side hit filter / hollow “window” | Same class of bug as the Geo radius ring |

## Tests (write these first, after approval)

| Test | Package | Asserts |
|------|---------|---------|
| `TestLabHTTPCluster` | `examples/lab` | keyspace list includes `stream` |
| `TestLabStreamXAddRange` | `examples/lab` | two `xadd`; `xlen` n=2 present; `xrange - +` both ids oldest-first; `xrevrange - +` newest first |
| `TestLabStreamWrongVerb` | `examples/lab` | `get` on `stream` → HTTP 400 + `invalid_argument` |
| `TestLabHoldFalse` | `examples/lab` | still passes (walkthrough unchanged) |

## Bench risk

- **Hot path?** no. Lab HTTP + demo keyspace only.
- Gate: shared smoke ±10%; Get-hit / StoreGetHit allocs flat.

## Key Decisions

1. Lab chapter only — reuse shipped Stream RPCs.
2. Default name `logs`; `StreamMaxLen=0`.
3. Timeline visual from the last range RPC, no extra filtering.
4. Same PR updates Lab README only (plus design index).

## Open Questions

None.

## PR Plan

### PR 1 — `feat: Lab ModeStream chapter`

- **Title:** feat: Lab ModeStream chapter
- **Branch:** `feat/lab-mode-stream`
- **Files:** `examples/lab` backend + `ui/src` widget + rebuilt `ui/dist`; this design; Lab README
- **Dependencies:** ModeStream on `main` (PR #50)
- **Description:** Explorer only. No Engine contract change.
