# SuperCache Lab — interactive visual explorer

**Status:** approved
**Branch (later):** feat/interactive-lab
**Date:** 2026-09-07

Revision: UI is React + Vite + TypeScript (was vanilla embed). Wide app shell is closed.
Approved in chat: start the changes (LocalView yes; first screen Anatomy).

## Problem

The library’s surface is large and its interesting behavior is **cluster-shaped**:

- owner ACK + async RF fan-out
- local hit vs owner-forward
- LoadThrough + singleflight
- versioned tombstones
- one mode per keyspace, wrong verb = invalid argument
- 15 keyspace modes (KV ×2 + Bloom / Set / ZSet / Geo / List / Hash / Counter / JSON / Bitmap / HLL / TopK / CMS / VectorSet)

What exists today does not let someone *see* that:

| Path | What it is | Gap |
|------|------------|-----|
| `examples/billboard` | Vertical music app + small HTML | Story, not a catalog. Cluster internals are logs + `/peers`. |
| `examples/{hash,json,bitmap,hll,vecset,ratelimit}` | Print walkthroughs | One type, no UI, process exits. |
| `examples/cluster3` | Script against 3 external nodes | No visual. |
| `cmd/sc` | REPL / CLI | Text only. |
| Admin `/docs` | Swagger | Admin HTTP + proto reference, not a live mesh. |

A newcomer cannot click a verb and watch which node owned it, which replicas filled, and what the structure looks like.

## Non-goals

- Not a replacement for billboard, `sc`, or admin Swagger.
- Not a production dashboard / attach-to-arbitrary-cluster debugger.
- No new Cache gRPC verbs, no proto change, no RF/TTL/hint/membership contract change.
- No Next.js, Redux, Tailwind, or component kit. React + Vite only. No WebSocket mesh protocol.
- No persistence, no real DataSource backends, no TLS in the demo.
- No animation of packet bytes or a fake “distributed debugger” that pretends to hook ApplyPut.
- Not a full Redis-compat playground.

## Contract

- **Public product API:** unchanged, except one optional read-only diagnostic (see [LocalView](#localview)).
- **Who stores a copy:** unchanged. Lab uses RF=2 on a 3-node in-process mesh so the canvas always has an owner, one replica, and one non-replica.
- **Clients:** existing `pkg/client` against each node’s cache port. Ingress node is user-selectable so owner-forward is visible.
- **What existing clients can assume:** nothing changes. Lab is `go run ./examples/lab` after the UI build (see [UI implementation](#ui-implementation)).

### LocalView

Add a **read-only** helper so the canvas can tell live / tombstone / negative / missing apart. `HasLocal` collapses those.

```go
// pkg/engine
type LocalKind uint8 // Missing, Live, Tombstone, Negative

type LocalView struct {
    Kind    LocalKind
    Version uint64
    Flags   uint32
    Bytes   int
}

func (e *Engine) LocalView(keyspace, key string) LocalView
```

Implementation is `store.Peek` + classify. No LRU touch (Peek already does that). Not on the Get-hit path. Missing keyspace → `Missing`.

If review rejects any Engine addition, the lab falls back to `HasLocal` + `OwnerOf` and the tombstone chapter is explanatory copy only.

## Approach

### What it is

**SuperCache Lab** — one process, one browser page.

```text
go run ./examples/lab
# http://127.0.0.1:19080/
```

Persistent frame is a **3-node cluster canvas**. The rest of the page is a **chapter list** (guided scenes) plus a **mode playground** (free verbs). Every action is a real Engine/client call; the picture is an observation of the mesh after the call, not a cartoon.

```text
┌────────────┬─────────────────────────────┬──────────────────┐
│ Chapters   │  Cluster canvas             │ Inspector        │
│ Anatomy    │   n1 ● owner                │ key / name       │
│ KV write   │   n2 ○ replica (filling)    │ owner, RF=2      │
│ LoadThrough│   n3 ░ non-replica          │ LocalView/node   │
│ Tombstone  │   arrows: ingress → owner   │ version, kind    │
│ Bloom …    │            → replicas       │                  │
│ VectorSet  │  Event strip (this action)  │ Result / error   │
├────────────┴─────────────────────────────┴──────────────────┤
│ Playground: mode verbs, pick ingress node, sample payload   │
└─────────────────────────────────────────────────────────────┘
```

### Process layout

Same pattern as `examples/billboard` (in-process engines + listeners), **different ports** so both can run:

| Role | Ports |
|------|--------|
| Lab HTTP | `127.0.0.1:19080` |
| Cache gRPC | `9301–9303` |
| Peer gRPC | `9401–9403` |
| Admin | `8181–8183` |
| Gossip | `7951–7953` |

One keyspace per mode, names = mode (`cacheonly`, `loadthrough`, `bloom`, …). `loadthrough` uses a mock DataSource with a knob for SoT latency (default 200ms). RF=2, small `MaxBytes`, short TTL on KV chapters so expiry is demoable.

Hold-and-serve is the default (`-hold=false` exits after a smoke walkthrough, for CI).

### Observation model (no data-path hooks)

`Engine.Events()` is membership-only. Do **not** add ApplyPut/fan-out listeners.

After each lab action the HTTP layer records:

1. **Ingress** — which cache client was used.
2. **Owner** — `Engine.OwnerOf(name)` (any node; ring is shared).
3. **Per-node LocalView** — immediately, then a short poll (e.g. 5× 20ms) so async fan-out becomes visible as replica `Missing → Live`.
4. **Client result** — value / present / invalid-argument.
5. **Derived trace** (honest, labeled as inferred):
   - write + ingress ≠ owner → “forwarded to owner, then ACK”
   - replica LocalView flips to Live after ACK → “async fan-out”
   - read + ingress Has LocalView Live → “local hit”
   - CacheOnly miss on non-owner → “owner-forward”
   - LoadThrough miss → “DataSource load” (mock increments a counter the UI shows)

Slow-mo is a **UI delay between poll snapshots**, not a sleep in `pkg/engine`.

### Chapters (guided)

Each chapter is a scripted sequence the UI can “Run scene” *and* then leave the user in free-play on that keyspace.

| # | Chapter | What you see |
|---|---------|----------------|
| 0 | Anatomy | 3 nodes join, `/peers`, ring gen, type a key → owner + replica set highlight |
| 1 | KV write | Put on the **non-owner**. Owner ACKs; replica fills on the next poll; non-replica stays empty. Get from each node: local vs forward. |
| 2 | LoadThrough | Cold Get → SoT log + latency. Second Get is a hit. Stampede button (N concurrent Gets) → one SoT load. |
| 3 | Tombstone | Delete; all nodes LocalView=Tombstone (or miss if LocalView rejected). Put a new value → new version Live. Copy explains delayed ApplyPut cannot resurrect. |
| 4–N | One chapter per structured mode | Tiny visual + the verbs that matter (table below). Each chapter ends with “wrong verb on this keyspace → invalid argument”. |

### Mode widgets (free-play + chapter)

Thin, mode-specific visuals. Not a second product.

| Mode | Widget | Verbs exposed |
|------|--------|----------------|
| CacheOnly | key/value editor | Get, Put, Delete |
| LoadThrough | same + SoT hit counter | Get, Delete (Put optional pin) |
| Bloom | chip list + Test maybe/no | BloomAdd, BloomTest, Delete |
| Set | member list | SetAdd, SetRemove, SetContains, SetCard, SetMembers, Delete |
| ZSet | ranked table | ZAdd, ZRem, ZScore, ZCard, ZRange, Delete |
| Geo | 2D plot + radius circle | GeoAdd, GeoRem, GeoPos, GeoRadius, GeoDist, Delete |
| List | horizontal cells | LPush, RPush, LPop, RPop, LLen, LRange, Delete |
| Hash | field table | HSet, HGet, HDel, HGetAll, HLen, Delete |
| Counter | big number | Incr, CounterGet, Delete |
| JSON | path + pretty tree | JsonSet, JsonGet, JsonDel, Delete |
| Bitmap | bit grid (first 64 bits) | BitSet, BitGet, BitCount, Delete |
| HLL | estimate vs exact UI set | HLLAdd, HLLCount, Delete |
| TopK | bar chart K=10 | TopKAdd, TopKList, Delete |
| CMS | query one item | CMSIncr, CMSQuery, Delete |
| VectorSet | 2D vectors + query arrow | VAdd, VRem, VSim, VCard, VEmb, Delete |

Copy on each widget: **when to use this mode** (one sentence) and **what it is not** (e.g. TopK is observations, not ZAdd scores).

### HTTP surface (lab process only)

Not part of node admin. JSON in/out.

| Route | Role |
|-------|------|
| `GET /` | Embedded Vite `index.html` (+ hashed assets under `/assets/`) |
| `GET /v1/cluster` | nodes, addrs, ring gen, keyspaces |
| `GET /v1/view?ks=&key=` | owner + LocalView per node |
| `POST /v1/op` | `{ks, op, name, args, via}` → run client verb, return result + before/after views + inferred trace |
| `POST /v1/scene/:id` | run a chapter’s scripted steps, return the step list |
| `POST /v1/reset` | Delete known demo names (or recreate keyspaces) |

No SSE in v1. The client polls `/v1/view` during slow-mo. Revisit SSE only if polling feels wrong.

### UI implementation

React SPA in `examples/lab/ui`, served by the lab process.

| Piece | Choice |
|-------|--------|
| UI | React 18 |
| Language | TypeScript |
| Bundler | Vite |
| Routing | none — one page; chapter is React state (and `?chapter=` for refresh) |
| Data | `fetch` to `/v1/*`; no React Query / Redux |
| Style | one `app.css`, billboard tokens (`#0b0f14` / `#121821`), wide app shell |
| Charts | DOM/CSS node cards + CSS/SVG arrows. Geo + VectorSet use inline SVG. No Canvas2D/WebGL, no chart library |
| Embed | `//go:embed ui/dist` on the lab HTTP server |

```text
examples/lab/
  main.go              # cluster + HTTP + embed
  http.go
  ui/
    package.json
    vite.config.ts     # proxy /v1 → :19080 in dev
    src/App.tsx
    src/cluster/Canvas.tsx
    src/chapters/...
    src/widgets/...    # one component per mode
    dist/              # Vite output, embedded
```

**Run modes**

```text
# hot-reload UI (two processes)
go run ./examples/lab                 # API + cluster on :19080
cd examples/lab/ui && npm run dev     # Vite :5173, proxies /v1

# single process (what README leads with)
cd examples/lab/ui && npm ci && npm run build
go run ./examples/lab                 # serves embedded dist at /
```

Vite `server.proxy`: `/v1` → `http://127.0.0.1:19080`. CORS is not required if every browser call goes through that proxy or same-origin embed.

**`go test ./examples/lab` does not need Node.** Tests hit JSON routes only. A missing `ui/dist` still starts the cluster; `GET /` returns a short “build the UI” page so API tests stay green.

**Commit `ui/dist`** so `go run ./examples/lab` works without Node after clone. Rebuild and commit dist when the React tree changes. `package-lock.json` is committed; `node_modules` is gitignored. `//go:embed` needs at least `ui/dist/index.html` in git (Vite output, or a stub until the first build).

Lab HTTP serves `GET /` and hashed Vite assets from the embed FS. No extra SPA fallback routes — there is no client router.

Component map (implement from this, do not invent a second tree):

| Component | Role |
|-----------|------|
| `App` | shell, chapter state, last op / views |
| `ChapterNav` | list in the left rail |
| `ClusterCanvas` | 3 `NodeCard`s + inferred arrows |
| `Inspector` | owner, RF, per-node LocalView, result |
| `Playground` | ingress picker + current mode widget |
| `widgets/*` | one file per mode from the table above |

### Tests (after approval)

| Test | Package | Asserts |
|------|---------|---------|
| `TestLabHTTPCluster` | `examples/lab` | process starts 3 nodes; `GET /v1/cluster` returns 3 peers + all mode keyspaces |
| `TestLabOpPutReplicaFill` | `examples/lab` | Put via non-owner; owner LocalView Live immediately; replica Live within poll window; non-replica not Live |
| `TestLabWrongVerb` | `examples/lab` | `Get` on `ModeSet` → 400 / invalid argument |
| `TestLabLoadThroughSingleflight` | `examples/lab` | N concurrent Gets on cold key → mock SoT loads == 1 |
| `TestLocalViewKinds` | `pkg/engine` | live / tombstone / negative / missing (only if LocalView is in scope) |
| `TestLabHoldFalse` | `examples/lab` | `-hold=false` walkthrough exits 0 |

No new scbench cells. No Playwright / browser tests in this PR — React is covered by building `ui/dist` and the Go JSON tests. A missing or stub `ui/dist` must not fail `go test ./examples/lab`.

### Bench risk

- **Hot path?** No, unless LocalView is mistakenly called from Get. It must use Peek only.
- Shared smoke / Get-hit allocs: **must not move**. LocalView is a new symbol, unused by Get/Put.
- Lab tests start a 3-node mesh (same cost class as `examples/hash`). Keep them in `go test ./examples/lab`, not the engine package.

### Docs

- `examples/lab/README.md` — `npm run dev` vs `npm run build` + `go run`, ports, chapter list.
- One paragraph + link from root `README.md` (next to billboard).
- Do not rewrite PLAN.md.

## Rejected alternatives

| Idea | Why not |
|------|---------|
| Grow billboard HTML into a catalog | Mixes a product story with a lab; billboard already has a job. |
| Attach Lab to a running `supercache-node` | No LocalView/HasLocal over admin HTTP today; would force a new admin API. |
| Instrument ApplyPut / fan-out with Engine events | Data-path change, Get-hit risk, more than a demo needs. |
| Vanilla one-file HTML (billboard style) | User wants React. The canvas + 15 widgets will not stay readable as a string const. |
| Next.js / SSR | Lab is a local single page talking to `:19080`. No SEO, no server render. |
| Tailwind / component kit | Extra design system for one example. Billboard tokens + one CSS file. |
| One chapter / one PR | Leaves a half-lab; SuperCache is one design → one PR. Widgets stay thin. |
| Fake the cluster in JS | Teaches the wrong thing the moment fan-out or RF is surprising. |

## Key Decisions

1. **New example, not a billboard fork** — catalog vs story.
2. **Cluster canvas is the frame** — SuperCache’s differentiator is the mesh, not another hash-map form.
3. **Observe after the call** — `OwnerOf` + `HasLocal` / `LocalView` + poll. No data-path event bus.
4. **RF=2 on N=3** — owner / replica / non-replica always visible.
5. **All modes in this PR, thin widgets** — otherwise it does not explore the library.
6. **Optional LocalView** — only Engine addition; read-only Peek wrapper.
7. **React + Vite, embedded `ui/dist`** — UI is a real SPA; `go run` still serves one origin after `npm run build`. Tests stay Go-only.

## Open Questions

1. **LocalView in this PR?** Recommended yes (tombstone chapter is honest). Alternative: HasLocal only, no `pkg/engine` change.
2. **Default first screen?** Recommended chapter 0 (Anatomy) auto-run on load, then stay on the canvas. Alternative: mode gallery grid first.
3. **Visual density?** Closed: wide React app shell (canvas + inspector). Billboard’s article layout does not fit 15 widgets.

## Tests (write these first, after approval)

See table above. New `examples/lab` tests should fail to compile until the HTTP server exists. `TestLocalViewKinds` should fail until the method exists.

## Bench risk

See above. Gate: no Get-hit / StoreGetHit alloc increase; no shared smoke cell worse than 10%. Lab itself is not a CI smoke cell.

## PR Plan

One design → one PR.

### PR 1 — `feat: SuperCache Lab interactive explorer`

- **Title:** SuperCache Lab: interactive cluster + mode explorer
- **Branch:** `feat/interactive-lab`
- **Files:**
  - `docs/design/2026-09-07-interactive-lab.md` (this file, status → approved)
  - `pkg/engine/status.go` (+ test) — `LocalView` if approved
  - `examples/lab/` — cluster, HTTP, embed of `ui/dist`, scenes, tests, README
  - `examples/lab/ui/` — Vite + React 18 + TypeScript (widgets, canvas, lockfile, committed `dist/`)
  - `README.md` — one link under examples
- **Dependencies:** none
- **Description:** In-process 3-node lab, observation API, guided chapters, thin widget per mode. No proto/RF/Get-Put contract change.
