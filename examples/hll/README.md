# ModeHLL example — approximate distinct count

A **self-contained 3-node** walkthrough of `ModeHLL`: a named HyperLogLog
sketch with `HLLAdd` / `HLLCount`.

This is the shape you want for “how many unique items have we seen?” —
not a `SetCard` of every member, not a Counter, not Bloom membership,
and not `BitCount` of addressable bits.

## What it shows

| Step | SuperCache behavior |
|------|---------------------|
| Why not Set / Counter / Bloom / Bitmap | Exact O(n); not distinct; membership; bit offsets |
| `HLLAdd` three members | `alice`, `bob`, `carol` on name `visitors` |
| `HLLCount` | ≈ 3, `ok=true` |
| Duplicate `HLLAdd("alice")` | Estimate does not grow |
| Missing name | `ok=false` |
| RF=2 | Two nodes keep a local snapshot |
| Empty-until-delete | `Delete` then recreate |

No new default process flags. The `go run` path starts its **own**
in-process mesh (ephemeral ports).

## Run

```bash
# from repo root — starts 3 nodes, prints the walkthrough, exits 0 on success
go run ./examples/hll
```

CI covers the same path: `go test ./examples/hll`.

## Manual `sc` against a node

Register a `ModeHLL` keyspace (there is no default `uniq` demo KS on the
stock node). Then:

```bash
# terminal 1 — after UpdateKeySpace ModeHLL name=uniq
go run ./cmd/supercache-node \
  -cache 127.0.0.1:9000 -peer 127.0.0.1:9001 -admin 127.0.0.1:8080

# terminal 2
go run ./cmd/sc -keyspace uniq hlladd visitors alice
go run ./cmd/sc -keyspace uniq hlladd visitors bob carol
go run ./cmd/sc -keyspace uniq hllcount visitors
go run ./cmd/sc -keyspace uniq del visitors
```

`hllcount` of a missing name prints `(nil)` and exits 1.

Wrong mode (e.g. `hlladd` on `demo` / `get` on `uniq`) → invalid argument.

## Contract reminders

- Owner applies the op, then fans out a full `FlagHLL` snapshot (12 KiB).
- A replica that already has the sketch answers `HLLCount` **locally**
  (replica reads may lag).
- Hint identity is still `(keyspace, name)` — that is why fan-out is a snapshot.
- Estimates use FNV-1a 64, `p=14`; they will **not** match Redis `PFCOUNT`.
- Design: [docs/design/2026-08-25-mode-hll.md](../../docs/design/2026-08-25-mode-hll.md).
