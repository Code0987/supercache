# ModeJSON example — nested document

A **self-contained 3-node** walkthrough of `ModeJSON`: a named JSON
document with path get/set/delete.

This is the shape you want for a nested profile or session blob — not a
`Put` of the whole JSON on every field change, and not a flat `ModeHash`.

## What it shows

| Step | SuperCache behavior |
|------|---------------------|
| Why not KV / Hash | Whole-blob LWW; Hash has no nesting, arrays, or typed JSON |
| `JsonSet $` | Installs the document |
| `JsonSet $.addr.city` | Missing **object** parents are created |
| `JsonGet $.n` | Integer stays an integer (`UseNumber`) |
| Missing path | `ok=false` |
| `JsonDel $.addr` | Subtree gone; document stays live |
| Empty-until-delete | `JsonDel $` → live `{}` until `Delete(name)` |
| RF=2 | Two nodes keep a local snapshot |
| No auto array | `$.a[0]` on `{}` is invalid |

No new default process flags beyond `-demo-keyspace` (see below). The
`go run` path starts its **own** in-process mesh (ephemeral ports).

## Run

```bash
# from repo root — starts 3 nodes, prints the walkthrough, exits 0 on success
go run ./examples/json
```

CI covers the same path: `go test ./examples/json`.

## Manual `sc` against a node

`supercache-node -demo-keyspace` (the default) now also registers
**`doc`** (`ModeJSON`).

`sc jsonset` joins remaining args like `put` / `hset`. The server rejects
non-JSON, so quote string values:

```bash
# terminal 1
go run ./cmd/supercache-node \
  -cache 127.0.0.1:9000 -peer 127.0.0.1:9001 -admin 127.0.0.1:8080

# terminal 2
go run ./cmd/sc -keyspace doc jsonset user $ '{"name":"Ada","n":1}'
go run ./cmd/sc -keyspace doc jsonset user $.name '"Ada"'
go run ./cmd/sc -keyspace doc jsonset user $.n 1
go run ./cmd/sc -keyspace doc jsonget user $.n
go run ./cmd/sc -keyspace doc jsonget user
go run ./cmd/sc -keyspace doc jsondel user $.name
go run ./cmd/sc -keyspace doc jsondel user          # clear to {}
go run ./cmd/sc -keyspace doc del user              # tombstone
```

`jsonget` of a missing path prints `(nil)` and exits 1.

Wrong mode (e.g. `jsonset` on `demo` / `get` on `doc`) → invalid argument.

## Contract reminders

- Owner applies the op, then fans out a full `FlagJSON` snapshot.
- A replica that already has the document answers `JsonGet` **locally**
  (a missing path is a miss, not an owner-forward). Replica reads may lag.
- Hint identity is still `(keyspace, name)` — that is why fan-out is a snapshot.
- Design: [docs/design/2026-08-21-mode-json.md](../../docs/design/2026-08-21-mode-json.md).
