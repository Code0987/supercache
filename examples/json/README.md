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

No default `-demo-keyspace` JSON keyspace. The `go run` path starts its
**own** in-process mesh (ephemeral ports).

## Run

```bash
# from repo root — starts 3 nodes, prints the walkthrough, exits 0 on success
go run ./examples/json
```

CI covers the same path: `go test ./examples/json`.

## Manual `sc` (register a ModeJSON keyspace yourself)

`sc jsonset` joins remaining args like `put` / `hset`. The server rejects
non-JSON, so quote string values:

```bash
sc -keyspace doc jsonset user $ '{"name":"Ada","n":1}'
sc -keyspace doc jsonset user $.name '"Ada"'
sc -keyspace doc jsonset user $.n 1
sc -keyspace doc jsonget user $.n
sc -keyspace doc jsonget user
sc -keyspace doc jsondel user $.name
sc -keyspace doc jsondel user          # clear to {}
sc -keyspace doc del user              # tombstone
```
