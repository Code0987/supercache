# ModeHash example — user profile

A **self-contained 3-node** walkthrough of `ModeHash`: a named map
`name → { field → []byte }` with **per-field** last-write-wins.

This is the shape you want for session attrs, feature payloads, or a user
profile — not a JSON blob you `Put` on every field change.

## What it shows

| Step | SuperCache behavior |
|------|---------------------|
| Why not KV | One `Put` of a JSON blob is a single LWW value; concurrent field writers clobber each other |
| `HSet` | Upsert fields on `alice`; creates the hash |
| RF=2 fan-out | Two nodes keep a local copy; the third owner-forwards reads |
| `HGet` / `HExists` / `HLen` / `HGetAll` | Point read, membership, count, full map (field-byte order) |
| Concurrent `HSet` | n1 updates `email`, n2 updates `bio` — **both** survive |
| Empty value | Stored empty is `present=true` + non-nil `[]byte{}`; missing field is `present=false` |
| `HDel` | Removes one field; the name stays |
| Empty-until-delete | After the last `HDel`, `HLen` is 0 until `Delete(name)` |
| Recreate | `HSet` after `Delete(name)` makes a new hash |

No new default process flags beyond `-demo-keyspace` (see below). The
`go run` path starts its **own** in-process mesh (ephemeral ports).

## Run

```bash
# from repo root — starts 3 nodes, prints the walkthrough, exits 0 on success
go run ./examples/hash
```

CI covers the same path: `go test ./examples/hash`.

## Manual `sc` against a node

`supercache-node -demo-keyspace` (the default) now also registers
**`profile`** (`ModeHash`).

```bash
# terminal 1
go run ./cmd/supercache-node \
  -cache 127.0.0.1:9000 -peer 127.0.0.1:9001 -admin 127.0.0.1:8080

# terminal 2
go run ./cmd/sc -keyspace profile hset alice email alice@example.com
go run ./cmd/sc -keyspace profile hset alice name Alice
go run ./cmd/sc -keyspace profile hset alice plan pro
go run ./cmd/sc -keyspace profile hget alice email
go run ./cmd/sc -keyspace profile hexists alice plan
go run ./cmd/sc -keyspace profile hlen alice
go run ./cmd/sc -keyspace profile hgetall alice
# values may contain spaces:
go run ./cmd/sc -keyspace profile hset alice bio writes caches
go run ./cmd/sc -keyspace profile hdel alice plan
go run ./cmd/sc -keyspace profile hlen alice
go run ./cmd/sc -keyspace profile del alice     # tombstone whole hash
```

`hgetall` prints one `field<TAB>value` line per pair. `hget` of a missing
field prints `(nil)` and exits 1.

Wrong mode (e.g. `hset` on `demo` / `get` on `profile`) → invalid argument.

## Contract reminders

- Owner serializes writes; replicas apply item-level `FlagHashSet` / `FlagHashDel`.
- A replica that already has the hash answers `HGet` **locally** (a missing
  field is a miss, not an owner-forward). Non-replicas `GetOrLoad` the owner.
- Hint identity is still `(keyspace, name)` except BloomAdd — same caveat as Set/Geo.
- Design: [docs/design/2026-08-20-mode-hash.md](../../docs/design/2026-08-20-mode-hash.md).
