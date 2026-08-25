# ModeBitmap example — packed bits

A **self-contained 3-node** walkthrough of `ModeBitmap`: a named packed
bit vector with Redis-order SETBIT / GETBIT / BITCOUNT / BITPOS.

This is the shape you want for presence flags or compact bitsets — not a
`Put` of a whole byte blob on every flip, and not a Bloom filter.

## What it shows

| Step | SuperCache behavior |
|------|---------------------|
| Why not KV / Bloom / Hash / Counter | Whole-blob LWW; approximate membership; field map; one int64 |
| `BitSet` two offsets | Bits 0 and 8 on name `seen` |
| `BitGet` | In-range `1` / `0`; past-end `0` still present; missing name → miss |
| `BitCount` / `BitPos` | Whole bitmap vs byte 0; first `1` in byte 1 is bit 8 |
| Empty-until-delete | Clearing the last `1` leaves the name live until `Delete(seen)` |
| Oversize | Offset that would exceed `MaxValueSize` is rejected |
| RF=2 | Two nodes keep a local snapshot |

No new default process flags beyond `-demo-keyspace` (see below). The
`go run` path starts its **own** in-process mesh (ephemeral ports).

## Run

```bash
# from repo root — starts 3 nodes, prints the walkthrough, exits 0 on success
go run ./examples/bitmap
```

CI covers the same path: `go test ./examples/bitmap`.

## Manual `sc` against a node

`supercache-node -demo-keyspace` (the default) now also registers
**`flags`** (`ModeBitmap`).

`sc bitset` / `bitget` take a numeric offset. The bit token must be `0` or `1`.

```bash
# terminal 1
go run ./cmd/supercache-node \
  -cache 127.0.0.1:9000 -peer 127.0.0.1:9001 -admin 127.0.0.1:8080

# terminal 2
go run ./cmd/sc -keyspace flags bitset seen 0 1
go run ./cmd/sc -keyspace flags bitset seen 8 1
go run ./cmd/sc -keyspace flags bitget seen 0
go run ./cmd/sc -keyspace flags bitget seen 3
go run ./cmd/sc -keyspace flags bitcount seen
go run ./cmd/sc -keyspace flags bitcount seen 0 0
go run ./cmd/sc -keyspace flags bitpos seen 1
go run ./cmd/sc -keyspace flags bitset seen 0 0
go run ./cmd/sc -keyspace flags bitcount seen
go run ./cmd/sc -keyspace flags del seen     # tombstone whole bitmap
```

`bitget` of a missing name prints `(nil)` and exits 1.

Wrong mode (e.g. `bitset` on `demo` / `get` on `flags`) → invalid argument.

## Contract reminders

- Owner applies the op, then fans out a full `FlagBitmap` snapshot.
- A replica that already has the bitmap answers `BitGet` **locally**
  (replica reads may lag).
- Hint identity is still `(keyspace, name)` — that is why fan-out is a snapshot.
- Bit 0 is the MSB of byte 0 (Redis order, not Bloom LSB).
- Design: [docs/design/2026-08-21-mode-bitmap.md](../../docs/design/2026-08-21-mode-bitmap.md).
