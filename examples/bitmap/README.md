# ModeBitmap example — packed bits

A **self-contained 3-node** walkthrough of `ModeBitmap`: a named packed
bit vector with Redis-order SETBIT / GETBIT / BITCOUNT / BITPOS.

## Run

```bash
go run ./examples/bitmap
```

CI: `go test ./examples/bitmap`.

## Manual `sc` (register a ModeBitmap keyspace yourself)

```bash
sc -keyspace flags bitset seen 0 1
sc -keyspace flags bitset seen 8 1
sc -keyspace flags bitget seen 0
sc -keyspace flags bitcount seen
sc -keyspace flags bitpos seen 1
sc -keyspace flags del seen
```

No default `-demo-keyspace` Bitmap name. Design: [docs/design/2026-08-21-mode-bitmap.md](../../docs/design/2026-08-21-mode-bitmap.md).
