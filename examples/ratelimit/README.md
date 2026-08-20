# Fixed-window rate limiter (`ModeCounter`)

Uses SuperCache **`Incr`**, which returns the new count. Each window is a
**new counter name** (`alice:<unix/windowSecs>`) because owner `Incr` slides
keyspace TTL — the window must live in the name, not in ExpireAt.

## What it shows

| Step | Behavior |
|------|----------|
| Why Counter | KV blob RMW loses concurrent incrs; Hash has no `HINCRBY` |
| `Allow(key, 3, 60s)` × 4 | allow, allow, allow, **deny** (`n` 1–4) |
| RF=2 | two nodes keep the window counter |
| `Delete(name)` | reset; next Allow is `n==1` |
| `window < 1s` | error (no divide-by-zero) |

The test/demo uses a **60s** window so a Unix-second boundary cannot flake
the 3-allow/1-deny burst. `Incr` is always owner-serialized — that `n` is
the authority. Replica `cget` may lag.

## Run

```bash
go run ./examples/ratelimit
go test ./examples/ratelimit
```

## Manual `sc` (register a ModeCounter keyspace yourself)

This PR does **not** add a default `-demo-keyspace` counter. Against a node
that has keyspace `rl` / `ModeCounter`:

```bash
go run ./cmd/sc -keyspace rl incr alice:1
go run ./cmd/sc -keyspace rl incr alice:1
go run ./cmd/sc -keyspace rl cget alice:1
go run ./cmd/sc -keyspace rl incr hits -- -1
```

Design: [docs/design/2026-08-20-mode-counter.md](../../docs/design/2026-08-20-mode-counter.md).
