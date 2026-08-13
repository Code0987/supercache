# sc — SuperCache CLI

Talk to SuperCache without writing Go: **get/put/del**, **bloom**, **z\*** (sorted set) over Cache gRPC, **admin** diagnostics over HTTP, **multi-seed** failover, and an interactive **REPL**.

## Quick start

```bash
# Terminal 1
go run ./cmd/supercache-node \
  -cache 127.0.0.1:9000 -peer 127.0.0.1:9001 -admin 127.0.0.1:8080

# One-shot
go run ./cmd/sc put greeting "hello world"
go run ./cmd/sc get greeting
go run ./cmd/sc del greeting
go run ./cmd/sc peers

# Interactive REPL
go run ./cmd/sc
# sc demo@:9000> put k v
# sc demo@:9000> get k
# sc demo@:9000> quit
```

Install:

```bash
go install ./cmd/sc
```

## Multi-seed

`-addr` (and `SC_ADDR`) accept a **comma-separated** list of Cache gRPC ports:

```bash
sc -addr 127.0.0.1:9000,127.0.0.1:9010,127.0.0.1:9020 ping
sc -addr n1:9000,n2:9000,n3:9000 get mykey
```

| Behavior | Detail |
|----------|--------|
| Dial order | Try seeds in list order; remember last success (sticky) |
| Failover | On connection/Unavailable errors, rotate to the next seed and retry once |
| Writes | Still routed to the **ring owner** via `ForwardPut` — seeds are only the entry point |
| Admin | `-admin a,b,c` tries each admin HTTP until one responds |

This is **not** client-side sharding. Any healthy cache node is a valid front door.

## Commands

| Command | Port | What it does |
|---------|------|----------------|
| `get <key> [key...]` | Cache gRPC | Fetch value(s); exit `1` if any missing |
| `put` / `set` | Cache gRPC | Store a value (string, `-file`, or stdin) — **KV modes only** |
| `del` / `delete` | Cache gRPC | Cluster invalidate (peer warnings on stderr); also wipes named Bloom/set/zset |
| `bloom add\|test <name> <item>` | Cache gRPC | `ModeBloom` membership |
| `zadd <name> <score> <member>` | Cache gRPC | `ModeZSet` upsert score |
| `zrem <name> <member>` | Cache gRPC | `ModeZSet` remove member |
| `zscore <name> <member>` | Cache gRPC | Print score or `(nil)` |
| `zcard <name>` | Cache gRPC | Member count |
| `zrange <name> <start> <stop>` | Cache gRPC | By rank (Redis-style); lines `score member` |
| `zrangebyscore <name> <min> <max>` | Cache gRPC | Inclusive score window |
| `ping` | both | Dial cache seeds + admin `/healthz` |
| `peers` / `keyspaces` / `metrics` | Admin HTTP | Diagnostics |
| `health` / `ready` | Admin HTTP | Probes |
| `repl` (or bare `sc` on a TTY) | — | Interactive shell |
| `version` | — | CLI version |

Use `-keyspace` / REPL `keyspace` to select the mode’s keyspace (`demo` KV, `tags` ModeSet via Go client, `board` ModeZSet, or your own). ModeSet has gRPC/client APIs; `sc set …` is not wired yet.

## REPL

```bash
sc -addr 127.0.0.1:9000,127.0.0.1:9010
```

```text
connected 127.0.0.1:9000  keyspace=demo  seeds=2
sc demo@:9000> put session:1 '{"user":1}'
sc demo@:9000> get session:1
sc demo@:9000> keyspace board
sc board@:9000> zadd lb 100 alice
sc board@:9000> zrange lb 0 -1
sc board@:9000> seeds
sc board@:9000> connect :9010
sc board@:9000> peers
sc board@:9000> quit
```

| REPL meta | Meaning |
|-----------|---------|
| `keyspace` / `use` / `ks` | Switch default keyspace |
| `seeds` | List cache/admin seeds (`*` = active) |
| `connect [seed]` | Re-dial; optional seed or substring |
| `ttl [duration\|default]` | Put TTL for this session |
| `json [on\|off]` | JSON output |
| `timeout [duration]` | Request timeout |
| `help` / `quit` | |

Quotes work: `put k "hello world"`. Per-line flags: `put k -file ./x.bin`, `get k -base64`.

## Flags

| Flag | Default | Env |
|------|---------|-----|
| `-addr` | `127.0.0.1:9000` | `SC_ADDR` (comma-separated OK) |
| `-admin` | `127.0.0.1:8080` | `SC_ADMIN` |
| `-keyspace` | `demo` | `SC_KEYSPACE` |
| `-timeout` | `5s` | |
| `-ttl` / `-no-expiry` | keyspace default | put only |
| `-file` | | put value from path (`-` = stdin) |
| `-base64` / `-json` / `-raw` / `-q` | false | |
| `-tls-ca` / `-tls-cert` / `-tls-key` / `-tls-server-name` | | `SC_TLS_*` |

Flags may appear before or after the subcommand.

## Notes

- Dial the **Cache** port (`-cache` on the node), never the Peer mesh port.
- SuperCache is eventually consistent: Put ACKs on the owner; Get on another seed may lag until fan-out.
- `UpdateKeySpace` is not on the wire — configure each node (see `docs/OPERATIONS.md`).
- Admin HTTP is plaintext in the default node binary.
- Value size is limited by gRPC / node max (~4 MiB receive default).
