# sc — SuperCache CLI

Talk to SuperCache without writing Go: **get/put/del**, **bloom**, **z\***, **geo\***, **l\***, **h\***, **incr** / **cget**, **jsonset** / **jsonget** / **jsondel** over Cache gRPC, **admin** diagnostics over HTTP, **multi-seed** failover, and an interactive **REPL**.

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
| `put` / `set` | Cache gRPC | Store a value (string, `-file`, or stdin) — **KV modes only** (`set` is put, not ModeSet) |
| `del` / `delete` | Cache gRPC | Cluster invalidate (peer warnings on stderr); also wipes named Bloom/set/zset/geo/list/hash/counter/json |
| `bloom add\|test <name> <item>` | Cache gRPC | `ModeBloom` membership |
| `sadd <name> <item>` | Cache gRPC | `ModeSet` add |
| `srem <name> <item>` | Cache gRPC | `ModeSet` remove |
| `sismember <name> <item>` | Cache gRPC | `ModeSet` contains (`true`/`false`; exit 1 if false) |
| `scard <name>` | Cache gRPC | `ModeSet` cardinality |
| `smembers <name>` | Cache gRPC | `ModeSet` members (one per line) |
| `zadd <name> <score> <member>` | Cache gRPC | `ModeZSet` upsert score |
| `zrem <name> <member>` | Cache gRPC | `ModeZSet` remove member |
| `zscore <name> <member>` | Cache gRPC | Print score or `(nil)` |
| `zcard <name>` | Cache gRPC | Member count |
| `zrange <name> <start> <stop>` | Cache gRPC | By rank (Redis-style); lines `score member` |
| `zrangebyscore <name> <min> <max>` | Cache gRPC | Inclusive score window |
| `geoadd <name> <lon> <lat> <member>` | Cache gRPC | `ModeGeo` upsert position |
| `georem <name> <member>` | Cache gRPC | `ModeGeo` remove |
| `geopos <name> <member>` | Cache gRPC | `lon lat` or `(nil)` |
| `geocard <name>` | Cache gRPC | `ModeGeo` count |
| `geodist <name> <a> <b>` | Cache gRPC | meters or `(nil)` |
| `georadius <name> <lon> <lat> <radius_m> [limit]` | Cache gRPC | nearest first |
| `lpush` / `rpush <name> <item>` | Cache gRPC | `ModeList` prepend / append |
| `lpop` / `rpop <name>` | Cache gRPC | `ModeList` pop (or `(nil)`) |
| `llen <name>` | Cache gRPC | `ModeList` length |
| `lindex <name> <idx>` | Cache gRPC | `ModeList` element (Redis negatives) |
| `lrange <name> <start> <stop>` | Cache gRPC | `ModeList` window (one item per line) |
| `hset <name> <field> <value...>` | Cache gRPC | `ModeHash` upsert (`Join` remaining args) |
| `hget <name> <field>` | Cache gRPC | Value or `(nil)` |
| `hdel <name> <field>` | Cache gRPC | Remove field |
| `hexists <name> <field>` | Cache gRPC | `true`/`false`; exit 1 if false |
| `hlen <name>` | Cache gRPC | Field count |
| `hgetall <name>` | Cache gRPC | `field<TAB>value` lines |
| `incr <name> [delta]` | Cache gRPC | `ModeCounter` add (default 1); print new value. Negatives: `incr hits -- -1` |
| `cget <name>` | Cache gRPC | Decimal or `(nil)`; exit 1 if missing |
| `jsonset <name> <path> <json...>` | Cache gRPC | `ModeJSON` upsert (`Join` remaining args; must be JSON, e.g. `'"Ada"'`) |
| `jsonget <name> [path]` | Cache gRPC | Raw JSON or `(nil)`; omitted path = `$`; exit 1 if missing |
| `jsondel <name> [path]` | Cache gRPC | Remove path; omitted path = `$` (clear to `{}`) |
| `ping` | both | Dial cache seeds + admin `/healthz` |
| `peers` / `keyspaces` / `metrics` | Admin HTTP | Diagnostics |
| `health` / `ready` | Admin HTTP | Probes |
| `repl` (or bare `sc` on a TTY) | — | Interactive shell |
| `version` | — | CLI version |

Use `-keyspace` / REPL `keyspace` to select the mode’s keyspace (`demo` KV, `tags` ModeSet, `board` ModeZSet, `profile` ModeHash, `doc` ModeJSON, or your own).

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
