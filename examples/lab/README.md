# SuperCache Lab

Interactive visual explorer. Default `go run` is **UI only** — it does not start
a mesh. Connect cache gRPC addresses in the page, or opt into a local 3-node demo.

## Run

```bash
# from repo root — build UI once, then serve (no cluster)
cd examples/lab/ui && npm ci && npm run build && cd ../../..
go run ./examples/lab
# open http://127.0.0.1:19080/  → Connect 127.0.0.1:9000,… or "Local 3-node"

# attach at startup
go run ./examples/lab -addr 127.0.0.1:9000,127.0.0.1:9010

# old in-process demo mesh
go run ./examples/lab -cluster
```

Hot-reload UI (two processes):

```bash
go run ./examples/lab
cd examples/lab/ui && npm run dev   # http://127.0.0.1:5173/  proxies /v1
```

Walkthrough then exit (CI):

```bash
go run ./examples/lab -hold=false
```

## What it shows

The canvas is the mesh. Chapters on the left drive a scripted scene; the playground
runs the same verbs by hand. After each call the UI polls `LocalView` on every node.

| Chapter | Keyspace | Point |
|---------|----------|--------|
| Anatomy | `cacheonly` | 3 nodes, owner of a name |
| KV write | `cacheonly` | owner ACK + async replica fill |
| LoadThrough | `loadthrough` | SoT miss, hit, singleflight |
| Tombstone | `cacheonly` | delete marker vs live |
| Bloom … VectorSet | matching mode | thin widget + wrong-verb on Set |
| Stream | `stream` | append-only log; XAdd returns id; XRange `-` `+` |

Lab HTTP defaults to `127.0.0.1:19080`. `-cluster` uses ephemeral cache/peer
ports (`internal/testcluster`). Mock SoT latency: `-sot-latency` (in-process only).
Remote attach talks cache gRPC only — `LocalView` owner/replica pixels need `-cluster`.

## Tests

```bash
go test ./examples/lab
```

JSON API only — Node is not required for `go test`.
