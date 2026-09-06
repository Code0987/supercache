# ModeVectorSet example — similar items

A **self-contained 3-node** walkthrough of `ModeVectorSet`: a named set of
`float32` embeddings with brute-force K-NN. Metric is a **keyspace** knob
(cosine / L2 / IP), not an argument on `VSim`.

## What it shows

| Step | SuperCache behavior |
|------|---------------------|
| Why not ZSet / Put | ZSet score is a scalar you write. A JSON blob `Put` is one LWW value |
| `VAdd` | Three members on cosine `items`; first add locks dim=2 |
| `VCard` / `VDim` / `VEmb` | Count, locked dim, stored vector copy |
| RF=2 + `VSim` | Every node returns the same cosine order; non-replica does not install |
| Replace | `VAdd` same id overwrites the vector; card stays 3 |
| Dim lock / zero | Wrong dim and cosine `{0,0}` are invalid; Get/Put rejected |
| L2 vs IP | Same two members: L2 picks nearest, IP picks largest dot |
| `k` / missing | `k>card` returns card hits; missing name is empty / `present=false` |
| Last `VRem` | Empty live set keeps dim; `Delete` is what drops the name |
| Recreate | `Delete` + `VAdd` makes a new set |

## Run

```bash
# from repo root — starts 3 nodes, prints the walkthrough, exits 0 on success
go run ./examples/vecset
```

CI covers the same path: `go test ./examples/vecset`.

## Manual `sc` against a stock node

`supercache-node -demo-keyspace` (the default) registers **`vectorset`**
(`ModeVectorSet`, cosine, dim unlocked until first `vadd`).

```bash
# terminal 1
go run ./cmd/supercache-node \
  -cache 127.0.0.1:9000 -peer 127.0.0.1:9001 -admin 127.0.0.1:8080

# terminal 2 — add, inspect, search
go run ./cmd/sc -keyspace vectorset vadd products east 1,0
go run ./cmd/sc -keyspace vectorset vadd products north 0,1
go run ./cmd/sc -keyspace vectorset vadd products ne 0.7,0.7
go run ./cmd/sc -keyspace vectorset vcard products
go run ./cmd/sc -keyspace vectorset vdim products
go run ./cmd/sc -keyspace vectorset vemb products east
go run ./cmd/sc -keyspace vectorset vsim products 1,0.05 3

# replace, rem, missing
go run ./cmd/sc -keyspace vectorset vadd products east 0,1
go run ./cmd/sc -keyspace vectorset vsim products 1,0 3
go run ./cmd/sc -keyspace vectorset vrem products north
go run ./cmd/sc -keyspace vectorset vcard products
go run ./cmd/sc -keyspace vectorset vdim missing   # prints missing, exit 1

# cosine rejects a zero vector (L2/IP would allow it on another keyspace)
# go run ./cmd/sc -keyspace vectorset vadd products origin 0,0
```
