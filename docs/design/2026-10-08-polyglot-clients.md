# Python and Node.js Cache clients

**Status:** approved
**Branch:** feat/polyglot-clients

## Problem

Applications outside Go can only speak SuperCache by hand-rolling gRPC against `api/proto/cache.proto`. `pkg/client` hides the awkward parts: `GetResponse.found=false` is `ErrNotFound`, batch and delete failures come back in the body (not as a status), and `ttl_set` is distinct from a zero TTL. `cmd/sc` adds sticky multi-seed dialing on top. There is no Python or Node package that does this, and nothing fails CI when `cache.proto` changes and those callers drift.

## Non-goals

- No HTTP/JSON gateway, no Peer-mesh client, no Redis protocol, no in-process engine, no browser / gRPC-Web client.
- No PyPI or npm publish. Packages are in-repo and `private` on the Node side.
- No change to Get, Put, Delete, replication, hints, tombstones, or membership.
- No asyncio client, admin HTTP SDK, or further languages in this change.

## Contract

- Public surface: `clients/python` (`supercache`, sync, Python 3.11+) and `clients/node` (`@code0987/supercache`, async Promises, Node 22+). Both dial the Cache port only.
- Generated stubs are checked in under `clients/python/src/supercache/_gen` and `clients/node/src/gen`. Hand-written facades are what READMEs document. `scripts/gen-clients.sh` regenerates stubs from `cache.proto` only (protoc 28.3, `grpcio-tools` 1.69.0, `ts-proto` 2.7.7). It does not touch `api/gen`.
- Method names mirror `pkg/client`. Python uses snake_case and Redis-style one-word commands (`zadd`, `lpush`, `hget`, `xrevrange`, `zrange_by_score`). Node uses camelCase (`putMany`, `zRangeByScore`, `topKList`, `xRevRange`).
- Keys, keyspaces, JSON paths, and stream ids are strings. Values, items, members, and payloads are bytes (`bytes` in Python, `Uint8Array` in Node). Node also accepts a string and UTF-8-encodes it. Hash fields accept string or bytes.
- `ttl` omitted → `ttl_set=false`. Explicit zero → `ttl_set=true` and `ttl_nanos=0`. Python takes `datetime.timedelta | None`. Node takes `ttlMs?: number`.
- `get` miss raises `NotFound`. That type is only `found=false`. A gRPC status, including `NOT_FOUND` for a missing keyspace, is `StatusError` with `code` and `message`.
- `PutMany` / `DeleteMany` body errors raise `KeyErrors`. `Delete` body peer failures raise `PeerFailures`. Those RPCs are successes on the wire.
- Bool reads (`bloom_test`, `set_contains`, `hexists`) return false for a miss. Byte reads (`hget`, `lpop`, `lindex`, `json_get`, `vemb`) return null. `bit_get` returns `{value, found}`. Counts that can be absent (`counter_get`, `hll_count`, `cms_query`, `vcard`, `vdim`, `xlen`) return `{value, found}`. `topk_list` returns `{entries, found}`. Missing structures return empty or 0, matching Go.
- `Session` copies `cmd/sc` sticky seeds: walk the list once on dial, and on gRPC `UNAVAILABLE` or a connection reset/refused/broken-pipe, drop the client, advance one seed, and retry the call once. Do not retry `NotFound`, `INVALID_ARGUMENT`, `KeyErrors`, `PeerFailures`, or a call-level deadline (an `Incr` may already have been applied). Library default is no per-call deadline. `Session` accepts an optional timeout applied to each attempt.
- Plaintext `dial` for local use. `dial_tls(addr, ca_file, server_name, client_cert=None, client_key=None)` for TLS. `server_name` is required. Client cert and key are both set or both omitted.
- CI job `clients-up-to-date` regenerates stubs and fails on `git diff`, then `scripts/check-client-surface.py` fails unless every `service Cache` RPC has a Python method, a Node method, and its RPC name in `docs/API.md`.

Who stores a copy, RF, hints, and TTL behavior are unchanged. An existing Go client can still assume the current server contract.

## Approach

`clients/conformance` is a single-node Engine plus Cache gRPC server. `go run ./clients/conformance` prints `addr=` and blocks. `-tls` / `-mtls` also print PEM paths. It registers one keyspace per mode, including LoadThrough backed by `datasource.Map{"seeded": "from-source"}`. Language tests build that binary and talk to it.

Facades are thin wrappers over the generated Cache stub. They do not pre-check keyspace mode. Wrong-mode verbs stay server `INVALID_ARGUMENT`.

Rejected alternatives:

- REST gateway: second public protocol, and the cache port is gRPC today.
- Separate client repos: stubs drift from `cache.proto`.
- KV-only first cut: a stale subset of one service.
- Connect-RPC: the server is `google.golang.org/grpc`.

## Tests (write these first, after approval)

| Test | Package | Asserts |
|------|---------|---------|
| Boot, keyspace list, load-through seed, TLS PEM files | `clients/conformance` | One keyspace per mode; `seeded` loads; certs parse; plaintext gRPC Put/Get works |
| Get/Put/Delete, PutMany empty key, missing keyspace, ttl 0 vs omitted, every mode happy path, wrong-mode Get, TLS and mTLS, session failover | `clients/python/tests` | `NotFound` is not status `NOT_FOUND`; `KeyErrors` on empty key; one round-trip per mode; closed port then live seed; `UNAVAILABLE` retried once; `NotFound` not retried |
| Same behavior table | `clients/node/test` | Same assertions on the Promise client |
| Surface check | `scripts/check-client-surface.py` | Every Cache RPC maps to both facades and appears in `docs/API.md` |

## Bench risk

No shared smoke or micro cell should move. Hot path (Get-hit, Put, store mutex): no. This change does not edit `pkg/engine` or `pkg/store`. CI `bench` still runs and must stay inside ±10% with flat Get-hit allocs.

## PR Plan

The work lands as one branch sequence. Do not merge until the bench comment is inside the gate and the user says merge.

1. **Conformance and freshness gate** — `clients/conformance`, `scripts/gen-clients.sh`, `scripts/check-client-surface.py`, `.github/workflows/ci.yml` job `clients-up-to-date`, this design. Depends on nothing.
2. **Python client** — `clients/python`, Python half of job `clients`, README / API.md / PLAN. Depends on PR 1.
3. **Node client** — `clients/node`, Node half of job `clients`, the same docs for Node. Depends on PR 1. Parallel with PR 2 after the generator exists.

Publishing to PyPI and npm is a later design.
