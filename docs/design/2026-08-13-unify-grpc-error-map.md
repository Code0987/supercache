# Unify gRPC engine→status error mapping

**Status:** approved  
**Branch:** `refactor/unify-grpc-error-map`

## Problem

Application Cache RPCs (`internal/cacheserver`) and mesh Peer RPCs (`internal/peerserver`) each maintain a nearly identical `mapErr` / `mapPeerErr` switch that translates `pkg/engine` sentinel errors into gRPC status codes.

```text
cacheserver.mapErr     peerserver.mapPeerErr
NotFound            →  NotFound
KeyspaceNotFound    →  NotFound
InvalidArgument     →  InvalidArgument
KeyTooLarge         →  InvalidArgument
ValueTooLarge       →  InvalidArgument
BatchTooLarge       →  InvalidArgument   (cache only today)
Unavailable         →  Unavailable
default             →  Internal
```

Drift already exists:

- Cache maps `ErrBatchTooLarge`; peer does not (peer has no batch RPCs — fine, but the *table* should live in one place).
- `ForwardDelete` on the peer server returns some non-`MultiError` failures **without** `mapPeerErr`, so clients can see raw Go errors instead of status codes (inconsistent with Apply/ForwardPut/GetOrLoad).

Two copies make the next status-code decision easy to fix in one server and miss the other.

## Non-goals

- No change to engine error types, messages, or data-path semantics (Get/Put/Delete/RF/hints/tombstones).
- No new public client API surface beyond more consistent gRPC status codes on peer RPCs.
- No HTTP admin error mapping.
- No broader engine file split (separate follow-up if wanted).

## Contract

| Surface | Before | After |
|---------|--------|--------|
| Cache gRPC status codes for engine sentinels | as today | **identical** |
| Peer gRPC status codes for engine sentinels on mapped paths | as today | **identical** for paths that already call `mapPeerErr` |
| Peer `ForwardDelete` non-`MultiError` failure | raw `error` (often not a status) | **status via shared mapper** (same codes as other peer methods) |
| `MultiError` on ForwardDelete / Delete | structured failures in response body, gRPC OK | **unchanged** |
| Public `pkg/client` (Cache API) | unchanged | unchanged |

Invariant this refactor must not break: every existing engine sentinel that already maps to a gRPC code keeps that code on both servers.

## Approach

1. Add a small helper package used only by the two gRPC servers, e.g. `internal/grpcmap`:

   ```go
   // Status maps engine (and compatible) errors to gRPC status errors.
   // nil → nil. Unknown → codes.Internal.
   func Status(err error) error
   ```

   Implementation is the **union** of today’s switches (include `ErrBatchTooLarge` → InvalidArgument). Peer never produces batch errors; mapping them is harmless.

2. Replace `cacheserver.mapErr` and `peerserver.mapPeerErr` with calls to `grpcmap.Status`.

3. Peer `ForwardDelete`: on non-`MultiError` failure, return `grpcmap.Status(err)` instead of bare `err` (align with ApplyDelete / ForwardPut).

4. Keep server files thin; no engine imports of gRPC.

Rejected alternatives:

- Put mapper on `pkg/engine` — couples engine to gRPC.
- Leave two copies and only document them — drift will return.
- Change `MultiError` into a gRPC status — would break structured peer_failures responses; out of scope.

## Tests (write/adjust after approval)

Existing RPC tests already hit most map paths. After approval:

| Test | Package | Asserts |
|------|---------|---------|
| Keep / extend `TestCacheRPCPutGetDeleteBloomAndMapErr` | `internal/cacheserver` | empty key → InvalidArgument; missing ks → NotFound; batch too large → InvalidArgument; protect miss → Unavailable |
| Keep / extend `TestPeerMapErrValueTooLargeUnavailableInternal` | `internal/peerserver` | value too large → InvalidArgument; protect → Unavailable; DS error → Internal |
| `TestForwardDeleteMissingKeyspaceIsNotFound` | `internal/peerserver` | ForwardDelete unknown ks → gRPC NotFound (**not** a raw error string without status) |
| Optional unit table | `internal/grpcmap` | each engine sentinel → expected `codes.Code` |

No new engine/store tests required if the above stay green (pure move + one peer path consistency fix).

## Bench risk

| Cell / micro | Risk |
|--------------|------|
| Get-hit / StoreGetHit | **None** — error path only; hot path does not call mapper on success |
| Put / Delete smoke | Negligible — one extra function call on error only |

Hot path? **no**.

## Implementation notes (for after approval)

- Track: **refactor** (contract same for Cache; Peer ForwardDelete error shape becomes consistent status codes).
- Order: add `grpcmap` + table test → wire both servers → strengthen ForwardDelete test → `go test ./...` → PR → bench.
