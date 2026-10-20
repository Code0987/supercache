# @code0987/supercache

Async client for the SuperCache **cache** gRPC port. Node.js 22+. The package is `"private": true` and is not published to npm.

Do not dial the peer port.

```bash
cd clients/node
npm ci
npm run build
```

```js
import { dial } from "@code0987/supercache";

const c = dial("127.0.0.1:9000");
await c.put("cacheonly", "greeting", "hello"); // string values are UTF-8
await c.put("cacheonly", "forever", "x", { ttlMs: 0 }); // ttlSet, no expiry
console.log(Buffer.from(await c.get("cacheonly", "greeting")).toString());
c.close();
```

Point `NODE_PATH` or a workspace at `clients/node` after `npm run build`. The built entry is `dist/src/index.js`.

TLS:

```js
import { dialTls } from "@code0987/supercache";

const c = dialTls("127.0.0.1:9000", {
  caFile: "ca.pem",
  serverName: "cache.example",
  clientCert: "client.pem", // optional; set key too
  clientKey: "client-key.pem",
});
```

`serverName` is required. Pass both client files or neither.

## Errors

| Error | When |
|-------|------|
| `NotFound` | `get` returned `found=false` |
| `StatusError` | gRPC status. `.code` is `NOT_FOUND`, `INVALID_ARGUMENT`, `UNAVAILABLE`, … |
| `KeyErrors` | `putMany` / `deleteMany` body listed per-key failures |
| `PeerFailures` | `delete` body listed replica failures |

A missing keyspace is `StatusError` code `NOT_FOUND`, not `NotFound`.

## Session

`Session` keeps a sticky seed list. It walks the list on dial. After `UNAVAILABLE` or a connection reset/refused/broken pipe it drops the channel, advances one seed, and retries that call once. It does not retry `NotFound`, `INVALID_ARGUMENT`, batch errors, or a call deadline.

```js
import { Session } from "@code0987/supercache";

const s = new Session(["127.0.0.1:9000", "127.0.0.1:9010"], { timeoutMs: 5000 });
await s.put("cacheonly", "k", "v");
s.close();
```

There is no default per-call deadline. Pass `timeoutMs` on the session or as the last argument of a call. `put` / `putMany` take `{ ttlMs, timeoutMs }` instead. `sc` uses 5s; this library does not.

## Methods

Names follow `pkg/client` in camelCase (`putMany`, `zRangeByScore`, `topKList`, `xRevRange`, `lLen`). The full map is `SURFACE` in `scripts/check-client-surface.py`. Returned bytes are `Uint8Array`. A vector is an array of numbers, not a string.

`bloomTest` / `setContains` / `hExists` return false when the entry is absent. `hGet`, `lPop`, `rPop`, `lIndex`, `jsonGet`, and `vEmb` return `null`. `bitGet` returns `{ value, found }`. Counts that can be absent (`counterGet`, `hllCount`, `cmsQuery`, `vCard`, `vDim`, `xLen`) return `{ value, found }`. `topKList` returns `{ entries, found }`.
