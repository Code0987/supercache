# supercache (Python)

Sync client for the SuperCache **cache** gRPC port. Python 3.11+. This package is not published to PyPI.

Do not dial the peer port.

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -e ./clients/python
```

```python
from datetime import timedelta

import supercache

c = supercache.dial("127.0.0.1:9000")
c.put("cacheonly", "greeting", b"hello")  # ttl omitted: server keyspace TTL
c.put("cacheonly", "forever", b"x", ttl=timedelta(0))  # ttl_set, no expiry
print(c.get("cacheonly", "greeting"))
c.close()
```

TLS:

```python
c = supercache.dial_tls("127.0.0.1:9000", "ca.pem", "cache.example", "client.pem", "client-key.pem")
```

`server_name` is required. Pass both client files or neither.

## Errors

| Exception | When |
|-----------|------|
| `NotFound` | `get` returned `found=false` |
| `StatusError` | gRPC status. `.code` is `NOT_FOUND`, `INVALID_ARGUMENT`, `UNAVAILABLE`, … |
| `KeyErrors` | `put_many` / `delete_many` body listed per-key failures |
| `PeerFailures` | `delete` body listed replica failures |

A missing keyspace is `StatusError` with code `NOT_FOUND`, not `NotFound`.

## Session

`Session` keeps a sticky seed list. It walks the list on dial. After `UNAVAILABLE` or a connection reset/refused/broken pipe it drops the channel, advances one seed, and retries the call once. It does not retry `NotFound`, `INVALID_ARGUMENT`, batch errors, or a call deadline.

```python
from datetime import timedelta

with supercache.Session(["127.0.0.1:9000", "127.0.0.1:9010"], timeout=timedelta(seconds=5)) as s:
    s.put("cacheonly", "k", b"v")
```

There is no default per-call deadline. `Session(..., timeout=...)` or a method's `timeout=` sets one. `sc` uses 5s; this library does not.

## Methods

Names follow `pkg/client` in snake_case. Redis-style commands stay one word (`zadd`, `lpush`, `hget`, `xrevrange`). The full map is `SURFACE` in `scripts/check-client-surface.py`. Values, items, members, and payloads are `bytes`. Hash fields are `str` or `bytes`. A vector is a sequence of floats, not a string.

`bloom_test` / `set_contains` / `hexists` return false when the entry is absent. `hget`, `lpop`, `rpop`, `lindex`, `json_get`, and `vemb` return `None`. `bit_get` returns `BitValue(value, found)`. Counts that can be absent (`counter_get`, `hll_count`, `cms_query`, `vcard`, `vdim`, `xlen`) return `Present(value, found)`. `topk_list` returns `TopKResult`.
