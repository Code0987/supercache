# Music Trending Billboard (SuperCache cluster demo)

A full use-case example: a **read-heavy music trending billboard** backed by a **3-node SuperCache cluster** in one process.

## What it shows

| SuperCache feature | Billboard usage |
|--------------------|-----------------|
| 3-node cluster | Gossip membership + peer mesh + separate cache ports |
| `LoadThrough` keyspace `charts` | Global/genre boards + track cards via mock chart aggregator |
| `CacheOnly` keyspace `meta` | Editorial pins |
| DataSource + latency | Expensive SoT with detailed `[SoT]` logs |
| singleflight | `/v1/demo/stampede` — concurrent miss coalescing |
| protect | Per-keyspace + global rate limit / circuit breaker |
| TTL / NegativeTTL | Chart freshness + unknown keys |
| Delete | Cluster invalidate + SoT reload |
| Put + fan-out | Pin write and read-back |
| WarmKeys / refresh-ahead | Prefetch global + genres; owner refresh |
| Admin HTTP | `/peers`, `/keyspaces`, `/metrics` on each node |
| `pkg/client` | App layer round-robins cache gRPC nodes |

## Run

```bash
# Full demo walkthrough, then keep serving (Ctrl+C to stop)
go run ./examples/billboard

# Demo then exit (good for CI / quick check)
go run ./examples/billboard -hold=false

# Serve only (no scripted demo)
go run ./examples/billboard -demo=false

# Slower mock SoT (makes hit vs miss obvious)
go run ./examples/billboard -sot-latency=300ms -hold=false
```

## Ports

| Service | Address |
|---------|---------|
| Billboard app HTTP | `http://127.0.0.1:18080` |
| Cache gRPC nodes | `9101`, `9102`, `9103` |
| Peer gRPC | `9201`, `9202`, `9203` |
| Admin | `8081`, `8082`, `8083` |
| Gossip | `7941`–`7943` |

## Try manually

```bash
curl -s http://127.0.0.1:18080/v1/charts/global | jq .
curl -s http://127.0.0.1:18080/v1/charts/pop | jq .
curl -s -X POST http://127.0.0.1:18080/v1/admin/invalidate/global
curl -s 'http://127.0.0.1:18080/v1/demo/stampede?board=global' | jq .
curl -s http://127.0.0.1:8081/peers | jq .
curl -s http://127.0.0.1:8081/keyspaces | jq .
```

Open `http://127.0.0.1:18080/` for a small HTML billboard UI.

## Logs

Watch for prefixes:

- `[main]` — process lifecycle / ring snapshots  
- `[billboard-N]` — per-node SuperCache (membership, listeners, keyspaces)  
- `[SoT]` — mock chart aggregator loads (begin/ok/fail, in_flight)  
- `[app]` — HTTP API → SuperCache client path  
- `[http]` — request access log  

The scripted demo prints a **features exercised** checklist at the end.
