# 3-node cluster demo

Exercises **CacheOnly** (`demo`: sessions / KV), **ModeSet** (`tags`: feature flags), and **ModeZSet** (`board`: leaderboard via `ZAdd` / `ZScore` / `ZRange` / `ZRem`).

Requires `supercache-node` with `-demo-keyspace` (default), which registers `demo`, `tags`, `board`, and `profile` (this example uses the first three).

Start three nodes (separate terminals), then run this example.

```bash
# from repo root — build
go build -o supercache-node ./cmd/supercache-node

# terminal 1
./supercache-node -cluster -node-id n1 \
  -admin 127.0.0.1:8081 -cache 127.0.0.1:9010 -peer 127.0.0.1:9001 \
  -gossip-bind 127.0.0.1 -gossip-port 7946 -gossip-advertise 127.0.0.1

# terminal 2
./supercache-node -cluster -node-id n2 \
  -admin 127.0.0.1:8082 -cache 127.0.0.1:9011 -peer 127.0.0.1:9002 \
  -gossip-bind 127.0.0.1 -gossip-port 7947 -gossip-advertise 127.0.0.1 \
  -seeds 127.0.0.1:7946

# terminal 3
./supercache-node -cluster -node-id n3 \
  -admin 127.0.0.1:8083 -cache 127.0.0.1:9012 -peer 127.0.0.1:9003 \
  -gossip-bind 127.0.0.1 -gossip-port 7948 -gossip-advertise 127.0.0.1 \
  -seeds 127.0.0.1:7946

# demo
go run ./examples/cluster3/
```
