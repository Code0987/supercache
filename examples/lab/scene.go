package main

import (
	"context"
	"encoding/json"
	"fmt"
)

var demoNames = []string{
	"session", "chart", "users", "flags", "board", "places", "inbox",
	"profile", "rl", "doc", "seen", "uniques", "hot", "freq", "items",
}

type sceneStep struct {
	Title string         `json:"title"`
	Op    opReq          `json:"op"`
	Resp  map[string]any `json:"resp"`
}

func (l *Lab) runScene(ctx context.Context, id string) (map[string]any, error) {
	ops, blurb, err := sceneOps(id)
	if err != nil {
		return nil, err
	}
	steps := make([]sceneStep, 0, len(ops))
	for _, req := range ops {
		resp, _ := l.runOp(ctx, req)
		steps = append(steps, sceneStep{Title: req.Op + " " + req.Name, Op: req, Resp: resp})
	}
	return map[string]any{
		"id":    id,
		"blurb": blurb,
		"steps": steps,
	}, nil
}

func sceneOps(id string) ([]opReq, string, error) {
	switch id {
	case "anatomy", "":
		return []opReq{
			mustOp("cacheonly", "put", "session", "", map[string]any{"value": "hello-lab"}),
			mustOp("cacheonly", "get", "session", "", nil),
		}, "3-node mesh, RF=2. Type a key in the inspector to see the owner.", nil
	case "kv-write":
		return []opReq{
			mustOp("cacheonly", "put", "session", "", map[string]any{"value": "v1"}),
			mustOp("cacheonly", "get", "session", "", nil),
		}, "Put ACKs on the owner; a replica fills asynchronously; the third node stays empty.", nil
	case "loadthrough":
		return []opReq{
			mustOp("loadthrough", "get", "chart", "", nil),
			mustOp("loadthrough", "get", "chart", "", nil),
		}, "First Get misses to the mock SoT; the second is a hit. Stampede coalesces to one load.", nil
	case "tombstone":
		return []opReq{
			mustOp("cacheonly", "put", "session", "", map[string]any{"value": "old"}),
			mustOp("cacheonly", "delete", "session", "", nil),
			mustOp("cacheonly", "put", "session", "", map[string]any{"value": "new"}),
		}, "Delete installs a versioned tombstone so a delayed ApplyPut cannot resurrect the old value.", nil
	case "bloom":
		return []opReq{
			mustOp("bloom", "bloomadd", "users", "", map[string]any{"item": "alice"}),
			mustOp("bloom", "bloomtest", "users", "", map[string]any{"item": "alice"}),
			mustOp("bloom", "bloomtest", "users", "", map[string]any{"item": "bob"}),
		}, "Approximate membership. maybe=false means definitely not.", nil
	case "set":
		return []opReq{
			mustOp("set", "sadd", "flags", "", map[string]any{"item": "dark_mode"}),
			mustOp("set", "sismember", "flags", "", map[string]any{"item": "dark_mode"}),
			mustOp("set", "smembers", "flags", "", nil),
		}, "Exact membership. Wrong verb (Get) is invalid argument.", nil
	case "zset":
		return []opReq{
			mustOp("zset", "zadd", "board", "", map[string]any{"member": "alice", "score": 100}),
			mustOp("zset", "zadd", "board", "", map[string]any{"member": "bob", "score": 80}),
			mustOp("zset", "zrange", "board", "", map[string]any{"start": 0, "stop": -1}),
		}, "Scored sorted set. Observations belong in TopK, not here.", nil
	case "geo":
		return []opReq{
			mustOp("geo", "geoadd", "places", "", map[string]any{"member": "shop", "lon": -74.0, "lat": 40.7}),
			mustOp("geo", "georadius", "places", "", map[string]any{"lon": -74.0, "lat": 40.7, "radius": 20000, "limit": 10}),
		}, "WGS84 points + haversine radius. Not an embedding space.", nil
	case "list":
		return []opReq{
			mustOp("list", "rpush", "inbox", "", map[string]any{"item": "event1"}),
			mustOp("list", "rpush", "inbox", "", map[string]any{"item": "event2"}),
			mustOp("list", "lrange", "inbox", "", map[string]any{"start": 0, "stop": -1}),
		}, "Ordered list. Replicas get a full snapshot after each mutate.", nil
	case "hash":
		return []opReq{
			mustOp("hash", "hset", "profile", "", map[string]any{"field": "email", "value": "a@b"}),
			mustOp("hash", "hset", "profile", "", map[string]any{"field": "name", "value": "Ada"}),
			mustOp("hash", "hgetall", "profile", "", nil),
		}, "Per-field LWW. Concurrent field writers do not clobber each other.", nil
	case "counter":
		return []opReq{
			mustOp("counter", "incr", "rl", "", map[string]any{"delta": 1}),
			mustOp("counter", "cget", "rl", "", nil),
		}, "Owner-serialized int64. Live 0 stays until Delete.", nil
	case "json":
		return []opReq{
			mustOp("json", "jsonset", "doc", "", map[string]any{"path": "$.name", "value": `"Ada"`}),
			mustOp("json", "jsonget", "doc", "", map[string]any{"path": "$"}),
		}, "Path set/get on one document. Arrays are not auto-created.", nil
	case "bitmap":
		return []opReq{
			mustOp("bitmap", "bitset", "seen", "", map[string]any{"offset": 0, "bit": true}),
			mustOp("bitmap", "bitget", "seen", "", map[string]any{"offset": 0}),
			mustOp("bitmap", "bitcount", "seen", "", map[string]any{"start": 0, "end": -1}),
		}, "Packed Redis-order bits. Clearing a bit is not Delete.", nil
	case "hll":
		return []opReq{
			mustOp("hll", "hlladd", "uniques", "", map[string]any{"item": "a"}),
			mustOp("hll", "hlladd", "uniques", "", map[string]any{"item": "b"}),
			mustOp("hll", "hllcount", "uniques", "", nil),
		}, "Approximate distinct count. Items are hashed, not stored.", nil
	case "topk":
		return []opReq{
			mustOp("topk", "topkadd", "hot", "", map[string]any{"item": "t001"}),
			mustOp("topk", "topkadd", "hot", "", map[string]any{"item": "t001"}),
			mustOp("topk", "topkadd", "hot", "", map[string]any{"item": "t002"}),
			mustOp("topk", "topklist", "hot", "", nil),
		}, "Space-Saving heavy hitters. Writes are observations, not ZAdd scores.", nil
	case "cms":
		return []opReq{
			mustOp("cms", "cmsincr", "freq", "", map[string]any{"item": "t003", "n": 5}),
			mustOp("cms", "cmsquery", "freq", "", map[string]any{"item": "t003"}),
		}, "Count-Min frequency of any named item, including TopK evictions.", nil
	case "vectorset":
		return []opReq{
			mustOp("vectorset", "vadd", "items", "", map[string]any{"member": "east", "vec": []float32{1, 0}}),
			mustOp("vectorset", "vadd", "items", "", map[string]any{"member": "north", "vec": []float32{0, 1}}),
			mustOp("vectorset", "vsim", "items", "", map[string]any{"vec": []float32{1, 0.05}, "k": 2}),
		}, "Brute-force K-NN. Metric is a keyspace knob, not a query argument.", nil
	default:
		return nil, "", fmt.Errorf("unknown scene %q", id)
	}
}

func mustOp(ks, op, name, via string, args map[string]any) opReq {
	var raw json.RawMessage
	if args != nil {
		raw, _ = json.Marshal(args)
	}
	return opReq{KS: ks, Op: op, Name: name, Via: via, Args: raw}
}
