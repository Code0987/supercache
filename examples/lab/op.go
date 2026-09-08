package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Code0987/supercache/pkg/client"
	"github.com/Code0987/supercache/pkg/engine"
)

type opReq struct {
	KS   string          `json:"ks"`
	Op   string          `json:"op"`
	Name string          `json:"name"`
	Via  string          `json:"via"`
	Args json.RawMessage `json:"args"`
}

type opArgs struct {
	Value  string          `json:"value"`
	Item   string          `json:"item"`
	Member string          `json:"member"`
	Field  string          `json:"field"`
	Path   string          `json:"path"`
	A      string          `json:"a"`
	B      string          `json:"b"`
	Score  float64         `json:"score"`
	Delta  int64           `json:"delta"`
	Offset uint64          `json:"offset"`
	Bit    *bool           `json:"bit"`
	Lon    float64         `json:"lon"`
	Lat    float64         `json:"lat"`
	Radius float64         `json:"radius"`
	Limit  int             `json:"limit"`
	Start  json.RawMessage `json:"start"`
	Stop   *int            `json:"stop"`
	End    json.RawMessage `json:"end"`
	K      int             `json:"k"`
	N      uint64          `json:"n"`
	Vec    []float32       `json:"vec"`
	ID     string          `json:"id"`
	Count  int             `json:"count"`
	MaxLen int             `json:"max_len"`
}

func (l *Lab) runOp(ctx context.Context, req opReq) (map[string]any, int) {
	if req.KS == "" || req.Op == "" || req.Name == "" {
		return map[string]any{"ok": false, "error": "ks, op, and name required", "invalid_argument": true}, http.StatusBadRequest
	}
	cli, node, err := l.pick(req.Via)
	if err != nil {
		return map[string]any{"ok": false, "error": err.Error()}, http.StatusBadRequest
	}
	var args opArgs
	if len(req.Args) > 0 {
		_ = json.Unmarshal(req.Args, &args)
	}
	before := l.snapshot(req.KS, req.Name)
	sot0 := l.sotLoads()
	result, err := dispatch(ctx, cli, req.KS, req.Op, req.Name, args)
	after := l.snapshot(req.KS, req.Name)
	// Poll so async fan-out can flip replica Missing → Live before we return.
	deadline := time.Now().Add(120 * time.Millisecond)
	for time.Now().Before(deadline) {
		if replicaFilled(before, after) {
			break
		}
		time.Sleep(20 * time.Millisecond)
		after = l.snapshot(req.KS, req.Name)
	}
	sotDelta := l.sotLoads() - sot0
	owner, _ := after["owner"].(string)
	out := map[string]any{
		"ok":               err == nil,
		"result":           result,
		"via":              node.ID,
		"owner":            owner,
		"before":           before,
		"after":            after,
		"trace":            inferTrace(req.Op, node.ID, owner, before, after, sotDelta),
		"sot_loads":        l.sotLoads(),
		"sot_delta":        sotDelta,
		"invalid_argument": false,
	}
	if err != nil {
		out["ok"] = false
		out["error"] = err.Error()
		if isInvalidArg(err) {
			out["invalid_argument"] = true
			return out, http.StatusBadRequest
		}
		if err == client.ErrNotFound {
			out["error"] = "not found"
			return out, http.StatusOK
		}
		return out, http.StatusOK
	}
	return out, http.StatusOK
}

func replicaFilled(before, after map[string]any) bool {
	b := nodesByID(before)
	a := nodesByID(after)
	for id, nv := range a {
		if nv["role"] == "owner" {
			continue
		}
		if nv["kind"] == "live" && b[id]["kind"] != "live" {
			return true
		}
	}
	return false
}

func nodesByID(snap map[string]any) map[string]map[string]any {
	out := map[string]map[string]any{}
	raw, _ := snap["nodes"].([]any)
	if raw == nil {
		if typed, ok := snap["nodes"].([]map[string]any); ok {
			for _, n := range typed {
				id, _ := n["id"].(string)
				out[id] = n
			}
			return out
		}
	}
	for _, v := range raw {
		n, _ := v.(map[string]any)
		id, _ := n["id"].(string)
		out[id] = n
	}
	return out
}

func inferTrace(op, via, owner string, before, after map[string]any, sotDelta int64) []string {
	var t []string
	write := isWriteOp(op)
	read := !write
	if write && via != "" && owner != "" && via != owner {
		t = append(t, "inferred: forwarded to owner, then ACK")
	}
	if write {
		b, a := nodesByID(before), nodesByID(after)
		for id, nv := range a {
			if nv["kind"] == "live" && b[id]["kind"] != "live" && id != owner {
				t = append(t, "inferred: async fan-out")
				break
			}
		}
	}
	if read {
		b := nodesByID(before)
		if via != "" && b[via]["kind"] == "live" {
			t = append(t, "inferred: local hit")
		} else if op == "get" && via != owner && owner != "" {
			t = append(t, "inferred: owner-forward")
		}
	}
	if sotDelta > 0 {
		t = append(t, fmt.Sprintf("inferred: DataSource load (×%d)", sotDelta))
	}
	if len(t) == 0 {
		t = append(t, "inferred: observed after the call (no extra hops visible)")
	}
	return t
}

func isWriteOp(op string) bool {
	switch strings.ToLower(op) {
	case "put", "delete", "bloomadd", "sadd", "srem", "zadd", "zrem",
		"geoadd", "georem", "lpush", "rpush", "lpop", "rpop",
		"hset", "hdel", "incr", "jsonset", "jsondel",
		"bitset", "hlladd", "topkadd", "cmsincr", "vadd", "vrem",
		"xadd", "xdel", "xtrim":
		return true
	}
	return false
}

func dispatch(ctx context.Context, cli *client.Client, ks, op, name string, a opArgs) (any, error) {
	stop := -1
	if a.Stop != nil {
		stop = *a.Stop
	}
	start := asInt(a.Start, 0)
	end := asInt(a.End, -1)
	bit := true
	if a.Bit != nil {
		bit = *a.Bit
	}
	if a.Delta == 0 && strings.ToLower(op) == "incr" {
		a.Delta = 1
	}
	if a.N == 0 {
		a.N = 1
	}
	if a.K <= 0 {
		a.K = 3
	}
	if a.Path == "" {
		a.Path = "$"
	}
	switch strings.ToLower(op) {
	case "get":
		v, err := cli.Get(ctx, ks, name)
		if err != nil {
			return nil, err
		}
		return map[string]any{"value": string(v)}, nil
	case "put":
		return map[string]any{"acked": true}, cli.Put(ctx, ks, name, []byte(a.Value))
	case "delete", "del":
		return map[string]any{"acked": true}, cli.Delete(ctx, ks, name)
	case "bloomadd":
		return map[string]any{"acked": true}, cli.BloomAdd(ctx, ks, name, []byte(a.Item))
	case "bloomtest":
		maybe, err := cli.BloomTest(ctx, ks, name, []byte(a.Item))
		return map[string]any{"maybe": maybe}, err
	case "sadd", "setadd":
		return map[string]any{"acked": true}, cli.SetAdd(ctx, ks, name, []byte(a.Item))
	case "srem", "setremove":
		return map[string]any{"acked": true}, cli.SetRemove(ctx, ks, name, []byte(a.Item))
	case "sismember", "setcontains":
		ok, err := cli.SetContains(ctx, ks, name, []byte(a.Item))
		return map[string]any{"present": ok}, err
	case "scard", "setcard":
		n, err := cli.SetCard(ctx, ks, name)
		return map[string]any{"card": n}, err
	case "smembers", "setmembers":
		ms, err := cli.SetMembers(ctx, ks, name)
		return map[string]any{"members": asStrings(ms)}, err
	case "zadd":
		return map[string]any{"acked": true}, cli.ZAdd(ctx, ks, name, []byte(a.Member), a.Score)
	case "zrem":
		return map[string]any{"acked": true}, cli.ZRem(ctx, ks, name, []byte(a.Member))
	case "zscore":
		sc, ok, err := cli.ZScore(ctx, ks, name, []byte(a.Member))
		return map[string]any{"score": sc, "present": ok}, err
	case "zcard":
		n, err := cli.ZCard(ctx, ks, name)
		return map[string]any{"card": n}, err
	case "zrange":
		ms, err := cli.ZRange(ctx, ks, name, start, stop)
		return zMembersJSON(ms), err
	case "geoadd":
		return map[string]any{"acked": true}, cli.GeoAdd(ctx, ks, name, []byte(a.Member), a.Lon, a.Lat)
	case "georem":
		return map[string]any{"acked": true}, cli.GeoRem(ctx, ks, name, []byte(a.Member))
	case "geopos":
		lon, lat, ok, err := cli.GeoPos(ctx, ks, name, []byte(a.Member))
		return map[string]any{"lon": lon, "lat": lat, "present": ok}, err
	case "geocard":
		n, err := cli.GeoCard(ctx, ks, name)
		return map[string]any{"card": n}, err
	case "geodist":
		d, ok, err := cli.GeoDist(ctx, ks, name, []byte(a.A), []byte(a.B))
		return map[string]any{"meters": d, "present": ok}, err
	case "georadius":
		ms, err := cli.GeoRadius(ctx, ks, name, a.Lon, a.Lat, a.Radius, a.Limit)
		out := make([]map[string]any, 0, len(ms))
		for _, m := range ms {
			out = append(out, map[string]any{"member": string(m.Member), "lon": m.Lon, "lat": m.Lat, "dist_meters": m.Dist})
		}
		return map[string]any{"members": out}, err
	case "lpush":
		return map[string]any{"acked": true}, cli.LPush(ctx, ks, name, []byte(a.Item))
	case "rpush":
		return map[string]any{"acked": true}, cli.RPush(ctx, ks, name, []byte(a.Item))
	case "lpop":
		v, ok, err := cli.LPop(ctx, ks, name)
		return map[string]any{"value": string(v), "present": ok}, err
	case "rpop":
		v, ok, err := cli.RPop(ctx, ks, name)
		return map[string]any{"value": string(v), "present": ok}, err
	case "llen":
		n, err := cli.LLen(ctx, ks, name)
		return map[string]any{"len": n}, err
	case "lindex":
		v, ok, err := cli.LIndex(ctx, ks, name, start)
		return map[string]any{"value": string(v), "present": ok}, err
	case "lrange":
		ms, err := cli.LRange(ctx, ks, name, start, stop)
		return map[string]any{"items": asStrings(ms)}, err
	case "hset":
		return map[string]any{"acked": true}, cli.HSet(ctx, ks, name, []byte(a.Field), []byte(a.Value))
	case "hget":
		v, ok, err := cli.HGet(ctx, ks, name, []byte(a.Field))
		return map[string]any{"value": string(v), "present": ok}, err
	case "hdel":
		return map[string]any{"acked": true}, cli.HDel(ctx, ks, name, []byte(a.Field))
	case "hexists":
		ok, err := cli.HExists(ctx, ks, name, []byte(a.Field))
		return map[string]any{"present": ok}, err
	case "hlen":
		n, err := cli.HLen(ctx, ks, name)
		return map[string]any{"len": n}, err
	case "hgetall":
		fs, err := cli.HGetAll(ctx, ks, name)
		pairs := make([]map[string]string, 0, len(fs))
		for _, f := range fs {
			pairs = append(pairs, map[string]string{"field": string(f.Field), "value": string(f.Value)})
		}
		return map[string]any{"fields": pairs}, err
	case "incr":
		n, err := cli.Incr(ctx, ks, name, a.Delta)
		return map[string]any{"value": n}, err
	case "cget", "counterget":
		n, ok, err := cli.CounterGet(ctx, ks, name)
		return map[string]any{"value": n, "present": ok}, err
	case "jsonset":
		return map[string]any{"acked": true}, cli.JsonSet(ctx, ks, name, a.Path, []byte(a.Value))
	case "jsonget":
		v, ok, err := cli.JsonGet(ctx, ks, name, a.Path)
		return map[string]any{"value": json.RawMessage(v), "present": ok, "raw": string(v)}, err
	case "jsondel":
		return map[string]any{"acked": true}, cli.JsonDel(ctx, ks, name, a.Path)
	case "bitset":
		return map[string]any{"acked": true}, cli.BitSet(ctx, ks, name, a.Offset, bit)
	case "bitget":
		b, ok, err := cli.BitGet(ctx, ks, name, a.Offset)
		return map[string]any{"bit": b, "present": ok}, err
	case "bitcount":
		n, err := cli.BitCount(ctx, ks, name, start, end)
		return map[string]any{"count": n}, err
	case "bitpos":
		pos, ok, err := cli.BitPos(ctx, ks, name, bit, start, end)
		return map[string]any{"pos": pos, "found": ok}, err
	case "hlladd":
		return map[string]any{"acked": true}, cli.HLLAdd(ctx, ks, name, []byte(a.Item))
	case "hllcount":
		n, ok, err := cli.HLLCount(ctx, ks, name)
		return map[string]any{"count": n, "present": ok}, err
	case "topkadd":
		return map[string]any{"acked": true}, cli.TopKAdd(ctx, ks, name, []byte(a.Item))
	case "topklist":
		es, ok, err := cli.TopKList(ctx, ks, name)
		rows := make([]map[string]any, 0, len(es))
		for _, e := range es {
			rows = append(rows, map[string]any{"item": string(e.Item), "count": e.Count})
		}
		return map[string]any{"entries": rows, "present": ok}, err
	case "cmsincr":
		return map[string]any{"acked": true}, cli.CMSIncr(ctx, ks, name, []byte(a.Item), a.N)
	case "cmsquery":
		n, ok, err := cli.CMSQuery(ctx, ks, name, []byte(a.Item))
		return map[string]any{"count": n, "present": ok}, err
	case "vadd":
		return map[string]any{"acked": true}, cli.VAdd(ctx, ks, name, []byte(a.Member), a.Vec)
	case "vrem":
		return map[string]any{"acked": true}, cli.VRem(ctx, ks, name, []byte(a.Member))
	case "vsim":
		hits, err := cli.VSim(ctx, ks, name, a.Vec, a.K)
		rows := make([]map[string]any, 0, len(hits))
		for _, h := range hits {
			rows = append(rows, map[string]any{"member": string(h.Member), "score": h.Score})
		}
		return map[string]any{"hits": rows}, err
	case "vcard":
		n, ok, err := cli.VCard(ctx, ks, name)
		return map[string]any{"card": n, "present": ok}, err
	case "vdim":
		n, ok, err := cli.VDim(ctx, ks, name)
		return map[string]any{"dim": n, "present": ok}, err
	case "vemb":
		v, ok, err := cli.VEmb(ctx, ks, name, []byte(a.Member))
		return map[string]any{"vec": v, "present": ok}, err
	case "xadd":
		id, err := cli.XAdd(ctx, ks, name, []byte(a.Value))
		return map[string]any{"id": id}, err
	case "xrange":
		rows, err := cli.XRange(ctx, ks, name, asBound(a.Start, "-"), asBound(a.End, "+"), a.Count)
		return streamJSON(rows), err
	case "xrevrange":
		rows, err := cli.XRevRange(ctx, ks, name, asBound(a.Start, "-"), asBound(a.End, "+"), a.Count)
		return streamJSON(rows), err
	case "xlen":
		n, ok, err := cli.XLen(ctx, ks, name)
		return map[string]any{"n": n, "present": ok}, err
	case "xdel":
		return map[string]any{"acked": true}, cli.XDel(ctx, ks, name, a.ID)
	case "xtrim":
		return map[string]any{"acked": true}, cli.XTrim(ctx, ks, name, a.MaxLen)
	default:
		return nil, fmt.Errorf("%w: unknown op %q", engine.ErrInvalidArgument, op)
	}
}

func asInt(raw json.RawMessage, def int) int {
	if len(raw) == 0 {
		return def
	}
	var n int
	if json.Unmarshal(raw, &n) == nil {
		return n
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		if n, err := strconv.Atoi(s); err == nil {
			return n
		}
	}
	return def
}

func asBound(raw json.RawMessage, def string) string {
	if len(raw) == 0 {
		return def
	}
	var s string
	if json.Unmarshal(raw, &s) == nil && s != "" {
		return s
	}
	var n int
	if json.Unmarshal(raw, &n) == nil {
		return strconv.Itoa(n)
	}
	return def
}

func streamJSON(rows []engine.StreamEntry) map[string]any {
	out := make([]map[string]any, 0, len(rows))
	for _, r := range rows {
		out = append(out, map[string]any{"id": r.ID, "payload": string(r.Payload)})
	}
	return map[string]any{"entries": out}
}

func asStrings(in [][]byte) []string {
	out := make([]string, len(in))
	for i, b := range in {
		out[i] = string(b)
	}
	return out
}

func zMembersJSON(ms []client.ZMember) map[string]any {
	out := make([]map[string]any, 0, len(ms))
	for _, m := range ms {
		out = append(out, map[string]any{"member": string(m.Member), "score": m.Score})
	}
	return map[string]any{"members": out}
}
