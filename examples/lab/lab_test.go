package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestLabHTTPCluster(t *testing.T) {
	lab := startTestLab(t)
	body := getJSON(t, lab, "/v1/cluster")
	if n := len(asSlice(body["nodes"])); n != 3 {
		t.Fatalf("nodes=%d body=%v", n, body)
	}
	want := []string{
		"cacheonly", "loadthrough", "bloom", "set", "zset", "geo", "list",
		"hash", "counter", "json", "bitmap", "hll", "topk", "cms", "vectorset",
	}
	got := map[string]bool{}
	for _, raw := range asSlice(body["keyspaces"]) {
		ks, _ := raw.(map[string]any)
		name, _ := ks["name"].(string)
		got[name] = true
	}
	for _, name := range want {
		if !got[name] {
			t.Fatalf("missing keyspace %q in %v", name, body["keyspaces"])
		}
	}
}

func TestLabOpPutReplicaFill(t *testing.T) {
	lab := startTestLab(t)
	key := "lab-rf-" + fmt.Sprint(time.Now().UnixNano())
	view := getJSON(t, lab, "/v1/view?ks=cacheonly&key="+key)
	owner, _ := view["owner"].(string)
	if owner == "" {
		t.Fatalf("no owner: %v", view)
	}
	via := otherNode(view, owner)
	resp := postJSON(t, lab, "/v1/op", map[string]any{
		"ks": "cacheonly", "op": "put", "name": key, "via": via,
		"args": map[string]any{"value": "hello"},
	})
	if ok, _ := resp["ok"].(bool); !ok {
		t.Fatalf("put: %v", resp)
	}
	deadline := time.Now().Add(2 * time.Second)
	var liveOwner, liveReplica, liveOther int
	for time.Now().Before(deadline) {
		v := getJSON(t, lab, "/v1/view?ks=cacheonly&key="+key)
		liveOwner, liveReplica, liveOther = classifyLive(v, owner)
		if liveOwner == 1 && liveReplica == 1 && liveOther == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("replica fill: ownerLive=%d replicaLive=%d otherLive=%d owner=%s", liveOwner, liveReplica, liveOther, owner)
}

func TestLabWrongVerb(t *testing.T) {
	lab := startTestLab(t)
	code, body := postJSONStatus(t, lab, "/v1/op", map[string]any{
		"ks": "set", "op": "get", "name": "flags",
		"args": map[string]any{},
	})
	if code != http.StatusBadRequest {
		t.Fatalf("status %d body=%s", code, body)
	}
	var resp map[string]any
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatal(err)
	}
	if inv, _ := resp["invalid_argument"].(bool); !inv {
		t.Fatalf("want invalid_argument: %v", resp)
	}
}

func TestLabLoadThroughSingleflight(t *testing.T) {
	lab := startTestLab(t)
	key := "stampede-" + fmt.Sprint(time.Now().UnixNano())
	view := getJSON(t, lab, "/v1/view?ks=loadthrough&key="+key)
	owner, _ := view["owner"].(string)
	before, _ := getJSON(t, lab, "/v1/cluster")["sot_loads"].(float64)
	const n = 16
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			postJSON(t, lab, "/v1/op", map[string]any{
				"ks": "loadthrough", "op": "get", "name": key, "via": owner,
			})
		}()
	}
	wg.Wait()
	cl := getJSON(t, lab, "/v1/cluster")
	loads, _ := cl["sot_loads"].(float64)
	if int(loads-before) != 1 {
		t.Fatalf("sot_loads delta=%v (before=%v after=%v) want 1", loads-before, before, loads)
	}
}

func TestLabStartsDisconnected(t *testing.T) {
	lab, err := startLab(labConfig{HTTPAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(lab.Close)
	body := getJSON(t, lab, "/v1/cluster")
	if n := len(asSlice(body["nodes"])); n != 0 {
		t.Fatalf("default run should not start a mesh, nodes=%d", n)
	}
	if on, _ := body["connected"].(bool); on {
		t.Fatalf("want disconnected: %v", body)
	}
	code, raw := postJSONStatus(t, lab, "/v1/op", map[string]any{
		"ks": "cacheonly", "op": "get", "name": "k",
	})
	if code != http.StatusBadRequest {
		t.Fatalf("op without backend: %d %s", code, raw)
	}
}

func TestLabConnectRemote(t *testing.T) {
	src := startTestLab(t) // in-process mesh we attach to
	info := getJSON(t, src, "/v1/cluster")
	var addrs []string
	for _, raw := range asSlice(info["nodes"]) {
		n, _ := raw.(map[string]any)
		if a, _ := n["cache"].(string); a != "" {
			addrs = append(addrs, a)
		}
	}
	if len(addrs) != 3 {
		t.Fatalf("src addrs: %v", addrs)
	}

	lab, err := startLab(labConfig{HTTPAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(lab.Close)
	got := postJSON(t, lab, "/v1/connect", map[string]any{"addrs": addrs})
	if n := len(asSlice(got["nodes"])); n != 3 {
		t.Fatalf("connected nodes=%d %v", n, got)
	}
	if mode, _ := got["mode"].(string); mode != "remote" {
		t.Fatalf("mode=%v", got["mode"])
	}
	key := "attach-" + fmt.Sprint(time.Now().UnixNano())
	put := postJSON(t, lab, "/v1/op", map[string]any{
		"ks": "cacheonly", "op": "put", "name": key,
		"args": map[string]any{"value": "via-remote"},
	})
	if ok, _ := put["ok"].(bool); !ok {
		t.Fatalf("remote put: %v", put)
	}
	got = postJSON(t, lab, "/v1/disconnect", map[string]any{})
	if on, _ := got["connected"].(bool); on {
		t.Fatalf("still connected: %v", got)
	}
}

func TestLabBloomGrid(t *testing.T) {
	lab := startTestLab(t)
	put := postJSON(t, lab, "/v1/op", map[string]any{
		"ks": "bloom", "op": "bloomadd", "name": "users",
		"args": map[string]any{"item": "alice"},
	})
	if ok, _ := put["ok"].(bool); !ok {
		t.Fatalf("bloomadd: %v", put)
	}
	viz := getJSON(t, lab, "/v1/bloom?ks=bloom&name=users&item=alice")
	if on, _ := viz["present"].(bool); !on {
		t.Fatalf("present: %v", viz)
	}
	if m, _ := viz["m"].(float64); m != 64 {
		t.Fatalf("m=%v want 64", viz["m"])
	}
	pos := asSlice(viz["positions"])
	if len(pos) != 4 {
		t.Fatalf("positions=%v", pos)
	}
	if maybe, _ := viz["maybe"].(bool); !maybe {
		t.Fatalf("alice should be maybe: %v", viz)
	}
	bits := asSlice(viz["bits"])
	if len(bits) != 64 {
		t.Fatalf("bits len %d", len(bits))
	}
}

func TestLabHoldFalse(t *testing.T) {
	var buf bytes.Buffer
	if err := runWalkthrough(&buf); err != nil {
		t.Fatalf("%v\n%s", err, buf.String())
	}
	if !strings.Contains(buf.String(), "OK: SuperCache Lab walkthrough passed") {
		t.Fatalf("missing OK line:\n%s", buf.String())
	}
}

func startTestLab(t *testing.T) *Lab {
	t.Helper()
	lab, err := startLab(labConfig{HTTPAddr: "127.0.0.1:0", SoTLatency: 30 * time.Millisecond, InProcess: true})
	if err != nil {
		t.Fatalf("startLab: %v", err)
	}
	t.Cleanup(lab.Close)
	return lab
}

func getJSON(t *testing.T, lab *Lab, path string) map[string]any {
	t.Helper()
	resp, err := http.Get("http://" + lab.Addr + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("GET %s → %d %s", path, resp.StatusCode, b)
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("json %s: %v %s", path, err, b)
	}
	return out
}

func postJSON(t *testing.T, lab *Lab, path string, payload map[string]any) map[string]any {
	t.Helper()
	code, body := postJSONStatus(t, lab, path, payload)
	if code != 200 {
		t.Fatalf("POST %s → %d %s", path, code, body)
	}
	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("json: %v %s", err, body)
	}
	return out
}

func postJSONStatus(t *testing.T, lab *Lab, path string, payload map[string]any) (int, []byte) {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post("http://"+lab.Addr+path, "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

func asSlice(v any) []any {
	s, _ := v.([]any)
	return s
}

func otherNode(view map[string]any, owner string) string {
	for _, raw := range asSlice(view["nodes"]) {
		n, _ := raw.(map[string]any)
		id, _ := n["id"].(string)
		if id != "" && id != owner {
			return id
		}
	}
	return ""
}

func classifyLive(view map[string]any, owner string) (ownerLive, replicaLive, otherLive int) {
	for _, raw := range asSlice(view["nodes"]) {
		n, _ := raw.(map[string]any)
		id, _ := n["id"].(string)
		kind, _ := n["kind"].(string)
		if kind != "live" {
			continue
		}
		if id == owner {
			ownerLive++
			continue
		}
		replicaLive++
	}
	return
}
