package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

func runWalkthrough(out io.Writer) error {
	lab, err := startLab(labConfig{HTTPAddr: "127.0.0.1:0", SoTLatency: 20 * time.Millisecond, InProcess: true})
	if err != nil {
		return err
	}
	defer lab.Close()
	base := "http://" + lab.Addr
	p := func(format string, args ...any) { fmt.Fprintf(out, format+"\n", args...) }
	p("SuperCache Lab walkthrough  cluster=%s", base)

	code, body, err := httpGet(base + "/v1/cluster")
	if err != nil {
		return err
	}
	if code != 200 {
		return fmt.Errorf("cluster HTTP %d %s", code, body)
	}
	var cl map[string]any
	if err := json.Unmarshal(body, &cl); err != nil {
		return err
	}
	nodes, _ := cl["nodes"].([]any)
	if len(nodes) != 3 {
		return fmt.Errorf("want 3 nodes, got %d", len(nodes))
	}
	p("    /v1/cluster nodes=%d sot_loads=%v", len(nodes), cl["sot_loads"])

	code, body, err = httpPost(base+"/v1/op", map[string]any{
		"ks": "set", "op": "get", "name": "flags",
	})
	if err != nil {
		return err
	}
	if code != http.StatusBadRequest {
		return fmt.Errorf("wrong verb want 400, got %d %s", code, body)
	}
	p("    Get on ModeSet → HTTP %d (invalid argument)", code)

	code, body, err = httpPost(base+"/v1/scene/kv-write", map[string]any{})
	if err != nil {
		return err
	}
	if code != 200 {
		return fmt.Errorf("scene kv-write HTTP %d %s", code, body)
	}
	p("    scene kv-write ok")

	p("OK: SuperCache Lab walkthrough passed")
	return nil
}

func httpGet(url string) (int, []byte, error) {
	resp, err := http.Get(url)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	return resp.StatusCode, b, err
}

func httpPost(url string, payload map[string]any) (int, []byte, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return 0, nil, err
	}
	resp, err := http.Post(url, "application/json", bytes.NewReader(raw))
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	return resp.StatusCode, b, err
}
