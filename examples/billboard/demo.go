package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

// runDemo walks through SuperCache features against the live cluster + app HTTP.
func runDemo(baseURL string, logger *log.Logger, src *ChartSource, app *appServer) error {
	logger.Println()
	logger.Println("╔══════════════════════════════════════════════════════════════════╗")
	logger.Println("║         BILLBOARD DEMO — SuperCache feature walkthrough          ║")
	logger.Println("╚══════════════════════════════════════════════════════════════════╝")

	client := &http.Client{Timeout: 30 * time.Second}
	get := func(path string) (int, []byte, http.Header, error) {
		resp, err := client.Get(baseURL + path)
		if err != nil {
			return 0, nil, nil, err
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, b, resp.Header, nil
	}
	post := func(path string) (int, []byte, error) {
		resp, err := client.Post(baseURL+path, "application/json", nil)
		if err != nil {
			return 0, nil, err
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, b, nil
	}
	del := func(path string) (int, []byte, error) {
		req, err := http.NewRequest(http.MethodDelete, baseURL+path, nil)
		if err != nil {
			return 0, nil, err
		}
		resp, err := client.Do(req)
		if err != nil {
			return 0, nil, err
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, b, nil
	}

	step := 0
	banner := func(title string) {
		step++
		logger.Println()
		logger.Printf("── Step %d · %s %s", step, title, strings.Repeat("─", max(0, 48-len(title))))
	}

	// ── 1 first read (may already be warm from topology prefetch — we force a miss)
	banner("Forced miss then load (LoadThrough → SoT)")
	logger.Printf("    WarmKeys prefetch may have filled charts already; invalidate then load")
	_, _, err := post("/v1/admin/invalidate/global")
	if err != nil {
		return err
	}
	time.Sleep(100 * time.Millisecond)
	loads0, _, _ := src.Stats()
	t0 := time.Now()
	code, body, hdr, err := get("/v1/charts/global")
	if err != nil {
		return err
	}
	logger.Printf("    HTTP %d in %s via node=%s", code, time.Since(t0), hdr.Get("X-SuperCache-Node"))
	logger.Printf("    SuperCache elapsed header: %sms", hdr.Get("X-SuperCache-Elapsed-Ms"))
	loads1, _, _ := src.Stats()
	logger.Printf("    SoT loads delta: %d → %d (+%d)  [expect ≥1 after invalidate]", loads0, loads1, loads1-loads0)
	var ch Chart
	if json.Unmarshal(body, &ch) == nil && len(ch.Entries) > 0 {
		logger.Printf("    Top #1: %s — %s (score %.1f)", ch.Entries[0].Track.Title, ch.Entries[0].Track.Artist, ch.Entries[0].Score)
		logger.Printf("    Chart SoT load_ms field: %d", ch.LoadMs)
	}

	// ── 2 warm hit
	banner("Warm hit (local cache — no SoT)")
	loads0, _, _ = src.Stats()
	t0 = time.Now()
	code, _, hdr, err = get("/v1/charts/global")
	if err != nil {
		return err
	}
	logger.Printf("    HTTP %d in %s via node=%s elapsed_hdr=%sms", code, time.Since(t0), hdr.Get("X-SuperCache-Node"), hdr.Get("X-SuperCache-Elapsed-Ms"))
	loads1, _, _ = src.Stats()
	logger.Printf("    SoT loads delta: +%d  [expect +0 on hit]", loads1-loads0)

	// ── 3 genre boards
	banner("Genre boards (key isolation under charts keyspace)")
	for _, g := range []string{"pop", "hiphop", "electronic"} {
		t0 = time.Now()
		code, body, hdr, err = get("/v1/charts/" + g)
		if err != nil {
			return err
		}
		_ = json.Unmarshal(body, &ch)
		top := "?"
		if len(ch.Entries) > 0 {
			top = ch.Entries[0].Track.Title
		}
		logger.Printf("    %-12s HTTP %d via=%s %s  top=%q", g, code, hdr.Get("X-SuperCache-Node"), time.Since(t0), top)
	}

	// ── 4 track card
	banner("Track card LoadThrough")
	code, body, hdr, err = get("/v1/tracks/t01")
	if err != nil {
		return err
	}
	logger.Printf("    HTTP %d via=%s body=%s", code, hdr.Get("X-SuperCache-Node"), truncate(string(body), 120))

	// ── 5 stampede
	banner("Stampede (32 concurrent gets after invalidate)")
	logger.Printf("    Feature: singleflight + owner GetOrLoad → SoT should load ~once (not 32)")
	logger.Printf("    Using board=rock (not in WarmKeys) so refresh-ahead does not mask the miss")
	t0 = time.Now()
	code, body, _, err = get("/v1/demo/stampede?board=rock")
	if err != nil {
		return err
	}
	logger.Printf("    HTTP %d wall=%s", code, time.Since(t0))
	var stamp map[string]any
	_ = json.Unmarshal(body, &stamp)
	logger.Printf("    result: concurrency=%v hits=%v sot_loads=%v wall_ms=%v min_rpc_ms=%v max_rpc_ms=%v",
		stamp["concurrency"], stamp["hits"], stamp["sot_loads"], stamp["wall_ms"], stamp["min_rpc_ms"], stamp["max_rpc_ms"])
	logger.Printf("    note: %v", stamp["feature"])

	// ── 6 invalidate + reload
	banner("Cluster Delete invalidate + reload")
	code, body, err = post("/v1/admin/invalidate/global")
	if err != nil {
		return err
	}
	logger.Printf("    invalidate HTTP %d %s", code, truncate(string(body), 200))
	time.Sleep(100 * time.Millisecond)
	loads0, _, gen0 := src.Stats()
	t0 = time.Now()
	code, _, hdr, err = get("/v1/charts/global")
	if err != nil {
		return err
	}
	loads1, _, gen1 := src.Stats()
	logger.Printf("    reload HTTP %d via=%s in %s", code, hdr.Get("X-SuperCache-Node"), time.Since(t0))
	logger.Printf("    SoT loads +%d  generation %d→%d", loads1-loads0, gen0, gen1)

	// ── 7 editorial pin CacheOnly + fan-out
	banner("Editorial pin (CacheOnly meta + async fan-out)")
	code, body, err = post("/v1/admin/pin/global")
	if err != nil {
		return err
	}
	logger.Printf("    pin HTTP %d %s", code, truncate(string(body), 280))

	// ── 8 mixed load → hot keys
	banner("Mixed load (hot-key tracker food)")
	code, body, _, err = get("/v1/demo/load")
	if err != nil {
		return err
	}
	logger.Printf("    load HTTP %d %s", code, truncate(string(body), 200))
	logger.Printf("    inspect hot keys: curl -s http://127.0.0.1:8081/keyspaces | head")

	// ── 9 admin surfaces
	banner("Admin surfaces (membership / metrics)")
	for _, u := range []string{
		"http://127.0.0.1:8081/peers",
		"http://127.0.0.1:8082/peers",
		"http://127.0.0.1:8083/healthz",
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		resp, err := client.Do(req)
		cancel()
		if err != nil {
			logger.Printf("    %s ERROR %v", u, err)
			continue
		}
		b, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		logger.Printf("    %s → %d %s", u, resp.StatusCode, truncate(string(b), 160))
	}

	// ── ModeSet tags (exact membership, fan-out)
	banner("ModeSet tags (SetAdd + SetContains fan-out)")
	for _, tag := range []string{"editorial", "homepage", "nsfw_safe"} {
		code, body, err = post("/v1/tags/billboard?tag=" + tag)
		if err != nil {
			return err
		}
		logger.Printf("    SetAdd %q → HTTP %d %s", tag, code, truncate(string(body), 100))
	}
	code, body, hdr, err = get("/v1/tags/billboard")
	if err != nil {
		return err
	}
	logger.Printf("    SetMembers HTTP %d via=%s %s", code, hdr.Get("X-SuperCache-Node"), truncate(string(body), 160))

	// ── ModeTopK live plays (observations, no score)
	banner("ModeTopK live plays (TopKAdd + TopKList)")
	logger.Printf("    ZSet of 500 distinct play ids is one LRU blob (~6.5 KiB and growing); a station-day (~100k) overflows DefaultMaxValueSize.")
	logger.Printf("    ModeTopK K=%d keeps ≤%d slots (~600 B typical). Writes are plays, not scores. ZSet stays the type for editorial ranks.", playK, playK)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	if err := app.ingestHonestPlays(ctx, playsName); err != nil {
		cancel()
		return err
	}
	cancel()
	logger.Printf("    ingested %d plays across %d ids on name=%s", len(honestPlayStream()), playIDs, playsName)

	var chart struct {
		Present bool `json:"present"`
		Entries []struct {
			Item  string `json:"item"`
			Count uint64 `json:"count"`
		} `json:"entries"`
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		code, body, hdr, err = get("/v1/plays/" + playsName)
		if err != nil {
			return err
		}
		_ = json.Unmarshal(body, &chart)
		if code == 200 && chart.Present && len(chart.Entries) == playK {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("plays chart not ready: http %d present=%v n=%d", code, chart.Present, len(chart.Entries))
		}
		time.Sleep(20 * time.Millisecond)
	}
	logger.Printf("    TopKList HTTP %d via=%s len=%d", code, hdr.Get("X-SuperCache-Node"), len(chart.Entries))
	for i, w := range lockedHonestChart {
		if i >= len(chart.Entries) || chart.Entries[i].Item != w.id || chart.Entries[i].Count != w.count {
			return fmt.Errorf("honest chart rank %d: got %v want %s=%d", i+1, chart.Entries, w.id, w.count)
		}
		logger.Printf("    #%d %s %d", i+1, chart.Entries[i].Item, chart.Entries[i].Count)
	}
	seen := map[string]bool{}
	for _, e := range chart.Entries {
		seen[e.Item] = true
	}
	for _, evicted := range []string{"t003", "t004", "t005"} {
		if seen[evicted] {
			return fmt.Errorf("%s still on the chart", evicted)
		}
	}
	logger.Printf("    evicted t003–t005 (Space-Saving replace-min, not the tail)")

	banner("ModeCMS point frequency (CMSIncr + CMSQuery)")
	logger.Printf("    TopK cannot answer evicted t003 (not in table ≠ 0). CMS still can (64 KiB, hashed, overestimate only).")
	var cmsBody struct {
		Present bool   `json:"present"`
		Count   uint64 `json:"count"`
	}
	code, body, _, err = get("/v1/counts/" + playsName + "?track=t003")
	if err != nil {
		return err
	}
	_ = json.Unmarshal(body, &cmsBody)
	if code != 200 || !cmsBody.Present || cmsBody.Count < 100 || cmsBody.Count > 116 {
		return fmt.Errorf("cms t003: http %d present=%v count=%d body=%s", code, cmsBody.Present, cmsBody.Count, body)
	}
	logger.Printf("    cms t003 %d", cmsBody.Count)

	code, body, _, err = get("/v1/counts/" + playsName + "?track=t001")
	if err != nil {
		return err
	}
	_ = json.Unmarshal(body, &cmsBody)
	if code != 200 || !cmsBody.Present || cmsBody.Count < 200 || cmsBody.Count > 216 {
		return fmt.Errorf("cms t001: http %d present=%v count=%d body=%s", code, cmsBody.Present, cmsBody.Count, body)
	}
	logger.Printf("    cms t001 %d", cmsBody.Count)

	code, body, _, err = get("/v1/counts/missing?track=t001")
	if err != nil {
		return err
	}
	_ = json.Unmarshal(body, &cmsBody)
	if code != 404 || cmsBody.Present {
		return fmt.Errorf("cms missing: http %d present=%v", code, cmsBody.Present)
	}

	code, body, err = post("/v1/plays/" + playsName + "?track=t003&n=100")
	if err != nil {
		return err
	}
	if code != 200 {
		return fmt.Errorf("cmsincr post: http %d %s", code, body)
	}
	code, body, _, err = get("/v1/counts/" + playsName + "?track=t003")
	if err != nil {
		return err
	}
	_ = json.Unmarshal(body, &cmsBody)
	if code != 200 || !cmsBody.Present || cmsBody.Count < 200 || cmsBody.Count > 216 {
		return fmt.Errorf("cmsincr t003: http %d present=%v count=%d", code, cmsBody.Present, cmsBody.Count)
	}
	logger.Printf("    cmsincr t003 %d", cmsBody.Count)

	code, body, err = del("/v1/plays/" + playsName)
	if err != nil {
		return err
	}
	logger.Printf("    Delete HTTP %d %s", code, truncate(string(body), 120))
	code, body, _, err = get("/v1/plays/" + playsName)
	if err != nil {
		return err
	}
	if code != 404 {
		return fmt.Errorf("after delete want 404 got %d %s", code, body)
	}
	code, body, _, err = get("/v1/counts/" + playsName + "?track=t003")
	if err != nil {
		return err
	}
	_ = json.Unmarshal(body, &cmsBody)
	if code != 404 || cmsBody.Present {
		return fmt.Errorf("after delete counts want 404 got %d present=%v", code, cmsBody.Present)
	}
	logger.Printf("    GET after Delete → HTTP 404 (plays + counts miss)")

	// ── summary
	loads, fails, gen := src.Stats()
	logger.Println()
	logger.Println("═══ Features exercised ═══")
	logger.Println("  ✓ 3-node cluster (gossip membership + peer mesh + cache gRPC)")
	logger.Println("  ✓ LoadThrough keyspace (charts) + DataSource SoT")
	logger.Println("  ✓ CacheOnly keyspace (meta editorial pins)")
	logger.Println("  ✓ ModeSet keyspace (tags exact membership)")
	logger.Println("  ✓ ModeTopK keyspace (plays live heavy-hitters)")
	logger.Println("  ✓ ModeCMS keyspace (plays-count point frequency)")
	logger.Println("  ✓ TTL / NegativeTTL configured on charts")
	logger.Println("  ✓ singleflight stampede coalescing")
	logger.Println("  ✓ protect: rate limit + circuit breaker wired")
	logger.Println("  ✓ Delete cluster invalidate + SoT reload")
	logger.Println("  ✓ Put + async fan-out (pin read-back)")
	logger.Println("  ✓ SetAdd + SetContains fan-out (tags)")
	logger.Println("  ✓ TopKAdd + TopKList fan-out (plays)")
	logger.Println("  ✓ CMSIncr + CMSQuery fan-out (plays-count)")
	logger.Println("  ✓ WarmKeys / topology prefetch / refresh-ahead")
	logger.Println("  ✓ Admin /healthz /peers /keyspaces /metrics")
	logger.Println("  ✓ pkg/client round-robin across cache ports")
	logger.Printf("  · SoT totals: loads=%d fails=%d generation=%d", loads, fails, gen)
	logger.Println()
	logger.Println("OK: ModeTopK")
	logger.Println("OK: ModeCMS")
	logger.Printf("UI:        %s/", baseURL)
	logger.Printf("API chart: %s/v1/charts/global", baseURL)
	logger.Printf("Live plays:%s/v1/plays/hot", baseURL)
	logger.Printf("Counts:    %s/v1/counts/hot?track=t003", baseURL)
	logger.Printf("Stampede:  %s/v1/demo/stampede", baseURL)
	logger.Println("Admin n1:  http://127.0.0.1:8081/peers")
	logger.Println()
	return nil
}

func truncate(s string, n int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
