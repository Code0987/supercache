package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Code0987/supercache/pkg/client"
)

// appServer is the billboard application HTTP front-end (uses SuperCache clients).
type appServer struct {
	log     *log.Logger
	clients []*client.Client
	nodes   []nodeSpec
	src     *ChartSource
	rr      atomic.Uint64
}

func newAppServer(logger *log.Logger, nodes []nodeSpec, src *ChartSource) (*appServer, error) {
	a := &appServer{log: logger, nodes: nodes, src: src}
	ctx := context.Background()
	for _, n := range nodes {
		c, err := client.Dial(ctx, n.CacheAddr)
		if err != nil {
			return nil, fmt.Errorf("dial cache %s: %w", n.CacheAddr, err)
		}
		a.clients = append(a.clients, c)
		logger.Printf("[app] dialed SuperCache node %s at %s", n.ID, n.CacheAddr)
	}
	return a, nil
}

func (a *appServer) Close() {
	for _, c := range a.clients {
		_ = c.Close()
	}
}

func (a *appServer) pick() (*client.Client, nodeSpec) {
	i := int(a.rr.Add(1)-1) % len(a.clients)
	return a.clients[i], a.nodes[i]
}

func (a *appServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", a.handleHome)
	mux.HandleFunc("/v1/charts/", a.handleChart)
	mux.HandleFunc("/v1/tracks/", a.handleTrack)
	mux.HandleFunc("/v1/admin/invalidate/", a.handleInvalidate)
	mux.HandleFunc("/v1/admin/pin/", a.handlePin)
	mux.HandleFunc("/v1/demo/stampede", a.handleStampede)
	mux.HandleFunc("/v1/demo/load", a.handleLoad)
	mux.HandleFunc("/v1/status", a.handleStatus)
	return loggingMiddleware(a.log, mux)
}

func loggingMiddleware(logger *log.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ww := &statusWriter{ResponseWriter: w, code: 200}
		next.ServeHTTP(ww, r)
		logger.Printf("[http] %s %s → %d %s", r.Method, r.URL.RequestURI(), ww.code, time.Since(start))
	})
}

type statusWriter struct {
	http.ResponseWriter
	code int
}

func (w *statusWriter) WriteHeader(code int) {
	w.code = code
	w.ResponseWriter.WriteHeader(code)
}

func (a *appServer) handleHome(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = io.WriteString(w, homeHTML)
}

func chartKey(board string) string {
	board = strings.TrimSpace(strings.ToLower(board))
	if board == "" || board == "global" {
		return "chart:global"
	}
	return "chart:genre:" + board
}

func (a *appServer) handleChart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	board := strings.TrimPrefix(r.URL.Path, "/v1/charts/")
	key := chartKey(board)
	cli, node := a.pick()
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	a.log.Printf("[app] GET chart board=%q key=%q via node=%s (%s)", board, key, node.ID, node.CacheAddr)
	start := time.Now()
	raw, err := cli.Get(ctx, "charts", key)
	elapsed := time.Since(start)
	if err != nil {
		a.log.Printf("[app] chart MISS/ERR key=%q via=%s elapsed=%s err=%v", key, node.ID, elapsed, err)
		writeJSON(w, http.StatusNotFound, map[string]any{"error": err.Error(), "key": key, "node": node.ID, "elapsed_ms": elapsed.Milliseconds()})
		return
	}
	a.log.Printf("[app] chart OK key=%q via=%s bytes=%d elapsed=%s", key, node.ID, len(raw), elapsed)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-SuperCache-Node", node.ID)
	w.Header().Set("X-SuperCache-Elapsed-Ms", fmt.Sprintf("%d", elapsed.Milliseconds()))
	_, _ = w.Write(raw)
}

func (a *appServer) handleTrack(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/v1/tracks/")
	key := "track:" + id
	cli, node := a.pick()
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	a.log.Printf("[app] GET track id=%q via=%s", id, node.ID)
	start := time.Now()
	raw, err := cli.Get(ctx, "charts", key)
	elapsed := time.Since(start)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": err.Error(), "node": node.ID})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-SuperCache-Node", node.ID)
	w.Header().Set("X-SuperCache-Elapsed-Ms", fmt.Sprintf("%d", elapsed.Milliseconds()))
	_, _ = w.Write(raw)
}

func (a *appServer) handleInvalidate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	board := strings.TrimPrefix(r.URL.Path, "/v1/admin/invalidate/")
	key := chartKey(board)
	cli, node := a.pick()
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	a.src.BumpGeneration("invalidate " + key)
	a.log.Printf("[app] DELETE chart key=%q via=%s (cluster invalidate)", key, node.ID)
	start := time.Now()
	err := cli.Delete(ctx, "charts", key)
	elapsed := time.Since(start)
	if err != nil {
		a.log.Printf("[app] DELETE note key=%q elapsed=%s err=%v (partial peer failures ok)", key, elapsed, err)
		writeJSON(w, http.StatusOK, map[string]any{
			"ok": true, "key": key, "node": node.ID, "elapsed_ms": elapsed.Milliseconds(),
			"peer_note": err.Error(),
		})
		return
	}
	a.log.Printf("[app] DELETE ok key=%q elapsed=%s", key, elapsed)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "key": key, "node": node.ID, "elapsed_ms": elapsed.Milliseconds()})
}

func (a *appServer) handlePin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	board := strings.TrimPrefix(r.URL.Path, "/v1/admin/pin/")
	key := "pin:" + chartKey(board)
	cli, node := a.pick()
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	msg := map[string]any{
		"board":   board,
		"pinned":  true,
		"note":    "editorial pin stored in CacheOnly keyspace meta",
		"at":      time.Now().UTC().Format(time.RFC3339),
		"via":     node.ID,
	}
	raw, _ := json.Marshal(msg)
	a.log.Printf("[app] PUT meta pin key=%q via=%s", key, node.ID)
	if err := cli.Put(ctx, "meta", key, raw, client.WithTTL(10*time.Minute)); err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
		return
	}
	// Read back from another node after short wait to show fan-out.
	time.Sleep(80 * time.Millisecond)
	other := a.clients[(int(a.rr.Load()))%len(a.clients)]
	v, err := other.Get(ctx, "meta", key)
	writeJSON(w, http.StatusOK, map[string]any{
		"put_via": node.ID,
		"read_back": string(v),
		"read_err":  fmt.Sprintf("%v", err),
		"payload":   msg,
	})
}

func (a *appServer) handleStampede(w http.ResponseWriter, r *http.Request) {
	n := 32
	// Prefer a board not in WarmKeys so refresh-ahead does not refill during the test.
	board := r.URL.Query().Get("board")
	if board == "" {
		board = "rock"
	}
	key := chartKey(board)
	_, node := a.pick()
	ctx := r.Context()
	a.log.Printf("[app] STAMPEDE prepare invalidate key=%q hint_node=%s concurrency=%d", key, node.ID, n)
	for i, c := range a.clients {
		if err := c.Delete(ctx, "charts", key); err != nil {
			a.log.Printf("[app] STAMPEDE pre-delete via clients[%d]: %v", i, err)
		}
	}
	time.Sleep(80 * time.Millisecond)

	beforeLoads, _, _ := a.src.Stats()
	start := time.Now()
	var wg sync.WaitGroup
	var hits, errs atomic.Int64
	var minMs, maxMs atomic.Int64
	minMs.Store(1 << 60)

	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			c, nd := a.pick()
			t0 := time.Now()
			_, err := c.Get(ctx, "charts", key)
			ms := time.Since(t0).Milliseconds()
			for {
				old := minMs.Load()
				if ms >= old || minMs.CompareAndSwap(old, ms) {
					break
				}
			}
			for {
				old := maxMs.Load()
				if ms <= old || maxMs.CompareAndSwap(old, ms) {
					break
				}
			}
			if err != nil {
				errs.Add(1)
				a.log.Printf("[app] stampede worker err via=%s: %v", nd.ID, err)
				return
			}
			hits.Add(1)
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	afterLoads, _, _ := a.src.Stats()
	sotLoads := afterLoads - beforeLoads
	a.log.Printf("[app] STAMPEDE done key=%q concurrent=%d hits=%d errs=%d SoT_loads=%d wall=%s min_ms=%d max_ms=%d",
		key, n, hits.Load(), errs.Load(), sotLoads, elapsed, minMs.Load(), maxMs.Load())
	writeJSON(w, http.StatusOK, map[string]any{
		"key":         key,
		"concurrency": n,
		"hits":        hits.Load(),
		"errors":      errs.Load(),
		"sot_loads":   sotLoads,
		"wall_ms":     elapsed.Milliseconds(),
		"min_rpc_ms":  minMs.Load(),
		"max_rpc_ms":  maxMs.Load(),
		"feature":     "singleflight + owner GetOrLoad coalescing — expect sot_loads ≈ 1 (not 32)",
	})
}

func (a *appServer) handleLoad(w http.ResponseWriter, r *http.Request) {
	n := 40
	boards := []string{"global", "pop", "hiphop", "electronic", "rock", "rnb"}
	start := time.Now()
	var wg sync.WaitGroup
	var okN atomic.Int64
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			b := boards[i%len(boards)]
			c, _ := a.pick()
			if _, err := c.Get(r.Context(), "charts", chartKey(b)); err == nil {
				okN.Add(1)
			}
		}(i)
	}
	wg.Wait()
	writeJSON(w, http.StatusOK, map[string]any{
		"requests": n,
		"ok":       okN.Load(),
		"wall_ms":  time.Since(start).Milliseconds(),
		"hint":     "check admin /keyspaces hot_keys and /metrics",
	})
}

func (a *appServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	loads, fails, gen := a.src.Stats()
	admins := make([]string, 0, len(a.nodes))
	for _, n := range a.nodes {
		admins = append(admins, "http://"+n.AdminAddr)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"nodes":        a.nodes,
		"admin_urls":   admins,
		"sot_loads":    loads,
		"sot_fails":    fails,
		"sot_generation": gen,
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

const homeHTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<title>SuperCache · Music Trending Billboard</title>
<style>
  :root { font-family: ui-sans-serif, system-ui, sans-serif; background:#0b0f14; color:#e8eef7; }
  body { max-width: 920px; margin: 2rem auto; padding: 0 1rem; }
  h1 { font-weight: 700; letter-spacing: -0.02em; }
  .muted { color: #8b9bb4; }
  a { color: #7dd3fc; }
  .card { background:#121821; border:1px solid #1e293b; border-radius:12px; padding:1rem 1.25rem; margin:1rem 0; }
  code { background:#1e293b; padding:0.15rem 0.4rem; border-radius:4px; }
  button { background:#38bdf8; color:#0b0f14; border:0; padding:0.5rem 0.9rem; border-radius:8px; font-weight:600; cursor:pointer; margin-right:0.5rem; margin-top:0.5rem; }
  pre { background:#0f172a; padding:1rem; border-radius:8px; overflow:auto; font-size:12px; }
  table { width:100%; border-collapse: collapse; }
  td, th { text-align:left; padding:0.4rem 0.5rem; border-bottom:1px solid #1e293b; }
</style>
</head>
<body>
  <h1>🎵 Trending Billboard</h1>
  <p class="muted">Demo app on a <strong>3-node SuperCache</strong> cluster — LoadThrough charts, fan-out, invalidate, stampede coalescing.</p>
  <div class="card">
    <button onclick="loadChart('global')">Global Top</button>
    <button onclick="loadChart('pop')">Pop</button>
    <button onclick="loadChart('hiphop')">Hip-Hop</button>
    <button onclick="loadChart('electronic')">Electronic</button>
    <button onclick="stampede()">Run stampede (32)</button>
    <button onclick="invalidate('global')">Invalidate global</button>
  </div>
  <div class="card">
    <div id="meta" class="muted">Pick a board…</div>
    <div id="out"></div>
  </div>
  <p class="muted">Admin: <a href="http://127.0.0.1:8081/peers">n1 /peers</a> ·
    <a href="http://127.0.0.1:8081/keyspaces">/keyspaces</a> ·
    <a href="http://127.0.0.1:8081/metrics">/metrics</a></p>
<script>
async function loadChart(board) {
  const t0 = performance.now();
  const r = await fetch('/v1/charts/' + board);
  const ms = (performance.now()-t0).toFixed(1);
  const node = r.headers.get('X-SuperCache-Node') || '?';
  const body = await r.json();
  document.getElementById('meta').textContent = board + ' via ' + node + ' in ' + ms + 'ms (client) · http ' + r.status;
  if (!r.ok) { document.getElementById('out').innerHTML = '<pre>'+JSON.stringify(body,null,2)+'</pre>'; return; }
  let html = '<h2>' + (body.title||board) + '</h2><p class="muted">' + (body.updated_at||'') +
    (body.load_ms!=null ? ' · SoT load_ms='+body.load_ms : '') + '</p><table><tr><th>#</th><th>Track</th><th>Artist</th><th>Score</th><th>Δ</th></tr>';
  for (const e of (body.entries||[])) {
    html += '<tr><td>'+e.rank+'</td><td>'+e.track.title+'</td><td>'+e.track.artist+
      '</td><td>'+e.score+'</td><td>'+e.delta+' '+ (e.spark||'') +'</td></tr>';
  }
  html += '</table>';
  document.getElementById('out').innerHTML = html;
}
async function stampede() {
  document.getElementById('meta').textContent = 'stampede running…';
  const r = await fetch('/v1/demo/stampede?board=global');
  const body = await r.json();
  document.getElementById('meta').textContent = 'stampede done';
  document.getElementById('out').innerHTML = '<pre>'+JSON.stringify(body,null,2)+'</pre>';
}
async function invalidate(board) {
  const r = await fetch('/v1/admin/invalidate/'+board, {method:'POST'});
  const body = await r.json();
  document.getElementById('meta').textContent = 'invalidated '+board;
  document.getElementById('out').innerHTML = '<pre>'+JSON.stringify(body,null,2)+'</pre>';
}
</script>
</body>
</html>
`
