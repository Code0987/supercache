package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Code0987/supercache/internal/testcluster"
	"github.com/Code0987/supercache/pkg/bloom"
	"github.com/Code0987/supercache/pkg/client"
	"github.com/Code0987/supercache/pkg/engine"
)

type labConfig struct {
	HTTPAddr   string
	SoTLatency time.Duration
	InProcess  bool     // start the 3-node demo mesh
	Addrs      []string // dial these cache gRPC addrs instead
}

type backendNode struct {
	ID        string
	CacheAddr string
	PeerAddr  string
	Client    *client.Client
	Engine    *engine.Engine
}

// Lab is the explorer HTTP server, optionally attached to a mesh.
type Lab struct {
	mu      sync.Mutex
	Cluster *testcluster.Cluster
	owned   bool
	nodes   []backendNode
	SoT     *mockSoT
	sotLat  time.Duration
	HTTP    *http.Server
	Addr    string
	ln      net.Listener
}

func startLab(cfg labConfig) (*Lab, error) {
	if cfg.HTTPAddr == "" {
		cfg.HTTPAddr = "127.0.0.1:19080"
	}
	if cfg.SoTLatency == 0 {
		cfg.SoTLatency = 200 * time.Millisecond
	}
	l := &Lab{SoT: &mockSoT{latency: cfg.SoTLatency}, sotLat: cfg.SoTLatency}
	if cfg.InProcess {
		if err := l.startInProcess(); err != nil {
			return nil, err
		}
	} else if len(cfg.Addrs) > 0 {
		if err := l.dialRemote(cfg.Addrs); err != nil {
			return nil, err
		}
	}
	ln, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		l.Close()
		return nil, err
	}
	l.ln = ln
	l.Addr = ln.Addr().String()
	l.HTTP = &http.Server{Handler: l.handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = l.HTTP.Serve(ln) }()
	return l, nil
}

func (l *Lab) Close() {
	if l == nil {
		return
	}
	if l.HTTP != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = l.HTTP.Shutdown(ctx)
		cancel()
	}
	if l.ln != nil {
		_ = l.ln.Close()
	}
	l.dropBackend()
}

func (l *Lab) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/cluster", l.handleCluster)
	mux.HandleFunc("/v1/connect", l.handleConnect)
	mux.HandleFunc("/v1/disconnect", l.handleDisconnect)
	mux.HandleFunc("/v1/view", l.handleView)
	mux.HandleFunc("/v1/bloom", l.handleBloom)
	mux.HandleFunc("/v1/op", l.handleOp)
	mux.HandleFunc("/v1/scene/", l.handleScene)
	mux.HandleFunc("/v1/reset", l.handleReset)
	mux.Handle("/", uiHandler())
	return mux
}

func uiHandler() http.Handler {
	sub, err := fs.Sub(uiDist, "ui/dist")
	if err != nil {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "ui embed missing — npm --prefix examples/lab/ui run build", http.StatusNotFound)
		})
	}
	return http.FileServer(http.FS(sub))
}

func (l *Lab) handleCluster(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, l.clusterJSON())
}

func (l *Lab) handleConnect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		Addrs     any  `json:"addrs"`
		InProcess bool `json:"in_process"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<16)).Decode(&body); err != nil && err != io.EOF {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	var err error
	if body.InProcess {
		err = l.startInProcess()
	} else {
		err = l.dialRemote(parseAddrs(body.Addrs))
	}
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, l.clusterJSON())
}

func (l *Lab) handleDisconnect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	l.dropBackend()
	writeJSON(w, http.StatusOK, l.clusterJSON())
}

func (l *Lab) handleView(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ks := r.URL.Query().Get("ks")
	key := r.URL.Query().Get("key")
	if ks == "" || key == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "ks and key required"})
		return
	}
	writeJSON(w, http.StatusOK, l.snapshot(ks, key))
}

func (l *Lab) handleBloom(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ks := r.URL.Query().Get("ks")
	name := r.URL.Query().Get("name")
	item := r.URL.Query().Get("item")
	if ks == "" {
		ks = "bloom"
	}
	if name == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "name required"})
		return
	}
	writeJSON(w, http.StatusOK, l.bloomViz(ks, name, item))
}

func (l *Lab) handleOp(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req opReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	resp, code := l.runOp(r.Context(), req)
	writeJSON(w, code, resp)
}

func (l *Lab) handleScene(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/v1/scene/")
	id = strings.Trim(id, "/")
	out, err := l.runScene(r.Context(), id)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (l *Lab) handleReset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	names := append([]string{}, demoNames...)
	var body struct {
		Names []string `json:"names"`
	}
	_ = json.NewDecoder(io.LimitReader(r.Body, 1<<16)).Decode(&body)
	names = append(names, body.Names...)
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	cli, _, err := l.pick("")
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	for _, ks := range modeNames {
		for _, name := range names {
			_ = cli.Delete(ctx, ks, name)
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "names": names})
}

func (l *Lab) pick(via string) (*client.Client, backendNode, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.nodes) == 0 {
		return nil, backendNode{}, errors.New("not connected — set cache gRPC addresses")
	}
	if via == "" {
		return l.nodes[0].Client, l.nodes[0], nil
	}
	for _, n := range l.nodes {
		if n.ID == via || n.CacheAddr == via {
			return n.Client, n, nil
		}
	}
	return nil, backendNode{}, errors.New("unknown via node")
}

func (l *Lab) snapshot(ks, key string) map[string]any {
	l.mu.Lock()
	nodes := append([]backendNode(nil), l.nodes...)
	l.mu.Unlock()
	ownerID := ""
	rfEff := rf
	var ringGen uint64
	if len(nodes) > 0 && nodes[0].Engine != nil {
		if o, ok := nodes[0].Engine.OwnerOf(key); ok {
			ownerID = o.ID
		}
		ringGen = nodes[0].Engine.RingGeneration()
		for _, s := range nodes[0].Engine.KeySpaceSnapshots() {
			if s.Name == ks {
				rfEff = s.ReplicationFactor
				break
			}
		}
	}
	out := make([]map[string]any, 0, len(nodes))
	for _, n := range nodes {
		kind, ver, flags, bytes := "missing", uint64(0), uint64(0), 0
		role := "other"
		if n.Engine != nil {
			v := n.Engine.LocalView(ks, key)
			kind, ver, flags, bytes = v.Kind.String(), v.Version, v.Flags, v.Bytes
			switch {
			case n.ID == ownerID:
				role = "owner"
			case v.Kind == engine.LocalLive:
				role = "replica"
			}
		}
		out = append(out, map[string]any{
			"id":      n.ID,
			"cache":   n.CacheAddr,
			"kind":    kind,
			"version": ver,
			"flags":   flags,
			"bytes":   bytes,
			"role":    role,
		})
	}
	return map[string]any{
		"ks":       ks,
		"key":      key,
		"owner":    ownerID,
		"rf":       rfEff,
		"ring_gen": ringGen,
		"nodes":    out,
	}
}

func (l *Lab) clusterJSON() map[string]any {
	l.mu.Lock()
	defer l.mu.Unlock()
	outNodes := make([]map[string]any, 0, len(l.nodes))
	var ringGen uint64
	var snaps []engine.KeySpaceSnapshot
	addrs := make([]string, 0, len(l.nodes))
	mode := "disconnected"
	if l.owned && l.Cluster != nil {
		mode = "in_process"
	} else if len(l.nodes) > 0 {
		mode = "remote"
	}
	for _, n := range l.nodes {
		node := map[string]any{"id": n.ID, "cache": n.CacheAddr, "peer": n.PeerAddr, "ready": n.Client != nil}
		if n.Engine != nil {
			node["peers"] = n.Engine.Peers()
			node["ready"] = n.Engine.Ready()
			node["ring"] = n.Engine.RingGeneration()
			ringGen = n.Engine.RingGeneration()
			if snaps == nil {
				snaps = n.Engine.KeySpaceSnapshots()
			}
		}
		outNodes = append(outNodes, node)
		addrs = append(addrs, n.CacheAddr)
	}
	if snaps == nil {
		snaps = []engine.KeySpaceSnapshot{}
	}
	lat := l.sotLat.String()
	loads := int64(0)
	if l.SoT != nil {
		loads = l.SoT.Loads()
		lat = l.SoT.latency.String()
	}
	return map[string]any{
		"mode":        mode,
		"connected":   len(l.nodes) > 0,
		"addrs":       addrs,
		"nodes":       outNodes,
		"keyspaces":   snaps,
		"ring_gen":    ringGen,
		"rf":          rf,
		"sot_loads":   loads,
		"sot_latency": lat,
	}
}

func (l *Lab) startInProcess() error {
	sot := &mockSoT{latency: l.sotLat}
	cl, clis, err := startMesh(sot)
	if err != nil {
		return err
	}
	nodes := make([]backendNode, 0, len(clis))
	for i, n := range cl.Nodes() {
		nodes = append(nodes, backendNode{
			ID: n.ID, CacheAddr: n.CacheAddr, PeerAddr: n.PeerAddr,
			Client: clis[i], Engine: n.Engine,
		})
	}
	l.replaceBackend(nodes, cl, sot, true)
	return nil
}

func (l *Lab) dialRemote(addrs []string) error {
	if len(addrs) == 0 {
		return errors.New("addrs required (cache gRPC host:port, comma-separated)")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	nodes := make([]backendNode, 0, len(addrs))
	for i, addr := range addrs {
		cli, err := client.Dial(ctx, addr)
		if err != nil {
			for _, n := range nodes {
				_ = n.Client.Close()
			}
			return fmt.Errorf("dial %s: %w", addr, err)
		}
		nodes = append(nodes, backendNode{
			ID: fmt.Sprintf("n%d", i), CacheAddr: addr, Client: cli,
		})
	}
	l.replaceBackend(nodes, nil, &mockSoT{latency: l.sotLat}, false)
	return nil
}

func (l *Lab) replaceBackend(nodes []backendNode, cl *testcluster.Cluster, sot *mockSoT, owned bool) {
	l.mu.Lock()
	oldNodes, oldCl, oldOwned := l.nodes, l.Cluster, l.owned
	l.nodes = nodes
	l.Cluster = cl
	l.owned = owned
	if sot != nil {
		l.SoT = sot
	}
	l.mu.Unlock()
	closeBackends(oldNodes, oldCl, oldOwned)
}

func (l *Lab) dropBackend() {
	l.mu.Lock()
	oldNodes, oldCl, oldOwned := l.nodes, l.Cluster, l.owned
	l.nodes = nil
	l.Cluster = nil
	l.owned = false
	l.SoT = &mockSoT{latency: l.sotLat}
	l.mu.Unlock()
	closeBackends(oldNodes, oldCl, oldOwned)
}

func closeBackends(nodes []backendNode, cl *testcluster.Cluster, owned bool) {
	for _, n := range nodes {
		if n.Client != nil {
			_ = n.Client.Close()
		}
	}
	if owned && cl != nil {
		cl.Close()
	}
}

func parseAddrs(v any) []string {
	var raw []string
	switch t := v.(type) {
	case string:
		raw = strings.Split(t, ",")
	case []any:
		for _, x := range t {
			if s, ok := x.(string); ok {
				raw = append(raw, s)
			}
		}
	case []string:
		raw = t
	}
	out := make([]string, 0, len(raw))
	for _, s := range raw {
		s = strings.TrimSpace(s)
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}

func (l *Lab) bloomViz(ks, name, item string) map[string]any {
	l.mu.Lock()
	nodes := append([]backendNode(nil), l.nodes...)
	l.mu.Unlock()
	m, k := 64, 4
	var raw []byte
	present := false
	for _, n := range nodes {
		if n.Engine == nil {
			continue
		}
		bits, bm, bk, ok := n.Engine.BloomDump(ks, name)
		if bm > 0 {
			m, k = bm, bk
		}
		if ok {
			raw, present = bits, true
			break
		}
	}
	on := make([]bool, m)
	if present {
		for i := 0; i < m; i++ {
			if i/8 < len(raw) && raw[i/8]&(1<<(i%8)) != 0 {
				on[i] = true
			}
		}
	}
	pos := []int{}
	maybe := false
	if item != "" {
		pos = bloom.Indexes(m, k, []byte(item))
		if present && len(pos) > 0 {
			maybe = true
			for _, p := range pos {
				if p < 0 || p >= len(on) || !on[p] {
					maybe = false
					break
				}
			}
		}
	}
	return map[string]any{
		"m": m, "k": k, "present": present, "bits": on,
		"positions": pos, "maybe": maybe, "item": item, "name": name,
	}
}

func (l *Lab) sotLoads() int64 {
	if l == nil || l.SoT == nil {
		return 0
	}
	return l.SoT.Loads()
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func isInvalidArg(err error) bool {
	if err == nil {
		return false
	}
	if st, ok := status.FromError(err); ok && st.Code() == codes.InvalidArgument {
		return true
	}
	return strings.Contains(strings.ToLower(err.Error()), "invalid argument")
}
