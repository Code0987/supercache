package admin

import (
	"encoding/json"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/telemetry"
)

// StatusProvider supplies readiness and diagnostic data.
type StatusProvider interface {
	Ready() bool
	NodeID() string
	Peers() []engine.PeerInfo
	RingGeneration() uint64
	KeySpaceSnapshots() []engine.KeySpaceSnapshot
	Metrics() telemetry.Snapshot
}

// Server is the admin HTTP surface.
type Server struct {
	provider StatusProvider
	started  time.Time
	ready    atomic.Bool
}

// New creates an admin Server. Call SetReady(true) when the node can serve traffic.
func New(p StatusProvider) *Server {
	s := &Server{provider: p, started: time.Now().UTC()}
	// Default ready from provider on first probe; also allow explicit SetReady.
	s.ready.Store(p != nil && p.Ready())
	return s
}

// SetReady overrides readiness (e.g. after listeners bind).
func (s *Server) SetReady(v bool) { s.ready.Store(v) }

// Handler returns the HTTP mux.
//
// Routes include diagnostics (/healthz, /readyz, /peers, /keyspaces, /metrics)
// and hosted OpenAPI docs (/docs, /openapi.yaml).
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/readyz", s.handleReadyz)
	mux.HandleFunc("/peers", s.handlePeers)
	mux.HandleFunc("/keyspaces", s.handleKeyspaces)
	mux.HandleFunc("/metrics", s.handleMetrics)
	s.mountDocs(mux)
	return mux
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":  "ok",
		"uptime":  time.Since(s.started).String(),
		"node_id": safeNodeID(s.provider),
	})
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ready := s.ready.Load() && s.provider != nil && s.provider.Ready()
	if !ready {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"status": "not ready",
			"ready":  false,
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status": "ready",
		"ready":  true,
	})
}

func (s *Server) handlePeers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	peers := []engine.PeerInfo{}
	var gen uint64
	if s.provider != nil {
		peers = s.provider.Peers()
		gen = s.provider.RingGeneration()
	}
	if peers == nil {
		peers = []engine.PeerInfo{}
	}
	hashes := map[string]string{}
	if s.provider != nil {
		for _, ks := range s.provider.KeySpaceSnapshots() {
			hashes[ks.Name] = ks.ConfigHash
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"node_id":          safeNodeID(s.provider),
		"ring_generation":  gen,
		"peers":            peers,
		"keyspace_hashes":  hashes,
		"config_note":      "UpdateKeySpace/DeleteKeySpace are local; re-issue on every node. Compare keyspace_hashes across nodes for drift.",
	})
}

func (s *Server) handleKeyspaces(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var snaps []engine.KeySpaceSnapshot
	if s.provider != nil {
		snaps = s.provider.KeySpaceSnapshots()
	}
	if snaps == nil {
		snaps = []engine.KeySpaceSnapshot{}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"keyspaces": snaps,
	})
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var snap telemetry.Snapshot
	if s.provider != nil {
		snap = s.provider.Metrics()
	}
	writeJSON(w, http.StatusOK, snap)
}

func safeNodeID(p StatusProvider) string {
	if p == nil {
		return ""
	}
	return p.NodeID()
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
