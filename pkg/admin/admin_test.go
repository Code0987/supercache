package admin_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Code0987/supercache/pkg/admin"
	"github.com/Code0987/supercache/pkg/engine"
	"github.com/Code0987/supercache/pkg/keyspace"
)

func TestHealthReadyKeyspaces(t *testing.T) {
	eng := engine.New()
	defer eng.Close()
	eng.SetNodeInfo("n1", "127.0.0.1:8080")
	_ = eng.UpdateKeySpace(keyspace.Config{
		Name:     "demo",
		Mode:     keyspace.ModeCacheOnly,
		MaxBytes: 1024,
		TTL:      time.Minute,
	})

	s := admin.New(eng)
	s.SetReady(true)
	h := s.Handler()

	for _, path := range []string{"/healthz", "/readyz", "/peers", "/keyspaces", "/metrics"} {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, path, nil)
		h.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("%s: status %d body %s", path, rr.Code, rr.Body.String())
		}
	}

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/keyspaces", nil))
	var body map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	ks, ok := body["keyspaces"].([]any)
	if !ok || len(ks) != 1 {
		t.Fatalf("keyspaces: %+v", body)
	}

	// not ready after close
	eng.Close()
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("ready after close: %d", rr.Code)
	}
}

func TestPeersJSON(t *testing.T) {
	eng := engine.New()
	defer eng.Close()
	eng.SetNodeInfo("n1", "addr")
	s := admin.New(eng)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/peers", nil))
	var body struct {
		NodeID string            `json:"node_id"`
		Peers  []engine.PeerInfo `json:"peers"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.NodeID != "n1" || len(body.Peers) != 1 {
		t.Fatalf("%+v", body)
	}
}
