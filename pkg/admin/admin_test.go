package admin_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
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

func TestDocsAndOpenAPI(t *testing.T) {
	s := admin.New(nil)
	h := s.Handler()

	// /docs redirects to /docs/
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/docs", nil)
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusFound {
		t.Fatalf("/docs status %d", rr.Code)
	}
	if loc := rr.Header().Get("Location"); loc != "/docs/" {
		t.Fatalf("Location=%q", loc)
	}

	// HTML shell
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/docs/", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("/docs/ %d", rr.Code)
	}
	if ct := rr.Header().Get("Content-Type"); !strings.Contains(ct, "text/html") {
		t.Fatalf("content-type %q", ct)
	}
	body, _ := io.ReadAll(rr.Body)
	if !strings.Contains(string(body), "swagger-ui") {
		t.Fatalf("expected swagger-ui in HTML")
	}

	for _, path := range []string{
		"/docs/admin.openapi.yaml",
		"/docs/cache.openapi.yaml",
		"/openapi.yaml",
		"/openapi/admin.yaml",
		"/openapi/cache.yaml",
	} {
		rr = httptest.NewRecorder()
		h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, path, nil))
		if rr.Code != http.StatusOK {
			t.Fatalf("%s: %d %s", path, rr.Code, rr.Body.String())
		}
		b := rr.Body.String()
		if !strings.Contains(b, "openapi:") {
			t.Fatalf("%s: missing openapi header", path)
		}
	}
}

func TestAdminRejectsNonGETAndNilProvider(t *testing.T) {
	s := admin.New(nil)
	h := s.Handler()

	for _, path := range []string{"/healthz", "/readyz", "/peers", "/keyspaces", "/metrics"} {
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, path, nil))
		if rr.Code != http.StatusMethodNotAllowed {
			t.Fatalf("%s POST: %d", path, rr.Code)
		}
	}

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rr.Code != http.StatusOK {
		t.Fatal(rr.Code)
	}
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatal(rr.Code)
	}
	for _, path := range []string{"/peers", "/keyspaces", "/metrics"} {
		rr = httptest.NewRecorder()
		h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, path, nil))
		if rr.Code != http.StatusOK {
			t.Fatalf("%s: %d", path, rr.Code)
		}
	}

	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodHead, "/docs/", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("HEAD docs: %d", rr.Code)
	}
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/docs/", nil))
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatal(rr.Code)
	}
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/docs/nope.yaml", nil))
	if rr.Code != http.StatusNotFound {
		t.Fatal(rr.Code)
	}
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodHead, "/openapi.yaml", nil))
	if rr.Code != http.StatusOK {
		t.Fatal(rr.Code)
	}
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/openapi.yaml", nil))
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatal(rr.Code)
	}
}

func TestPeersIncludesKeyspaceHashes(t *testing.T) {
	eng := engine.New()
	defer eng.Close()
	eng.SetNodeInfo("n1", "addr")
	_ = eng.UpdateKeySpace(keyspace.Config{Name: "demo", Mode: keyspace.ModeCacheOnly, MaxBytes: 1024, TTL: time.Minute})
	s := admin.New(eng)
	s.SetReady(true)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/peers", nil))
	if rr.Code != http.StatusOK {
		t.Fatal(rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "keyspace_hashes") {
		t.Fatal(rr.Body.String())
	}
}
