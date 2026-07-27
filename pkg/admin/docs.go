package admin

import (
	"net/http"
	"strings"

	"github.com/Code0987/supercache/api/openapi"
	"github.com/Code0987/supercache/pkg/admin/docsui"
)

// mountDocs registers OpenAPI + Swagger UI under /docs and convenience aliases.
func (s *Server) mountDocs(mux *http.ServeMux) {
	mux.HandleFunc("/docs", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/docs" {
			http.NotFound(w, r)
			return
		}
		http.Redirect(w, r, "/docs/", http.StatusFound)
	})
	mux.HandleFunc("/docs/", s.handleDocs)
	// Root aliases for tooling
	mux.HandleFunc("/openapi.yaml", serveYAML(openapi.AdminSpec))
	mux.HandleFunc("/openapi/admin.yaml", serveYAML(openapi.AdminSpec))
	mux.HandleFunc("/openapi/cache.yaml", serveYAML(openapi.CacheSpec))
}

func (s *Server) handleDocs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/docs")
	path = strings.TrimPrefix(path, "/")
	if path == "" || path == "index.html" {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		if r.Method == http.MethodHead {
			return
		}
		_, _ = w.Write(docsui.IndexHTML)
		return
	}
	switch path {
	case "admin.openapi.yaml", "openapi.yaml":
		serveYAML(openapi.AdminSpec)(w, r)
		return
	case "cache.openapi.yaml":
		serveYAML(openapi.CacheSpec)(w, r)
		return
	default:
		http.NotFound(w, r)
	}
}

func serveYAML(body []byte) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/yaml; charset=utf-8")
		w.Header().Set("Cache-Control", "public, max-age=60")
		if r.Method == http.MethodHead {
			return
		}
		_, _ = w.Write(body)
	}
}
