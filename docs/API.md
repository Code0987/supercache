# SuperCache API documentation

## Hosted docs (Swagger UI)

| Where | URL |
|-------|-----|
| **On a running node** | `http://<admin-addr>/docs` (default `http://127.0.0.1:8080/docs`) |
| **GitHub Pages** | `https://code0987.github.io/supercache/` (after Pages is enabled) |
| **OpenAPI YAML (admin)** | `/openapi.yaml` or `/docs/admin.openapi.yaml` |
| **OpenAPI YAML (cache ref)** | `/docs/cache.openapi.yaml` |

```bash
go run ./cmd/supercache-node \
  -cache 127.0.0.1:9000 -peer 127.0.0.1:9001 -admin 127.0.0.1:8080
# open http://127.0.0.1:8080/docs
```

### Specs

| Spec | Source | Try it out? |
|------|--------|-------------|
| Admin HTTP | [`api/openapi/admin.openapi.yaml`](../api/openapi/admin.openapi.yaml) | Yes (against the node) |
| Cache gRPC | [`api/openapi/cache.openapi.yaml`](../api/openapi/cache.openapi.yaml) | No — reference only |

### Clients

- **Go:** `pkg/client`
- **CLI:** `cmd/sc` (`sc get` / `put` / `del`, or REPL)
- **Protos:** `api/proto/cache.proto`, `api/proto/peer.proto` (peer is mesh-internal)

## Enabling GitHub Pages

1. Repo **Settings → Pages → Build and deployment → GitHub Actions**
2. Push to `main` (workflow [`.github/workflows/docs.yml`](../.github/workflows/docs.yml))
3. Site URL: `https://<owner>.github.io/supercache/`

The workflow copies OpenAPI + UI from source into the Pages artifact so the site
stays aligned with the embedded node docs.
