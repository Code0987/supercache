# Design drafts

One file per change: `<yyyy-mm-dd>-<short-name>.md`.

Use the template in [WORKFLOW.md](../WORKFLOW.md). Status is `draft` until review, then `approved`.

Do not implement from a draft.

## Shipped (reference)

| Design | Feature |
|--------|---------|
| [2026-08-11-bloom-filter.md](./2026-08-11-bloom-filter.md) | `ModeBloom` |
| [2026-08-13-mode-set.md](./2026-08-13-mode-set.md) | `ModeSet` |
| [2026-08-13-mode-zset.md](./2026-08-13-mode-zset.md) | `ModeZSet` |
| [2026-08-19-mode-geo.md](./2026-08-19-mode-geo.md) | `ModeGeo` |
| [2026-08-19-mode-list.md](./2026-08-19-mode-list.md) | `ModeList` |
| [2026-08-20-mode-hash.md](./2026-08-20-mode-hash.md) | `ModeHash` |
| [2026-08-20-mode-counter.md](./2026-08-20-mode-counter.md) | `ModeCounter` |
| [2026-08-21-mode-json.md](./2026-08-21-mode-json.md) | `ModeJSON` |
| [2026-08-21-mode-bitmap.md](./2026-08-21-mode-bitmap.md) | `ModeBitmap` |
| [2026-08-25-mode-hll.md](./2026-08-25-mode-hll.md) | `ModeHLL` |
| [2026-08-31-mode-topk.md](./2026-08-31-mode-topk.md) | `ModeTopK` |
| [2026-09-01-mode-cms.md](./2026-09-01-mode-cms.md) | `ModeCMS` |
| [2026-09-02-refactor-hll.md](./2026-09-02-refactor-hll.md) | ModeHLL file layout (no contract) |
| [2026-09-02-refactor-bitmap.md](./2026-09-02-refactor-bitmap.md) | ModeBitmap file layout (no contract) |
| [2026-09-02-refactor-counter.md](./2026-09-02-refactor-counter.md) | ModeCounter file layout (no contract) |
| [2026-09-02-refactor-list.md](./2026-09-02-refactor-list.md) | ModeList file layout (no contract) |
| [2026-09-02-refactor-hash.md](./2026-09-02-refactor-hash.md) | ModeHash file layout (no contract) |
| [2026-09-02-refactor-json.md](./2026-09-02-refactor-json.md) | ModeJSON file layout (no contract) |
| [2026-09-02-refactor-bloom.md](./2026-09-02-refactor-bloom.md) | ModeBloom file layout (no contract) |
| [2026-08-25-list-counter-version.md](./2026-08-25-list-counter-version.md) | List/Counter snapshot version |
| [2026-08-13-unify-grpc-error-map.md](./2026-08-13-unify-grpc-error-map.md) | grpcmap |

Product surface docs: [API.md](../API.md), [OPERATIONS.md](../OPERATIONS.md), [OpenAPI](../../api/openapi/cache.openapi.yaml).
