## Summary

<!-- What does this PR change and why? -->

## Release

A release is cut only when this **exact** line appears as:

1. A line in the **git commit message** on `main`, or  
2. The entire **PR title**

```text
release: vX.Y.Z
```

| Allowed | Not allowed |
|---------|-------------|
| `release: v1.2.3` | `release: 1.2.3` (missing `v`) |
| | `release: v1.2.3-rc.1` |
| | `release: v1.2.3 ship it` (extra text) |
| | Marker only in the PR **description** |

- Version must be **greater than** the latest git tag  
- Notes (optional) come from lines after the marker in the **commit message**  
- Full rules: [docs/RELEASING.md](../docs/RELEASING.md)

### Shipping a version

**Option A — commit message (preferred):** include in squash/merge commit:

```text
Short summary

release: v0.3.0

- Release note bullet
```

**Option B — PR title:** set title to exactly `release: v0.3.0` (summary stays in this description).

- [ ] This PR should **not** cut a release
- [ ] This PR **should** cut a release via commit message or PR title
- [ ] Target version: `v`__.__.__

## Test plan

- [ ] `go test ./... -race` (or CI green)
- [ ] Manual check (if applicable):

## Notes for reviewers

<!-- Anything else? Breaking changes, rollout, follow-ups. -->
