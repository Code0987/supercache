## Summary

<!-- What does this PR change and why? -->

## Release

A release is cut only when the version marker appears in the **commit message**
or **PR title** (PR description is ignored).

| Where | Required format |
|-------|-----------------|
| Commit message (own line) | `release: vX.Y.Z` |
| PR title | `[release: vX.Y.Z]` |

Examples:

```text
release: v1.2.3
```

```text
Ship multi-seed CLI [release: v1.2.3]
```

- **`v` required**; no prerelease suffix  
- Must be **greater than** the latest git tag  
- Notes (optional) only from lines after a **commit** marker  
- Full rules: [docs/RELEASING.md](../docs/RELEASING.md)

### Shipping a version

- [ ] This PR should **not** cut a release  
- [ ] Release via **commit message** line `release: vX.Y.Z`  
- [ ] Release via **PR title** containing `[release: vX.Y.Z]`  
- [ ] Target version: `v`__.__.__

## Test plan

- [ ] `go test ./... -race` (or CI green)
- [ ] Manual check (if applicable):

## Notes for reviewers

<!-- Anything else? Breaking changes, rollout, follow-ups. -->
