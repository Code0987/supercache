## Summary

<!-- What does this PR change and why? -->

## Release

Merging to `main` **does not** create a release unless this **exact** line appears in the
**commit message** or at the **top of this PR description** (not inside a code fence):

```text
release: vX.Y.Z
```

- **`v` is required** — `release: 1.2.3` is ignored  
- **Semver only** — `v1.2.3`, not `v1.2.3-rc.1`  
- **Must be greater than the current latest tag** — downgrades / equals are rejected  
- Optional notes = lines after the marker until `---` or a `##` heading  
- Full rules: [docs/RELEASING.md](../docs/RELEASING.md)

### If this PR should ship a version

Put this **above** the rest of the description (or in the squash commit message):

```text
release: v0.3.0

- Bullet one for the GitHub Release notes
- Bullet two
```

- [ ] This PR should **not** cut a release (omit any live `release: v…` line outside fences)
- [ ] This PR **should** cut a release — live `release: vX.Y.Z` is in the PR body or squash message
- [ ] Target version: `v`__.__.__  (fill in if releasing)

## Test plan

- [ ] `go test ./... -race` (or CI green)
- [ ] Manual check (if applicable):

## Notes for reviewers

<!-- Anything else? Breaking changes, rollout, follow-ups. -->
