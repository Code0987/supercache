## Summary

<!-- What does this PR change and why? -->

## Release

Merging to `main` **does not** create a release unless the **squash commit message** (or merge commit body) contains this **exact** line:

```text
release: vX.Y.Z
```

- **`v` is required** — `release: 1.2.3` is ignored  
- **Semver only** — `v1.2.3`, not `v1.2.3-rc.1`  
- Optional release notes = lines **after** that marker  
- Full rules: [docs/RELEASING.md](../docs/RELEASING.md)

### If this PR should ship a version

Use a squash message like:

```text
Short summary of the change

release: v0.3.0

- Bullet one for the GitHub Release notes
- Bullet two
```

- [ ] This PR should **not** cut a release (normal merge — omit the `release:` line)
- [ ] This PR **should** cut a release — I will set `release: vX.Y.Z` in the squash commit message
- [ ] Target version: `v`__.__.__  (fill in if releasing)

## Test plan

- [ ] `go test ./... -race` (or CI green)
- [ ] Manual check (if applicable):

## Notes for reviewers

<!-- Anything else? Breaking changes, rollout, follow-ups. -->
