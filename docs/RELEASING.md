# Releasing SuperCache

Releases are **automatic** when a commit lands on `main` (or `master`) whose
message includes a version marker. The **Release** GitHub Action then:

1. Runs tests
2. Creates git tag `vX.Y.Z`
3. Builds `sc` and `supercache-node` for linux/darwin × amd64/arm64
4. Publishes a [GitHub Release](https://github.com/Code0987/supercache/releases) with notes + archives

## Commit message format

Put the version on its **own line** (subject or body). Optional notes follow.

```text
Short summary of what this release is

release: v0.3.0

- Multi-seed CLI and REPL
- OpenAPI /docs on admin port
- GitHub Pages API docs
```

### Accepted markers

| Line | Example |
|------|---------|
| `release:` | `release: v1.2.3` |
| `Release-As:` | `Release-As: 1.2.3` |
| `Release-Version:` | `Release-Version: v1.2.3-rc.1` |
| `version:` | `version: v1.0.0` |

- Leading `v` is optional (`1.2.3` → tag `v1.2.3`)
- Semver core `MAJOR.MINOR.PATCH` required; optional prerelease suffix (`-rc.1`) marks the GitHub release as **prerelease**
- Everything **after** the marker line becomes the release body (git trailers like `Co-authored-by:` are stripped)
- If notes are empty, the commit subject is used

### Non-release commits

Normal commits without a marker **do not** create a release (CI still runs).

## How to ship (recommended)

### Option A — squash-merge a PR

1. Open a PR with your changes
2. In the **squash commit message** (GitHub UI), set:

```text
Add sc CLI multi-seed and REPL

release: v0.3.0

- Sticky multi-seed failover
- Interactive REPL
```

3. Merge to `main` → Release workflow runs

### Option B — direct commit on main

```bash
git commit -m "$(cat <<'EOF'
Polish admin OpenAPI schemas

release: v0.3.1

- Fix PeerInfo JSON field names
EOF
)"
git push origin main
```

### Option C — manual dispatch

**Actions → Release → Run workflow** with `version` (and optional `notes`) inputs.
Use this if you need to re-cut or publish without a special commit.

## Artifacts

Each release attaches zip archives named like:

```text
supercache-node_v0.3.0_linux_amd64.zip
sc_v0.3.0_darwin_arm64.zip
checksums.txt
```

Binaries are stamped with the version via `-ldflags` (`sc version`, `supercache-node -version`).

## Go modules

Tagging `vX.Y.Z` on the default branch is enough for consumers:

```bash
go get github.com/Code0987/supercache@v0.3.0
```

## Local dry-run of the parser

```bash
python3 scripts/parse-release-commit.py --message "$(git log -1 --pretty=%B)"
python3 scripts/parse-release-commit_test.py
```

## Failure modes

| Situation | Behavior |
|-----------|----------|
| Tag `vX.Y.Z` already exists | Job fails (no overwrite) |
| Invalid / missing marker | No release (success skip) |
| Tests fail | No tag / no release |
