# Releasing SuperCache

Releases are **automatic** when a commit lands on `main` (or `master`) with the
version line below. The **Release** GitHub Action then:

1. Runs tests
2. Creates git tag `vX.Y.Z`
3. Builds `sc` and `supercache-node` for linux/darwin × amd64/arm64
4. Publishes a [GitHub Release](https://github.com/Code0987/supercache/releases) with notes + archives

## Commit message format

**Only one pattern is accepted** — a full line:

```text
release: v1.2.3
```

- Must include the **`v`** prefix (`release: 1.2.3` is ignored)
- Must be `MAJOR.MINOR.PATCH` only (no `-rc` / prerelease)
- Optional notes = everything **after** that line (git trailers like `Co-authored-by:` are stripped)
- If notes are empty, the commit subject is used

### Example

```text
Short summary of what this release is

release: v0.3.0

- Multi-seed CLI and REPL
- OpenAPI /docs on admin port
- GitHub Pages API docs
```

### Non-release commits

Commits without that exact line **do not** create a release (CI still runs).

## How to ship

### Option A — squash-merge a PR

In the **squash commit message**:

```text
Add sc CLI multi-seed and REPL

release: v0.3.0

- Sticky multi-seed failover
- Interactive REPL
```

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

**Actions → Release → Run workflow** with version `v1.2.3` (and optional notes).

## Artifacts

```text
supercache-node_v0.3.0_linux_amd64.zip
sc_v0.3.0_darwin_arm64.zip
checksums.txt
```

Binaries are stamped via `-ldflags` (`sc version`, `supercache-node -version`).

## Go modules

```bash
go get github.com/Code0987/supercache@v0.3.0
```

## Local dry-run

```bash
python3 scripts/parse-release-commit.py --message "$(git log -1 --pretty=%B)"
python3 scripts/parse-release-commit_test.py
```

## Failure modes

| Situation | Behavior |
|-----------|----------|
| Tag `vX.Y.Z` already exists | Job fails (no overwrite) |
| Missing / wrong pattern | No release (success skip) |
| Tests fail | No tag / no release |
