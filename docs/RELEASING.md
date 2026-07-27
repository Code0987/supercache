# Releasing SuperCache

Releases are **automatic** when a qualifying marker lands on `main` (or `master`).
The **Release** GitHub Action then:

1. Runs tests
2. Creates git tag `vX.Y.Z`
3. Builds `sc` and `supercache-node` for linux/darwin × amd64/arm64
4. Publishes a [GitHub Release](https://github.com/Code0987/supercache/releases) with notes + archives

## Marker formats

| Where | Format | Example |
|-------|--------|---------|
| **Commit message** | Markdown **YAML front matter** | see below |
| **PR title** | Square brackets | `Ship CLI [release: v1.2.3]` |

### Commit message (YAML front matter)

```text
Short summary of what this release is

---
release: v0.3.0
---

- Multi-seed CLI and REPL
- OpenAPI /docs on admin port
```

Or front matter first:

```text
---
release: v0.3.0
---

Short summary / release notes
```

Rules:

- Opening and closing `---` fences are required (Markdown metadata / YAML front matter)
- Inside the block: exactly `release: vX.Y.Z` on its own line (`v` required, no prerelease)
- A bare line `release: v1.2.3` **outside** front matter does **not** release
- Optional notes = text **after** the closing `---` (until another `---` / `##` / git trailers)
- Version must be **strictly greater** than the latest existing git tag
- **PR body is ignored**

### PR title

```text
[release: v0.3.0]
```

```text
Ship multi-seed CLI [release: v0.3.0]
```

Unbracketed titles (`release: v0.3.0`) are not accepted.

Merge commits that embed the PR title still match via the bracket form.

## How to ship

### Option A — commit front matter (preferred for notes)

```text
Add sc CLI multi-seed and REPL

---
release: v0.3.0
---

- Sticky multi-seed failover
```

### Option B — PR title

1. Title: `Something descriptive [release: v0.3.0]`
2. Merge to `main`

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
python3 scripts/check-release-version.py --tag v0.3.0
python3 scripts/parse-release-commit_test.py
python3 scripts/check-release-version_test.py
```

## Failure modes

| Situation | Behavior |
|-----------|----------|
| Tag `vX.Y.Z` already exists | Job fails (no overwrite) |
| Version ≤ latest existing tag | Job fails (must bump higher) |
| Missing / wrong pattern | No release (success skip) |
| Tests fail | No tag / no release |
