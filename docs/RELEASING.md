# Releasing SuperCache

Releases are **automatic** when a commit lands on `main` (or `master`) with the
version line below. The **Release** GitHub Action then:

1. Runs tests
2. Creates git tag `vX.Y.Z`
3. Builds `sc` and `supercache-node` for linux/darwin × amd64/arm64
4. Publishes a [GitHub Release](https://github.com/Code0987/supercache/releases) with notes + archives

## Marker format

**Only one pattern is accepted** — an entire line (or the entire PR title):

```text
release: v1.2.3
```

- Must include the **`v`** prefix (`release: 1.2.3` is ignored)
- Must be `MAJOR.MINOR.PATCH` only (no `-rc` / prerelease)
- No extra text on the same line (`release: v1.2.3 ship it` is ignored)
- Must be **strictly greater** than the latest existing git tag

## Where it is read

| Source | Used? |
|--------|--------|
| **Git commit message** on `main` | Yes (first) |
| **PR title** | Yes (fallback if commit has no marker) |
| PR description / body | **No** |

Optional release notes = lines **after** the marker in the **commit message** (until `---` / `##` / trailers). If the only hit is a PR title with no commit notes, the notes default to `Release vX.Y.Z`.

### Example — commit message (preferred)

```text
Short summary of what this release is

release: v0.3.0

- Multi-seed CLI and REPL
- OpenAPI /docs on admin port
```

### Example — PR title

Set the PR title to exactly:

```text
release: v0.3.0
```

(Use the description for human context; it does not drive the version.)

### Non-release commits

No matching line in the commit message **and** PR title is not exactly `release: vX.Y.Z` → no release (CI still runs).

## How to ship

### Option A — squash-merge

Squash commit message:

```text
Add sc CLI multi-seed and REPL

release: v0.3.0

- Sticky multi-seed failover
```

### Option B — merge commit + PR title

1. Set PR **title** to `release: v0.3.0`
2. Merge (merge commit is fine)

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
