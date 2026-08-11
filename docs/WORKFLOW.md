# SuperCache change workflow

Same order for humans and agents. **Never push or commit to `main`.**

## Which track

| Kind of change | Design | Tests first | Bench before merge |
|----------------|:------:|:-----------:|:------------------:|
| New product behavior (types, API, replication, consistency) | yes | yes | yes |
| Bugfix on Get / Put / Delete / store / fan-out / membership | yes (can be short) | yes | yes |
| Refactor with no contract change | short note in PR is enough if you can name the invariant you are not breaking | keep existing tests green; add only if a path is untested | yes |
| Docs, comments, workflow, OpenAPI wording | no | no | no (still a PR) |

If you are unsure, use the **product behavior** track.

One design → one PR. Do not stack a second feature on the same branch.

## 1. Design

Create `docs/design/<yyyy-mm-dd>-<short-name>.md` from the template below.

Do **not** write tests or production code yet. Commit the design on a branch if you want review as a PR, or paste it in chat. **Stop.** Wait for an explicit go-ahead (`looks good`, `do it`, `implement`).

### Template

```markdown
# <title>

**Status:** draft | approved
**Branch (later):** feat/<short-name>

## Problem
What is wrong or missing today? Point at code paths (file + behavior), not vibes.

## Non-goals
What this change will not do.

## Contract
- API / proto / keyspace fields (or "no public change")
- Who stores a copy (RF, owner, replica, non-replica)
- Failure / hint / TTL behavior
- What an existing client can still assume

## Approach
The chosen design in enough detail to implement without guessing.
Rejected alternatives: one line each.

## Tests (write these first, after approval)
| Test | Package | Asserts |
|------|---------|---------|
| ... | pkg/engine | ... |

## Bench risk
Which CI smoke cells / micro names could move and why.
Hot path? (Get-hit, Put, store mutex) yes/no.
```

## 2. Review

Review means a person approved the **design**, not the code.

Until that happens: no tests, no implementation, no “quick spike on main”.

If review asks for a different approach, update the design doc and stop again.

## 3. Tests first

After approval, on `feat/<short-name>` (or `fix/` / `refactor/`):

1. Add or extend tests from the design table **before** changing product code.
2. Run them. New tests should fail (or fail to compile) for the new behavior.
3. Do not weaken a test to match current code.

Where tests live:

| Layer | Package |
|-------|---------|
| LRU, tombstone, LWW | `pkg/store` |
| Get/Put/Delete, keyspace, RF | `pkg/engine` |
| Replica apply + hints | `internal/peer` |
| Join / handoff | `pkg/engine`, `pkg/warmup` |
| In-process mesh | `internal/testcluster` |
| Public gRPC / client | `pkg/client`, `internal/cacheserver` |

Prefer one focused test that would fail if the design is not implemented over a pile of similar cases.

## 4. Code

Implement only what the approved design listed. Stop when those tests pass and `go test ./...` is green.

Do not:

- drive-by rename or reformat unrelated files
- change RF / TTL / fan-out defaults unless the design said so
- add a second feature “while you are there”

`gofmt` the files you touched.

## 5. PR

```text
git checkout -b feat/<short-name>   # fix/ refactor/ chore/ docs/
git push -u origin HEAD
gh pr create
```

PR body must include:

- link to the design doc (path or prior comment)
- what landed vs the design
- test commands already run (`go test ./...`)
- bench risk (copy from the design)

Do not merge your own PR from this step.

## 6. Bench

Wait until both `test` and `bench` checks are green:

```text
gh pr checks <n> --watch
```

Then read the sticky comment `<!-- supercache-bench-comment -->`.

Same runner, one run each side. Ops/s up is better; ns/op down is better. Details: [BENCHMARKS.md](./BENCHMARKS.md).

| Result | What to do |
|--------|------------|
| All smoke Δ ops/s and micro Δ ns/op **inside ±20%**, allocs/op **unchanged** on Get-hit / StoreGetHit | Noise. Say so and ask to merge. |
| Get-hit or StoreGetHit **allocs/op** increased | Not noise. Investigate; do not merge. |
| Any cell **worse than 20%** (ops/s down or ns/op up) | Investigate. Re-run once if you suspect a flake; still do not merge on a single ugly cell without saying why. |
| `bench` or `test` red | Fix on the same branch. Do not merge. |
| No bench comment after a green `bench` job | Something is wrong with the workflow. Do not merge. |

Post the smoke table (or a 3-line summary) in the PR or chat before merge.

## 7. Merge

Merge **only after the user says to merge**.

```text
gh pr merge <n> --merge --delete-branch
git checkout main && git pull origin main
```

Do not squash unless asked (keeps design/test/code commits readable). Do not `--admin` around failing checks.

## Resume in a later session

1. Read this file and [AGENTS.md](../AGENTS.md).
2. `git status`, `git branch`, `gh pr list`.
3. If a design is `draft`, do not code. If `approved`, continue from tests or implementation.
4. Never assume an open PR was approved for merge.
