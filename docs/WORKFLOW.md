# SuperCache change workflow

How new work lands. Agents and humans follow the same order. **Do not push to `main`.**

## Order

1. **Design** — write a short design doc for the feature or change.
2. **Review** — stop and wait for review of the design. Do not implement yet.
3. **Tests** — write tests that lock the intended behavior (they may fail).
4. **Code** — implement until tests pass. Keep the diff to the design.
5. **PR** — branch + pull request. Never commit straight to `main`.
6. **Bench** — wait for the CI bench comment (same-runner `main` vs PR).
7. **Merge** — merge only if there is no drastic perf drop.

Tiny doc-only or comment-only changes may skip design + tests. They still use a PR.

## Design

Put the doc under `docs/` (new file, or a section in the architecture plan / [OPERATIONS.md](./OPERATIONS.md) if it is a small contract change).

Say:

- what changes and why
- the API / wire / keyspace contract
- what stays the same
- tests you will add
- bench risk (which cells could move)

Do not start tests or code until the design is reviewed.

## Tests then code

Write failing tests first: engine/store unit tests, cluster/replay tests, or scbench cells if the design calls for them.

Then implement. Do not expand scope past the reviewed design.

## PR

```text
git checkout -b <type>/<short-name>   # feat/ fix/ refactor/ chore/ docs/
# work, commit
git push -u origin HEAD
gh pr create
```

PR body: what changed, how to test, any bench risk.

## Bench then merge

Wait for the sticky `<!-- supercache-bench-comment -->` on the PR. Same runner, one run per side. Not a merge gate in CI — we still read it before merge.

| Signal | Action |
|--------|--------|
| Smoke ops/s and micro ns/op move **under ~15–20%** | Treat as noise. Merge. |
| Allocs/op jump on a hot path (e.g. Get-hit 1 → 5+) | Stop. Investigate. |
| Smoke or micro moves **well past ~20%** the wrong way | Stop. Investigate. |
| Bench job red (crash, infra) | Fix the PR. Do not merge. |

Ops/s up is better; ns/op down is better. See [BENCHMARKS.md](./BENCHMARKS.md).

Merge with `gh pr merge --merge` (or the GitHub UI) after the bench look. Delete the feature branch.

## Later sessions

Read this file and [AGENTS.md](../AGENTS.md) before starting feature work.
