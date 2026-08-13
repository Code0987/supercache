# SuperCache agent notes

Read [docs/WORKFLOW.md](./docs/WORKFLOW.md) before any behavior change.

## Default path

```text
docs → review → revision → tests → coding → review → revision
  → bench (local) → revision → commit → PR → bench monitor (CI)
  → merge only if overall drift < 10% (and user says merge)
```

## Hard rules

- Never push or commit to `main`. Branch + PR only.
- Never start tests or code until a design is **approved** in chat (`looks good` / `do it` / `implement`).
- Never merge until the user says **merge** (or clearly pre-authorizes merge-if-green) **and** the CI gate passes.
- Never treat CI green alone as “ship it”. Read `<!-- supercache-bench-comment -->` first.
- One approved design per PR. No extra features.
- **Overall drift bar: ±10%** on shared smoke/micro cells; **Get-hit / StoreGetHit allocs/op must not rise**.

## Stop and ask

Stop if any of these are true:

- the change touches Get, Put, Delete, RF, hints, tombstones, or membership and there is no approved design
- design review asked for revision and the design is not re-approved
- bench allocs/op went up on Get-hit / StoreGetHit
- any shared smoke/micro cell is worse than **10%**
- the user has not said to merge (and did not pre-authorize merge-if-green)

## Track (pick one)

| If… | Then… |
|-----|--------|
| New behavior or bug on the data path | Full path in WORKFLOW.md |
| Pure refactor, same contract | Short design note in the PR; keep tests; still PR + CI bench + 10% gate |
| Docs / comments / this workflow | commit → PR only |

## Design location

`docs/design/<yyyy-mm-dd>-<short-name>.md` using the template in WORKFLOW.md.

## After design approval

1. Tests first (they should fail).
2. Minimum code to pass `go test ./...`.
3. Code review / self-check vs design → revision if needed.
4. Local bench for design risk areas → revision if needed.
5. Commit on the feature branch (when user asks, or when local path is green and commit is the next step).

## PR + bench monitor

```text
gh pr create
gh pr checks <n> --watch
# read <!-- supercache-bench-comment -->
```

Summarize benches. Eligible to merge only if **every shared cell is within ±10%** and Get-hit allocs are flat. Then wait for **merge** (unless already pre-authorized).
