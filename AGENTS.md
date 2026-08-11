# SuperCache agent notes

Read [docs/WORKFLOW.md](./docs/WORKFLOW.md) before any behavior change.

## Hard rules

- Never push or commit to `main`. Branch + PR only.
- Never start tests or code until a design is **approved** in chat (`looks good` / `do it` / `implement`).
- Never merge until the user says **merge**.
- Never treat CI green as “ship it”. Read the bench comment first.
- One approved design per PR. No extra features.

## Stop and ask

Stop if any of these are true:

- the change touches Get, Put, Delete, RF, hints, tombstones, or membership and there is no approved design
- bench allocs/op went up on Get-hit / StoreGetHit
- any smoke/micro cell is worse than 20%
- the user has not said to merge

## Track (pick one)

| If… | Then… |
|-----|--------|
| New behavior or bug on the data path | Full track: design → wait → tests → code → PR → bench → ask to merge |
| Pure refactor, same contract | Short design note in the PR; keep tests; still PR + bench |
| Docs / comments / this workflow | PR only |

## Design location

`docs/design/<yyyy-mm-dd>-<short-name>.md` using the template in WORKFLOW.md.

## After approval: tests then code

New tests first (they should fail). Then the minimum code to pass `go test ./...`.

## PR + bench

```text
gh pr create
gh pr checks <n> --watch
# read <!-- supercache-bench-comment -->
```

±20% on same-runner smoke/micro is noise **only if allocs/op on Get-hit did not jump**. Summarize benches, then wait for “merge”.
