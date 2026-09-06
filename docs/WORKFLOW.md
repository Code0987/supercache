# SuperCache change workflow

Same order for humans and agents. **Never push or commit to `main`.**

## Default path (product / data-path work)

```text
docs → review → revision* → tests → coding → product docs* → review → revision*
  → bench (local) → revision* → commit → PR → bench monitor (CI)
  → merge only if overall drift < 10%  (and user says merge)
```

`revision*` means: fix feedback, re-stop for the next gate. Do not skip a gate.

| Step | What | Stop until |
|------|------|------------|
| **1. Docs** | Design doc (draft) | — |
| **2. Review** | Person reviews design in chat/PR | Explicit go-ahead (`looks good` / `do it` / `implement`) |
| **3. Revision** | Update design from feedback | Re-approval if the contract changed |
| **4. Tests** | Failing tests for the design table | — |
| **5. Coding** | Minimum code to pass `go test ./...` | — |
| **5b. Product docs** | Update public docs to match the new surface (**same PR**) | See [Product docs](#5b-product-docs-same-pr) |
| **6. Review** | Self-check vs design + person feedback if given | Issues fixed |
| **7. Revision** | Address code review | Tests still green |
| **8. Bench** | Local micros / smoke that the design flagged | No Get-hit alloc jump; no ugly local cells |
| **9. Revision** | Perf or correctness fixes from local bench | Re-run tests + local bench |
| **10. Commit** | On `feat/` / `fix/` / `refactor/` only | User asked to commit, or path says commit after green local work |
| **11. PR** | `gh pr create` (never straight to `main`) | — |
| **12. Bench monitor** | `gh pr checks <n> --watch` + read `<!-- supercache-bench-comment -->` | CI `test` + `bench` green |
| **13. Merge** | Only if **overall drift &lt; 10%** and user says **merge** | See [Merge gate](#13-merge-gate) |

Refactor / docs-only tracks skip or shorten steps (below). When unsure, use the full path.

One design → one PR. Do not stack a second feature on the same branch.

---

## Which track

| Kind of change | Path |
|----------------|------|
| New product behavior (types, API, replication, consistency) | Full path above |
| Bug fix on Get / Put / Delete / store / fan-out / membership | Full path (design can be short) |
| Refactor with no contract change | Short design note in PR; keep tests; local bench if hot path; still PR + CI bench + merge gate |
| Docs, comments, this workflow | Docs → commit → PR (no product tests/bench required) |

---

## 1. Docs (design)

Create `docs/design/<yyyy-mm-dd>-<short-name>.md` from the template below.

Do **not** write tests or production code yet. Commit the design on a branch if you want review as a PR, or paste it in chat.

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

## 2–3. Design review → revision

Review means a person approved the **design**, not the code.

Until that happens: no tests, no implementation, no “quick spike on main”.

If review asks for a different approach, update the design doc (**revision**), set status back to draft if needed, and stop again for go-ahead.

Approval phrases: `looks good`, `do it`, `implement`. Mark the design **Status: approved**.

## 4. Tests first

After design approval, on `feat/<short-name>` (or `fix/` / `refactor/`):

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

## 5. Coding

Implement only what the approved design listed. Stop when those tests pass and `go test ./...` is green.

Do not:

- drive-by rename or reformat unrelated files
- change RF / TTL / fan-out defaults unless the design said so
- add a second feature “while you are there”

`gofmt` the files you touched.

## 5b. Product docs (same PR)

If the change adds or changes anything a user, client, or operator can see, **update the product docs in the same PR**. Do not ship code and “docs later.” Hosted Swagger only refreshes when OpenAPI lands on `main`.

Required whenever you touch API verbs, modes, proto, `cmd/sc`, or cluster behavior:

| File | Update when |
|------|-------------|
| `docs/API.md` | New mode, RPC, or CLI verb |
| `api/openapi/cache.openapi.yaml` | New Cache RPC or message (bump `info.version`); copy to `docs/api/cache.openapi.yaml` |
| `PLAN.md` | New mode / Engine method / Cache or Peer RPC / consistency row (§5, §7, §11, §14) |
| `docs/OPERATIONS.md` | New mode, fan-out/hint rule, or ops verb |
| `docs/CLUSTER_FLOWS.md` | New structure type or peer path (e.g. `ListPop`) |
| `README.md` | Modes list, packages, `sc` examples |
| `cmd/sc/README.md` + `printUsage` / REPL help | New `sc` commands |
| `docs/design/README.md` | New approved/shipped design |

Examples/node demo keyspaces may stay a **follow-up PR** if the design said so. Product catalog (API / OpenAPI / PLAN / ops / README / sc) may not.

**Do not merge** a product PR if those files still describe the old surface.

## 6–7. Code review → revision

Check implementation against the design contract (API, flags, RF, mode guards, CLI if required).

If the user or a reviewer requests changes: fix, re-run `go test ./...`, then continue. Do not open a PR while known review items are open.

## 8–9. Local bench → revision

Before commit, exercise what the design listed under **Bench risk**:

```text
go test ./pkg/store ./pkg/engine -run=^$ -bench='Benchmark(Store|Engine)' -benchmem -benchtime=200ms -count=1
# plus any new micros for the feature
```

Hard local stops (same as CI):

- Get-hit / StoreGetHit **allocs/op** must not jump
- No intentional hot-path regression “to fix later”

If local numbers look bad: **revision**, then re-test and re-bench. Details: [BENCHMARKS.md](./BENCHMARKS.md).

## 10. Commit

Commit on the feature branch only (never `main`). Prefer readable history (design / tests+code / demos as separate commits when useful).

Do not commit secrets or generated noise outside the usual `api/gen` path for this repo.

## 11. PR

```text
git push -u origin HEAD
gh pr create
```

PR body must include:

- link to the design doc (path or prior comment)
- what landed vs the design
- which product docs were updated (or “no public surface change”)
- test commands already run (`go test ./...`)
- local bench notes if any
- bench risk (copy from the design)

Do not merge from this step.

## 12. Bench monitor (CI)

Wait until both `test` and `bench` checks are green:

```text
gh pr checks <n> --watch
```

Then read the sticky comment `<!-- supercache-bench-comment -->`.

Same runner, one run each side. Ops/s up is better; ns/op down is better.

### Drift rules (overall &lt; 10%)

Compare PR vs `main` on the same runner. **Overall drift** means every reported smoke cell and every existing micro that appears on both sides:

| Check | Pass condition |
|-------|----------------|
| Smoke Δ ops/s | each cell within **±10%** (ops/s down worse) |
| Micro Δ ns/op | each shared bench within **±10%** (ns/op up worse) |
| Get-hit / StoreGetHit **allocs/op** | **unchanged** (any increase = fail) |
| New benches (n/a on main) | report only; do not fail on “new” alone |

| Result | What to do |
|--------|------------|
| All shared cells inside **±10%**, Get-hit allocs flat | Eligible. Summarize and **ask to merge** (or merge if user already said merge / merge-if-green). |
| Get-hit or StoreGetHit **allocs/op** increased | Not eligible. Investigate; **revision** on the branch; do not merge. |
| Any shared cell **worse than 10%** | Not eligible. Investigate; re-run once if flake suspected; still do not merge without fixing or an explicit user override. |
| `bench` or `test` red | Fix on the same branch. Do not merge. |
| No bench comment after a green `bench` job | Workflow broken. Do not merge. |

Post the smoke table (or a short summary) in chat before merge.

±10% is the **merge bar**, not a claim of statistical significance. One-run CI is noisy; treat clear Get-hit alloc jumps as real even inside 10% ns/op.

## 13. Merge gate

Merge **only when**:

1. User said **merge** (or clearly pre-authorized: e.g. “merge if green / if drift ok”), **and**
2. CI merge gate above is satisfied (**overall drift &lt; 10%**, Get-hit allocs flat), **and**
3. `test` + `bench` are green, **and**
4. Product docs match the shipped surface (see [§5b](#5b-product-docs-same-pr)), or the PR states “no public surface change”.

```text
gh pr merge <n> --merge --delete-branch
git checkout main && git pull origin main
```

Do not squash unless asked (keeps design/test/code commits readable). Do not `--admin` around failing checks.

If the user says merge but drift is ≥10% or allocs jumped: **do not merge**; report the numbers and wait.

---

## Resume in a later session

1. Read this file and [AGENTS.md](../AGENTS.md).
2. `git status`, `git branch`, `gh pr list`.
3. Locate the last completed step in the path; continue from the next gate.
4. If a design is `draft`, do not code. If `approved`, continue from tests or implementation.
5. Never assume an open PR was approved for merge.
