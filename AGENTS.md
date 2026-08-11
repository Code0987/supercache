# SuperCache agent notes

Follow [docs/WORKFLOW.md](./docs/WORKFLOW.md) for every feature or behavior change.

1. Design doc first (`docs/`). Stop for review.
2. Tests that specify the behavior.
3. Implementation.
4. Branch + PR. **Never push `main`.**
5. Wait for the CI bench comment.
6. Merge only if there is no drastic perf drop (moves under ~15–20% on `ubuntu-latest` are usually noise; allocs/op jumps on Get-hit are not).

Do not skip to code because the change “seems small” if it touches Get/Put/Delete, replication, or the store.
