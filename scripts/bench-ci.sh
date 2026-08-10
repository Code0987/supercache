#!/usr/bin/env bash
# Usage: scripts/bench-ci.sh [outdir]
# Writes micro.txt and smoke.json into outdir (default: repo root).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
OUT="${1:-$ROOT}"
mkdir -p "$OUT"

export GOMAXPROCS="${GOMAXPROCS:-2}"

go test ./pkg/store ./pkg/engine \
  -run=^$ \
  -bench='Benchmark(Store|Engine)' \
  -skip=Cluster3 \
  -benchmem -benchtime=200ms -count=3 \
  -timeout=5m \
  | tee "$OUT/micro.txt"

go run ./cmd/scbench -matrix="$ROOT/bench/ci-smoke.yaml" -json="$OUT/smoke.json"
