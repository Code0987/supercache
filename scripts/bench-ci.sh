#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

export GOMAXPROCS="${GOMAXPROCS:-2}"

go test ./pkg/store ./pkg/engine \
  -run=^$ \
  -bench='Benchmark(Store|Engine)' \
  -skip=Cluster3 \
  -benchmem -benchtime=200ms -count=1 \
  -timeout=5m \
  | tee micro.txt

go run ./cmd/scbench -matrix=bench/ci-smoke.yaml -json=smoke.json
