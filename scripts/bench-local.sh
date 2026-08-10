#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
tier="${1:-laptop}"

echo "Quiet-machine checklist:"
echo "  - close browsers / extra load"
echo "  - pin GOMAXPROCS (default: nproc)"
echo "  - do not use Docker Redis for SuperCache-only matrix"
echo

export GOMAXPROCS="${GOMAXPROCS:-$(nproc)}"

case "$tier" in
  laptop|full)
    go test ./pkg/store ./pkg/engine \
      -run=^$ \
      -bench='Benchmark(Store|Engine)' \
      -benchmem -benchtime=2s -count=5 \
      -timeout=30m \
      | tee "micro-${tier}.txt"
    go run ./cmd/scbench -tier="$tier" -json="${tier}.json"
    ;;
  *)
    echo "usage: $0 [laptop|full]" >&2
    exit 1
    ;;
esac
