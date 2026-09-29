#!/usr/bin/env bash
set -euo pipefail

URL="${URL:-http://127.0.0.1:8080/api/users/benchmark}"
THREADS="${THREADS:-4}"
DURATION="${DURATION:-30s}"
CONCURRENCIES="${CONCURRENCIES:-100 200 300 400 500 750 1000}"
RESULTS_DIR="${RESULTS_DIR:-wrk-results-$(date +%Y%m%d-%H%M%S)}"

if ! command -v wrk >/dev/null 2>&1; then
  echo "wrk is not installed. On macOS: brew install wrk"
  exit 1
fi

mkdir -p "$RESULTS_DIR"

echo "URL: $URL"
echo "Threads: $THREADS"
echo "Duration per run: $DURATION"
echo "Concurrencies: $CONCURRENCIES"
echo "Results: $RESULTS_DIR"
echo

echo "Warming up..."
wrk -t"$THREADS" -c100 -d10s "$URL" >/dev/null

for concurrency in $CONCURRENCIES; do
  out="$RESULTS_DIR/c${concurrency}.txt"
  echo "============================================================"
  echo "Concurrency: $concurrency"
  echo "============================================================"
  wrk -t"$THREADS" -c"$concurrency" -d"$DURATION" --latency "$URL" | tee "$out"
  echo

  if command -v curl >/dev/null 2>&1; then
    curl -s http://127.0.0.1:8080/metrics > "$RESULTS_DIR/metrics-c${concurrency}.txt" || true
  fi

done

echo "Done. Results saved in $RESULTS_DIR"
