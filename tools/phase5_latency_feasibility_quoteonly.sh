#!/usr/bin/env bash
set -euo pipefail

REPO="/home/deploy/quantlab-backend"
cd "$REPO"

has_mode=0
for arg in "$@"; do
  if [[ "$arg" == "--execution-mode" ]]; then
    has_mode=1
    break
  fi
done

if [[ "$has_mode" -eq 1 ]]; then
  python3 tools/phase5_latency_feasibility_quoteonly.py "$@"
else
  python3 tools/phase5_latency_feasibility_quoteonly.py --execution-mode taker_taker "$@"
fi
