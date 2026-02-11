set -euo pipefail
timeout --signal=INT 900 env -u DISCOVERY_PERMUTATION_TEST NODE_OPTIONS="--max-old-space-size=6144" \
  node tools/run-multi-day-discovery.js \
    --exchange binance --symbol ADA/USDT --stream bbo \
    --start 20260117 --end 20260118 \
    --heapMB 6144 \
    --mode smoke \
    --progressEvery 1
