set -euo pipefail
for sym in BTC/USDT ETH/USDT ADA/USDT SOL/USDT; do
  slug=$(printf "%s" "$sym" | tr A-Z a-z | tr -d /)
  files=""
  for d in data/test data/sprint2 data; do
    if [ -d "$d" ]; then
      files="$files $(ls -1 $d/${slug}_2026*.parquet 2>/dev/null || true)"
    fi
  done

  dates=$(printf "%s\n" $files \
    | sed -n 's/.*_\([0-9]\{8\}\)\.parquet$/\1/p' \
    | awk '$1>=20260108 && $1<=20260208' \
    | sort -u \
    | tail -n 10 \
    | paste -sd ', ' -)

  if [ -z "$dates" ]; then
    dates='(none found)'
  fi
  echo "$sym -> $dates"
done
