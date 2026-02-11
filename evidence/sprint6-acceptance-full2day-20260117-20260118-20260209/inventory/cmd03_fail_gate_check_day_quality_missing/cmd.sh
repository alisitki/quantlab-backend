set -euo pipefail
root="evidence/sprint6-acceptance-full2day-__PENDING__-__PENDING__-20260209"
miss="$root/inventory/adausdt_bbo_daily_inventory_missing_day_quality.tsv"

if [ -s "$miss" ]; then
  echo "FAIL: FULL selection meta missing day_quality; stopping per rules."
  mkdir -p "$root/inventory/meta_snippets"
  cut -f3 "$miss" | while read -r mp; do
    cp -a "$mp" "$root/inventory/meta_snippets/"
  done
  python3 -c 'import sys; sys.exit(3)'
fi

echo "OK: day_quality present for all inventoried days"
